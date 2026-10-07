package reconcile

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetryUpdatesATargetNoLongerOnTheBuild(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	r := h.haltOnBadBuild("a", imgA, d2, tA2)

	updates := len(h.world.callsFor("update:" + tA2))
	h.revert(tA2)
	h.c.InspectAll(h.ctx)
	require.NoError(t, h.c.Retry(h.ctx, actor, r.ID, "rolled back by hand"))
	h.tick()

	require.Greater(t, len(h.world.callsFor("update:"+tA2)), updates, "a target that no longer runs the build is updated again")
	require.Equal(t, Complete, h.drive(r.ID, 80, 20*time.Second).State)
}

func TestRetryOfATargetAlreadyOnTheDigestOnlyReinspects(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.soakFail[soakA] = true })
	h.release(imgA, d2)
	h.ticks(5, 0)
	h.clock.Advance(20 * time.Second)
	h.tick()

	r := h.active("a")
	require.Equal(t, Halted, r.State)

	h.c.InspectAll(h.ctx)
	updates := len(h.world.callsFor("update:" + tA1))
	h.world.set(func(w *world) { w.soakFail[soakA] = false })
	require.NoError(t, h.c.Retry(h.ctx, actor, r.ID, "probe fixed"))
	require.Equal(t, Complete, h.drive(r.ID, 80, 20*time.Second).State)

	// The landed target passes a retry batch, soak included, with no new update.
	done := h.rollout(r.ID)
	rt := done.target(tA1)
	b := done.Batches[rt.Batch-1]

	require.Equal(t, updates, len(h.world.callsFor("update:"+tA1)))
	require.True(t, rt.Updated)
	require.True(t, b.Retried)
	require.True(t, b.Passed)
	require.True(t, slices.ContainsFunc(b.Soak.Checks, func(check SoakCheck) bool {
		return check.Program == soakA && check.OK && !check.Error
	}), "the retry passed its soak program")
	require.Equal(t, Healthy, h.view(tA1).Health)
}

func TestFlushAlsoWritesDeletions(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.soakFail[soakA] = true })
	h.release(imgA, d2)
	h.ticks(5, 0)
	h.clock.Advance(20 * time.Second)
	h.tick()

	r := h.active("a")
	require.Equal(t, Halted, r.State)

	_, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("node=b-1"), Reason: "r", Expires: time.Minute})
	require.NoError(t, err)
	_, err = h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("node=a-6"), Reason: "r", Expires: time.Hour})
	require.NoError(t, err)

	// a-1 leaves the targets file, dropping its quarantine, and a suspension
	// expires while the store is refusing writes; both deletions must reach
	// it afterwards.
	rules := h.set.Rules()
	smaller, err := parseTargets(replaceLine(testTargets, "- {id: a-1/cl, node: a-1, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: \"0\"}, hooks: {soak: soak-a}}", ""), &rules)
	require.NoError(t, err)

	h.store.Fail = errFake
	h.swap(smaller)
	h.c.Forget(h.ctx, []string{tA1})
	h.clock.Advance(2 * time.Minute)
	h.tick()
	h.store.Fail = nil
	h.tick()

	snap, err := h.store.Load(h.ctx)
	require.NoError(t, err)
	require.Equal(t, []string{tA2}, slices.Collect(maps.Keys(snap.Degraded)))
	require.Len(t, snap.Suspensions, 1)
	require.Equal(t, "node=a-6", snap.Suspensions[0].Selector.String())
}

func TestEventsAboutPagesBackPastOnePage(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	about := h.set.Select(mustSel("id=a-1/cl"))

	require.NoError(t, h.store.AppendEvent(h.ctx, &Event{ID: 1, Action: "old", Target: tA1}))

	for i := int64(2); i <= eventPage+10; i++ {
		require.NoError(t, h.store.AppendEvent(h.ctx, &Event{ID: i, Action: "noise", Target: "elsewhere"}))
	}

	got, err := h.c.EventsAbout(h.ctx, about, &EventQuery{Limit: 5})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "old", got[0].Action)

	// A search stops after the scan limit rather than read everything.
	for i := int64(eventPage + 11); i <= eventScanLimit+eventPage+20; i++ {
		require.NoError(t, h.store.AppendEvent(h.ctx, &Event{ID: i, Action: "noise", Target: "elsewhere"}))
	}

	got, err = h.c.EventsAbout(h.ctx, about, &EventQuery{})
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestTargetViewKeepsItsHooksBesideTheirRuns(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	raw, err := json.Marshal(h.view(tA1))
	require.NoError(t, err)
	require.Contains(t, string(raw), `"hooks":{"soak":"soak-a"}`)
	require.Contains(t, string(raw), `"hookRuns":{"inspect":`)
}

func TestCompleteReasonCountsOnlyTargetsMoved(t *testing.T) {
	r := &Rollout{Desired: map[string]string{imgA: d2}, Targets: []RolloutTarget{
		{ID: tA1, Phase: PhasePassed}, {ID: tA2, Phase: PhasePassed},
	}}
	require.Equal(t, "All 2 targets on 2222222.", completeReason(r))

	r.Targets = append(r.Targets, RolloutTarget{ID: tA3, Phase: PhaseSkipped})
	require.Equal(t, "2 of 3 targets on 2222222; 1 skipped and left for the next rollout.", completeReason(r))
}

func TestSoakProgressStaysOutOfHistory(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	h.ticks(5, 0)
	h.ticks(4, 20*time.Second)

	require.Equal(t, 1, count(h.notes.actions(), "rollout.soaking"), "one line when the soak starts, none for its checks")
}
