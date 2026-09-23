package reconcile

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetryAfterAFailedUpdateRunsTheUpdateAgain(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.updateFail[tA2] = "updater unreachable" })
	h.release(imgA, d2)

	r := h.active("a")
	require.Equal(t, Halted, r.State)

	updates := len(h.world.callsFor("update:" + tA2))
	h.world.set(func(w *world) { delete(w.updateFail, tA2) })
	require.NoError(t, h.c.Retry(h.ctx, actor, r.ID, "updater back"))
	h.tick()

	require.Greater(t, len(h.world.callsFor("update:"+tA2)), updates, "a target that never got the digest is updated again")
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
	h.tick()

	require.Equal(t, updates, len(h.world.callsFor("update:"+tA1)))
	require.Equal(t, PhaseUpdating, h.phases(h.active("a"))[tA1])
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

	// The retry lifts the quarantine and the suspension expires while the
	// store is refusing writes; both deletions must reach it afterwards.
	h.world.set(func(w *world) { w.soakFail[soakA] = false })
	require.NoError(t, h.c.Retry(h.ctx, actor, r.ID, "probe fixed"))
	h.store.Fail = errFake
	h.clock.Advance(2 * time.Minute)
	h.tick()
	h.store.Fail = nil
	h.tick()

	snap, err := h.store.Load(h.ctx)
	require.NoError(t, err)
	require.Empty(t, snap.Degraded)
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

	got, err := h.c.EventsAbout(h.ctx, about, EventQuery{Limit: 5})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "old", got[0].Action)

	// A search stops after the scan limit rather than read everything.
	for i := int64(eventPage + 11); i <= eventScanLimit+eventPage+20; i++ {
		require.NoError(t, h.store.AppendEvent(h.ctx, &Event{ID: i, Action: "noise", Target: "elsewhere"}))
	}

	got, err = h.c.EventsAbout(h.ctx, about, EventQuery{})
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

	events, err := h.c.Events(h.ctx, EventQuery{})
	require.NoError(t, err)

	var started string

	for i := range events {
		if events[i].Action == "batch.started" {
			started = events[i].Reason
		}
	}

	require.Equal(t, "batch 1: 2 targets on 2 nodes in wave 0", started)
}
