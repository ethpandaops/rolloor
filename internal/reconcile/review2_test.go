package reconcile

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/targets"
)

// These tests pin the second review round.

func TestSyncLeavesNothingBehindWhenTheStoreRefuses(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	require.NoError(t, h.c.SetPolicy(h.ctx, actor, "a", Policy{Mode: ModeManual}))
	h.world.set(func(w *world) { w.registry[imgA] = d2 })

	h.store.Fail = errFake

	_, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a"), Force: true})
	require.ErrorIs(t, err, errFake)
	require.Empty(t, h.c.Rollouts(), "a rollout the store never saw is not kept")

	// The same for an existing rollout: force and human stay as they were.
	h.store.Fail = nil
	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()

	r := h.active("a")
	require.Equal(t, WaitingForSync, r.State)

	h.store.Fail = errFake
	_, err = h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a"), Force: true})
	require.ErrorIs(t, err, errFake)

	r = h.active("a")
	require.Equal(t, WaitingForSync, r.State)
	require.False(t, r.Force)
	require.False(t, r.Human)
}

func TestAbortIsAtomicWithItsMarker(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	r := h.active("a")

	// The rollout save fails after the marker went in: the marker is taken
	// back and the rollout carries on.
	h.store.FailRollouts = errFake
	require.ErrorIs(t, h.c.Abort(h.ctx, actor, r.ID), errFake)
	h.store.FailRollouts = nil
	require.True(t, h.rollout(r.ID).State.Active())

	snap, err := h.store.Load(h.ctx)
	require.NoError(t, err)
	require.Empty(t, snap.Aborted)

	// A store that lost the rollout save but kept the marker: on restart the
	// marker wins and the rollout is closed.
	require.NoError(t, h.c.Abort(h.ctx, actor, r.ID))
	require.Equal(t, Aborted, h.rollout(r.ID).State)

	h.store.mu.Lock()
	h.store.rollouts[r.ID].State = Running
	h.store.rollouts[r.ID].EndedAt = time.Time{}
	h.store.mu.Unlock()

	restarted := h.newController()
	got, ok := restarted.Rollout(r.ID)
	require.True(t, ok)
	require.Equal(t, Aborted, got.State)
	require.Contains(t, got.Reason, "before the last restart")
}

func TestBudgetStaysHeldWhileAnEndedRolloutMayStillBeLanding(t *testing.T) {
	// Weighted nodes a-3..a-6 carry 100 each of 500; budget 50% admits two.
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	// Move group a's wave 0 targets out of the way so the first batch is on
	// weighted nodes, then abort while their updates have been dispatched
	// but the digest has not shown.
	h.world.set(func(w *world) {
		w.registry[imgA] = d2
		w.running[tA1], w.running[tA2] = d2, d2
	})
	h.c.InspectAll(h.ctx)
	h.clock.Advance(h.cfg.Registry.Poll)

	h.world.set(func(w *world) { w.inspectFail[tA3], w.inspectFail[tA4] = true, true })
	h.tick()

	r := h.active("a")
	require.Equal(t, []string{tA3, tA4}, r.Batches[0].Targets)
	require.NoError(t, h.c.Abort(h.ctx, actor, r.ID))

	for _, id := range []string{tA3, tA4} {
		rt := h.rollout(r.ID).Targets
		for i := range rt {
			if rt[i].ID == id {
				require.False(t, rt[i].HoldUntil.IsZero(), id)
			}
		}
	}

	// Group b's rollout finds the budget still taken: a-3 is already counted
	// so it costs nothing more, but b-1 would exceed the budget and waits.
	require.Equal(t, "40.0%", h.c.Fleet().Unavailable)
	h.world.set(func(w *world) { w.registry[imgB] = d2 })
	h.c.Refresh(h.ctx, actor)
	h.tick()
	rb := h.active("b")
	require.Equal(t, []string{tA3el}, rb.Batches[0].Targets)

	// Once the update could no longer be landing, the hold lifts.
	h.clock.Advance(h.cfg.Hooks.Timeout*5 + time.Second)

	h.c.mu.Lock()
	busy := h.c.inFlightNodes(h.set, h.clock.Now())
	h.c.mu.Unlock()
	require.NotContains(t, busy, tA4[:3], "a-4 is no longer held")
	require.Contains(t, busy, "a-3", "still in group b's open batch")
}

func TestSuspendedDuringItsUpdateDoesNotHaltTheBatch(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) {
		w.registry[imgA] = d2
		w.updateFail[tA1] = refused
	})
	h.clock.Advance(h.cfg.Registry.Poll)

	entered, release := h.world.gate("update")

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		h.tick()
	}()

	<-entered

	// The update program is running when the target is suspended.
	_, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: other, Selector: mustSel("id=" + tA1), Reason: mine})
	require.NoError(t, err)
	release()
	wg.Wait()

	r := h.active("a")
	require.NotEqual(t, Halted, r.State, "the refused update belonged to a target that was no longer in play")
	require.Equal(t, PhaseSkipped, h.phases(r)[tA1])
	require.Contains(t, r.Targets[0].Reason, "suspended by robin")
	require.Equal(t, PhaseUpdating, h.phases(r)[tA2])
}

func TestTargetEditsMidRolloutRetireTheTarget(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)

	rules := h.set.Rules()
	edited, err := parseTargets(replaceLine(replaceLine(testTargets,
		"- {id: a-1/cl, node: a-1, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: \"0\"}, hooks: {soak: soak-a}}",
		"- {id: a-1/cl, node: a-9, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: \"0\"}, hooks: {soak: soak-a}}"),
		"- {id: a-2/cl, node: a-2, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: \"0\"}, hooks: {soak: soak-a}}",
		"- {id: a-2/cl, node: a-2, weight: 0,   image: org/z:t, labels: {client: a, owner: a, role: cl, wave: \"0\"}, hooks: {soak: soak-a}}"), &rules)
	require.NoError(t, err)

	h.set = edited
	h.world.set(func(w *world) { w.registry["org/z:t"] = d1 })
	h.tick()

	r := h.active("a")
	require.Equal(t, PhaseSkipped, h.phases(r)[tA1])
	require.Equal(t, "moved to node a-9", r.Targets[0].Reason)
	require.Equal(t, PhaseSkipped, h.phases(r)[tA2])
	require.Equal(t, "image changed to org/z:t", r.Targets[1].Reason)
}

func TestRejectedObservationDoesNotAdvanceTheRollout(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)

	// A newer observation says the old digest is still running. The rollout's
	// own inspect started earlier, so what it reports is not accepted, and
	// the target is not marked as on the new build.
	h.clock.Advance(10 * time.Second)
	h.c.SetLive(h.ctx, tA1, d1, true, "", h.clock.Now().Add(time.Hour))
	h.tick()

	r := h.active("a")
	require.Equal(t, PhaseUpdating, h.phases(r)[tA1])
	require.False(t, r.Targets[0].Updated)
	require.Equal(t, d1, h.view(tA1).Live)
}

func TestSoakEvidenceIsTheCompletedCheck(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	h.ticks(4, 0)
	require.Equal(t, Soaking, h.active("a").State)

	id := h.active("a").ID

	// Leave the store looking like a check was dispatched and never answered,
	// with streak and duration met on paper: without a completed check the
	// batch must not pass on a restart an hour later.
	h.c.mu.Lock()
	r := h.c.rollouts[id]
	require.False(t, r.Soak.LastCheckStartedAt.IsZero())
	require.False(t, r.Soak.LastCheckAt.IsZero(), "the check that ran in this tick completed")
	r.Soak.LastCheckAt = time.Time{}
	r.Soak.Streak = 5
	r.Soak.StartedAt = h.clock.Now().Add(-2 * time.Hour)
	require.NoError(t, h.c.saveRollout(h.ctx, r))
	h.c.mu.Unlock()

	h.world.set(func(w *world) { w.soakFail[soakA] = true })

	restarted := h.newController()
	h.clock.Advance(time.Hour)
	soaks := len(h.world.callsFor("soak"))
	require.NoError(t, restarted.Tick(h.ctx))

	got, _ := restarted.Rollout(id)
	require.Equal(t, Soaking, got.State, "a check ran and failed instead of the batch passing on paper")
	require.Greater(t, len(h.world.callsFor("soak")), soaks)
	require.Equal(t, 0, got.Soak.Streak)
}

func TestDegradedNodeCostsNothingForAnotherGroup(t *testing.T) {
	// a-3 carries a-3/cl (group a) and a-3/el (group b). Quarantine a-3/cl,
	// then group b's rollout must not pay for a-3 either.
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) {
		w.registry[imgA] = d2
		w.running[tA1], w.running[tA2] = d2, d2
		w.updateFail[tA3] = refused
	})
	h.c.InspectAll(h.ctx)
	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()
	h.tick()
	require.Equal(t, Halted, h.active("a").State)
	require.Equal(t, Degraded, h.view(tA3).Health)

	h.release(imgB, d2)
	rb := h.active("b")
	require.Equal(t, Running, rb.State)
	require.Equal(t, []string{tA3el, "b-1/el"}, rb.Batches[0].Targets, "a-3 is free, so both weighted nodes fit")
	require.Equal(t, "20.0% of 50%", rb.Unavailable)
}

func TestLateInspectionOfAForgottenTargetIsDropped(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	rules := h.set.Rules()
	without, err := parseTargets(replaceLine(testTargets,
		"- {id: a-1/cl, node: a-1, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: \"0\"}, hooks: {soak: soak-a}}", ""), &rules)
	require.NoError(t, err)

	entered, release := h.world.gate("inspect")

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		h.c.InspectAll(h.ctx)
	}()

	<-entered

	h.swap(without)
	h.c.Forget(h.ctx, []string{tA1})
	release()
	wg.Wait()

	snap, err := h.store.Load(h.ctx)
	require.NoError(t, err)
	require.NotContains(t, snap.Live, tA1)
	require.NotContains(t, snap.HookRuns, tA1)
	_, ok := h.c.Target(tA1)
	require.False(t, ok)
}

func TestHookRunsSurviveARestart(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.updateFail[tA1] = refused })
	h.release(imgA, d2)
	require.Equal(t, Halted, h.active("a").State)

	restarted := h.newController()
	v := mustView(t, restarted, tA1)
	require.Equal(t, refused, v.Hooks["update"].Result.Reason)
	require.False(t, v.Hooks["update"].Result.OK)
}

func TestEventsAboutTargets(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.updateFail[tA3el] = refused })
	h.release(imgB, d2)
	require.Equal(t, Halted, h.active("b").State)

	_, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("node=a-3"), Reason: "x"})
	require.NoError(t, err)

	// Everything about node a-3: the halt (a group event), the suspension
	// (a selector event) and its own target events; nothing about group a's
	// image alone.
	events, err := h.c.EventsAbout(h.ctx, h.set.Node("a-3"), EventQuery{Limit: 50})
	require.NoError(t, err)

	acts := map[string]bool{}
	for _, e := range events {
		acts[e.Action] = true
	}

	require.True(t, acts["rollout.halted"], "%v", acts)
	require.True(t, acts["suspend"], "%v", acts)
	require.True(t, acts["rollout.created"], "%v", acts)

	// Node b-1 is only in group b: the digest change for org/a is not its business.
	events, err = h.c.EventsAbout(h.ctx, h.set.Node("b-1"), EventQuery{Limit: 2})
	require.NoError(t, err)
	require.Len(t, events, 2, "the limit applies after filtering")

	for _, e := range events {
		require.NotEqual(t, imgA, e.Target)
	}

	// A selector that no longer parses matches nothing; a group event with
	// no group but a rollout is resolved through the rollout.
	require.False(t, eventConcerns(&Event{Selector: "??"}, nil, nil, nil, nil))
	require.True(t, eventConcerns(&Event{Rollout: "r"}, nil, nil, map[string]struct{}{"g": {}}, map[string]string{"r": "g"}))

	h.store.Fail = errFake
	_, err = h.c.EventsAbout(h.ctx, nil, EventQuery{})
	require.ErrorIs(t, err, errFake)
}

func TestRefreshAskedForDuringAScanIsKept(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	entered, release := h.world.gate("resolve")
	h.clock.Advance(h.cfg.Registry.Poll)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		h.tick()
	}()

	<-entered
	h.c.Refresh(h.ctx, actor)
	release()
	wg.Wait()

	h.c.mu.Lock()
	wanted := h.c.refreshWanted
	h.c.mu.Unlock()
	require.True(t, wanted, "the refresh arrived after the scan had started, so it is still owed")

	// The next tick honours it without waiting for the poll interval.
	resolves := h.world.resolves()
	h.tick()
	require.Greater(t, h.world.resolves(), resolves)
}

func TestInterruptedUpdateRunsAgainAfterRestart(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)

	id := h.active("a").ID

	h.c.mu.Lock()
	r := h.c.rollouts[id]
	r.Targets[0].UpdateDone = false
	require.NoError(t, h.c.saveRollout(h.ctx, r))
	h.c.mu.Unlock()

	updates := len(h.world.callsFor("update"))
	restarted := h.newController()
	require.NoError(t, restarted.Tick(h.ctx))

	calls := h.world.callsFor("update")
	require.Len(t, calls, updates+1)
	require.Contains(t, calls[len(calls)-1], tA1)
}

func TestDesiredRef(t *testing.T) {
	require.Equal(t, "org/a@"+d3, DesiredRef(imgA, d3))
	require.Equal(t, "localhost:5050/app@x", DesiredRef("localhost:5050/app:latest", "x"))

	var s targets.Selector

	require.Nil(t, s)
}

func TestFlushRewritesQuarantineAndAbortMarkers(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.updateFail[tA1] = refused })
	h.release(imgA, d2)
	require.Equal(t, Halted, h.active("a").State)
	require.NoError(t, h.c.Abort(h.ctx, actor, h.active("a").ID))

	// A failing tick marks the state dirty; the recovering tick writes the
	// quarantine and the abort marker again along with the rollouts.
	h.store.Fail = errFake
	h.world.set(func(w *world) { w.registry[imgB] = d2 })
	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()

	h.store.Fail = nil
	h.store.mu.Lock()
	delete(h.store.degraded, tA1)
	delete(h.store.aborted, "a")
	h.store.mu.Unlock()

	h.tick()

	snap, err := h.store.Load(h.ctx)
	require.NoError(t, err)
	require.Contains(t, snap.Degraded, tA1)
	require.Contains(t, snap.Aborted, "a")
}
