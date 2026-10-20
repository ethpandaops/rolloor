package reconcile

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUpdateHookReceivesTheDesiredDigest(t *testing.T) {
	h := newHarness(t, replaceLine(testConfig, "registry: {poll: 60s}", "registry: {poll: 1h}"), testTargets)
	h.prime()
	h.release(imgA, d2)

	// The tag moves on before the next poll: updates still deploy the digest
	// the rollout moves to, not whatever the tag points at when they run.
	h.world.set(func(w *world) { w.registry[imgA] = d3 })

	r := h.active("a")
	require.Equal(t, "2222222", r.Digest)
	require.Equal(t, Complete, h.drive(r.ID, 80, 20*time.Second).State)

	for _, v := range h.c.Targets(mustSel("client=a")) {
		require.Equal(t, d2, v.Live, v.ID)
		require.Equal(t, Synced, v.Sync, v.ID)
	}

	require.Equal(t, "org/a@"+d3, imageRepo(imgA)+"@"+d3)
	require.Equal(t, "localhost:5050/app", imageRepo("localhost:5050/app:latest"))
	require.Equal(t, "org/a", imageRepo("org/a"))
}

func TestSuspensionAndGroupMoveMidBatch(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.notReady[tA1], w.notReady[tA2] = true, true })
	h.release(imgA, d2)
	r := h.active("a")
	h.tick()
	require.Equal(t, PhaseUpdating, h.phases(h.active("a"))[tA1])

	// Suspending a target mid-update stops further work on it.
	_, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: other, Selector: mustSel("id=" + tA1), Reason: mine})
	require.NoError(t, err)

	// Moving the other target to another group does the same.
	rules := h.set.Rules()
	moved, err := parseTargets(replaceLine(testTargets,
		"- {id: a-2/cl, node: a-2, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: \"0\"}, hooks: {soak: soak-a}}",
		"- {id: a-2/cl, node: a-2, weight: 0,   image: org/a:t, labels: {client: b, owner: b, role: cl, wave: \"0\"}, hooks: {soak: soak-a}}"), &rules)
	require.NoError(t, err)

	h.set = moved
	h.tick()

	got := h.active("a")
	require.Equal(t, PhaseSkipped, h.phases(got)[tA1])
	require.Contains(t, got.Targets[0].Reason, "suspended by robin")
	require.Equal(t, PhaseSkipped, h.phases(got)[tA2])
	require.Equal(t, "moved to group b", got.Targets[1].Reason)
	require.Equal(t, Suspended, h.view(tA1).Health)

	// The batch had nothing left, so it passed and the rollout moved on.
	require.Equal(t, Complete, h.drive(r.ID, 80, 20*time.Second).State)
}

func TestAbortSurvivesRestartAndTemporaryIneligibility(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	require.NoError(t, h.c.Abort(h.ctx, actor, h.active("a").ID))

	// Suspending every remaining target and lifting it again does not lift the abort.
	_, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("client=a"), Reason: "x", Expires: time.Hour})
	require.NoError(t, err)
	h.ticks(2, 0)
	_, err = h.c.Resume(h.ctx, actor, mustSel("client=a"))
	require.NoError(t, err)
	h.ticks(2, 0)
	require.Len(t, h.c.Rollouts(), 1)

	// Nor does a restart.
	restarted := h.newController()
	require.NoError(t, restarted.Tick(h.ctx))
	require.Len(t, restarted.Rollouts(), 1)

	// A new digest does.
	h.world.set(func(w *world) { w.registry[imgA] = d3 })
	h.clock.Advance(h.cfg.Registry.Poll)
	require.NoError(t, restarted.Tick(h.ctx))
	require.Len(t, restarted.Rollouts(), 2)
}

func TestStoreFailuresSurfaceOnActions(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	r := h.active("a")

	h.store.Fail = errFake

	_, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("node=b-1"), Reason: "x"})
	require.ErrorIs(t, err, errFake)
	require.Empty(t, h.c.Suspensions(), "not kept in memory either")

	require.ErrorIs(t, h.pause(r.ID, 0), errFake)
	require.False(t, h.rollout(r.ID).PausePending, "a refused pause leaves nothing behind")
	require.ErrorIs(t, h.c.Promote(h.ctx, actor, r.ID), ErrState, "so there is nothing to promote")
	require.ErrorIs(t, h.c.Retry(h.ctx, actor, r.ID, "x"), ErrState)
	require.ErrorIs(t, h.c.Abort(h.ctx, actor, r.ID), errFake)
	require.True(t, h.rollout(r.ID).State.Active(), "abort did not happen")

	h.store.Fail = nil
	require.NoError(t, h.pause(r.ID, 0))
	require.True(t, h.rollout(r.ID).PausePending)

	h.store.Fail = errFake
	require.ErrorIs(t, h.c.Promote(h.ctx, actor, r.ID), errFake)
	require.True(t, h.rollout(r.ID).PausePending, "a refused promote keeps the pause")

	_, err = h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a")})
	require.ErrorIs(t, err, errFake)

	h.store.Fail = nil
	_, err = h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("node=b-1"), Reason: "x"})
	require.NoError(t, err)

	h.store.Fail = errFake
	_, err = h.c.Resume(h.ctx, actor, mustSel("node=b-1"))
	require.ErrorIs(t, err, errFake)
	require.Len(t, h.c.Suspensions(), 1)

	h.store.Fail = nil
	require.NoError(t, h.c.Abort(h.ctx, actor, r.ID))

	h.store.Fail = errFake
	_, err = h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a")})
	require.ErrorIs(t, err, errFake, "clearing the abort could not be persisted")

	// Halting with a failing store still quarantines in memory.
	h.store.Fail = nil
}

func TestObservationsApplyInOrder(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	now := h.clock.Now()
	h.c.SetLive(h.ctx, tA1, d2, true, "", now)
	require.Equal(t, d2, h.view(tA1).Live)

	// An observation that began earlier arrives later: ignored.
	h.c.SetLive(h.ctx, tA1, d1, true, "", now.Add(-time.Second))
	require.Equal(t, d2, h.view(tA1).Live)

	// A later one applies.
	h.c.SetLive(h.ctx, tA1, d3, true, "", now.Add(time.Second))
	require.Equal(t, d3, h.view(tA1).Live)
}

func TestSoakNeedsFreshEvidenceAfterAGap(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	h.ticks(5, 0)
	h.ticks(2, 20*time.Second)
	require.Equal(t, 2, h.active("a").Soak.Streak)

	// The process was away for an hour with a qualifying streak stored: the
	// first tick back runs a check instead of passing on old evidence.
	h.world.set(func(w *world) { w.soakFail[soakA] = true })
	h.clock.Advance(time.Hour)
	soaks := len(h.world.callsFor("soak"))
	h.tick()
	require.Equal(t, Soaking, h.active("a").State)
	require.Equal(t, soaks+1, len(h.world.callsFor("soak")))
	require.Equal(t, 0, h.active("a").Soak.Streak)
}

func TestSoakWithNoProgramsLeftPasses(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	h.ticks(5, 0)
	require.Equal(t, Soaking, h.active("a").State)

	rules := h.set.Rules()
	noSoak, err := parseTargets(replaceLine(replaceLine(testTargets,
		"- {id: a-1/cl, node: a-1, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: \"0\"}, hooks: {soak: soak-a}}",
		"- {id: a-1/cl, node: a-1, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: \"0\"}}"),
		"- {id: a-2/cl, node: a-2, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: \"0\"}, hooks: {soak: soak-a}}",
		"- {id: a-2/cl, node: a-2, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: \"0\"}}"), &rules)
	require.NoError(t, err)

	h.set = noSoak
	h.clock.Advance(20 * time.Second)
	h.tick()

	r := h.active("a")
	require.Equal(t, Running, r.State)
	require.True(t, r.Batches[0].Passed)
	require.NotNil(t, r.Batches[0].Soak, "the soak stays with its batch")
	require.Len(t, r.Batches[0].Soak.Checks, 1)
}

func TestForceOnARunningRolloutSkipsReadyAndSoak(t *testing.T) {
	// Stuck in readiness.
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.notReady[tA1] = true })
	h.release(imgA, d2)
	h.ticks(3, 0)
	require.Contains(t, h.view(tA1).Reason, "not ready")

	_, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a"), Force: true})
	require.NoError(t, err)
	h.tick()
	// The other target was already ready, so the forced one completes the
	// batch and the batch passes without a soak in the same tick.
	require.Equal(t, PhasePassed, h.phases(h.active("a"))[tA1])
	require.Equal(t, "no soak", h.active("a").Targets[0].Reason)
	require.True(t, h.active("a").Batches[0].Passed)

	// Mid-soak.
	h2 := newHarness(t, testConfig, testTargets)
	h2.prime()
	h2.release(imgA, d2)
	h2.ticks(5, 0)
	require.Equal(t, Soaking, h2.active("a").State)

	_, err = h2.c.Sync(h2.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a"), Force: true})
	require.NoError(t, err)
	h2.tick()
	require.True(t, h2.active("a").Batches[0].Passed)
	require.Equal(t, "forced", h2.active("a").Targets[0].Reason)

	// A soak result landing after force is set passes too.
	h3 := newHarness(t, testConfig, testTargets)
	h3.prime()
	h3.release(imgA, d2)
	h3.ticks(4, 0)

	require.Equal(t, Soaking, h3.active("a").State)

	id := h3.active("a").ID

	h3.c.mu.Lock()
	h3.c.rollouts[id].Force = true
	h3.c.applySoak(h3.ctx, h3.clock.Now(), h3.c.rollouts[id], nil)
	h3.c.mu.Unlock()
	require.Equal(t, Running, h3.active("a").State)
}

func TestWavesAreContiguousAfterDegradedTargets(t *testing.T) {
	// Halt a wave-2 target so it sorts first with wave 2, then confirm the
	// wave-1 targets are not pulled in beside it.
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	for _, id := range []string{tA1, tA2, tA3, tA4, tA5} {
		h.world.set(func(w *world) { w.running[id] = d2 })
	}

	h.c.InspectAll(h.ctx)
	h.haltOnBadBuild("a", imgA, d2, tA6)

	h.world.set(func(w *world) { delete(w.breakOnUpdate, tA6) })

	for range h.cfg.ReadinessProbe.FailureThreshold {
		h.c.ProbeAll(h.ctx)
	}

	h.release(imgA, d3)

	r := h.active("a")
	require.True(t, r.Targets[0].NotReadyBefore)
	require.Equal(t, 2, r.Targets[0].Wave)
	require.Equal(t, []string{tA6}, r.Batches[0].Targets, "the degraded wave-2 target goes alone")
	require.True(t, r.Targets[0].Free)
	require.Equal(t, "20.0% of 50%", r.Unavailable)
}

func TestNodeDegradedCostsNothingForAllItsTargets(t *testing.T) {
	doc := testTargets + "- {id: a-3/vc, node: a-3, weight: 100, image: org/a:t, labels: {client: a, owner: a, role: vc, wave: \"1\"}, hooks: {soak: soak-a}}\n"
	h := newHarness(t, testConfig, doc)
	h.prime()

	// Quarantine only a-3/cl with a build that never becomes ready there.
	h.world.set(func(w *world) { w.running[tA1], w.running[tA2] = d2, d2 })
	h.c.InspectAll(h.ctx)
	h.haltOnBadBuild("a", imgA, d2, tA3)

	h.world.set(func(w *world) {
		delete(w.breakOnUpdate, tA3)

		for _, id := range []string{tA3, tA3VC, tA4} {
			w.notReady[id] = true
		}
	})

	for range h.cfg.ReadinessProbe.FailureThreshold {
		h.c.ProbeAll(h.ctx)
	}

	h.release(imgA, d3)

	r := h.active("a")
	require.Equal(t, []string{tA3, tA3VC, tA4}, r.Batches[0].Targets)
	require.Equal(t, "40.0% of 50%", r.Unavailable)

	for _, rt := range r.Targets[:3] {
		require.True(t, rt.Free)
	}
}

func TestRestartRerunsInterruptedUpdates(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)

	id := h.active("a").ID

	// Rewind the first target to the moment its update was dispatched, which
	// is what a crash before the result landed leaves in the store.
	h.c.mu.Lock()
	r := h.c.rollouts[id]
	require.Equal(t, PhaseUpdating, r.Targets[0].Phase)
	r.Targets[0].UpdateDone = false
	require.NoError(t, h.c.saveRollout(h.ctx, r))
	h.c.mu.Unlock()

	restarted := h.newController()
	got, ok := restarted.Rollout(r.ID)
	require.True(t, ok)
	require.Equal(t, PhasePending, got.Targets[0].Phase)
	require.Contains(t, got.Targets[0].Reason, "interrupted")
}

func TestTargetsErrorsAndRegistryEpisodes(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	h.c.ReportTargetsError(h.ctx, errFake)
	h.c.ReportTargetsError(h.ctx, errFake)
	h.c.ReportTargetsError(h.ctx, nil)
	h.c.ReportTargetsError(h.ctx, nil)

	acts := h.notes.actions()
	require.Equal(t, 1, count(acts, "targets.invalid"))
	require.Equal(t, 1, count(acts, "targets.reloaded"))

	// One event when the registry starts failing, one when it recovers.
	h.world.set(func(w *world) { w.registryErr[imgA] = errFake })
	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()
	h.world.set(func(w *world) { w.registryErr[imgA] = errOther })
	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()
	require.Equal(t, 1, count(h.notes.actions(), "registry.error"))
	require.Equal(t, "other", h.c.Fleet().Resolve[imgA])

	h.world.set(func(w *world) { delete(w.registryErr, imgA) })
	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()
	require.Equal(t, 1, count(h.notes.actions(), "registry.recovered"))
	require.Empty(t, h.c.Fleet().Resolve)

	// Desired digests survive a restart even if the registry is down.
	h.world.set(func(w *world) { w.registryErr[imgA] = errFake })
	restarted := h.newController()
	require.NoError(t, restarted.Tick(h.ctx))
	require.Equal(t, d1, mustView(t, restarted, tA1).Desired)
	require.False(t, restarted.LastTick().IsZero())
}

func TestRefreshDuringResolutionIsNotLost(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	// The flag is consumed when resolution begins, so one set during it stays.
	h.c.mu.Lock()
	h.c.refreshWanted = true
	h.c.mu.Unlock()

	require.NoError(t, h.c.resolveIfDue(h.ctx, h.clock.Now()))

	h.c.mu.Lock()
	wanted := h.c.refreshWanted
	h.c.mu.Unlock()
	require.False(t, wanted)
}

func TestViewHooksAreACopy(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	v := h.view(tA1)
	v.Hooks["x"] = HookRun{}
	require.NotContains(t, h.view(tA1).Hooks, "x")
	require.Nil(t, h.c.hookRunsFor("nope"))
}

func TestSyncResolvesBeforeDeciding(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	// The tag has just moved but no poll has noticed. One sync is enough.
	h.world.set(func(w *world) { w.registry[imgA] = d2 })

	ids, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a")})
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Equal(t, Running, h.rollout(ids[0]).State)
	require.True(t, h.rollout(ids[0]).Human)

	// A human rollout is superseded by a newer digest at sync time and the new
	// one keeps the person's intent.
	h.world.set(func(w *world) { w.registry[imgA] = d3 })
	ids, err = h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a"), Strategy: speedAll})
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Equal(t, "3333333", h.rollout(ids[0]).Digest)
	require.Equal(t, speedAll, h.rollout(ids[0]).Strategy)
	require.True(t, h.rollout(ids[0]).Human)
}

func TestOnNewBuildCountsObservedDigestsOnly(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.updateStuck[tA1], w.updateStuck[tA2] = true, true })
	h.release(imgA, d2)
	h.tick()

	r := h.active("a")
	require.Equal(t, PhaseUpdating, h.phases(r)[tA2])
	require.Equal(t, 0, r.OnNewBuild, "an update that has not landed is not on the build")

	h.world.set(func(w *world) { w.running[tA1] = d2 })
	h.tick()
	require.Equal(t, 1, h.active("a").OnNewBuild)
}

func count(list []string, want string) int {
	n := 0

	for _, s := range list {
		if s == want {
			n++
		}
	}

	return n
}

func TestRetryAndSyncReportStoreAndContextErrors(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	r := h.haltOnBadBuild("a", imgA, d2, tA1)

	h.store.Fail = errFake
	require.ErrorIs(t, h.c.Retry(h.ctx, actor, r.ID, "again"), errFake)
	require.Equal(t, r.Rollout, h.rollout(r.ID).Rollout, "a refused retry leaves the halted rollout as it was")

	h.store.Fail = nil
	require.NoError(t, h.c.Retry(h.ctx, actor, r.ID, "store back"))
	require.Equal(t, Running, h.rollout(r.ID).State)

	// A sync resolves the registry first, so a cancelled context stops it there.
	cancelled, cancel := context.WithCancel(h.ctx)
	cancel()

	_, err := h.c.Sync(cancelled, SyncRequest{Actor: actor, Selector: mustSel("client=b")})
	require.ErrorIs(t, err, context.Canceled)
}

func TestTargetRemovedDuringSoakIsLeftOut(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	h.ticks(5, 0)
	require.Equal(t, Soaking, h.active("a").State)

	rules := h.set.Rules()
	without, err := parseTargets(replaceLine(testTargets,
		"- {id: a-2/cl, node: a-2, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: \"0\"}, hooks: {soak: soak-a}}", ""), &rules)
	require.NoError(t, err)

	h.set = without
	checks := len(h.active("a").Soak.Checks)
	h.clock.Advance(20 * time.Second)
	h.tick()

	r := h.active("a")
	require.Equal(t, Soaking, r.State)
	require.Len(t, r.Soak.Checks, checks+1, "the soak keeps running on the target that is left")
}
