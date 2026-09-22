package reconcile

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSteadyStateIsSynced(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	f := h.c.Fleet()
	require.Equal(t, speedTest, f.Environment)
	require.Equal(t, 9, f.Targets)
	require.Equal(t, 7, f.Nodes)
	require.InDelta(t, 500.0, f.Weight, 0.001)
	require.Equal(t, "0.0%", f.BudgetInUse)
	require.Len(t, f.Groups, 3)
	require.Empty(t, h.c.Rollouts())

	for _, g := range f.Groups {
		require.Equal(t, Synced, g.Sync, g.Name)
		require.Equal(t, Healthy, g.Health, g.Name)
		require.Equal(t, "On 1111111", g.Reason, g.Name)
	}

	require.True(t, f.Groups[2].Hidden)
	require.Equal(t, "cl", f.Groups[0].Section)
	require.Equal(t, "reva", f.Groups[0].Revisions[imgA])
	require.Contains(t, h.notes.actions(), "digest.changed")
}

func TestHappyPathWavesBatchesSoak(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)

	r := h.active("a")
	require.Equal(t, Running, r.State)
	require.Equal(t, 6, r.Total)
	require.Equal(t, "2222222", r.Digest)
	require.Equal(t, "reva", r.Revision)
	require.Equal(t, d1, r.From[imgA])
	require.Len(t, r.Batches, 1)
	require.Equal(t, []string{tA1, tA2}, r.Batches[0].Targets, "wave 0 first")
	require.Contains(t, r.Reason, "Wave 0, batch 1 of about 4")

	// update → inspect → ready → soak begins.
	h.tick()
	require.Equal(t, PhaseUpdating, h.phases(h.active("a"))[tA1])
	require.Equal(t, Progressing, h.view(tA1).Health)
	h.tick()
	require.True(t, h.active("a").Targets[0].Updated)
	h.tick()
	require.Equal(t, PhaseReady, h.phases(h.active("a"))[tA1])
	h.tick()
	require.Equal(t, Soaking, h.active("a").State)
	require.Equal(t, 2, h.active("a").OnNewBuild)
	require.Equal(t, 4, h.active("a").NotReached)

	// Two passing checks are not enough before the duration has elapsed.
	h.ticks(3, 20*time.Second)
	r = h.active("a")
	require.Equal(t, Soaking, r.State)
	require.Equal(t, 3, r.Soak.Streak)
	require.Equal(t, "in soak for batch 1", h.view(tA1).Reason)
	require.Equal(t, Progressing, h.view(tA1).Health)

	h.tick()
	r = h.active("a")
	require.Equal(t, Running, r.State)
	require.True(t, r.Batches[0].Passed)
	require.Equal(t, PhasePassed, h.phases(r)[tA1])
	require.Equal(t, "updated in batch 1", h.view(tA1).Reason)

	// Wave 1 batch is two weighted nodes, which is exactly the budget.
	h.tick()
	r = h.active("a")
	require.Equal(t, []string{tA3, tA4}, r.Batches[1].Targets)
	require.Equal(t, "40.0% of 50%", r.BudgetPercent)
	require.Equal(t, "not reached yet", h.view(tA5).Reason)

	final := h.drive(r.ID, 60, 20*time.Second)
	require.Equal(t, Complete, final.State)
	require.Len(t, final.Batches, 4)
	require.Equal(t, []string{tA5}, final.Batches[2].Targets)
	require.Equal(t, []string{tA6}, final.Batches[3].Targets, "wave 2 last")
	require.Equal(t, 6, final.OnNewBuild)
	require.Equal(t, "All 6 targets on 2222222.", final.Reason)

	acts := h.notes.actions()
	require.Contains(t, acts, "rollout.created")
	require.Contains(t, acts, "batch.started")
	require.Contains(t, acts, "batch.passed")
	require.Contains(t, acts, "rollout.complete")

	// Soak inputs carried the not-yet-updated targets.
	soaks := h.world.callsFor("soak")
	require.NotEmpty(t, soaks)

	// Everything reads Synced again and group b never moved.
	for _, v := range h.c.Targets(nil) {
		require.Equal(t, Synced, v.Sync, v.ID)
	}

	require.Empty(t, h.world.callsFor("update:a-3/el"))
}

func TestSoakFailureHaltsQuarantinesAndRetries(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.soakFail[soakA] = true })
	h.release(imgA, d2)
	h.ticks(4, 0)
	require.Equal(t, Soaking, h.active("a").State)

	// grace 1: first failure tolerated, second halts.
	h.tick()
	require.Equal(t, Soaking, h.active("a").State)
	require.Equal(t, 1, h.active("a").Soak.Failures)
	h.clock.Advance(20 * time.Second)
	h.tick()

	r := h.active("a")
	require.Equal(t, Halted, r.State)
	require.Contains(t, r.Reason, "soak soak-a failed 2 times: 61% vs 98%")
	require.Contains(t, r.Reason, "2 targets left running 2222222")
	require.Equal(t, PhaseFailed, h.phases(r)[tA1])
	require.Equal(t, Degraded, h.view(tA1).Health)
	require.Equal(t, Degraded, h.c.Fleet().Groups[0].Health)
	require.Contains(t, h.c.Fleet().Groups[0].Reason, "Halted")

	// Nothing moves while halted.
	before := len(h.world.callsFor("update"))
	h.ticks(3, 20*time.Second)
	require.Equal(t, before, len(h.world.callsFor("update")))
	require.Equal(t, Halted, h.active("a").State)

	// A retry with the probe fixed passes the batch and clears the quarantine.
	require.Error(t, h.c.Retry(h.ctx, actor, r.ID, ""))
	require.ErrorIs(t, h.c.Retry(h.ctx, actor, "nope", "x"), ErrNotFound)
	require.NoError(t, h.c.Retry(h.ctx, actor, r.ID, "probe was wrong"))
	h.world.set(func(w *world) { w.soakFail[soakA] = false })
	h.tick()

	// The failed targets are inspected again before anything is claimed, and
	// the soak that failed stays on the batch's record.
	retried := h.active("a")
	require.Equal(t, Running, retried.State)
	require.Equal(t, PhaseUpdating, h.phases(retried)[tA1])
	require.Equal(t, 2, retried.OnNewBuild, "inspect saw the digest again in the same tick")
	require.Len(t, retried.Batches[0].PriorSoaks, 1)
	require.Equal(t, Progressing, h.view(tA1).Health)

	h.ticks(3, 0)
	require.Equal(t, Soaking, h.active("a").State)
	h.ticks(4, 20*time.Second)
	require.Equal(t, Running, h.active("a").State)
	require.Equal(t, PhasePassed, h.phases(h.active("a"))[tA1])
	require.Equal(t, Healthy, h.view(tA1).Health)
	require.ErrorIs(t, h.c.Retry(h.ctx, actor, r.ID, "again"), ErrState)
}

func TestNewDigestSupersedesHaltAndDegradedGoFirst(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.soakFail[soakA] = true })
	h.release(imgA, d2)
	h.ticks(5, 0)
	h.clock.Advance(20 * time.Second)
	h.tick()

	first := h.active("a")
	require.Equal(t, Halted, first.State)

	h.world.set(func(w *world) { w.soakFail[soakA] = false })
	h.release(imgA, d3)

	old := h.rollout(first.ID)
	require.Equal(t, Superseded, old.State)
	require.Contains(t, old.Reason, "moved to 3333333")

	next := h.active("a")
	require.NotEqual(t, first.ID, next.ID)
	require.Equal(t, "3333333", next.Digest)
	require.Equal(t, []string{tA1, tA2}, next.Batches[0].Targets)
	require.True(t, next.Targets[0].DegradedBefore)
	require.Equal(t, d1, next.From[imgA], "most targets never left the first build")

	// Degraded nodes cost nothing against the budget.
	require.Equal(t, "0.0% of 50%", next.BudgetPercent)

	require.Equal(t, Complete, h.drive(next.ID, 80, 20*time.Second).State)
	require.Equal(t, Healthy, h.view(tA1).Health)
}

func TestUpdateFailureAndReadyTimeoutHalt(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.updateFail[tA2] = "watcher said no" })
	h.release(imgA, d2)
	h.tick()

	r := h.active("a")
	require.Equal(t, Halted, r.State)
	require.Contains(t, r.Reason, "a-2/cl update failed: watcher said no")
	require.Equal(t, PhaseFailed, h.phases(r)[tA1])
	require.Equal(t, "in the halted batch", h.view(tA1).Reason)

	// A target whose digest never lands times out after 5× the hook timeout.
	h2 := newHarness(t, testConfig, testTargets)
	h2.prime()
	h2.world.set(func(w *world) { w.updateStuck[tA1] = true })
	h2.release(imgA, d2)
	h2.tick()
	h2.ticks(3, 20*time.Second)
	require.Equal(t, Running, h2.active("a").State)
	require.False(t, h2.active("a").Targets[0].Updated)
	h2.tick()

	r2 := h2.active("a")
	require.Equal(t, Halted, r2.State)
	require.Contains(t, r2.Reason, "did not reach 2222222 within 50s")
}

func TestNotReadyKeepsWaitingThenPasses(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.notReady[tA1] = true })
	h.release(imgA, d2)
	h.ticks(3, 0)
	require.Equal(t, "not ready: still syncing", h.view(tA1).Reason)
	h.world.set(func(w *world) { w.notReady[tA1] = false })
	h.tick()
	require.Equal(t, PhaseReady, h.phases(h.active("a"))[tA1])

	// A target that never becomes ready halts with the readiness reason.
	h2 := newHarness(t, testConfig, testTargets)
	h2.prime()
	h2.world.set(func(w *world) { w.notReady[tA1] = true })
	h2.release(imgA, d2)
	h2.ticks(3, 0)
	h2.ticks(4, 20*time.Second)
	require.Equal(t, Halted, h2.active("a").State)
	require.Contains(t, h2.active("a").Reason, "a-1/cl not ready: still syncing within 50s")
}

func TestBudgetIsSharedAcrossGroupsAndNodesCountOnce(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	// Group a jumps straight to wave 1 by having wave 0 already on d2.
	h.world.set(func(w *world) {
		w.registry[imgA] = d2
		w.registry[imgB] = d2
		w.running[tA1] = d2
		w.running[tA2] = d2
	})
	h.c.InspectAll(h.ctx)
	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()

	ra, rb := h.active("a"), h.active("b")
	require.Equal(t, []string{tA3, tA4}, ra.Batches[0].Targets)

	// Node a-3 is already in flight, so a-3/el is free; b-1 would exceed the budget.
	require.Equal(t, []string{tA3el}, rb.Batches[0].Targets)
	require.Equal(t, "40.0%", h.c.Fleet().BudgetInUse)

	h.drive(ra.ID, 120, 20*time.Second)
	require.Equal(t, Complete, h.rollout(ra.ID).State)
	require.Equal(t, Complete, h.drive(rb.ID, 120, 20*time.Second).State)
}

func TestWaitingForBudgetSaysHowMuch(t *testing.T) {
	// A 20% budget is one weighted node at a time.
	h := newHarness(t, replaceLine(testConfig, "budget: 50%", "budget: 20%"), testTargets)
	h.prime()
	h.world.set(func(w *world) {
		w.registry[imgA] = d2
		w.registry[imgB] = d2
		w.running[tA1] = d2
		w.running[tA2] = d2
	})
	h.c.InspectAll(h.ctx)
	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()

	ra, rb := h.active("a"), h.active("b")
	require.Equal(t, []string{tA3}, ra.Batches[0].Targets)
	require.Equal(t, []string{tA3el}, rb.Batches[0].Targets, "same node, no extra cost")

	// After the first batches pass, a takes the next node and b has to wait.
	h.ticks(4, 0)
	h.ticks(4, 20*time.Second)

	for i := 0; i < 4 && h.active("b").State != WaitingForBudget; i++ {
		h.tick()
	}

	rb = h.active("b")
	require.Equal(t, WaitingForBudget, rb.State)
	require.Equal(t, "Waiting for budget: 20.0% of 20% in use; next batch needs 20.0%.", rb.Reason)
	require.Equal(t, rb.Reason, h.c.Fleet().Groups[1].Reason)
	require.Contains(t, h.notes.actions(), "rollout.waitingforbudget")

	require.Equal(t, Complete, h.drive(ra.ID, 200, 20*time.Second).State)
	require.Equal(t, Complete, h.drive(rb.ID, 200, 20*time.Second).State)
}

func TestEnvironmentCheckPausesAutomatedOnly(t *testing.T) {
	cfg := testConfig + "\n"
	cfg = replaceLine(cfg, "  defaults: {soak: \"\"}", "  defaults: {soak: \"\"}\n  environment: {program: env, interval: 30s}")

	h := newHarness(t, cfg, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.envOK, w.envReason = false, "the world is on fire" })
	h.clock.Advance(30 * time.Second)
	h.release(imgA, d2)

	r := h.active("a")
	require.Equal(t, WaitingForEnvironment, r.State)
	require.Contains(t, r.Reason, "the world is on fire")

	ok, reason, at := h.c.EnvironmentStatus()
	require.False(t, ok)
	require.Equal(t, "the world is on fire", reason)
	require.False(t, at.IsZero())
	require.Contains(t, h.notes.actions(), "environment.failing")

	f := h.c.Fleet()
	require.False(t, f.EnvOK)
	require.Equal(t, "env", f.EnvCheck)

	// A person syncing is the fix arriving; it proceeds.
	ids, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a")})
	require.NoError(t, err)
	require.Equal(t, []string{r.ID}, ids)
	h.tick()
	require.Equal(t, Running, h.active("a").State)
	require.Len(t, h.active("a").Batches, 1)

	// The check recovering is an event too.
	h.world.set(func(w *world) { w.envOK = true })
	h.clock.Advance(30 * time.Second)
	h.tick()
	require.Contains(t, h.notes.actions(), "environment.passing")

	// A hook that cannot run at all counts as failing.
	h.world.set(func(w *world) { w.runErr["env"] = errFake })
	h.clock.Advance(30 * time.Second)
	h.tick()
	ok, reason, _ = h.c.EnvironmentStatus()
	require.False(t, ok)
	require.Equal(t, "fake", reason)
}

func TestManualPolicyWaitsForSyncAndSpeedOverride(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	require.NoError(t, h.c.SetPolicy(h.ctx, actor, "a", Policy{Mode: ModeManual, Speed: speedTest}))
	require.Error(t, h.c.SetPolicy(h.ctx, actor, "a", Policy{Mode: "sometimes", Speed: speedTest}))
	require.Error(t, h.c.SetPolicy(h.ctx, actor, "a", Policy{Mode: ModeManual, Speed: "warp"}))
	require.ErrorIs(t, h.c.SetPolicy(h.ctx, actor, "zzz", Policy{Mode: ModeManual, Speed: speedTest}), ErrNotFound)

	h.release(imgA, d2)
	r := h.active("a")
	require.Equal(t, WaitingForSync, r.State)
	require.Equal(t, "Manual policy. Build 2222222 is waiting for a sync.", r.Reason)
	require.Equal(t, "Manual policy. Build 2222222 is waiting for a sync.", h.c.Fleet().Groups[0].Reason)
	require.ErrorIs(t, h.c.Pause(h.ctx, actor, r.ID), ErrState)

	h.ticks(3, 0)
	require.Empty(t, h.world.callsFor("update"))

	_, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a"), Speed: "warp"})
	require.Error(t, err)
	_, err = h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=nope")})
	require.ErrorIs(t, err, ErrNotFound)

	ids, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a"), Speed: speedAll, Force: true})
	require.NoError(t, err)
	require.Equal(t, []string{r.ID}, ids)

	r = h.active("a")
	require.Equal(t, Running, r.State)
	require.Equal(t, speedAll, r.Speed)
	require.True(t, r.Force)
	require.True(t, r.Human)

	// all: one batch, no waves, forced: ready and soak skipped, budget still applies.
	h.tick()
	r = h.active("a")
	require.Len(t, r.Batches, 1)
	require.Len(t, r.Batches[0].Targets, 4, "two weight-0 nodes plus two weighted within 50%")
	h.tick()
	require.Equal(t, PhaseReady, h.phases(h.active("a"))[tA1], "forced: ready as soon as the digest lands")
	require.Equal(t, "ready (forced)", h.active("a").Targets[0].Reason)
	require.Empty(t, h.world.callsFor("ready"))
	h.tick()
	require.True(t, h.active("a").Batches[0].Passed)
	require.Empty(t, h.world.callsFor("soak"))

	require.Equal(t, Complete, h.drive(r.ID, 30, 0).State)

	// Sync on an already synced group starts nothing.
	ids, err = h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a")})
	require.NoError(t, err)
	require.Empty(t, ids)
}

func TestSyncCreatesRolloutWhenAbortedAndClearsAbort(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	r := h.active("a")
	h.tick()

	require.ErrorIs(t, h.c.Abort(h.ctx, actor, "nope"), ErrNotFound)
	require.NoError(t, h.c.Abort(h.ctx, actor, r.ID))
	require.ErrorIs(t, h.c.Abort(h.ctx, actor, r.ID), ErrState)

	got := h.rollout(r.ID)
	require.Equal(t, Aborted, got.State)
	require.Equal(t, PhaseSkipped, h.phases(got)[tA1])
	require.Equal(t, OutOfSync, h.view(tA3).Sync)

	// No new rollout for the same digests.
	h.ticks(3, 20*time.Second)
	require.Len(t, h.c.Rollouts(), 1)
	require.Contains(t, h.c.Fleet().Groups[0].Reason, "not on 2222222")

	// A sync creates one.
	ids, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a")})
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.True(t, h.rollout(ids[0]).Human)

	// A new digest would also have.
	h2 := newHarness(t, testConfig, testTargets)
	h2.prime()
	h2.release(imgA, d2)
	require.NoError(t, h2.c.Abort(h2.ctx, actor, h2.active("a").ID))
	h2.release(imgA, d3)
	require.Equal(t, "3333333", h2.active("a").Digest)
}

func TestPauseAndPromote(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	r := h.active("a")

	require.ErrorIs(t, h.c.Pause(h.ctx, actor, "nope"), ErrNotFound)
	require.ErrorIs(t, h.c.Promote(h.ctx, actor, "nope"), ErrNotFound)
	require.ErrorIs(t, h.c.Promote(h.ctx, actor, r.ID), ErrState)

	// Pause mid-batch: the batch finishes its soak, then the rollout pauses.
	require.NoError(t, h.c.Pause(h.ctx, actor, r.ID))
	require.True(t, h.active("a").PausePending)
	h.ticks(4, 0)
	require.Equal(t, Soaking, h.active("a").State)
	h.ticks(4, 20*time.Second)
	require.Equal(t, Paused, h.active("a").State)
	require.Contains(t, h.active("a").Reason, "Paused after batch 1")

	h.ticks(2, 0)
	require.Len(t, h.active("a").Batches, 1, "nothing starts while paused")
	require.Equal(t, Paused, h.active("a").State)

	// Pausing again while paused is an error; promote continues.
	require.ErrorIs(t, h.c.Pause(h.ctx, actor, r.ID), ErrState)
	require.NoError(t, h.c.Promote(h.ctx, actor, r.ID))
	require.Equal(t, Running, h.active("a").State)
	h.tick()
	require.Len(t, h.active("a").Batches, 2)

	// Pause between batches takes effect immediately, promote while pending clears it.
	h2 := newHarness(t, testConfig, testTargets)
	h2.prime()
	h2.release(imgA, d2)
	r2 := h2.active("a")
	require.NoError(t, h2.c.Pause(h2.ctx, actor, r2.ID))
	require.NoError(t, h2.c.Promote(h2.ctx, actor, r2.ID))
	require.False(t, h2.active("a").PausePending)
	require.Equal(t, Running, h2.active("a").State)

	// Pause once the batch is done but before the next starts.
	h2.ticks(4, 0)
	h2.ticks(4, 20*time.Second)
	between := h2.active("a")
	require.Equal(t, Running, between.State)
	require.Nil(t, between.CurrentBatch())
	require.NoError(t, h2.c.Pause(h2.ctx, actor, r2.ID))
	require.Equal(t, Paused, h2.active("a").State)
	require.Contains(t, h2.active("a").Reason, "Paused by sam")
}

func TestCarefulPresetPausesAfterFirstTarget(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	require.NoError(t, h.c.SetPolicy(h.ctx, actor, "a", Policy{Mode: ModeAutomated, Speed: "careful"}))
	h.release(imgA, d2)

	r := h.active("a")
	require.Equal(t, []string{tA1}, r.Batches[0].Targets)
	h.ticks(4, 0)
	require.Equal(t, Paused, h.active("a").State)
	require.Equal(t, "Paused after batch 1. Run promote to continue.", h.active("a").Reason)
	require.Equal(t, "Paused after batch 1. Run promote to continue.", h.c.Fleet().Groups[0].Reason)

	require.NoError(t, h.c.Promote(h.ctx, actor, r.ID))
	h.tick()
	require.Len(t, h.active("a").Batches[1].Targets, 1, "50% of 6 is 3 but wave 0 has one left")

	require.Equal(t, Complete, h.drive(r.ID, 40, 0).State)
}

func TestSuspendResumeAndExpiry(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	_, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: other, Selector: mustSel("node=a-1")})
	require.Error(t, err, "reason required")
	_, err = h.c.Suspend(h.ctx, SuspendRequest{Actor: other, Selector: mustSel("node=zzz"), Reason: "x"})
	require.ErrorIs(t, err, ErrNotFound)

	s, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: other, Selector: mustSel("node=a-1"), Reason: "debugging peer churn", Expires: time.Hour})
	require.NoError(t, err)
	require.Equal(t, h.clock.Now().Add(time.Hour), s.ExpiresAt)
	require.Len(t, h.c.Suspensions(), 1)

	v := h.view(tA1)
	require.Equal(t, Suspended, v.Health)
	require.Contains(t, v.Reason, "suspended by nflaig until")
	require.Contains(t, v.Reason, "debugging peer churn")
	require.Equal(t, Suspended, h.c.Fleet().Groups[0].Health)

	// Suspended targets are not part of a rollout.
	h.release(imgA, d2)
	r := h.active("a")
	require.Equal(t, 5, r.Total)
	require.Equal(t, []string{tA2}, r.Batches[0].Targets)

	// A suspension arriving mid-rollout skips the target when its turn comes,
	// and it leaves the soak comparison group.
	_, err = h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("id=a-6/cl"), Reason: mine})
	require.NoError(t, err)

	final := h.drive(r.ID, 60, 20*time.Second)
	require.Equal(t, Complete, final.State)
	require.Equal(t, PhaseSkipped, h.phases(final)[tA6])
	require.Contains(t, final.Targets[4].Reason, "suspended by sam")

	// Resume lifts by exact selector; expiry lifts on its own.
	_, err = h.c.Resume(h.ctx, actor, mustSel("node=zzz"))
	require.ErrorIs(t, err, ErrNotFound)
	n, err := h.c.Resume(h.ctx, actor, mustSel("id=a-6/cl"))
	require.NoError(t, err)
	require.Equal(t, 1, n)

	h.clock.Advance(2 * time.Hour)
	h.tick()
	require.Empty(t, h.c.Suspensions())
	require.Contains(t, h.notes.actions(), "suspend.expired")

	// The lifted target is out of sync, so a new rollout picks it up at once.
	require.Equal(t, OutOfSync, h.view(tA1).Sync)
	require.Equal(t, Progressing, h.view(tA1).Health)
	require.Equal(t, []string{tA1}, h.active("a").Batches[0].Targets)

	// Default expiry is 24h.
	s, err = h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("client=b"), Reason: "fork"})
	require.NoError(t, err)
	require.Equal(t, 24*time.Hour, s.ExpiresAt.Sub(s.CreatedAt))
}

func TestUnknownTargetsAreLeftAlone(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)

	// Nothing inspected yet: everything is Unknown and no rollout opens.
	h.world.set(func(w *world) { w.registry[imgA] = d2 })
	h.tick()
	require.Empty(t, h.c.Rollouts())
	require.Equal(t, Unknown, h.view(tA1).Sync)
	require.Equal(t, "not reachable", h.view(tA1).Reason)
	require.Equal(t, Unknown, h.c.Fleet().Groups[0].Sync)
	require.Contains(t, h.c.Fleet().Groups[0].Reason, "unreachable or unresolved")

	// One unreachable node is skipped by the rollout; the rest proceed.
	h.world.set(func(w *world) { w.inspectFail[tA2] = true })
	h.c.InspectAll(h.ctx)
	h.c.InspectAll(h.ctx)
	require.Equal(t, "not reachable: connection refused", h.view(tA2).Reason)
	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()

	r := h.active("a")
	require.Equal(t, 5, r.Total)
	require.Equal(t, []string{tA1}, r.Batches[0].Targets)

	// A target that becomes unreachable mid-rollout is skipped at its turn.
	h.world.set(func(w *world) { w.inspectFail[tA6] = true })
	h.c.InspectAll(h.ctx)
	h.c.InspectAll(h.ctx)

	final := h.drive(r.ID, 60, 20*time.Second)
	require.Equal(t, Complete, final.State)
	require.Equal(t, "unreachable", final.Targets[4].Reason)

	// Forget drops state for removed targets.
	h.c.Forget(h.ctx, []string{tA2})
	require.Equal(t, Unknown, h.view(tA2).Sync)
	require.Equal(t, "not reachable", h.view(tA2).Reason)
}

func TestSetLiveDirectlyAndAlreadyOnBuild(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	// A target that reaches the digest on its own before its batch is passed
	// without an update.
	h.world.set(func(w *world) { w.registry[imgA] = d2 })
	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()
	h.c.SetLive(h.ctx, tA6, d2, true, "", h.clock.Now())

	r := h.active("a")

	final := h.drive(r.ID, 60, 20*time.Second)
	require.Equal(t, Complete, final.State)
	require.Equal(t, "already on the new build", final.Targets[5].Reason)
	require.Len(t, final.Batches, 3)

	// Failures accumulate and reset.
	h.c.SetLive(h.ctx, tA1, "", false, "timeout", h.clock.Now())
	require.Equal(t, Synced, h.view(tA1).Sync, "one failure is not yet unknown")
	h.c.SetLive(h.ctx, tA1, "", false, "timeout", h.clock.Now())
	require.Equal(t, Unknown, h.view(tA1).Sync)
	h.c.SetLive(h.ctx, tA1, d2, true, "", h.clock.Now())
	require.Equal(t, Synced, h.view(tA1).Sync)
}

func TestTargetRemovedMidRollout(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	r := h.active("a")
	h.tick()

	// Drop a-2 (in the open batch) and a-6 (not yet reached) from the files.
	smaller := replaceLine(testTargets, "- {id: a-2/cl, node: a-2, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: \"0\"}, probes: {soak: soak-a}}", "")
	smaller = replaceLine(smaller, "- {id: a-6/cl, node: a-6, weight: 100, image: org/a:t, labels: {client: a, owner: a, role: cl, wave: \"2\"}, probes: {soak: soak-a}}", "")

	rules := h.set.Rules()

	newSet, err := parseTargets(smaller, &rules)
	require.NoError(t, err)

	h.set = newSet

	final := h.drive(r.ID, 60, 20*time.Second)
	require.Equal(t, Complete, final.State)
	require.Equal(t, PhaseSkipped, h.phases(final)[tA2])
	require.Equal(t, "removed from the targets file", final.Targets[1].Reason)
	require.Equal(t, PhaseSkipped, h.phases(final)[tA6])
}

func TestRegistryErrorsAndRefresh(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	h.world.set(func(w *world) { w.registryErr[imgA] = errFake })
	h.c.Refresh(h.ctx, actor)
	h.tick()

	f := h.c.Fleet()
	require.Equal(t, "fake", f.Resolve[imgA])
	require.Contains(t, h.notes.actions(), "registry.error")
	require.Equal(t, Synced, h.view(tA1).Sync, "last known digest is kept")

	// The same error is reported once, not every poll.
	n := len(h.notes.actions())
	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()
	require.Len(t, h.notes.actions(), n)

	// An image never resolved shows as unresolved.
	h2 := newHarness(t, testConfig, testTargets)
	h2.world.set(func(w *world) { w.registryErr[imgS] = errFake })
	h2.c.InspectAll(h2.ctx)
	h2.tick()
	require.Equal(t, Unknown, h2.view("a-3/side").Sync)
	require.Equal(t, "registry: fake", h2.view("a-3/side").Reason)

	h3 := newHarness(t, testConfig, testTargets)
	h3.world.set(func(w *world) { delete(w.registry, imgS) })
	h3.c.InspectAll(h3.ctx)
	h3.tick()
	require.Equal(t, "registry: unknown image", h3.view("a-3/side").Reason)
}

func TestPinsHoldAnImage(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	require.NoError(t, h.c.SetPolicy(h.ctx, actor, "a", Policy{Mode: ModeAutomated, Speed: speedTest, Pins: map[string]string{imgA: d1}}))
	h.release(imgA, d2)
	require.Empty(t, h.c.Rollouts(), "pinned groups do not move")
	require.Equal(t, Synced, h.view(tA1).Sync)

	// Unpinning supersedes nothing but opens the rollout.
	require.NoError(t, h.c.SetPolicy(h.ctx, actor, "a", Policy{Mode: ModeAutomated, Speed: speedTest}))
	h.tick()
	require.Equal(t, "2222222", h.active("a").Digest)

	// Pinning during a rollout supersedes it.
	require.NoError(t, h.c.SetPolicy(h.ctx, actor, "a", Policy{Mode: ModeAutomated, Speed: speedTest, Pins: map[string]string{imgA: d2}}))
	h.tick()
	require.Equal(t, "2222222", h.active("a").Digest, "pin equals desired: nothing changes")
	require.NoError(t, h.c.SetPolicy(h.ctx, actor, "a", Policy{Mode: ModeAutomated, Speed: speedTest, Pins: map[string]string{imgA: d3}}))
	h.tick()
	require.Equal(t, "3333333", h.active("a").Digest)
}

func TestNoSoakProgramPassesImmediately(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgS, d2)

	r := h.active("side")
	h.ticks(4, 0)
	require.Equal(t, Complete, h.rollout(r.ID).State)
	require.Empty(t, h.world.callsFor("soak"))
	require.Equal(t, "no soak", h.rollout(r.ID).Targets[0].Reason)
}

func TestSoakNumbersAndPerProgramGrouping(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.soakStdout = "updated=97 remaining=98 unit=%" })
	h.release(imgA, d2)
	h.ticks(5, 0)

	r := h.active("a")
	require.Len(t, r.Soak.Checks, 1)
	require.Equal(t, "97", r.Soak.Checks[0].Updated)
	require.Equal(t, "98", r.Soak.Checks[0].Remaining)
	require.Equal(t, "%", r.Soak.Checks[0].Unit)
	require.Equal(t, soakA, r.Soak.Checks[0].Program)
	require.Contains(t, r.Reason, "1 of 2 checks passed")

	hr := h.view(tA1).Hooks
	require.Equal(t, "fine", hr["soak"].Result.Reason)
	require.Equal(t, "update started", hr["update"].Result.Reason)

	u, rem, unit := parseSoakNumbers("only one line")
	require.Empty(t, u+rem+unit)
	u, rem, unit = parseSoakNumbers("ok\nupdated=1 junk remaining=2")
	require.Equal(t, "1", u)
	require.Equal(t, "2", rem)
	require.Empty(t, unit)
}

func TestSoakHookErrorCountsAsFailure(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.runErr[soakA] = errFake })
	h.release(imgA, d2)
	h.ticks(5, 0)
	require.Equal(t, 1, h.active("a").Soak.Failures)
	h.clock.Advance(20 * time.Second)
	h.tick()
	require.Equal(t, Halted, h.active("a").State)
	require.Contains(t, h.active("a").Reason, "fake")
}

func TestSupersededMidBatchSkipsInFlightTargets(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	first := h.active("a")
	h.tick()
	require.Equal(t, PhaseUpdating, h.phases(h.active("a"))[tA1])

	h.release(imgA, d3)
	old := h.rollout(first.ID)
	require.Equal(t, Superseded, old.State)
	require.Equal(t, PhaseSkipped, h.phases(old)[tA1])
	require.Equal(t, "Superseded", old.Targets[0].Reason)
	require.Equal(t, "3333333", h.active("a").Digest)
}
