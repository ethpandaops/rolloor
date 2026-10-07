package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/targets"
)

func TestReadinessThresholdsSurviveRestart(t *testing.T) {
	h := newHarness(t, testConfig+"\nreadinessProbe: {period: 2s, failureThreshold: 3, successThreshold: 2}\n", testTargets)
	h.c.InspectAll(h.ctx)
	require.Equal(t, HealthUnknown, h.view(tA1).Health)
	h.c.ProbeAll(h.ctx)
	require.Equal(t, Degraded, h.view(tA1).Health)
	h.clock.Advance(time.Second)
	h.c.ProbeAll(h.ctx)
	require.Equal(t, Healthy, h.view(tA1).Health)
	since := h.view(tA1).Readiness.Since

	h.world.set(func(w *world) { w.runErr["ready"] = errFake })

	for range 2 {
		h.clock.Advance(time.Second)
		h.c.ProbeAll(h.ctx)
		require.Equal(t, Healthy, h.view(tA1).Health)
	}

	h.c = h.newController()
	require.Equal(t, 2, h.view(tA1).Readiness.Failures)
	h.clock.Advance(time.Second)
	h.c.ProbeAll(h.ctx)
	failed := h.view(tA1)
	require.Equal(t, Degraded, failed.Health)
	require.True(t, failed.Readiness.Since.After(since))
	require.Contains(t, failed.Readiness.Reason, errFake.Error())
	require.Equal(t, failed.Readiness.ProbedAt, h.clock.Now())

	h.world.set(func(w *world) { delete(w.runErr, "ready") })
	h.clock.Advance(time.Second)
	h.c.ProbeAll(h.ctx)
	require.Equal(t, Degraded, h.view(tA1).Health)
	h.world.set(func(w *world) { w.notReady[tA1] = true })
	h.c.ProbeAll(h.ctx)
	h.world.set(func(w *world) { delete(w.notReady, tA1) })
	h.c.ProbeAll(h.ctx)
	require.Equal(t, Degraded, h.view(tA1).Health)
	h.c.ProbeAll(h.ctx)
	require.Equal(t, Healthy, h.view(tA1).Health)
	require.Equal(t, 0, h.view(tA1).Readiness.Failures)
}

func TestProbeMustBeginAfterDigestObservation(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.updateStuck[tA1] = true })
	h.release(imgA, d2)
	id := h.active("a").ID
	entered, release := h.world.gate("ready:" + tA1)

	var wg sync.WaitGroup
	wg.Go(func() { h.c.ProbeAll(h.ctx) })

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("probe did not start")
	}

	h.clock.Advance(time.Second)
	h.world.set(func(w *world) { w.running[tA1] = d2 })
	h.c.InspectAll(h.ctx)
	release()
	wg.Wait()
	require.NoError(t, h.c.Tick(h.ctx))
	require.True(t, h.view(tA1).Readiness.Ready)
	require.Equal(t, PhaseUpdating, h.phases(h.rollout(id))[tA1])
	require.Equal(t, Progressing, h.view(tA1).Health)

	h.clock.Advance(time.Nanosecond)
	h.c.ProbeAll(h.ctx)
	require.NoError(t, h.c.Tick(h.ctx))
	require.Equal(t, PhaseReady, h.phases(h.rollout(id))[tA1])
	require.Equal(t, Healthy, h.view(tA1).Health)
}

func TestLateAndRemovedProbeResultsAreForgotten(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	before := h.view(tA1).Readiness
	h.c.mu.Lock()
	h.c.setReadinessLocked(h.ctx, &observation{id: tA1, started: before.ObservedAt.Add(-time.Second)}, false, boom)
	h.c.mu.Unlock()
	require.Equal(t, before, h.view(tA1).Readiness)

	entered, release := h.world.gate("ready:" + tA1)

	var wg sync.WaitGroup
	wg.Go(func() { h.c.ProbeAll(h.ctx) })

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("probe did not start")
	}

	full := h.current()
	rules := full.Rules()
	without, err := parseTargets(dropLines(testTargets, "id: a-1/cl,"), &rules)
	require.NoError(t, err)
	h.swap(without)
	h.c.Forget(h.ctx, []string{tA1})
	release()
	wg.Wait()

	snap, err := h.store.Load(h.ctx)
	require.NoError(t, err)
	require.NotContains(t, snap.Live, tA1)
	require.NotContains(t, snap.HookRuns, tA1)
	h.swap(full)
	h.c = h.newController()
	require.Equal(t, HealthUnknown, h.view(tA1).Health)
}

func TestProberLoopAssessesHealthWithoutRollouts(t *testing.T) {
	h := newHarness(t, testConfig+"\nreadinessProbe: {period: 5ms, failureThreshold: 1, successThreshold: 1}\n", testTargets)
	h.prime()
	h.world.set(func(w *world) { w.notReady[tA1] = true })
	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan error, 1)

	go func() { done <- h.c.RunProber(ctx) }()

	// The loop is paced on the fake clock, so each check makes another pass due.
	healthIs := func(want Health) func() bool {
		return func() bool {
			h.clock.Advance(earlyProbeSpacing)

			return h.view(tA1).Health == want
		}
	}

	require.Eventually(t, healthIs(Degraded), 3*time.Second, time.Millisecond)
	h.world.set(func(w *world) { delete(w.notReady, tA1) })
	require.Eventually(t, healthIs(Healthy), 3*time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Empty(t, h.c.Rollouts())

	before := h.view(tA1).Readiness
	h.c.ProbeAll(ctx)
	require.Equal(t, before, h.view(tA1).Readiness)
}

func TestReadinessSurvivesStoreRecoveryAndOfflineRemoval(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.store.Fail = errFake
	h.world.set(func(w *world) { w.notReady[tA2] = true })

	for range 2 {
		h.clock.Advance(time.Second)
		h.c.ProbeAll(h.ctx)
	}

	h.c.Forget(h.ctx, []string{tA1})
	h.store.Fail = nil
	require.NoError(t, h.c.Tick(h.ctx))
	h.c = h.newController()
	require.Equal(t, HealthUnknown, h.view(tA1).Health)
	require.Equal(t, 2, h.view(tA2).Readiness.Failures)
	h.c.ProbeAll(h.ctx)
	require.Equal(t, Degraded, h.view(tA2).Health)

	rules := h.current().Rules()
	without, err := parseTargets(dropLines(testTargets, "id: a-2/cl,"), &rules)
	require.NoError(t, err)
	h.swap(without)
	h.c = h.newController()
	snap, err := h.store.Load(h.ctx)
	require.NoError(t, err)
	require.NotContains(t, snap.Live, tA2)
	require.NotContains(t, snap.HookRuns, tA2)
}

func TestInspectionFailureDoesNotChangeDigestSync(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.inspectFail[tA1] = true })
	h.c.InspectAll(h.ctx)
	require.Equal(t, Synced, h.view(tA1).Sync)
	require.Equal(t, HealthUnknown, h.view(tA1).Health)
	h.c.InspectAll(h.ctx)
	require.Equal(t, Unknown, h.view(tA1).Sync)
	h.world.set(func(w *world) { delete(w.inspectFail, tA1) })
	h.c.InspectAll(h.ctx)
	require.Equal(t, Synced, h.view(tA1).Sync)
	require.Equal(t, Healthy, h.view(tA1).Health)
}

func TestRemovedForcedTargetRetainsSharedNodeUntilDeadline(t *testing.T) {
	const fleet = `
- {id: n1/a, node: n1, weight: 100, image: org/a:t, labels: {client: a, owner: a}}
- {id: n1/b, node: n1, weight: 100, image: org/b:t, labels: {client: b, owner: b}}
- {id: n2/c, node: n2, weight: 100, image: org/c:t, labels: {client: c, owner: c}}
`

	h := newHarness(t, budgetConfig, fleet)
	h.prime()
	h.release(imgA, d2)
	r := h.active("a")
	_, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a"), Force: true})
	require.NoError(t, err)
	h.c.InspectAll(h.ctx)

	for range 4 {
		require.NoError(t, h.c.Tick(h.ctx))
	}

	require.Equal(t, Complete, h.rollout(r.ID).State)
	require.InDelta(t, 100, h.c.unavailableWeight(h.current()), 0.001)

	rules := h.current().Rules()
	without, err := parseTargets(dropLines(fleet, "id: n1/a,"), &rules)
	require.NoError(t, err)
	h.swap(without)
	h.c.Forget(h.ctx, []string{tN1A})
	require.InDelta(t, 100, h.c.unavailableWeight(h.current()), 0.001)
	h.world.set(func(w *world) { w.registry["org/c:t"] = d2 })
	h.c.Refresh(h.ctx, actor)
	h.tick()
	pending := h.active("c")
	require.Equal(t, WaitingForBudget, pending.State)

	h.clock.Advance(h.rolloutTarget(r.ID, tN1A).HoldUntil.Sub(h.clock.Now()))
	h.tick()
	require.Equal(t, []string{"n2/c"}, h.rollout(pending.ID).Batches[0].Targets)
}

func TestFreshProbeRemainsValidAfterLaterInspection(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	id := h.active("a").ID
	firstSeen := h.clock.Now()
	h.clock.Advance(time.Second)
	h.c.InspectAll(h.ctx)
	h.clock.Advance(time.Nanosecond)
	h.c.ProbeAll(h.ctx)
	h.clock.Advance(time.Second)
	h.c.InspectAll(h.ctx)
	require.NoError(t, h.c.Tick(h.ctx))
	r := h.rollout(id)
	rt := r.target(tA1)
	require.Equal(t, PhaseReady, rt.Phase)
	require.Equal(t, firstSeen, rt.DigestSeenAt)
}

func TestRetryDoesNotReviveReleasedHoldForSuspendedTarget(t *testing.T) {
	h := newHarness(t, budgetConfig, `
- {id: n1/a, node: n1, weight: 100, image: org/a:t, labels: {client: a, owner: a}}
- {id: n2/b, node: n2, weight: 100, image: org/b:t, labels: {client: b, owner: b}}
`)
	h.prime()
	r := h.haltOnBadBuild("a", imgA, d2, tN1A)
	h.world.set(func(w *world) {
		delete(w.breakOnUpdate, tN1A)
		delete(w.notReady, tN1A)
		w.notReady["n2/b"] = true
	})
	h.clock.Advance(time.Second)
	h.c.InspectAll(h.ctx)
	h.clock.Advance(time.Nanosecond)

	for range h.cfg.ReadinessProbe.FailureThreshold {
		h.c.ProbeAll(h.ctx)
	}

	require.InDelta(t, 100, h.c.unavailableWeight(h.current()), 0.001)
	_, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("id=n1/a"), Reason: maintenanceReason, Expires: time.Hour})
	require.NoError(t, err)
	require.NoError(t, h.c.Retry(h.ctx, actor, r.ID, "recovered"))
	h.clock.Advance(time.Nanosecond)
	require.NoError(t, h.c.Tick(h.ctx))
	require.InDelta(t, 100, h.c.unavailableWeight(h.current()), 0.001)
	require.Equal(t, PhaseSkipped, h.phases(h.rollout(r.ID))[tN1A])
}

func TestHealthyReadinessWithUnresolvedDesiredBuild(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.world.set(func(w *world) { w.registryErr[imgA] = errFake })
	h.prime()
	group, _, found := h.c.Group("a")
	require.True(t, found)
	require.Equal(t, Unknown, group.Sync)
	require.Equal(t, Healthy, group.Health)
	require.Empty(t, group.Desired)
	require.Equal(t, 0, group.OnDesired)
	require.Equal(t, Unknown, h.c.Fleet().Sync)
	require.Equal(t, Healthy, h.c.Fleet().Health)
	require.Empty(t, h.c.Rollouts())
	require.Empty(t, h.world.callsFor("update"))
}

func TestProbeDuringInspectionCannotAcknowledgeNewBuild(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.updateStuck[tA1] = true })
	h.release(imgA, d2)
	id := h.active("a").ID
	entered, release := h.world.gate("inspect:" + tA1)

	var wg sync.WaitGroup
	wg.Go(func() { h.c.InspectAll(h.ctx) })

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("inspection did not start")
	}

	h.clock.Advance(time.Second)
	h.world.set(func(w *world) { w.running[tA1] = d2 })
	h.c.ProbeAll(h.ctx)
	h.clock.Advance(time.Second)
	release()
	wg.Wait()
	require.NoError(t, h.c.Tick(h.ctx))
	require.True(t, h.view(tA1).Readiness.Ready)
	require.Equal(t, PhaseUpdating, h.phases(h.rollout(id))[tA1])
	h.clock.Advance(time.Nanosecond)
	h.c.ProbeAll(h.ctx)
	require.NoError(t, h.c.Tick(h.ctx))
	require.Equal(t, PhaseReady, h.phases(h.rollout(id))[tA1])
}

func TestRetryUsesTargetsOwnDesiredDigest(t *testing.T) {
	h := newHarness(t, testConfig, `
- {id: n1/a, node: n1, weight: 100, image: org/a:t, labels: {client: a, owner: a}}
- {id: n1/b, node: n1, weight: 100, image: org/b:t, labels: {client: a, owner: a}}
`)
	h.prime()
	h.world.set(func(w *world) {
		w.registry[imgA], w.registry[imgB] = d2, d3
		w.breakOnUpdate["n1/b"] = true
	})
	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()
	r := h.active("a")

	for i := 0; i < 20 && h.rollout(r.ID).State != Halted; i++ {
		h.ticks(1, 20*time.Second)
	}

	require.Equal(t, Halted, h.rollout(r.ID).State)

	// n1/b is put on image a's digest by hand; the retry must still move it
	// to its own image's digest.
	h.revert("n1/b")
	h.world.set(func(w *world) { w.running["n1/b"] = d2 })
	h.c.InspectAll(h.ctx)
	require.NoError(t, h.c.Retry(h.ctx, actor, r.ID, "recovered"))
	require.Equal(t, Complete, h.drive(r.ID, 40, h.cfg.Strategy.Soak.Interval).State)
	h.world.mu.Lock()
	defer h.world.mu.Unlock()

	require.Equal(t, d3, h.world.running["n1/b"])
}

func TestBatchObservesDigestAndReadinessWithoutPeriodicLoops(t *testing.T) {
	cfg := replaceLine(testConfig, "interval: 30s", "interval: 1h")
	cfg += "\nreadinessProbe: {period: 1h, failureThreshold: 1, successThreshold: 1}\n"
	h := newHarness(t, cfg, retryFleet+"- {id: a-1/cl, node: a-1, weight: 0, image: org/b:t, labels: {client: b, owner: b}}\n")
	h.prime()
	h.world.set(func(w *world) {
		w.registry[imgA] = d2
		w.notReady[tA1] = true
	})
	h.clock.Advance(h.cfg.Registry.Poll)
	require.NoError(t, h.c.Tick(h.ctx))
	r := h.active("a")
	require.Equal(t, d2, h.view(tN1A).Live)
	require.Equal(t, 1, h.rolloutTarget(r.ID, tN1A).UpdateAttempts)
	require.Equal(t, PhaseUpdating, h.phases(r)[tN1A])
	// Readiness probed in the instant the digest was seen cannot acknowledge
	// it; the next batch probe waits for the spacing.
	h.clock.Advance(earlyProbeSpacing)
	require.NoError(t, h.c.Tick(h.ctx))
	require.Equal(t, Healthy, h.view(tN1A).Health)
	require.Equal(t, Healthy, h.view(tA1).Health)
	require.NoError(t, h.c.Tick(h.ctx))
	require.Equal(t, Soaking, h.rollout(r.ID).State)
	require.Equal(t, Healthy, h.view(tN1A).Health)
}

func TestBatchObservationsArePacedPerTargetAndHook(t *testing.T) {
	cfg := replaceLine(testConfig, "interval: 30s", "interval: 1h")
	cfg += "\nreadinessProbe: {period: 1h, failureThreshold: 1, successThreshold: 1}\n"
	h := newHarness(t, cfg, retryFleet)
	h.prime()
	h.world.set(func(w *world) {
		w.registry[imgA] = d2
		w.updateStuck[tN1A] = true
	})
	h.clock.Advance(h.cfg.Registry.Poll)

	counts := func() [2]int {
		return [2]int{len(h.world.callsFor("inspect:" + tN1A)), len(h.world.callsFor("ready:" + tN1A))}
	}
	step := func(d time.Duration, want [2]int, why string) {
		t.Helper()
		h.clock.Advance(d)
		require.NoError(t, h.c.Tick(h.ctx))
		require.Equal(t, want, counts(), why)
	}

	// Explicit observations run at once, and count toward the spacing.
	h.c.InspectAll(h.ctx)
	h.c.ProbeAll(h.ctx)
	require.Equal(t, [2]int{2, 2}, counts())

	step(0, [2]int{3, 3}, "the dispatch earns one immediate observation of each kind")
	require.Equal(t, 1, h.rolloutTarget(h.active("a").ID, tN1A).UpdateAttempts)
	step(0, [2]int{3, 3}, "a nudged tick does not spend it again")
	step(earlyProbeSpacing-time.Second, [2]int{3, 3}, "still within the spacing")
	step(time.Second, [2]int{4, 4}, "the spacing has passed")

	h.clock.Advance(earlyProbeSpacing / 2)
	h.c.ProbeAll(h.ctx)
	step(earlyProbeSpacing/2, [2]int{5, 5}, "the explicit probe holds back the batch probe")

	cancelled, cancel := context.WithCancel(h.ctx)
	cancel()
	require.ErrorIs(t, h.c.RunInspector(cancelled), context.Canceled)
	require.ErrorIs(t, h.c.RunProber(cancelled), context.Canceled)
	require.Equal(t, [2]int{5, 5}, counts(), "the background loops keep the spacing too")

	// The update lands. Readiness probed in the instant inspection saw it
	// cannot acknowledge it, so the target waits for the next paced probe.
	h.world.set(func(w *world) { w.running[tN1A] = d2 })
	step(earlyProbeSpacing, [2]int{6, 6}, "both kinds are due again")
	step(0, [2]int{6, 6}, "nothing is due")
	require.Equal(t, PhaseUpdating, h.phases(h.active("a"))[tN1A])
	step(earlyProbeSpacing, [2]int{7, 7}, "the next paced probe acknowledges readiness")
	step(0, [2]int{7, 7}, "a target past updating is not observed early")
	require.Equal(t, PhaseReady, h.phases(h.active("a"))[tN1A])
}

func TestHookInputSaysWhenTheRunningDigestWasFirstSeen(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	first := h.clock.Now()
	h.prime()

	// A program can tell a fresh restart from steady state: the input names
	// when the target was first seen on the digest it runs now.
	tg, _ := h.current().Get(tA1)

	h.c.mu.RLock()
	in := h.c.hookInput(&tg)
	h.c.mu.RUnlock()

	require.Equal(t, first, in.DigestSince)

	h.clock.Advance(time.Minute)
	h.release(imgA, d2)
	h.ticks(3, 20*time.Second)
	h.c.InspectAll(h.ctx)

	h.c.mu.RLock()
	in = h.c.hookInput(&tg)
	h.c.mu.RUnlock()

	require.Equal(t, d2, h.view(tA1).Live)
	require.True(t, in.DigestSince.After(first), "a new digest has a new since")
}

func TestReplacingTargetsForgetsWhatChangedOrLeft(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	kept := h.view(tA2)
	next := mustParse(t, h, dropLines(replaceLine(testTargets, b1Line, b1Moved), "id: a-1/cl,"))

	h.c.ReplaceTargets(h.ctx, func() (old, current *targets.Set) {
		old = h.current()
		h.swap(next)

		return old, next
	})

	require.Same(t, next, h.current())
	v := h.view(tB1)
	require.Empty(t, v.Live)
	require.Zero(t, v.Readiness)
	require.Empty(t, v.Hooks)
	require.Equal(t, kept.Readiness, h.view(tA2).Readiness)
	require.Equal(t, kept.Hooks, h.view(tA2).Hooks)
	require.InDelta(t, 100, h.c.unavailableWeight(h.current()), 0.001)

	snap, err := h.store.Load(h.ctx)
	require.NoError(t, err)
	require.NotContains(t, snap.Live, tA1)
	require.NotContains(t, snap.Live, tB1)
	require.Contains(t, snap.Live, tA2)

	h.c.InspectAll(h.ctx)
	h.c.ProbeAll(h.ctx)
	require.Zero(t, h.c.unavailableWeight(h.current()), "observing the changed target again counts it available")
}
