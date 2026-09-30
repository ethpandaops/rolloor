package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	require.Equal(t, errFake.Error(), failed.Readiness.Reason)
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
	h.c.InspectAll(h.ctx)
	require.NoError(t, h.c.Tick(h.ctx))
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
	h.c.setReadinessLocked(h.ctx, tA1, before.ObservedAt.Add(-time.Second), false, boom)
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

	require.Eventually(t, func() bool { return h.view(tA1).Health == Degraded }, 3*time.Second, time.Millisecond)
	h.world.set(func(w *world) { delete(w.notReady, tA1) })
	require.Eventually(t, func() bool { return h.view(tA1).Health == Healthy }, 3*time.Second, time.Millisecond)
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

func TestRemovedForcedTargetDoesNotReserveSharedNode(t *testing.T) {
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
	require.InDelta(t, 0, h.c.unavailableWeight(h.current()), 0.001)
	h.release("org/c:t", d2)
	require.Equal(t, []string{"n2/c"}, h.active("c").Batches[0].Targets)
}

func TestFreshProbeRemainsValidAfterLaterInspection(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	id := h.active("a").ID
	h.clock.Advance(time.Second)
	h.c.InspectAll(h.ctx)
	firstSeen := h.clock.Now()
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
	h.world.set(func(w *world) { w.updateFail[tN1A] = boom })
	h.release(imgA, d2)
	r := h.active("a")
	require.Equal(t, Halted, r.State)
	h.world.set(func(w *world) {
		w.running[tN1A] = d2
		w.notReady["n2/b"] = true
	})
	h.clock.Advance(time.Second)
	h.c.InspectAll(h.ctx)
	h.clock.Advance(time.Nanosecond)

	for range h.cfg.ReadinessProbe.FailureThreshold {
		h.c.ProbeAll(h.ctx)
	}

	require.InDelta(t, 100, h.c.unavailableWeight(h.current()), 0.001)
	_, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("id=n1/a"), Reason: "maintenance", Expires: time.Hour})
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
		w.updateFail["n1/b"] = boom
	})
	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()
	r := h.active("a")
	require.Equal(t, Halted, r.State)
	h.world.set(func(w *world) {
		delete(w.updateFail, "n1/b")
		w.running["n1/b"] = d2
	})
	h.c.InspectAll(h.ctx)
	require.NoError(t, h.c.Retry(h.ctx, actor, r.ID, "recovered"))
	require.Equal(t, Complete, h.drive(r.ID, 40, h.cfg.Strategy.Soak.Interval).State)
	h.world.mu.Lock()
	defer h.world.mu.Unlock()

	require.Equal(t, d3, h.world.running["n1/b"])
}
