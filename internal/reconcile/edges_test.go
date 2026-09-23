package reconcile

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/hooks"
)

func TestSyncWithSpeedCreatesRolloutAtThatSpeed(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	require.NoError(t, h.c.Abort(h.ctx, actor, h.active("a").ID))

	ids, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a"), Strategy: speedAll})
	require.NoError(t, err)
	require.Equal(t, speedAll, h.rollout(ids[0]).Strategy)
}

func TestNoWavePolicyFlattensWavesAtCreation(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	require.NoError(t, h.c.SetPolicy(h.ctx, actor, "a", Policy{Mode: ModeAutomated, Strategy: speedAll}))
	h.release(imgA, d2)

	r := h.active("a")
	for _, rt := range r.Targets {
		require.Equal(t, 0, rt.Wave)
	}

	require.Len(t, r.Batches[0].Targets, 4)
	require.Contains(t, r.Reason, "batch 1 of about 2")
}

func TestTwoTargetsOnOneNodeSortByID(t *testing.T) {
	doc := testTargets + "- {id: a-3/vc, node: a-3, weight: 100, image: org/a:t, labels: {client: a, owner: a, role: vc, wave: \"1\"}, hooks: {soak: soak-a}}\n"
	h := newHarness(t, testConfig, doc)
	h.prime()
	h.release(imgA, d2)

	r := h.active("a")
	require.Equal(t, tA3, r.Targets[2].ID)
	require.Equal(t, "a-3/vc", r.Targets[3].ID)

	// Batch sizes count nodes: two nodes take a-3's two targets and a-4's,
	// and a-3 costs its weight once.
	h.ticks(1, 0)
	h.ticks(4, 0)
	h.ticks(4, 20*time.Second)
	h.tick()
	r = h.active("a")
	require.Equal(t, []string{tA3, "a-3/vc", tA4}, r.Batches[1].Targets)
	require.Equal(t, "40.0% of 50%", r.Unavailable)
	require.Contains(t, r.Reason, "updating 3 targets on 2 nodes")
}

func TestMixedSoakProgramsInOneBatch(t *testing.T) {
	doc := replaceLine(testTargets,
		"- {id: a-2/cl, node: a-2, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: \"0\"}, hooks: {soak: soak-a}}",
		"- {id: a-2/cl, node: a-2, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: \"0\"}}")
	h := newHarness(t, testConfig, doc)
	h.prime()
	h.release(imgA, d2)
	h.ticks(5, 0)

	r := h.active("a")
	require.Equal(t, Soaking, r.State)
	require.Len(t, r.Soak.Checks, 1)
	require.Equal(t, soakA, r.Soak.Checks[0].Program)

	_, ran := h.view(tA1).Hooks["soak"]
	require.True(t, ran)
	_, ran = h.view(tA2).Hooks["soak"]
	require.False(t, ran)
}

func TestCarefulWithNothingLeftDoesNotPause(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	require.NoError(t, h.c.SetPolicy(h.ctx, actor, "side", Policy{Mode: ModeAutomated, Strategy: "careful"}))
	h.release(imgS, d2)

	r := h.active("side")
	require.Equal(t, Complete, h.drive(r.ID, 10, 0).State)
}

func TestHaltSkipsRemovedTargetInBatch(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.soakFail[soakA] = true })
	h.release(imgA, d2)
	r := h.active("a")

	rules := h.set.Rules()
	smaller, err := parseTargets(replaceLine(testTargets,
		"- {id: a-2/cl, node: a-2, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: \"0\"}, hooks: {soak: soak-a}}", ""), &rules)
	require.NoError(t, err)

	h.set = smaller

	h.ticks(5, 0)
	h.clock.Advance(20 * time.Second)
	h.tick()

	got := h.rollout(r.ID)
	require.Equal(t, Halted, got.State)
	require.Equal(t, PhaseSkipped, h.phases(got)[tA2])
	require.Equal(t, PhaseFailed, h.phases(got)[tA1])
}

func TestFirstUpdateFailureHaltsBeforeSecondResultLands(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.updateFail[tA1] = refused })
	h.release(imgA, d2)
	h.tick()

	r := h.active("a")
	require.Equal(t, Halted, r.State)
	require.Equal(t, PhaseFailed, h.phases(r)[tA2], "the second result arrives after the halt and changes nothing")
}

func TestApplyIgnoresStaleResults(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	r := h.active("a")

	h.c.mu.Lock()
	h.c.applyResults(h.ctx, h.clock.Now(), []outcome{
		{job: job{rollout: "nope", target: tA1, hook: config.HookUpdate}, res: hooks.Result{OK: true}},
		{job: job{rollout: r.ID, target: "b-1/el", hook: config.HookUpdate}, res: hooks.Result{OK: true}},
	})
	h.c.applySoak(h.ctx, h.clock.Now(), h.c.rollouts[r.ID], []outcome{{res: hooks.Result{OK: true}}})
	h.c.mu.Unlock()

	require.Equal(t, Running, h.active("a").State)
	require.Equal(t, 0, h.active("a").Soak.Streak)
	require.Equal(t, PhaseUpdating, h.phases(h.active("a"))[tA1], "the real update result stands; the stale ones changed nothing")
}

func TestSoakPassDecidedAtApplyTime(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	h.ticks(5, 0)
	require.Equal(t, 1, h.active("a").Soak.Streak)

	// A failure in the middle resets the streak so the pass lands on the
	// check made exactly as the duration ends.
	h.world.set(func(w *world) { w.soakFail[soakA] = true })
	h.clock.Advance(20 * time.Second)
	h.tick()
	require.Equal(t, 0, h.active("a").Soak.Streak)
	h.world.set(func(w *world) { w.soakFail[soakA] = false })
	h.clock.Advance(20 * time.Second)
	h.tick()
	require.Equal(t, 1, h.active("a").Soak.Streak)
	h.clock.Advance(20 * time.Second)
	h.tick()
	require.Equal(t, Running, h.active("a").State)
	require.True(t, h.active("a").Batches[0].Passed)
}

func TestSoakRunsPastDurationUntilACheckPasses(t *testing.T) {
	cfg := replaceLine(testConfig, "failureLimit: 1}}", "failureLimit: 3}}")
	h := newHarness(t, cfg, testTargets)
	h.prime()
	h.release(imgA, d2)
	h.ticks(5, 0)

	// Three failures at 20, 40 and 60 seconds use up the duration.
	h.world.set(func(w *world) { w.soakFail[soakA] = true })

	for i := 0; i < 3; i++ {
		h.clock.Advance(20 * time.Second)
		h.tick()
	}

	r := h.active("a")
	require.Equal(t, 3, r.Soak.Failures)
	require.Equal(t, Soaking, r.State)
	require.Contains(t, r.Reason, "0s left; 3 checks failed of 3 allowed")

	// The soak keeps checking past the duration until a check passes.
	h.world.set(func(w *world) { w.soakFail[soakA] = false })
	h.clock.Advance(20 * time.Second)
	h.tick()

	r = h.active("a")
	require.True(t, r.Batches[0].Passed)
	require.Equal(t, "soak passed: 5 checks, 3 failed", r.Targets[0].Reason)
}

func TestRestoreBringsBackQuarantineAndStrategyFallback(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.soakFail[soakA] = true })
	h.release(imgA, d2)
	h.ticks(5, 0)
	h.clock.Advance(20 * time.Second)
	h.tick()
	require.Equal(t, Halted, h.active("a").State)

	restarted := h.newController()
	require.Equal(t, Degraded, mustView(t, restarted, tA1).Health)
	require.Equal(t, Halted, restarted.Rollouts()[0].State)

	// A stored strategy that has since been removed falls back to the default.
	require.Equal(t, h.cfg.Strategy, restarted.strategy("gone"))
}

func TestRunWithCancelledContextAndTickerPaths(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)

	ctx, cancel := context.WithCancel(h.ctx)
	cancel()
	require.ErrorIs(t, h.c.Run(ctx, time.Hour), context.Canceled)

	// Resolve due under a cancelled context reports it.
	h.clock.Advance(h.cfg.Registry.Poll)
	require.ErrorIs(t, h.c.Tick(ctx), context.Canceled)

	// Environment check due under a cancelled context reports it.
	cfg := replaceLine(testConfig, "  defaults: {soak: \"\"}", "  defaults: {soak: \"\"}\n  environment: {program: env, interval: 30s}")
	cfg = replaceLine(cfg, "inspect: {interval: 30s, concurrency: 4, failureThreshold: 2}", "inspect: {interval: 5ms, concurrency: 4, failureThreshold: 2}")
	h2 := newHarness(t, cfg, testTargets)
	h2.tick()
	h2.clock.Advance(30 * time.Second)
	require.ErrorIs(t, h2.c.Tick(ctx), context.Canceled)

	// The inspector and the run loop both wake on their tickers.
	ctx2, cancel2 := context.WithCancel(h2.ctx)
	done := make(chan error, 2)

	go func() { done <- h2.c.RunInspector(ctx2) }()
	go func() { done <- h2.c.Run(ctx2, 5*time.Millisecond) }()

	require.Eventually(t, func() bool { return len(h2.world.callsFor("inspect")) >= 27 }, 3*time.Second, 5*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	cancel2()
	<-done
	<-done
}

func TestRemovingEveryTargetOfAnImageMidRollout(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgB, d2)
	r := h.active("b")

	rules := h.set.Rules()
	doc := replaceLine(testTargets, "- {id: a-3/el, node: a-3, weight: 100, image: org/b:t, labels: {client: b, owner: b, role: el, wave: \"1\"}, hooks: {soak: soak-b}}", "")
	doc = replaceLine(doc, "- {id: b-1/el, node: b-1, weight: 100, image: org/b:t, labels: {client: b, owner: b, role: el, wave: \"1\"}, hooks: {soak: soak-b}}", "")
	smaller, err := parseTargets(doc, &rules)
	require.NoError(t, err)

	h.set = smaller

	final := h.drive(r.ID, 10, 0)
	require.Equal(t, Complete, final.State)
	require.Equal(t, PhaseSkipped, h.phases(final)["b-1/el"])
}

func TestGroupReasonShowsQuarantineWithoutRollout(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.soakFail[soakA] = true })
	h.release(imgA, d2)
	h.ticks(5, 0)
	h.clock.Advance(20 * time.Second)
	h.tick()

	r := h.active("a")
	require.NoError(t, h.c.Abort(h.ctx, actor, r.ID))
	require.Contains(t, h.c.Fleet().Groups[0].Reason, "soak soak-a failed")
	require.Equal(t, Degraded, h.c.Fleet().Groups[0].Health)
}

func TestSnapshotSortsRolloutsAndSuspensions(t *testing.T) {
	m := NewMemoryStore()
	ctx := context.Background()
	require.NoError(t, m.SaveRollout(ctx, &Rollout{ID: "b"}))
	require.NoError(t, m.SaveRollout(ctx, &Rollout{ID: "a"}))
	require.NoError(t, m.SaveSuspension(ctx, &Suspension{ID: "2"}))
	require.NoError(t, m.SaveSuspension(ctx, &Suspension{ID: "1"}))

	snap, err := m.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, "a", snap.Rollouts[0].ID)
	require.Equal(t, "1", snap.Suspensions[0].ID)

	h := newHarness(t, testConfig, testTargets)
	h.prime()
	_, err = h.c.Suspend(h.ctx, SuspendRequest{Actor: "a", Selector: mustSel("node=b-1"), Reason: "x"})
	require.NoError(t, err)
	_, err = h.c.Suspend(h.ctx, SuspendRequest{Actor: "a", Selector: mustSel("node=a-1"), Reason: "y"})
	require.NoError(t, err)
	require.Len(t, h.c.Suspensions(), 2)
	require.Equal(t, "id-1", h.c.Suspensions()[0].ID)
}
