package reconcile

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/hooks"
)

const retryFleet = `
- {id: n1/a, node: n1, weight: 100, image: org/a:t, labels: {client: a, owner: a}, hooks: {soak: soak-a}}
`

func TestAObservedBuildLostBeforeReadinessMustBeSeenAgain(t *testing.T) {
	h := newHarness(t, testConfig, retryFleet)
	h.prime()
	h.world.set(func(w *world) { w.breakOnUpdate[tN1A] = true })
	h.release(imgA, d2)
	id := h.active("a").ID
	h.tick()

	first := h.rolloutTarget(id, tN1A)
	require.True(t, first.Updated)
	require.Equal(t, 1, h.rollout(id).OnNewBuild)

	h.world.set(func(w *world) {
		w.running[tN1A] = d1
		delete(w.breakOnUpdate, tN1A)
		delete(w.notReady, tN1A)
	})
	h.ticks(2, 0)

	lost := h.rolloutTarget(id, tN1A)
	require.Equal(t, PhaseUpdating, lost.Phase, "readiness on the old build cannot pass it")
	require.False(t, lost.Updated)
	require.True(t, lost.DigestSeenAt.IsZero())
	require.Equal(t, 0, h.rollout(id).OnNewBuild)

	h.world.set(func(w *world) { w.running[tN1A] = d2 })
	h.tick()

	again := h.rolloutTarget(id, tN1A)
	require.Equal(t, PhaseReady, again.Phase)
	require.True(t, again.DigestSeenAt.After(first.DigestSeenAt))
	require.Equal(t, Complete, h.drive(id, 20, 20*time.Second).State)
	require.Len(t, h.world.callsFor("update:"+tN1A), 1)
}

func TestUpdateProgressDeadlineSurvivesRestartAndStopsRetries(t *testing.T) {
	cfg := replaceLine(testConfig, "retry: {limit: 1}", "retry: {limit: 5}")
	cfg = replaceLine(cfg, "progressDeadline: 50s", "progressDeadline: 15s")
	h := newHarness(t, cfg, retryFleet)
	h.prime()
	h.world.set(func(w *world) { w.runErr["update"] = errFake })
	h.release(imgA, d2)
	id := h.active("a").ID
	h.clock.Advance(10 * time.Second)
	h.tick()
	require.Equal(t, 2, h.rolloutTarget(id, tN1A).UpdateAttempts)
	h.c = h.newController()
	h.clock.Advance(5 * time.Second)
	h.tick()
	rt := h.rolloutTarget(id, tN1A)
	require.NotEqual(t, Halted, h.rollout(id).State)
	require.Equal(t, PhaseSkipped, rt.Phase, rt.Reason)
	require.False(t, rt.HoldUntil.IsZero())
	require.Len(t, h.world.callsFor("update:"+tN1A), 2)
}

func TestUpdateFailureAtTheProgressDeadlineDoesNotScheduleAnotherAttempt(t *testing.T) {
	h := newHarness(t, replaceLine(testConfig, "retry: {limit: 1}", "retry: {limit: 5}"), retryFleet)
	h.prime()
	h.world.set(func(w *world) {
		w.registry[imgA] = d2
		w.updateFail[tN1A] = boom
	})
	h.c.Refresh(h.ctx, actor)
	entered, release := h.world.gate("update:" + tN1A)

	var once sync.Once

	unblock := func() { once.Do(release) }
	defer unblock()

	done := make(chan error, 1)
	go func() { done <- h.c.Tick(h.ctx) }()

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("update did not start")
	}

	h.clock.Advance(h.cfg.Strategy.ProgressDeadline)
	unblock()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("update did not finish")
	}

	r := h.active("a")
	rt := r.target(tN1A)
	require.NotEqual(t, Halted, r.State)
	require.Equal(t, PhaseSkipped, rt.Phase, rt.Reason)
	require.True(t, rt.RetryAt.IsZero())
	require.False(t, rt.HoldUntil.IsZero())
	require.Len(t, h.world.callsFor("update:"+tN1A), 1)
}

func TestZeroRetryLimitAllowsOnlyTheInitialUpdateAttempt(t *testing.T) {
	h := newHarness(t, replaceLine(testConfig, "retry: {limit: 1}", "retry: {limit: 0}"), retryFleet)
	h.prime()
	h.world.set(func(w *world) { w.updateFail[tN1A] = boom })
	h.release(imgA, d2)
	r := h.active("a")
	rt := r.target(tN1A)
	require.Equal(t, PhaseSkipped, rt.Phase, rt.Reason)
	require.Equal(t, 1, rt.UpdateAttempts)
	h.ticks(3, 20*time.Second)
	require.Equal(t, 1, h.rolloutTarget(r.ID, tN1A).UpdateAttempts, "the rollout never tries again")
	require.NotEqual(t, Halted, h.rollout(r.ID).State)
}

func TestUpdateBackoffCapsWithoutOverflow(t *testing.T) {
	cases := []struct {
		name     string
		duration string
		factor   int
		maximum  string
		delays   []time.Duration
	}{
		{name: "constant", duration: "10s", factor: 1, maximum: "30s", delays: []time.Duration{10 * time.Second, 10 * time.Second, 10 * time.Second}},
		{name: "capped", duration: "10s", factor: 2, maximum: "25s", delays: []time.Duration{10 * time.Second, 20 * time.Second, 25 * time.Second}},
		{name: "large factor", duration: "1s", factor: int(^uint(0) >> 1), maximum: "3m", delays: []time.Duration{time.Second, 3 * time.Minute, 3 * time.Minute}},
		{name: "initial cap", duration: "30s", factor: 2, maximum: "5s", delays: []time.Duration{5 * time.Second, 5 * time.Second, 5 * time.Second}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			retry := fmt.Sprintf("retry: {limit: 5, backoff: {duration: %s, factor: %d, maxDuration: %s}}", tc.duration, tc.factor, tc.maximum)
			cfg := replaceLine(testConfig, "retry: {limit: 1}", retry)
			cfg = replaceLine(cfg, "progressDeadline: 50s", "progressDeadline: 1h")
			h := newHarness(t, cfg, retryFleet)
			h.prime()
			h.world.set(func(w *world) { w.updateFail[tN1A] = boom })
			h.release(imgA, d2)
			id := h.active("a").ID

			for i, delay := range tc.delays {
				r := h.rollout(id)
				require.Equal(t, Running, r.State)
				require.Equal(t, i+1, r.target(tN1A).UpdateAttempts)
				require.Equal(t, h.clock.Now().Add(delay), r.target(tN1A).RetryAt)
				h.clock.Advance(delay)
				h.tick()
			}
		})
	}
}

func TestUndurableUpdateRetryDoesNotConsumeAnAttempt(t *testing.T) {
	h := newHarness(t, replaceLine(testConfig, "retry: {limit: 1}", "retry: {limit: 5}"), retryFleet)
	h.prime()
	h.world.set(func(w *world) { w.updateFail[tN1A] = boom })
	h.release(imgA, d2)
	id := h.active("a").ID
	retryAt := h.rolloutTarget(id, tN1A).RetryAt
	h.store.FailRollouts = errFake
	h.clock.Advance(10 * time.Second)
	h.tick()
	require.Equal(t, 1, h.rolloutTarget(id, tN1A).UpdateAttempts)
	require.Equal(t, retryAt, h.rolloutTarget(id, tN1A).RetryAt)
	require.Len(t, h.world.callsFor("update:"+tN1A), 1)
	h.store.FailRollouts = nil
	h.world.set(func(w *world) { delete(w.updateFail, tN1A) })
	require.Equal(t, Complete, h.drive(id, 20, 20*time.Second).State)
	require.Equal(t, 2, h.rolloutTarget(id, tN1A).UpdateAttempts)
}

func TestManualRetryStartsANewUpdateAttemptWindow(t *testing.T) {
	h := newHarness(t, replaceLine(testConfig, "retry: {limit: 1}", "retry: {limit: 2}"), retryFleet)
	h.prime()
	h.world.set(func(w *world) { w.updateFail[tN1A] = boom })
	h.release(imgA, d2)
	id := h.active("a").ID

	// The second attempt lands a build that never becomes ready.
	h.world.set(func(w *world) {
		delete(w.updateFail, tN1A)
		w.breakOnUpdate[tN1A] = true
	})

	for i := 0; i < 20 && h.rollout(id).State != Halted; i++ {
		h.ticks(1, 10*time.Second)
	}

	require.Equal(t, Halted, h.rollout(id).State)
	require.Equal(t, 2, h.rolloutTarget(id, tN1A).UpdateAttempts)
	h.revert(tN1A)
	require.NoError(t, h.c.Retry(h.ctx, actor, id, "readiness fixed"))
	require.Equal(t, Complete, h.drive(id, 20, 20*time.Second).State)
	require.Equal(t, 1, h.rolloutTarget(id, tN1A).UpdateAttempts)
	require.Nil(t, h.view(tN1A).Quarantine)
}

func TestInterruptedUpdateCannotBypassItsLimitOrDeadline(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(fmt.Sprintf("expired-%t", expired), func(t *testing.T) {
			h := newHarness(t, testConfig, retryFleet)
			h.prime()
			h.world.set(func(w *world) { w.updateStuck[tN1A] = true })
			h.release(imgA, d2)
			id := h.active("a").ID
			h.store.mu.Lock()
			h.store.rollouts[id].Targets[0].UpdateDone = false
			h.store.mu.Unlock()
			h.c = h.newController()

			if expired {
				h.clock.Advance(h.cfg.Strategy.ProgressDeadline)
			}

			h.tick()
			r := h.rollout(id)
			rt := r.target(tN1A)
			require.NotEqual(t, Halted, r.State)
			require.Equal(t, PhaseSkipped, rt.Phase, rt.Reason)
			require.False(t, rt.HoldUntil.IsZero(), "the interrupted update may still land")
			require.Len(t, h.world.callsFor("update:"+tN1A), 1)
		})
	}
}

func TestSoakExecutionErrorsAreNeutralAndResetAfterAnExecutedCheck(t *testing.T) {
	cfg := replaceLine(testConfig, "duration: 60s", "duration: 40s")
	h := newHarness(t, cfg, retryFleet)
	h.prime()
	h.release(imgA, d2)
	id := h.active("a").ID
	h.ticks(2, 0)
	require.Equal(t, 1, h.rollout(id).Soak.Streak)
	h.world.set(func(w *world) { w.runErr[soakA] = errFake })
	h.ticks(2, 20*time.Second)
	h.tick()
	r := h.rollout(id)
	require.Equal(t, Soaking, r.State)
	require.Equal(t, 1, r.Soak.Streak)
	require.Equal(t, 0, r.Soak.Failures)
	require.Equal(t, 2, r.Soak.ConsecutiveErrors)
	h.world.set(func(w *world) {
		delete(w.runErr, soakA)
		w.soakFail[soakA] = true
	})
	h.clock.Advance(20 * time.Second)
	h.tick()
	r = h.rollout(id)
	require.Equal(t, Soaking, r.State)
	require.Equal(t, 0, r.Soak.Streak)
	require.Equal(t, 1, r.Soak.Failures)
	require.Equal(t, 0, r.Soak.ConsecutiveErrors)
	h.world.set(func(w *world) { delete(w.soakFail, soakA) })
	h.clock.Advance(20 * time.Second)
	h.tick()
	require.True(t, h.rollout(id).Batches[0].Passed)
}

func TestSoakIndeterminateResultsAreErrorsAndNegativeExitsFail(t *testing.T) {
	cases := []struct {
		name   string
		result hooks.Result
		error  bool
	}{
		{name: "timeout", result: hooks.Result{TimedOut: true, ExitCode: -1, Reason: "timed out"}, error: true},
		{name: "killed", result: hooks.Result{ExitCode: -1, Reason: "killed"}, error: true},
		{name: "could not check", result: hooks.Result{ExitCode: 3, Reason: "no baseline"}, error: true},
		{name: "exit", result: hooks.Result{ExitCode: 1, Reason: "outside tolerance"}},
		{name: "unexpected exit", result: hooks.Result{ExitCode: 2, Reason: "bad usage"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, replaceLine(testConfig, "failureLimit: 1", "failureLimit: 0"), retryFleet)
			h.prime()
			h.world.set(func(w *world) { w.runResult[soakA] = tc.result })
			h.release(imgA, d2)
			r := h.drive(h.active("a").ID, 4, 0)
			require.Equal(t, tc.error, r.Soak.Checks[0].Error)

			if tc.error {
				require.Equal(t, Soaking, r.State, "an indeterminate check never halts")
				require.Equal(t, 0, r.Soak.Failures)
				require.Equal(t, 1, r.Soak.ConsecutiveErrors)
			} else {
				require.Equal(t, Halted, r.State)
				require.Equal(t, 1, r.Soak.Failures)
				require.Equal(t, 0, r.Soak.ConsecutiveErrors)
				require.Contains(t, r.Reason, "failed 1 times")
			}
		})
	}
}

func TestTighterSoakFailureLimitUsesExecutedFailureEvidenceDuringAnError(t *testing.T) {
	cfg := replaceLine(testConfig, "strategies:", "strategies:\n  strict: {soak: {failureLimit: 0}}")
	h := newHarness(t, cfg, retryFleet)
	h.prime()
	h.world.set(func(w *world) { w.soakFail[soakA] = true })
	h.release(imgA, d2)
	id := h.active("a").ID
	h.ticks(2, 0)
	require.Equal(t, 1, h.rollout(id).Soak.Failures)
	_, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a"), Strategy: "strict"})
	require.NoError(t, err)
	h.world.set(func(w *world) { w.runErr[soakA] = errFake })
	h.clock.Advance(20 * time.Second)
	h.tick()
	r := h.rollout(id)
	require.Equal(t, Halted, r.State)
	require.Equal(t, 1, r.Soak.Failures)
	require.Equal(t, 1, r.Soak.ConsecutiveErrors)
	require.Contains(t, r.Reason, "soak soak-a failed 1 times: 61% vs 98%")
}

func TestTargetRemovedDuringSoakCannotGainLateHookHistory(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	h.tick()
	require.Equal(t, Soaking, h.active("a").State)
	rules := h.set.Rules()
	without, err := parseTargets(replaceLine(testTargets,
		"- {id: a-2/cl, node: a-2, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: \"0\"}, hooks: {soak: soak-a}}", ""), &rules)
	require.NoError(t, err)

	entered, release := h.world.gate("soak")

	var once sync.Once

	unblock := func() { once.Do(release) }
	defer unblock()

	done := make(chan error, 1)
	go func() { done <- h.c.Tick(h.ctx) }()

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("soak did not start")
	}

	h.swap(without)
	unblock()

	select {
	case tickErr := <-done:
		require.NoError(t, tickErr)
	case <-time.After(3 * time.Second):
		t.Fatal("soak did not finish")
	}

	r := h.active("a")
	require.Equal(t, Soaking, r.State)
	require.Equal(t, 1, r.Soak.Streak)

	state, err := h.store.Load(h.ctx)
	require.NoError(t, err)
	require.NotContains(t, state.HookRuns[tA2], "soak")
}
