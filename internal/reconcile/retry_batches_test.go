package reconcile

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/config"
)

const (
	retryNodeOne   = "node-1"
	retryNodeTwo   = "node-2"
	retryNodeThree = "node-3"
	wavesStrategy  = "nosoak"
	quarterBudget  = "25%"
)

// alphaRetried are alpha's targets on node-1 and node-2: its first batch.
var alphaRetried = []string{"node-1/agent", "node-1/server", tNode2Agent, tNode2Server}

// retryConfig is the usage environment with a strategy and a budget.
func retryConfig(strategy, budget string) string {
	cfg := replaceLine(usageConfig, "firstBatch: 1, batchSize: 2", strategy)

	return replaceLine(cfg, "maxUnavailable: 25%", "maxUnavailable: "+budget)
}

// haltAlpha ships alpha a build that never becomes ready on node-1 and
// node-2, so the rollout halts there; then those targets are put back on
// their old build by hand and seen so.
func haltAlpha(t *testing.T, cfg string) (*harness, RolloutView) {
	t.Helper()

	h := newFleet(t, cfg)
	h.world.set(func(w *world) {
		w.running[tObs1Server] = d2

		for _, id := range alphaRetried {
			w.breakOnUpdate[id] = true
		}
	})
	h.release(imgAlpha, d2)

	r := h.until(h.active(clientAlpha).ID, Halted, 30)
	require.Equal(t, [][]string{{retryNodeOne, retryNodeTwo}}, batchNodes(&r))
	h.revert(alphaRetried...)
	h.c.InspectAll(h.ctx)
	h.clock.Advance(time.Nanosecond)
	h.c.ProbeAll(h.ctx)

	return h, r
}

// heavyPair doubles node-1 and node-2 to 200 of the fleet's 1000, so a 25%
// budget of 250 takes one of them at a time.
func heavyPair(doc string) string {
	for _, node := range []string{retryNodeOne, retryNodeTwo} {
		doc = strings.ReplaceAll(doc, "node: "+node+", weight: 100", "node: "+node+", weight: 200")
	}

	return doc
}

// requireRetryBatches checks that the batches between the halted first and
// the ordinary last take retried targets and nothing else.
func requireRetryBatches(t *testing.T, r *RolloutView) {
	t.Helper()

	last := len(r.Batches) - 1

	for i, b := range r.Batches {
		require.Equal(t, i > 0 && i < last, b.Retried, "batch %d", b.Number)

		if i == 0 {
			continue
		}

		for _, id := range b.Targets {
			require.Equal(t, b.Retried, r.target(id).Retried, "batch %d takes %s", b.Number, id)
		}
	}
}

func TestRetryBatchesTakeRetriedTargetsUnderTheBudget(t *testing.T) {
	for _, tc := range []struct {
		name     string
		strategy string
		budget   string
		heavy    bool
		expire   bool
		batches  [][]string
	}{
		{
			name: "heavy nodes go one at a time once their holds expire", strategy: "firstBatch: 2, batchSize: 2", budget: quarterBudget, heavy: true, expire: true,
			batches: [][]string{{retryNodeOne, retryNodeTwo}, {retryNodeOne}, {retryNodeTwo}, {retryNodeThree}},
		},
		{
			name: "nodes still held as unavailable are retried free", strategy: "firstBatch: 2, batchSize: 2", budget: quarterBudget, heavy: true,
			batches: [][]string{{retryNodeOne, retryNodeTwo}, {retryNodeOne, retryNodeTwo}, {retryNodeThree}},
		},
		{
			name: "the budget, not the batch size, bounds a retry", strategy: "firstBatch: 2, batchSize: 1", budget: "50%", expire: true,
			batches: [][]string{{retryNodeOne, retryNodeTwo}, {retryNodeOne, retryNodeTwo}, {retryNodeThree}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, r := haltAlpha(t, retryConfig(tc.strategy, tc.budget))
			halted := cloneRollout(&r.Rollout).Batches[0]
			require.False(t, halted.EndedAt.IsZero())

			if tc.heavy {
				h.swap(h.fleetWith(heavyPair))
			}

			if tc.expire {
				h.clock.Advance(h.cfg.Strategy.ProgressDeadline + time.Second)
			}

			before := h.updates()
			require.NoError(t, h.c.Retry(h.ctx, actor, r.ID, "updater recovered"))

			r = h.rollout(r.ID)
			require.Equal(t, Running, r.State)
			require.Equal(t, []Batch{halted}, r.Batches)

			for _, id := range alphaRetried {
				rt := r.target(id)
				require.Equal(t, PhasePending, rt.Phase, id)
				require.Zero(t, rt.Batch, id)
				require.True(t, rt.Retried, id)
				require.Zero(t, rt.UpdateAttempts, id)
				require.Equal(t, !tc.expire, h.clock.Now().Before(rt.HoldUntil), "%s keeps an unexpired hold", id)
			}

			// settle fails if any admission breaks the budget on the way.
			h.settle(100)

			r = h.rollout(r.ID)
			require.Equal(t, Complete, r.State, r.Reason)
			require.Equal(t, tc.batches, batchNodes(&r))
			require.Equal(t, halted, r.Batches[0], "the halted batch keeps its record")
			requireRetryBatches(t, &r)
			require.Subset(t, h.on(d2), h.clientTargets(clientAlpha))

			after := h.updates()

			for _, id := range h.clientTargets(clientAlpha) {
				if id != tObs1Server {
					require.Equal(t, before[id]+1, after[id], "%s is updated once after the retry", id)
				}
			}
		})
	}
}

func TestRetryVerifiesLandedTargetsBeforeLiftingTheirQuarantine(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) { w.soakFail[soakA] = true })
	h.release(imgA, d2)

	r := h.until(h.active("a").ID, Halted, 20)
	require.Equal(t, []string{tA1, tA2}, r.Batches[0].Targets)

	first := cloneRollout(&r.Rollout).Batches[0]
	require.Positive(t, first.Soak.Failures)

	updates := len(h.world.callsFor("update:" + tA1))

	// The soak still fails: targets already on the build go through a batch
	// and its soak again instead of passing on sight.
	require.NoError(t, h.c.Retry(h.ctx, actor, r.ID, "the probe may be wrong"))
	require.Equal(t, r.ID, h.view(tA1).Quarantine.Rollout)
	h.step()

	r = h.rollout(r.ID)
	require.Len(t, r.Batches, 2)
	require.True(t, r.Batches[1].Retried)
	require.Equal(t, []string{tA1, tA2}, r.Batches[1].Targets)
	require.Equal(t, r.ID, h.view(tA1).Quarantine.Rollout)

	r = h.until(r.ID, Halted, 20)
	require.Len(t, r.Batches, 2)
	require.Equal(t, first, r.Batches[0])

	second := cloneRollout(&r.Rollout).Batches[1]
	require.Positive(t, second.Soak.Failures)

	// Retried again with the probe fixed, the batch passes and only then is
	// the quarantine lifted, before the untried waves go on.
	h.world.set(func(w *world) { w.soakFail[soakA] = false })
	require.NoError(t, h.c.Retry(h.ctx, actor, r.ID, "probe fixed"))

	for i := 0; i < 20 && (len(r.Batches) < 3 || !r.Batches[2].Passed); i++ {
		require.NotNil(t, h.view(tA1).Quarantine)
		h.step()

		r = h.rollout(r.ID)
	}

	require.Len(t, r.Batches, 3)
	require.True(t, r.Batches[2].Passed)
	require.True(t, r.Batches[2].Retried)
	require.True(t, r.State.Active())
	require.Nil(t, h.view(tA1).Quarantine)
	require.Nil(t, mustView(t, h.newController(), tA2).Quarantine)

	h.settle(80)

	r = h.rollout(r.ID)
	require.Equal(t, Complete, r.State, r.Reason)
	require.Equal(t, first, r.Batches[0])
	require.Equal(t, second, r.Batches[1])
	require.Equal(t, updates, len(h.world.callsFor("update:"+tA1)), "a landed target is never updated again")

	for _, b := range r.Batches[3:] {
		require.False(t, b.Retried, "batch %d", b.Number)
	}
}

func TestRetryBatchIgnoresWavesItsHaltedBatchSpanned(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.declare(false, map[string]config.Group{"a": {Paused: true, Strategy: wavesStrategy}})
	h.world.set(func(w *world) { w.breakOnUpdate[tA3] = true })
	h.release(imgA, d2)

	r := h.active("a")
	require.Empty(t, r.Batches)

	// Synced without waves, one batch takes waves 0 and 1 and halts.
	_, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a"), Strategy: speedAll})
	require.NoError(t, err)
	h.declare(false, map[string]config.Group{"a": {Strategy: wavesStrategy}})

	for i := 0; i < 20 && h.rollout(r.ID).State != Halted; i++ {
		h.ticks(1, 20*time.Second)
	}

	r = h.rollout(r.ID)
	require.Equal(t, Halted, r.State, r.Reason)
	require.Equal(t, []string{tA1, tA2, tA3, tA4}, r.Batches[0].Targets)

	halted := cloneRollout(&r.Rollout).Batches[0]

	// Back on a strategy with waves and a batch of three nodes, the retry
	// still takes all four nodes, which the budget allows.
	_, err = h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a"), Strategy: wavesStrategy})
	require.NoError(t, err)
	require.Equal(t, Halted, h.rollout(r.ID).State)
	h.revert(tA3)
	h.clock.Advance(h.cfg.Strategy.ProgressDeadline + time.Second)
	require.NoError(t, h.c.Retry(h.ctx, actor, r.ID, "readiness fixed"))
	h.tick()

	r = h.rollout(r.ID)
	require.Len(t, r.Batches, 2)
	require.True(t, r.Batches[1].Retried)
	require.Equal(t, halted.Targets, r.Batches[1].Targets)

	r = h.drive(r.ID, 40, 20*time.Second)
	require.Equal(t, Complete, r.State, r.Reason)
	require.Equal(t, halted, r.Batches[0])
	require.Len(t, r.Batches, 4)
	require.Equal(t, []string{tA5}, r.Batches[2].Targets)
	require.Equal(t, []string{tA6}, r.Batches[3].Targets)
	require.False(t, r.Batches[2].Retried || r.Batches[3].Retried)
}

func TestRetrySurvivesAStoreFailureAndARestart(t *testing.T) {
	h, r := haltAlpha(t, retryConfig("firstBatch: 2, batchSize: 2", quarterBudget))
	h.swap(h.fleetWith(heavyPair))
	h.clock.Advance(h.cfg.Strategy.ProgressDeadline + time.Second)

	halted := h.rollout(r.ID)

	h.store.Fail = errFake
	require.ErrorIs(t, h.c.Retry(h.ctx, actor, r.ID, "updater recovered"), errFake)
	require.Equal(t, halted.Rollout, h.rollout(r.ID).Rollout, "a retry the store refused changes nothing")

	h.store.Fail = nil
	require.NoError(t, h.c.Retry(h.ctx, actor, r.ID, "updater recovered"))

	// The first retry batch cannot be stored, so nothing runs; after a restart
	// the retry is still owed and its batch is cut again.
	before := h.updates()
	h.store.FailRollouts = errFake
	h.step()
	require.Equal(t, before, h.updates())

	h.store.FailRollouts = nil
	h.c = h.newController()

	r = h.rollout(r.ID)
	require.Equal(t, Running, r.State)
	require.Equal(t, halted.Batches, r.Batches)

	for _, id := range alphaRetried {
		require.True(t, r.target(id).Retried, id)
		require.Zero(t, r.target(id).Batch, id)
	}

	h.settle(100)

	r = h.rollout(r.ID)
	require.Equal(t, Complete, r.State, r.Reason)
	require.Equal(t, [][]string{{retryNodeOne, retryNodeTwo}, {retryNodeOne}, {retryNodeTwo}, {retryNodeThree}}, batchNodes(&r))
	requireRetryBatches(t, &r)

	after := h.updates()

	for _, id := range alphaRetried {
		require.Equal(t, before[id]+1, after[id], "%s is updated once after the retry", id)
	}
}

func TestRetryUpdatesFailedTargetsDespiteUnknownInspections(t *testing.T) {
	h, r := haltAlpha(t, retryConfig("firstBatch: 2, batchSize: 2", quarterBudget))
	h.world.set(func(w *world) {
		for _, id := range alphaRetried {
			w.inspectFail[id] = true
		}
	})

	for range h.cfg.Inspect.FailureThreshold {
		h.c.InspectAll(h.ctx)
	}

	for _, id := range alphaRetried {
		require.Equal(t, HealthUnknown, h.view(id).Health)
	}

	before := h.updates()
	require.NoError(t, h.c.Retry(h.ctx, actor, r.ID, "updater recovered"))
	h.tick()

	for _, id := range alphaRetried {
		require.Equal(t, before[id]+1, h.updates()[id], "%s retries its failed update", id)
	}

	h.world.set(func(w *world) { clear(w.inspectFail) })
	h.settle(100)

	resumed := h.rollout(r.ID)
	require.Equal(t, Complete, resumed.State)
	require.Equal(t, [][]string{{retryNodeOne, retryNodeTwo}, {retryNodeOne, retryNodeTwo}, {retryNodeThree}}, batchNodes(&resumed))
	requireRetryBatches(t, &resumed)
}
