package reconcile

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestAnActionWritesTheDecisionsItFindsOwedFirst(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	first := h.active("a").ID

	// The old rollout's end is owed before the newer build can open a rollout.
	h.store.FailRollouts = errFake
	h.release(imgA, d3)

	h.store.FailRollouts = nil

	// Sync must store the old end before creating another active rollout.
	started, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a")})
	require.NoError(t, err)
	require.Len(t, started, 1)
	require.NotEqual(t, first, started[0])

	restarted := h.newController()

	var active []string

	for _, r := range restarted.Rollouts() {
		if r.Group == "a" && r.State.Active() {
			active = append(active, r.ID)
		}
	}

	require.Equal(t, started, active)
}

// saveOrder records the order rollout saves reach the store in.
type saveOrder struct {
	*MemoryStore

	saves []Rollout
}

func (s *saveOrder) SaveRollout(ctx context.Context, r *Rollout) error {
	s.saves = append(s.saves, *cloneRollout(r))

	return s.MemoryStore.SaveRollout(ctx, r)
}

func TestASkipReachesTheStoreBeforeTheBudgetItFreesIsSpent(t *testing.T) {
	for _, tc := range []struct {
		name  string
		lands bool
	}{
		{name: "the hold ends at the progress deadline"},
		{name: "the hold ends once the target is seen ready on the build", lands: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, oneNodeConfig, `
- {id: n0/a, node: n0, weight: 0,   image: org/a:t, labels: {client: a, owner: a}}
- {id: n1/a, node: n1, weight: 100, image: org/a:t, labels: {client: a, owner: a}}
- {id: n2/b, node: n2, weight: 100, image: org/b:t, labels: {client: b, owner: b}}
`)
			disk := &saveOrder{MemoryStore: h.store}

			c, err := New(h.ctx, &Options{
				Config: h.cfg, Targets: h.current, Resolver: h.world, Runner: h.world, Store: disk,
				Notifier: h.notes, Clock: h.clock, Log: logrus.New(), NewID: h.nextID,
			})
			require.NoError(t, err)

			h.c = c
			h.prime()

			// a moves n0 and n1, with n1's update never landing and n0 never
			// ready; b waits for n1's share of the budget.
			h.world.set(func(w *world) {
				w.updateStuck[tN1A] = true
				w.notReady[tN0A] = true
				w.registry[imgB] = d2
			})
			h.release(imgA, d2)
			h.tick()
			ra, rb := h.active("a"), h.active("b")
			require.Equal(t, WaitingForBudget, rb.State)

			// Suspending n1/a skips it after its update was dispatched. That
			// update may still land, so n1 stays held and b keeps waiting.
			_, err = h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("id=n1/a"), Reason: debugReason})
			require.NoError(t, err)

			disk.saves = nil

			h.tick()

			held := h.clock.Now().Add(h.cfg.Strategy.ProgressDeadline)
			skipped := slices.IndexFunc(disk.saves, func(r Rollout) bool {
				rt := r.target(tN1A)

				return r.ID == ra.ID && rt.Phase == PhaseSkipped && rt.HoldUntil.Equal(held)
			})
			require.GreaterOrEqual(t, skipped, 0, "the skip and its hold are stored")
			require.Equal(t, WaitingForBudget, h.rollout(rb.ID).State)

			if tc.lands {
				h.world.set(func(w *world) { w.running[tN1A] = d2 })
			} else {
				h.clock.Advance(h.cfg.Strategy.ProgressDeadline - time.Second)
				h.tick()
				require.Equal(t, WaitingForBudget, h.rollout(rb.ID).State)
				h.clock.Advance(time.Second)
			}

			h.tick()

			started := slices.IndexFunc(disk.saves, func(r Rollout) bool { return r.ID == rb.ID && len(r.Batches) == 1 })
			require.Greater(t, started, skipped)
		})
	}
}

func TestAPauseTheStoreRefusesLeavesNothingBehind(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	r := h.active("a")

	h.store.FailRollouts = errFake
	require.ErrorIs(t, h.pause(r.ID, time.Hour), errFake)
	require.False(t, h.rollout(r.ID).PausePending)
	require.True(t, h.rollout(r.ID).PauseExpiresAt.IsZero())
}

// refusingStore fails whole-state rewrites while refuse is set.
type refusingStore struct {
	*MemoryStore

	refuse bool
}

func (s *refusingStore) ReplaceDecisions(ctx context.Context, snap *Snapshot) error {
	if s.refuse {
		return errFake
	}

	return s.MemoryStore.ReplaceDecisions(ctx, snap)
}

func TestARecoveredAbortIsStoredOnceTheStoreTakesIt(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	r := h.active("a")
	unsaved := storedRollout(h, r.ID)
	require.NoError(t, h.c.Abort(h.ctx, actor, r.ID))
	loseRolloutSave(h, unsaved)

	disk := &refusingStore{MemoryStore: h.store, refuse: true}
	c, err := New(h.ctx, &Options{
		Config: h.cfg, Targets: h.current, Resolver: h.world, Runner: h.world, Store: disk,
		Notifier: h.notes, Clock: h.clock, Log: logrus.New(), NewID: h.nextID,
	})
	require.NoError(t, err)

	got, ok := c.Rollout(r.ID)
	require.True(t, ok)
	require.Equal(t, Aborted, got.State)
	require.True(t, c.FleetStatus().StoreWritesOwed)
	require.Equal(t, Running, durableState(h.store, r.ID))

	disk.refuse = false

	require.NoError(t, c.Tick(h.ctx))
	require.False(t, c.FleetStatus().StoreWritesOwed)
	require.Equal(t, Aborted, durableState(h.store, r.ID))
}
