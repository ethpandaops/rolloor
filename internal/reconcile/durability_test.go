package reconcile

import (
	"context"
	"slices"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestAnActionWritesTheDecisionsItFindsOwedFirst(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	first := h.active("a").ID

	// d3 supersedes the rollout and opens another while rollout saves fail;
	// both exist only in memory.
	h.store.FailRollouts = errFake
	h.release(imgA, d3)
	second := h.active("a").ID
	require.NotEqual(t, first, second)

	h.store.FailRollouts = nil

	// A sync saves the second rollout. The first's end must reach the store
	// before it, or a restart would find two active rollouts for a.
	_, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a")})
	require.NoError(t, err)

	restarted := h.newController()

	var active []string

	for _, r := range restarted.Rollouts() {
		if r.Group == "a" && r.State.Active() {
			active = append(active, r.ID)
		}
	}

	require.Equal(t, []string{second}, active)
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

	// a moves n0 and n1, with n1's update never landing and n0 never ready;
	// b waits for n1's share of the budget.
	h.world.set(func(w *world) {
		w.updateStuck["n1/a"] = true
		w.notReady[tN0A] = true
		w.registry[imgB] = d2
	})
	h.release(imgA, d2)
	h.tick()
	ra, rb := h.active("a"), h.active("b")
	require.Equal(t, WaitingForBudget, rb.State)

	// Suspending n1/a skips it, which frees n1 for b in the same tick. The
	// skip must be saved first: a crash between the two writes would
	// otherwise restart with both nodes counted.
	_, err = h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("id=n1/a"), Reason: "debugging"})
	require.NoError(t, err)

	disk.saves = nil

	h.tick()

	skipped := slices.IndexFunc(disk.saves, func(r Rollout) bool {
		return r.ID == ra.ID && r.target("n1/a").Phase == PhaseSkipped
	})
	started := slices.IndexFunc(disk.saves, func(r Rollout) bool { return r.ID == rb.ID && len(r.Batches) == 1 })

	require.GreaterOrEqual(t, skipped, 0)
	require.Greater(t, started, skipped)
}

func TestAPauseTheStoreRefusesLeavesNothingBehind(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	r := h.active("a")

	h.store.FailRollouts = errFake
	require.ErrorIs(t, h.c.Pause(h.ctx, actor, r.ID), errFake)
	require.False(t, h.rollout(r.ID).PausePending)
}
