package reconcile

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyncLiftsAbortWithoutCurrentDrift(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	id := h.active("a").ID
	require.NoError(t, h.c.Abort(h.ctx, actor, id))
	h.world.set(func(w *world) {
		for _, target := range h.current().InGroup("a") {
			w.running[target.ID] = d2
		}
	})
	h.c.InspectAll(h.ctx)
	h.c.ProbeAll(h.ctx)
	h.store.Fail = errFake
	_, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a")})
	require.ErrorIs(t, err, errFake)

	h.store.Fail = nil
	before, err := h.store.Load(h.ctx)
	require.NoError(t, err)
	require.Contains(t, before.Aborted, "a")

	h.c = h.newController()
	started, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a")})
	require.NoError(t, err)
	require.Empty(t, started)

	snap, err := h.store.Load(h.ctx)
	require.NoError(t, err)
	require.NotContains(t, snap.Aborted, "a")
	h.world.set(func(w *world) { w.running[tA1] = d1 })
	h.c.InspectAll(h.ctx)
	h.tick()
	require.NotEqual(t, id, h.active("a").ID)
}

func TestSyncCannotCreateAnotherRolloutWhileItsSupersedeIsOwed(t *testing.T) {
	h := newHarness(t, testConfig, retryFleet)
	h.prime()
	h.release(imgA, d2)
	id := h.active("a").ID
	disk := &eventFailureStore{MemoryStore: h.store, failAction: "rollout.superseded"}
	h.c.store = disk
	h.world.set(func(w *world) { w.registry[imgA] = d3 })
	started, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a")})
	require.ErrorIs(t, err, errFake)
	require.Empty(t, started)

	snap, err := h.store.Load(h.ctx)
	require.NoError(t, err)
	require.Len(t, snap.Rollouts, 1)
	require.Equal(t, id, snap.Rollouts[0].ID)
	require.Equal(t, Running, snap.Rollouts[0].State)

	events, err := h.c.Events(h.ctx, &EventQuery{Rollout: id})
	require.NoError(t, err)

	for _, e := range events {
		require.NotEqual(t, "rollout.superseded", e.Action)
	}

	disk.failAction = ""

	h.tick()
	require.Equal(t, Superseded, h.rollout(id).State)
	require.NotEqual(t, id, h.active("a").ID)
	events, err = h.c.Events(h.ctx, &EventQuery{Rollout: id})
	require.NoError(t, err)
	require.Equal(t, "rollout.superseded", events[0].Action)
}
