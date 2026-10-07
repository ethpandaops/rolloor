package reconcile

import (
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestQueuedRetryPastFirstDispatchDeadlineIsNotSent(t *testing.T) {
	h := newHarness(t, replaceLine(queuedConfig, "retry: {limit: 1}", "retry: {limit: 2}"), queuedFleet)
	c, err := New(h.ctx, &Options{
		Config: h.cfg, Targets: h.current, Resolver: h.world, Runner: h.world, Store: h.store,
		Notifier: h.notes, Clock: h.clock, Log: logrus.New(), NewID: h.nextID, Concurrency: 1,
	})
	require.NoError(t, err)

	h.c = c
	h.prime()
	h.world.set(func(w *world) {
		w.updateFail[tN1A], w.updateFail[tN2A] = boom, boom
	})
	h.release(imgA, d2)
	id := h.active("a").ID
	require.Equal(t, 1, h.rolloutTarget(id, tN2A).UpdateAttempts)
	h.clock.Advance(10 * time.Second)
	entered, release := h.world.gate("update:" + tN1A)

	var once sync.Once

	unblock := func() { once.Do(release) }
	defer unblock()

	done := make(chan error, 1)
	go func() { done <- h.c.Tick(h.ctx) }()

	awaitGate(t, entered)
	h.clock.Advance(h.cfg.Strategy.ProgressDeadline - 10*time.Second + time.Second)
	unblock()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("retry tick did not finish")
	}

	require.Equal(t, 1, stored(t, h, id, tN2A).UpdateAttempts)
	require.Len(t, h.world.callsFor("update:"+tN2A), 1)
	h.tick()
	require.Equal(t, PhaseSkipped, h.rolloutTarget(id, tN2A).Phase)
	require.Equal(t, PhaseUpdating, h.rolloutTarget(id, tN1A).Phase)
	require.Len(t, h.world.callsFor("update:"+tN1A), 2)
}
