package reconcile

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLegacyLastDispatchRecoverySurvivesAnotherRestart(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		name := "lost response"
		if accepted {
			name = "accepted update"
		}

		t.Run(name, func(t *testing.T) {
			h := newHarness(t, replaceLine(testConfig, "retry: {limit: 1}", "retry: {limit: 2}"), retryFleet)
			h.prime()
			h.world.set(func(w *world) { w.updateFail[tN1A] = boom })
			h.release(imgA, d2)
			id := h.active("a").ID
			h.clock.Advance(10 * time.Second)
			h.tick()
			require.Equal(t, 2, h.rolloutTarget(id, tN1A).UpdateAttempts)
			h.store.mu.Lock()
			rt := h.store.rollouts[id].target(tN1A)
			rt.DispatchedAt, rt.RetryAt = time.Time{}, time.Time{}
			rt.UpdateDone, rt.Reason = accepted, "waiting for the digest"
			h.store.mu.Unlock()
			h.clock.Advance(h.cfg.Strategy.ProgressDeadline + time.Second)
			h.c = h.newController()
			h.tick()
			recovered := h.rolloutTarget(id, tN1A).DispatchedAt
			require.Equal(t, PhaseUpdating, h.rolloutTarget(id, tN1A).Phase)
			h.store.mu.Lock()
			persisted := h.store.rollouts[id].target(tN1A).DispatchedAt
			h.store.mu.Unlock()
			require.Equal(t, recovered, persisted)

			h.c = h.newController()
			h.clock.Advance(h.cfg.Strategy.ProgressDeadline - time.Second)
			h.tick()
			require.Equal(t, PhaseUpdating, h.rolloutTarget(id, tN1A).Phase)
			require.Equal(t, recovered, h.rolloutTarget(id, tN1A).DispatchedAt)
			h.clock.Advance(time.Second)
			h.tick()
			require.Equal(t, PhaseSkipped, h.rolloutTarget(id, tN1A).Phase)
			require.Len(t, h.world.callsFor("update:"+tN1A), 2)
		})
	}
}
