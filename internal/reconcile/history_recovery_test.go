package reconcile

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type eventFailureStore struct {
	*MemoryStore
	failEvents bool
	failAction string
}

func (s *eventFailureStore) rejects(events []Event) bool {
	for _, e := range events {
		if s.failEvents || (s.failAction != "" && e.Action == s.failAction) {
			return true
		}
	}

	return false
}

func (s *eventFailureStore) AppendEvent(ctx context.Context, e *Event) error {
	if s.rejects([]Event{*e}) {
		return errFake
	}

	return s.MemoryStore.AppendEvent(ctx, e)
}

func (s *eventFailureStore) SaveDecision(ctx context.Context, d *Decision) error {
	if s.rejects(d.Events) {
		return errFake
	}

	return s.MemoryStore.SaveDecision(ctx, d)
}

func (s *eventFailureStore) ReplaceDecisions(ctx context.Context, snap *Snapshot) error {
	if s.rejects(snap.Events) {
		return errFake
	}

	return s.MemoryStore.ReplaceDecisions(ctx, snap)
}

func TestGuaranteeHistorySurvivesStoreFailures(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	disk := &eventFailureStore{MemoryStore: h.store, failEvents: true}
	h.c.store = disk

	h.c.Refresh(h.ctx, actor)
	h.c.ReportTargetsError(h.ctx, errOther)
	require.True(t, h.c.Fleet().StoreWritesOwed)
	require.NotContains(t, h.notes.actions(), "refresh")

	_, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("client=a"), Reason: "stop"})
	require.ErrorIs(t, err, errFake)
	require.Empty(t, h.c.Suspensions())

	disk.failEvents = false
	_, err = h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("client=a"), Reason: "stop"})
	require.NoError(t, err)
	require.False(t, h.c.Fleet().StoreWritesOwed)

	events, err := h.c.Events(h.ctx, &EventQuery{})
	require.NoError(t, err)
	require.Equal(t, "suspend", events[0].Action)
	require.Equal(t, "targets.invalid", events[1].Action)
	require.Equal(t, "refresh", events[2].Action)
	require.Equal(t, events[2].ID+1, events[1].ID)
	require.Equal(t, events[1].ID+1, events[0].ID)
	require.Contains(t, h.notes.actions(), "refresh")
	require.Contains(t, h.notes.actions(), "targets.invalid")
}

func TestHaltHistorySurvivesWriteFailure(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	h.tick()
	r := h.active("a")
	disk := &eventFailureStore{MemoryStore: h.store, failEvents: true}
	h.c.store = disk

	h.c.mu.Lock()
	h.c.halt(h.ctx, h.clock.Now(), h.c.rollouts[r.ID], "", "bad comparison")
	h.c.mu.Unlock()
	require.NotContains(t, h.notes.actions(), "rollout.halted")

	h.tick()
	require.Equal(t, Halted, h.rollout(r.ID).State)

	disk.failEvents = false

	h.tick()

	events, err := h.c.Events(h.ctx, &EventQuery{Rollout: r.ID})
	require.NoError(t, err)
	require.Equal(t, "rollout.halted", events[0].Action)
	require.Contains(t, events[0].Reason, "bad comparison")
	require.Contains(t, h.notes.actions(), "rollout.halted")
}

func TestOwedHistoryStopsFurtherDecisionsInTheSameTick(t *testing.T) {
	release := func(h *harness, digest string) {
		h.world.set(func(w *world) {
			w.registry[imgA], w.registry[imgB] = digest, digest
		})
		h.clock.Advance(h.cfg.Registry.Poll)
	}

	for _, tc := range []struct {
		action  string
		prepare func(t *testing.T, h *harness)
		check   func(t *testing.T, h *harness)
	}{
		{
			action: eventRolloutCreated,
			check: func(t *testing.T, h *harness) {
				t.Helper()

				rollouts := h.c.Rollouts()
				require.Len(t, rollouts, 1)
				require.Empty(t, rollouts[0].Batches)
				require.Empty(t, h.world.callsFor("update"))
			},
		},
		{
			action: "batch.started",
			check: func(t *testing.T, h *harness) {
				t.Helper()

				rollouts := h.c.Rollouts()
				require.Len(t, rollouts, 2)
				batches := len(rollouts[0].Batches) + len(rollouts[1].Batches)
				require.Equal(t, 1, batches)
				require.Empty(t, h.world.callsFor("update"))
			},
		},
		{
			action: "rollout.superseded",
			prepare: func(t *testing.T, h *harness) {
				t.Helper()
				release(h, d2)
				h.tick()
			},
			check: func(t *testing.T, h *harness) {
				t.Helper()

				rollouts := h.c.Rollouts()
				require.Len(t, rollouts, 2)

				superseded := 0

				for _, rollout := range rollouts {
					if rollout.State == Superseded {
						superseded++
					}
				}

				require.Equal(t, 1, superseded)
			},
		},
		{
			action: "suspend.expired",
			prepare: func(t *testing.T, h *harness) {
				t.Helper()
				release(h, d2)
				h.tick()

				for _, id := range []string{tA1, tA2} {
					_, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("id=" + id), Reason: maintenanceReason, Expires: time.Minute})
					require.NoError(t, err)
				}
			},
			check: func(t *testing.T, h *harness) {
				t.Helper()
				require.Len(t, h.c.Suspensions(), 1)

				for _, rollout := range h.c.Rollouts() {
					require.True(t, rollout.State.Active())
				}
			},
		},
		{
			action: "pause.expired",
			prepare: func(t *testing.T, h *harness) {
				t.Helper()
				release(h, d2)
				h.tick()

				for _, rollout := range h.c.Rollouts() {
					require.NoError(t, h.pause(rollout.ID, time.Minute))
				}
			},
			check: func(t *testing.T, h *harness) {
				t.Helper()

				expired := 0

				for _, rollout := range h.c.Rollouts() {
					require.True(t, rollout.State.Active())

					if rollout.PauseExpiresAt.IsZero() {
						expired++
					}
				}

				require.Equal(t, 1, expired)
			},
		},
	} {
		t.Run(tc.action, func(t *testing.T) {
			h := newHarness(t, testConfig, testTargets)
			h.prime()

			if tc.prepare != nil {
				tc.prepare(t, h)
			}

			disk := &eventFailureStore{MemoryStore: h.store, failAction: tc.action}
			h.c.store = disk
			release(h, d3)
			h.tick()
			require.True(t, h.c.Fleet().StoreWritesOwed)
			tc.check(t, h)

			disk.failAction = ""

			h.tick()
			require.False(t, h.c.Fleet().StoreWritesOwed)
		})
	}
}

func TestSyncStoresOlderGroupHistoryBeforeChangingAnotherGroup(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.world.set(func(w *world) {
		w.registry[imgA], w.registry[imgB] = d2, d2
	})
	disk := &eventFailureStore{MemoryStore: h.store, failAction: "rollout.created"}
	h.c.store = disk

	started, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("node=a-3")})
	require.ErrorIs(t, err, errFake)
	require.Empty(t, started)
	require.Empty(t, h.c.Rollouts())
	require.Empty(t, h.world.callsFor("update"))

	disk.failAction = ""
	started, err = h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("node=a-3")})
	require.NoError(t, err)
	require.Len(t, started, 2)
	require.Len(t, h.c.Rollouts(), 2)
}

func TestHaltDecisionCannotOutliveItsHistory(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	h.tick()
	id := h.active("a").ID
	before := h.rollout(id)
	disk := &eventFailureStore{MemoryStore: h.store, failEvents: true}
	h.c.store = disk

	h.c.mu.Lock()
	h.c.halt(h.ctx, h.clock.Now(), h.c.rollouts[id], "", "bad comparison")
	h.c.mu.Unlock()

	snap, err := h.store.Load(h.ctx)
	require.NoError(t, err)

	for _, r := range snap.Rollouts {
		if r.ID == id {
			require.Equal(t, before.State, r.State)
			require.Equal(t, before.Batches, r.Batches)
		}
	}

	require.Empty(t, snap.Degraded)

	h.c = h.newController()
	require.Equal(t, before.State, h.rollout(id).State)
	events, err := h.c.Events(h.ctx, &EventQuery{Rollout: id})
	require.NoError(t, err)

	for _, e := range events {
		require.NotEqual(t, "rollout.halted", e.Action)
	}
}
