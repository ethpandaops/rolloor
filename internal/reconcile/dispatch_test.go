package reconcile

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/config"
)

// queuedFleet is one group's two weighted targets on separate nodes, both
// admitted to the first batch under queuedConfig.
const queuedFleet = `
- {id: n1/a, node: n1, weight: 100, image: org/a:t, labels: {client: a, owner: a}}
- {id: n2/a, node: n2, weight: 100, image: org/a:t, labels: {client: a, owner: a}}
`

var queuedConfig = replaceLine(testConfig, "maxUnavailable: 50%", "maxUnavailable: 200")

// queueUpdates opens a rollout whose updates run one at a time.
// It returns while n1/a runs; finish releases it and waits for the tick.
func queueUpdates(t *testing.T, doc string) (h *harness, rollout string, finish func()) {
	t.Helper()

	h = newHarness(t, queuedConfig, doc)

	c, err := New(h.ctx, &Options{
		Config: h.cfg, Targets: h.current, Resolver: h.world, Runner: h.world, Store: h.store,
		Notifier: h.notes, Clock: h.clock, Log: logrus.New(), NewID: h.nextID, Concurrency: 1,
	})
	require.NoError(t, err)

	h.c = c
	h.prime()
	h.world.set(func(w *world) {
		w.registry[imgA] = d2
		if _, hasSecondImage := w.registry[imgB]; hasSecondImage {
			w.registry[imgB] = d3
		}
	})
	h.clock.Advance(h.cfg.Registry.Poll)

	entered, release := h.world.gate("update:" + tN1A)
	done := make(chan error, 1)

	go func() { done <- h.c.Tick(h.ctx) }()

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the first update did not start")
	}

	return h, h.active("a").ID, func() {
		release()

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("the update tick did not finish")
		}
	}
}

// stored reads a rollout target as the store holds it.
func stored(t *testing.T, h *harness, rollout, id string) RolloutTarget {
	t.Helper()

	snap, err := h.store.Load(h.ctx)
	require.NoError(t, err)

	for _, r := range snap.Rollouts {
		if r.ID == rollout {
			return *r.target(id)
		}
	}

	t.Fatalf("rollout %s is not stored", rollout)

	return RolloutTarget{}
}

// busy is the controller's own view of the unavailable nodes.
func (h *harness) busy() map[string]float64 {
	h.c.mu.RLock()
	defer h.c.mu.RUnlock()

	return h.c.unavailableNodes(h.clock.Now())
}

func suspendTarget(t *testing.T, h *harness, id string) {
	t.Helper()

	_, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("id=" + id), Reason: debugReason})
	require.NoError(t, err)
}

// swapFleet replaces the targets with doc while the controller runs.
func swapFleet(t *testing.T, h *harness, doc string) {
	t.Helper()

	rules := h.current().Rules()
	set, err := parseTargets(doc, &rules)
	require.NoError(t, err)
	h.swap(set)
}

func TestAQueuedUpdateIsCheckedAgainBeforeItRuns(t *testing.T) {
	updatedLater := func(t *testing.T, h *harness, _ string) {
		t.Helper()
		h.tick()
		require.Len(t, h.world.callsFor("update:"+tN2A), 1, "the update runs once the store has every decision")
	}

	for _, tc := range []struct {
		name   string
		change func(t *testing.T, h *harness, rollout string)
		phase  TargetPhase
		after  func(t *testing.T, h *harness, rollout string)
	}{
		{
			name: "the target was suspended",
			change: func(t *testing.T, h *harness, _ string) {
				t.Helper()
				suspendTarget(t, h, tN2A)
			},
			phase: PhasePending,
			after: func(t *testing.T, h *harness, rollout string) {
				t.Helper()
				require.NoError(t, h.c.Tick(h.ctx))
				rt := h.rolloutTarget(rollout, tN2A)
				require.Equal(t, PhaseSkipped, rt.Phase)
				require.True(t, rt.HoldUntil.IsZero(), "an update that never ran holds nothing")
			},
		},
		{
			name: "the target moved to another node",
			change: func(t *testing.T, h *harness, _ string) {
				t.Helper()
				swapFleet(t, h, replaceLine(queuedFleet, "id: n2/a, node: n2,", "id: n2/a, node: n9,"))
			},
			phase: PhasePending,
		},
		{
			name: "the rollout was aborted",
			change: func(t *testing.T, h *harness, rollout string) {
				t.Helper()
				require.NoError(t, h.c.Abort(h.ctx, actor, rollout))
			},
			phase: PhaseSkipped,
			after: func(t *testing.T, h *harness, rollout string) {
				t.Helper()
				require.Equal(t, h.clock.Now().Add(h.cfg.Strategy.ProgressDeadline), h.rolloutTarget(rollout, tN1A).HoldUntil,
					"the update already running keeps its node held")
			},
		},
		{
			name: "the image moved on",
			change: func(t *testing.T, h *harness, _ string) {
				t.Helper()
				h.world.set(func(w *world) { w.registry[imgA] = d3 })
				require.NoError(t, h.c.resolveAll(h.ctx, h.clock.Now()))
			},
			phase: PhasePending,
			after: func(t *testing.T, h *harness, rollout string) {
				t.Helper()
				require.NoError(t, h.c.Tick(h.ctx))
				require.Equal(t, Superseded, h.rollout(rollout).State)
			},
		},
		{
			name: "the target was already observed on the desired digest",
			change: func(t *testing.T, h *harness, _ string) {
				t.Helper()
				h.world.set(func(w *world) { w.running[tN2A] = d2 })
				h.c.InspectAll(h.ctx)
			},
			phase: PhasePending,
			after: func(t *testing.T, h *harness, rollout string) {
				t.Helper()
				h.ticks(3, 0)
				require.Equal(t, Complete, h.rollout(rollout).State)
				require.Empty(t, h.world.callsFor("update:"+tN2A))
			},
		},
		{
			name: "another tick saw its build land first",
			change: func(t *testing.T, h *harness, _ string) {
				t.Helper()
				h.clock.Advance(time.Second)
				h.world.set(func(w *world) { w.running[tN2A] = d2 })
				h.c.InspectAll(h.ctx)
				require.NoError(t, h.c.Tick(h.ctx))
			},
			phase: PhaseUpdating,
			after: func(t *testing.T, h *harness, rollout string) {
				t.Helper()

				rt := h.rolloutTarget(rollout, tN2A)
				require.True(t, rt.UpdateDone)
				require.False(t, rt.DigestSeenAt.IsZero())
				require.Equal(t, rt.DigestSeenAt, rt.UpdatedAt, "its deadline starts when the build is seen")
				require.Equal(t, Complete, h.drive(rollout, 5, 0).State)
			},
		},
		{
			name: "another tick already skipped the target",
			change: func(t *testing.T, h *harness, _ string) {
				t.Helper()
				suspendTarget(t, h, tN2A)
				require.NoError(t, h.c.Tick(h.ctx))
			},
			phase: PhaseSkipped,
		},
		{
			name: "the group was paused by config",
			change: func(t *testing.T, h *harness, _ string) {
				t.Helper()
				h.declare(false, map[string]config.Group{"a": {Paused: true}})
			},
			phase: PhasePending,
			after: func(t *testing.T, h *harness, rollout string) {
				t.Helper()
				h.ticks(2, 0)
				require.Empty(t, h.world.callsFor("update:"+tN2A))
				require.True(t, h.rolloutTarget(rollout, tN2A).UpdatedAt.IsZero())
				h.declare(false, nil)
				updatedLater(t, h, rollout)
			},
		},
		{
			name: "an earlier decision is still owed to the store",
			change: func(t *testing.T, h *harness, rollout string) {
				t.Helper()

				h.store.FailRollouts = errFake
				require.ErrorIs(t, h.pause(rollout, 0), errFake)
				h.store.FailRollouts = nil
			},
			phase: PhasePending, after: updatedLater,
		},
		{
			name:   "the store refused to record the dispatch",
			change: func(_ *testing.T, h *harness, _ string) { h.store.FailRollouts = errFake },
			phase:  PhasePending, after: updatedLater,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, id, finish := queueUpdates(t, queuedFleet)

			// The store counts the running update before it started.
			require.Equal(t, 1, stored(t, h, id, tN1A).UpdateAttempts)

			tc.change(t, h, id)
			finish()

			h.store.FailRollouts = nil

			rt := h.rolloutTarget(id, tN2A)
			require.Equal(t, tc.phase, rt.Phase)
			require.Zero(t, rt.UpdateAttempts)
			require.True(t, rt.HoldUntil.IsZero(), "an update that never ran holds nothing")
			require.Zero(t, stored(t, h, id, tN2A).UpdateAttempts)
			require.Empty(t, h.world.callsFor("update:"+tN2A))
			require.Len(t, h.world.callsFor("update:"+tN1A), 1)

			if tc.after != nil {
				tc.after(t, h, id)
			}
		})
	}
}

func TestADispatchedUpdateThatLeavesItsBatchKeepsItsNodeHeld(t *testing.T) {
	t.Run("until it is seen ready on the build", func(t *testing.T) {
		h, id, finish := queueUpdates(t, queuedFleet)
		suspendTarget(t, h, tN1A)
		finish()

		rt := h.rolloutTarget(id, tN1A)
		require.Equal(t, PhaseSkipped, rt.Phase)
		require.Equal(t, h.clock.Now().Add(h.cfg.Strategy.ProgressDeadline), rt.HoldUntil)
		require.Len(t, h.world.callsFor("update:"+tN2A), 1, "the queued update still belonged to the batch")
		require.Contains(t, h.busy(), "n1")
		require.Contains(t, h.oracleUnavailable(), "n1")

		h.clock.Advance(time.Second)
		h.c.InspectAll(h.ctx)
		h.clock.Advance(time.Nanosecond)
		h.c.ProbeAll(h.ctx)
		require.NotContains(t, h.busy(), "n1")
		require.NotContains(t, h.oracleUnavailable(), "n1")
	})

	t.Run("after its rollout ends, until an update landing late is seen ready", func(t *testing.T) {
		h, id, finish := queueUpdates(t, queuedFleet)
		h.world.set(func(w *world) { w.updateStuck[tN1A] = true })
		require.NoError(t, h.c.Abort(h.ctx, actor, id))
		finish()

		hold := h.rolloutTarget(id, tN1A).HoldUntil
		require.Equal(t, h.clock.Now().Add(h.cfg.Strategy.ProgressDeadline), hold)

		// The update returned without the build running yet; readiness on the
		// build it ran before cannot acknowledge it.
		h.clock.Advance(time.Second)
		h.c.InspectAll(h.ctx)
		h.clock.Advance(time.Nanosecond)
		h.c.ProbeAll(h.ctx)
		require.Contains(t, h.busy(), "n1")
		require.Contains(t, h.oracleUnavailable(), "n1")

		h.world.set(func(w *world) { w.running[tN1A] = d2 })
		h.clock.Advance(time.Second)
		h.c.InspectAll(h.ctx)
		h.clock.Advance(time.Nanosecond)
		h.c.ProbeAll(h.ctx)
		require.True(t, h.clock.Now().Before(hold))
		require.NotContains(t, h.busy(), "n1")
		require.NotContains(t, h.oracleUnavailable(), "n1")
	})

	t.Run("at its admitted weight after its node leaves the targets", func(t *testing.T) {
		h, id, finish := queueUpdates(t, queuedFleet)
		swapFleet(t, h, replaceLine(queuedFleet, "id: n1/a, node: n1,", "id: n1/a, node: n9,"))
		finish()
		require.Equal(t, PhaseSkipped, h.rolloutTarget(id, tN1A).Phase)

		require.Zero(t, h.current().NodeWeight("n1"))

		// n1 counts its admitted weight beside n2's open update.
		require.InDelta(t, 200, h.c.unavailableWeight(h.current()), 0)
		used, _ := h.unavailable()
		require.InDelta(t, 200, used, 0)

		h.clock.Advance(time.Second)
		h.c.InspectAll(h.ctx)
		h.clock.Advance(time.Nanosecond)
		h.c.ProbeAll(h.ctx)
		require.Contains(t, h.busy(), "n1", "readiness on a new node cannot acknowledge the dispatched old node")
		require.Contains(t, h.oracleUnavailable(), "n1")

		h.clock.Advance(h.cfg.Strategy.ProgressDeadline - time.Second - time.Nanosecond)
		require.InDelta(t, 100, h.c.unavailableWeight(h.current()), 0)
		used, _ = h.unavailable()
		require.InDelta(t, 100, used, 0)
	})

	t.Run("readiness on another image cannot acknowledge the dispatched build", func(t *testing.T) {
		doc := queuedFleet + "- {id: n2/b, node: n2, weight: 100, image: org/b:t, labels: {client: a, owner: a}}\n"
		h, id, finish := queueUpdates(t, doc)
		swapFleet(t, h, replaceLine(doc, "id: n1/a, node: n1, weight: 100, image: org/a:t,", "id: n1/a, node: n1, weight: 100, image: org/b:t,"))
		suspendTarget(t, h, tN1A)
		finish()
		require.Equal(t, PhaseSkipped, h.rolloutTarget(id, tN1A).Phase)

		h.world.set(func(w *world) { w.running[tN1A] = d3 })
		h.clock.Advance(time.Second)
		h.c.InspectAll(h.ctx)
		h.clock.Advance(time.Nanosecond)
		h.c.ProbeAll(h.ctx)
		require.Contains(t, h.busy(), "n1")
		require.Contains(t, h.oracleUnavailable(), "n1")

		h.clock.Advance(h.rolloutTarget(id, tN1A).HoldUntil.Sub(h.clock.Now()))
		require.NotContains(t, h.busy(), "n1")
		require.NotContains(t, h.oracleUnavailable(), "n1")
	})

	t.Run("a forced passed target remains held after it leaves the files", func(t *testing.T) {
		h, id, finish := queueUpdates(t, queuedFleet)
		h.world.set(func(w *world) { w.notReady[tN1A] = true })
		_, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a"), Force: true})
		require.NoError(t, err)
		finish()
		h.ticks(3, 0)
		require.Equal(t, Complete, h.rollout(id).State)

		swapFleet(t, h, replaceLine(queuedFleet, "- {id: n1/a, node: n1, weight: 100, image: org/a:t, labels: {client: a, owner: a}}\n", ""))
		require.InDelta(t, 100, h.c.unavailableWeight(h.current()), 0)
		used, _ := h.unavailable()
		require.InDelta(t, 100, used, 0)

		h.clock.Advance(h.rolloutTarget(id, tN1A).HoldUntil.Sub(h.clock.Now()))
		require.InDelta(t, 0, h.c.unavailableWeight(h.current()), 0)
		used, _ = h.unavailable()
		require.InDelta(t, 0, used, 0)
	})
}

func TestAQueuedUpdateRunsTheTargetAsItIsWhenDispatched(t *testing.T) {
	h, id, finish := queueUpdates(t, queuedFleet)
	swapFleet(t, h, replaceLine(queuedFleet, "id: n2/a, node: n2, weight: 100, image: org/a:t, labels: {client: a, owner: a}}",
		"id: n2/a, node: n2, weight: 100, image: org/a:t, labels: {client: a, owner: a}, hooks: {update: fresh}}"))
	finish()

	require.Equal(t, []string{"update:n2/a:fresh"}, h.world.callsFor("update:"+tN2A))
	require.Equal(t, 1, h.rolloutTarget(id, tN2A).UpdateAttempts)
}

func TestAQueuedUpdateStartsItsClockWhenItIsDispatched(t *testing.T) {
	h, id, finish := queueUpdates(t, queuedFleet)
	planned := h.clock.Now()
	require.True(t, h.rolloutTarget(id, tN2A).UpdatedAt.IsZero(), "a queued update has not started its deadline")

	// The queued update waits for a worker for longer than the deadline.
	h.clock.Advance(h.cfg.Strategy.ProgressDeadline + time.Second)
	require.NoError(t, h.c.Tick(h.ctx))
	require.Equal(t, PhaseUpdating, h.rolloutTarget(id, tN2A).Phase, "an unstarted update cannot time out in the queue")
	finish()

	rt := h.rolloutTarget(id, tN2A)
	require.Equal(t, PhaseUpdating, rt.Phase, rt.Reason)
	require.Equal(t, 1, rt.UpdateAttempts)
	require.Len(t, h.world.callsFor("update:"+tN2A), 1)
	require.True(t, rt.UpdatedAt.After(planned.Add(h.cfg.Strategy.ProgressDeadline)), "the clock starts at dispatch")

	require.Equal(t, Complete, h.drive(id, 5, 0).State)
	require.Equal(t, PhasePassed, h.rolloutTarget(id, tN2A).Phase)
}

func TestARestartCountsAnUpdateStoredAsDispatched(t *testing.T) {
	for _, tc := range []struct {
		name  string
		leave func(t *testing.T, c *Controller, rollout string)
	}{
		{
			name: "the targets are suspended",
			leave: func(t *testing.T, c *Controller, _ string) {
				t.Helper()
				_, err := c.Suspend(t.Context(), SuspendRequest{Actor: actor, Selector: mustSel("client=a"), Reason: "debugging"})
				require.NoError(t, err)
				require.NoError(t, c.Tick(t.Context()))
			},
		},
		{
			name: "the rollout is aborted",
			leave: func(t *testing.T, c *Controller, rollout string) {
				t.Helper()
				require.NoError(t, c.Abort(t.Context(), actor, rollout))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, id, finish := queueUpdates(t, queuedFleet)
			defer finish()

			// The process stops while n1/a's update runs; only that one was
			// stored as dispatched.
			restarted := h.newController()
			tc.leave(t, restarted, id)

			r, ok := restarted.Rollout(id)
			require.True(t, ok)
			require.Equal(t, h.clock.Now().Add(h.cfg.Strategy.ProgressDeadline), r.target(tN1A).HoldUntil)
			require.True(t, r.target(tN2A).HoldUntil.IsZero())
		})
	}
}
