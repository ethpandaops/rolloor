package reconcile

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/targets"
)

var (
	errFake  = errors.New("fake")
	errOther = errors.New("other")
)

func mustSel(s string) targets.Selector {
	sel, err := targets.ParseSelector(s)
	if err != nil {
		panic(err)
	}

	return sel
}

func replaceLine(doc, line, with string) string {
	return strings.Replace(doc, line, with, 1)
}

func parseTargets(doc string, rules *targets.Rules) (*targets.Set, error) {
	return targets.Parse([]byte(doc), rules)
}

func TestNewValidatesOptionsAndRestores(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)

	_, err := New(h.ctx, nil)
	require.Error(t, err)
	_, err = New(h.ctx, &Options{Config: h.cfg})
	require.Error(t, err)
	_, err = New(h.ctx, &Options{Config: h.cfg, Targets: func() *targets.Set { return h.set }, Resolver: h.world, Runner: h.world, Store: h.store})
	require.ErrorContains(t, err, "logger")

	h.store.Fail = errFake
	_, err = New(h.ctx, &Options{Config: h.cfg, Targets: func() *targets.Set { return h.set }, Resolver: h.world, Runner: h.world, Store: h.store, Log: logrus.New()})
	require.ErrorIs(t, err, errFake)

	h.store.Fail = nil

	// State survives a restart through the store.
	h.prime()
	h.release(imgA, d2)
	_, err = h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("node=b-1"), Reason: "r"})
	require.NoError(t, err)
	require.NoError(t, h.c.SetPolicy(h.ctx, actor, "b", Policy{Mode: ModeManual}))
	h.tick()

	restarted := h.newController()
	r := restarted.Rollouts()
	require.Len(t, r, 1)
	require.Equal(t, Running, r[0].State)
	require.Len(t, restarted.Suspensions(), 1)
	require.Equal(t, ModeManual, restarted.Fleet().Groups[1].Policy.Mode)
	require.Equal(t, Progressing, mustView(t, restarted, tA1).Health)

	// Default id generator and clock work too.
	c, err := New(h.ctx, &Options{Config: h.cfg, Targets: func() *targets.Set { return h.set }, Resolver: h.world, Runner: h.world, Store: NewMemoryStore(), Log: logrus.New()})
	require.NoError(t, err)
	require.Len(t, c.newID(), 12)
	require.False(t, c.clock.Now().IsZero())
}

func mustView(t *testing.T, c *Controller, id string) TargetView {
	t.Helper()

	v, ok := c.Target(id)
	require.True(t, ok)

	return v
}

func TestStoreFailureStopsNewWorkUntilFlushed(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)
	require.Equal(t, Running, h.active("a").State)

	// The store fails during a tick: the decisions made in that tick stay in
	// memory, but no further program runs until they are written.
	h.store.Fail = errFake
	h.tick()

	updates := len(h.world.callsFor("update"))
	inspects := len(h.world.callsFor("inspect"))
	readies := len(h.world.callsFor("ready"))
	h.ticks(3, 0)
	require.Equal(t, updates, len(h.world.callsFor("update")))
	require.Equal(t, inspects, len(h.world.callsFor("inspect")))
	require.Equal(t, readies, len(h.world.callsFor("ready")))

	_, err := h.c.Events(h.ctx, EventQuery{})
	require.ErrorIs(t, err, errFake)

	// Once the store is back, everything is rewritten and work resumes.
	h.store.Fail = nil
	h.tick()
	require.Greater(t, len(h.world.callsFor("ready")), readies)
	require.Equal(t, Complete, h.drive(h.active("a").ID, 80, 20*time.Second).State)
}

func TestRunAndInspectorLoops(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)

	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan error, 2)

	go func() { done <- h.c.RunInspector(ctx) }()
	go func() { done <- h.c.Run(ctx, 10*time.Millisecond) }()

	h.c.Nudge()
	h.c.Nudge()

	require.Eventually(t, func() bool { return len(h.world.callsFor("inspect")) >= 9 }, 2*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		v, _ := h.c.Target(tA1)

		return v.Sync == Synced
	}, 2*time.Second, 5*time.Millisecond)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.ErrorIs(t, <-done, context.Canceled)

	// A tick under a cancelled context reports it.
	require.ErrorIs(t, h.c.Tick(ctx), context.Canceled)
}

func TestInspectorRecordsHookErrors(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.world.set(func(w *world) { w.runErr["inspect"] = errFake })
	h.c.InspectAll(h.ctx)
	h.c.InspectAll(h.ctx)
	require.Equal(t, "not reachable: fake", h.view(tA1).Reason)
}

func TestViewsCoverEveryShape(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()

	// Unknown group and node and rollout.
	_, _, ok := h.c.Group("nope")
	require.False(t, ok)
	_, ok = h.c.Node("nope")
	require.False(t, ok)
	_, ok = h.c.Rollout("nope")
	require.False(t, ok)
	_, ok = h.c.Target("nope")
	require.False(t, ok)

	n, ok := h.c.Node("a-3")
	require.True(t, ok)
	require.Len(t, n.Targets, 3)
	require.Equal(t, "20.0%", n.Share)
	require.Equal(t, map[string]string{"role": "sidecar", labelWave: "1"}, n.Labels)

	g, views, ok := h.c.Group("a")
	require.True(t, ok)
	require.Len(t, views, 6)
	require.Equal(t, 6, g.OnDesired)
	require.Equal(t, 6, g.Nodes)
	require.InDelta(t, 400.0, g.Weight, 0.001)
	require.Equal(t, []string{imgA}, g.Images)
	require.Equal(t, "a", g.Owner)

	sel := h.c.Targets(mustSel("role=el"))
	require.Len(t, sel, 2)

	h.release(imgA, d2)
	h.tick()

	g, _, _ = h.c.Group("a")
	require.NotNil(t, g.Rollout)
	require.Equal(t, 2, g.Rollout.OnNewBuild, "targets mid-update count as on the new build once the digest landed")
	require.Equal(t, Progressing, g.Health)

	rs := h.c.Rollouts()
	require.Len(t, rs, 1)
	require.Equal(t, 6, rs[0].Total)
	require.Positive(t, rs[0].Running+1)

	// Multiple rollouts sort newest first.
	h.release(imgB, d2)
	rs = h.c.Rollouts()
	require.Len(t, rs, 2)
	require.Equal(t, "b", rs[0].Group)

	require.Equal(t, "1234567", shortDigest("sha256:1234567890"))
	require.Equal(t, "abc", shortDigest("abc"))
	require.Equal(t, "none", shortDigest(""))
	require.Equal(t, "0%", pct(1, 0))
	require.Equal(t, "waitingforsync", lower(WaitingForSync))
	require.Equal(t, Unknown, worseSync(OutOfSync, Unknown))
	require.Equal(t, OutOfSync, worseSync(OutOfSync, Synced))
	require.Equal(t, Degraded, worseHealth(Progressing, Degraded))
	require.Equal(t, Progressing, worseHealth(Progressing, Healthy))
	require.Empty(t, firstValue(nil))
	require.Equal(t, "a=1,b=2", desiredKey(map[string]string{"b": "2", "a": "1"}))
	require.False(t, RolloutState("bogus").Active())
	require.Empty(t, (&Rollout{}).DigestShort())
	require.Nil(t, (&Rollout{}).target("x"))
}

func TestMemoryStoreEventsFilterAndFail(t *testing.T) {
	m := NewMemoryStore()
	ctx := context.Background()

	for i, e := range []Event{
		{ID: 1, Group: "a", Rollout: "r1", Target: "t1"},
		{ID: 2, Group: "b", Rollout: "r2"},
		{ID: 3, Group: "a", Rollout: "r3", Target: "t2"},
	} {
		require.NoError(t, m.AppendEvent(ctx, &e), i)
	}

	got, err := m.Events(ctx, EventQuery{Group: "a"})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, int64(3), got[0].ID)

	got, err = m.Events(ctx, EventQuery{Rollout: "r2"})
	require.NoError(t, err)
	require.Len(t, got, 1)

	got, err = m.Events(ctx, EventQuery{Target: "t1", Limit: 1})
	require.NoError(t, err)
	require.Len(t, got, 1)

	got, err = m.Events(ctx, EventQuery{Limit: 2})
	require.NoError(t, err)
	require.Len(t, got, 2)

	// Replay after an id is oldest first.
	got, err = m.Events(ctx, EventQuery{After: 1})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, int64(2), got[0].ID)

	require.NoError(t, m.SaveHookRun(ctx, "t", &HookRun{Hook: "h"}))
	require.NoError(t, m.DeleteHookRuns(ctx, "t"))

	require.NoError(t, m.SaveDegraded(ctx, "t", "why"))
	require.NoError(t, m.SaveLive(ctx, "t", &Live{Digest: d1}))
	snap, err := m.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, "why", snap.Degraded["t"])
	require.Equal(t, int64(4), snap.NextEventID)
	require.NoError(t, m.ClearDegraded(ctx, "t"))
	require.NoError(t, m.DeleteLive(ctx, "t"))

	m.Fail = errFake
	require.ErrorIs(t, m.SaveRollout(ctx, &Rollout{}), errFake)
	require.ErrorIs(t, m.SavePolicy(ctx, "g", Policy{}), errFake)
	require.ErrorIs(t, m.SaveSuspension(ctx, &Suspension{}), errFake)
	require.ErrorIs(t, m.DeleteSuspension(ctx, "x"), errFake)
	require.ErrorIs(t, m.SaveLive(ctx, "x", &Live{}), errFake)
	require.ErrorIs(t, m.DeleteLive(ctx, "x"), errFake)
	require.ErrorIs(t, m.SaveDegraded(ctx, "x", ""), errFake)
	require.ErrorIs(t, m.ClearDegraded(ctx, "x"), errFake)
	require.ErrorIs(t, m.AppendEvent(ctx, &Event{}), errFake)
	require.ErrorIs(t, m.SaveDesired(ctx, "img", Desired{}), errFake)
	require.ErrorIs(t, m.SaveAborted(ctx, "g", "k"), errFake)
	require.ErrorIs(t, m.ClearAborted(ctx, "g"), errFake)
	require.ErrorIs(t, m.SaveHookRun(ctx, "t", &HookRun{Hook: "h"}), errFake)
	require.ErrorIs(t, m.DeleteHookRuns(ctx, "t"), errFake)
	_, err = m.Events(ctx, EventQuery{})
	require.ErrorIs(t, err, errFake)
	_, err = m.Load(ctx)
	require.ErrorIs(t, err, errFake)
}

func TestHistoryThroughController(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	h.prime()
	h.release(imgA, d2)

	events, err := h.c.Events(h.ctx, EventQuery{Group: "a"})
	require.NoError(t, err)
	require.NotEmpty(t, events)
	require.Equal(t, "batch.started", events[0].Action)
	require.Equal(t, ControllerActor, events[0].Actor)
}
