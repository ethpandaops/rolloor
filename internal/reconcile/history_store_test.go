package reconcile

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemoryHistoryIdentityAndAtomicDecisions(t *testing.T) {
	h := newHarness(t, testConfig, testTargets)
	ctx, m := h.ctx, h.store
	identity := h.c.HistoryID()
	snap, err := m.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, identity, snap.HistoryID)

	again, err := m.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, snap.HistoryID, again.HistoryID)

	other, err := NewMemoryStore().Load(ctx)
	require.NoError(t, err)
	require.NotEqual(t, snap.HistoryID, other.HistoryID)

	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	r := &Rollout{ID: "r", Group: "a", State: Running, CreatedAt: now}
	require.NoError(t, m.SaveRollout(ctx, r))
	require.NoError(t, m.SaveDesired(ctx, imgA, Desired{Digest: d1}))
	require.NoError(t, m.SaveAborted(ctx, "old", "old-build"))
	require.NoError(t, m.SaveSuspension(ctx, &Suspension{ID: "expired"}))
	require.NoError(t, m.DeleteSuspension(ctx, "expired"))

	r.State = Halted
	d := &Decision{Rollouts: []*Rollout{r}, ClearAborted: []string{"old"},
		Desired: map[string]Desired{imgA: {Digest: d2}},
		Events:  []Event{{ID: 1, At: now, Action: eventRolloutHalted, Rollout: r.ID}}}
	m.FailRollouts = errFake
	require.ErrorIs(t, m.SaveDecision(ctx, d), errFake)
	snap, err = m.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, Running, snap.Rollouts[0].State)
	require.Equal(t, d1, snap.Desired[imgA].Digest)
	require.Equal(t, "old-build", snap.Aborted["old"])

	events, err := m.Events(ctx, &EventQuery{})
	require.NoError(t, err)
	require.Empty(t, events)

	m.FailRollouts = nil
	require.NoError(t, m.SaveDecision(ctx, d))
	snap, err = m.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, Halted, snap.Rollouts[0].State)
	require.Equal(t, d2, snap.Desired[imgA].Digest)
	require.NotContains(t, snap.Aborted, "old")
	require.Equal(t, int64(2), snap.NextEventID)

	events, err = m.Events(ctx, &EventQuery{})
	require.NoError(t, err)
	require.Equal(t, eventRolloutHalted, events[0].Action)

	m.Fail = errFake
	snap.Events = []Event{{ID: 2, At: now, Action: "retry", Rollout: r.ID}}
	snap.Rollouts[0].State = Running
	require.ErrorIs(t, m.ReplaceDecisions(ctx, snap), errFake)
	m.Fail = nil
	durable, err := m.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, Halted, durable.Rollouts[0].State)
	require.Equal(t, int64(2), durable.NextEventID)
	require.NoError(t, m.ReplaceDecisions(ctx, snap))
	durable, err = m.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, Running, durable.Rollouts[0].State)
	require.Equal(t, int64(3), durable.NextEventID)
	require.Equal(t, identity, h.newController().HistoryID())
}
