package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/reconcile"
	"github.com/ethpandaops/rolloor/internal/targets"
)

const (
	r1 = "r1"
	t1 = "t1"
)

func open(t *testing.T) (*SQLite, string) {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "data")

	s, err := Open(context.Background(), dir)
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close() })

	return s, dir
}

func TestRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, dir := open(t)

	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	r := &reconcile.Rollout{ID: r1, Group: "a", State: reconcile.Running, Desired: map[string]string{"img": "sha256:1"}, CreatedAt: now,
		Targets: []reconcile.RolloutTarget{{ID: t1, Node: "n1", Phase: reconcile.PhasePending}}}
	require.NoError(t, s.SaveRollout(ctx, r))

	r.State = reconcile.Complete
	require.NoError(t, s.SaveRollout(ctx, r), "upsert")
	require.NoError(t, s.SaveRollout(ctx, &reconcile.Rollout{ID: "r0", Group: "b", CreatedAt: now}))

	require.NoError(t, s.SavePolicy(ctx, "a", reconcile.Policy{Mode: reconcile.ModeManual, Speed: "careful"}))
	require.NoError(t, s.SavePolicy(ctx, "a", reconcile.Policy{Mode: reconcile.ModeAutomated, Speed: "fast"}))

	sp := &reconcile.Suspension{ID: "s1", Selector: targets.Selector{"node": "n1"}, Reason: "x", Actor: "sam", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	require.NoError(t, s.SaveSuspension(ctx, sp))
	require.NoError(t, s.SaveSuspension(ctx, &reconcile.Suspension{ID: "s2"}))
	require.NoError(t, s.DeleteSuspension(ctx, "s2"))

	require.NoError(t, s.SaveLive(ctx, t1, reconcile.Live{Digest: "sha256:1", SeenAt: now}))
	require.NoError(t, s.SaveLive(ctx, "t2", reconcile.Live{Failures: 2}))
	require.NoError(t, s.DeleteLive(ctx, "t2"))

	require.NoError(t, s.SaveDegraded(ctx, t1, "bad"))
	require.NoError(t, s.SaveDegraded(ctx, t1, "worse"))
	require.NoError(t, s.SaveDegraded(ctx, "t3", "gone"))
	require.NoError(t, s.ClearDegraded(ctx, "t3"))

	for i := int64(1); i <= 3; i++ {
		e := &reconcile.Event{ID: i, At: now.Add(time.Duration(i) * time.Second), Actor: "sam", Action: "sync", Group: "a", Rollout: r1, Target: t1, Reason: "why"}
		if i == 2 {
			e.Group, e.Rollout, e.Target = "b", "r0", ""
		}

		require.NoError(t, s.AppendEvent(ctx, e))
	}

	snap, err := s.Load(ctx)
	require.NoError(t, err)
	require.Len(t, snap.Rollouts, 2)
	require.Equal(t, "r0", snap.Rollouts[0].ID)
	require.Equal(t, reconcile.Complete, snap.Rollouts[1].State)
	require.Equal(t, t1, snap.Rollouts[1].Targets[0].ID)
	require.Equal(t, "fast", snap.Policies["a"].Speed)
	require.Len(t, snap.Suspensions, 1)
	require.Equal(t, "n1", snap.Suspensions[0].Selector["node"])
	require.Equal(t, "sha256:1", snap.Live[t1].Digest)
	require.Len(t, snap.Live, 1)
	require.Equal(t, map[string]string{t1: "worse"}, snap.Degraded)
	require.Equal(t, int64(4), snap.NextEventID)

	events, err := s.Events(ctx, reconcile.EventQuery{})
	require.NoError(t, err)
	require.Len(t, events, 3)
	require.Equal(t, int64(3), events[0].ID)
	require.Equal(t, now.Add(3*time.Second), events[0].At)

	events, err = s.Events(ctx, reconcile.EventQuery{Group: "a", Rollout: r1, Target: t1, Limit: 1})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, int64(3), events[0].ID)

	events, err = s.Events(ctx, reconcile.EventQuery{Group: "b"})
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "r0", events[0].Rollout)

	// Reopening the same directory sees the same data.
	require.NoError(t, s.Close())

	again, err := Open(ctx, dir)
	require.NoError(t, err)

	t.Cleanup(func() { _ = again.Close() })

	snap, err = again.Load(ctx)
	require.NoError(t, err)
	require.Len(t, snap.Rollouts, 2)
	require.Equal(t, int64(4), snap.NextEventID)
}

func TestEmptyLoad(t *testing.T) {
	s, _ := open(t)

	snap, err := s.Load(context.Background())
	require.NoError(t, err)
	require.Empty(t, snap.Rollouts)
	require.Equal(t, int64(1), snap.NextEventID)
}

func TestOpenErrors(t *testing.T) {
	ctx := context.Background()

	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o644))

	_, err := Open(ctx, filepath.Join(file, "sub"))
	require.Error(t, err, "a file where the directory should be")

	// A directory holding a non-database file fails at the schema.
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "rolloor.db"), []byte("this is not sqlite"), 0o644))

	_, err = Open(ctx, dir)
	require.Error(t, err)
}

func TestClosedStoreFailsEveryCall(t *testing.T) {
	ctx := context.Background()
	s, _ := open(t)
	require.NoError(t, s.Close())

	require.Error(t, s.SaveRollout(ctx, &reconcile.Rollout{ID: "x"}))
	require.Error(t, s.SavePolicy(ctx, "g", reconcile.Policy{}))
	require.Error(t, s.SaveSuspension(ctx, &reconcile.Suspension{ID: "x"}))
	require.Error(t, s.DeleteSuspension(ctx, "x"))
	require.Error(t, s.SaveLive(ctx, "x", reconcile.Live{}))
	require.Error(t, s.DeleteLive(ctx, "x"))
	require.Error(t, s.SaveDegraded(ctx, "x", "r"))
	require.Error(t, s.ClearDegraded(ctx, "x"))
	require.Error(t, s.AppendEvent(ctx, &reconcile.Event{ID: 1}))

	_, err := s.Events(ctx, reconcile.EventQuery{})
	require.Error(t, err)
	_, err = s.Load(ctx)
	require.Error(t, err)
}

func TestCorruptRowsAreErrors(t *testing.T) {
	ctx := context.Background()
	s, _ := open(t)

	_, err := s.db.ExecContext(ctx, `INSERT INTO rollouts (id, group_name, state, created_at, data) VALUES ('bad', 'g', 's', 'now', 'not json')`)
	require.NoError(t, err)
	_, err = s.Load(ctx)
	require.ErrorContains(t, err, "load rollouts")
	_, err = s.db.ExecContext(ctx, `DELETE FROM rollouts`)
	require.NoError(t, err)

	_, err = s.db.ExecContext(ctx, `INSERT INTO policies (group_name, data) VALUES ('g', 'not json')`)
	require.NoError(t, err)
	_, err = s.Load(ctx)
	require.ErrorContains(t, err, "load policies")
	_, err = s.db.ExecContext(ctx, `DELETE FROM policies`)
	require.NoError(t, err)

	_, err = s.db.ExecContext(ctx, `INSERT INTO suspensions (id, data) VALUES ('s', 'not json')`)
	require.NoError(t, err)
	_, err = s.Load(ctx)
	require.ErrorContains(t, err, "load suspensions")
	_, err = s.db.ExecContext(ctx, `DELETE FROM suspensions`)
	require.NoError(t, err)

	_, err = s.db.ExecContext(ctx, `INSERT INTO live (target_id, data) VALUES ('t', 'not json')`)
	require.NoError(t, err)
	_, err = s.Load(ctx)
	require.ErrorContains(t, err, "load live")
	_, err = s.db.ExecContext(ctx, `DELETE FROM live`)
	require.NoError(t, err)

	_, err = s.db.ExecContext(ctx, `INSERT INTO events (id, at, actor, action) VALUES (1, 'not a time', 'a', 'b')`)
	require.NoError(t, err)
	_, err = s.Events(ctx, reconcile.EventQuery{})
	require.ErrorContains(t, err, "bad timestamp")
}
