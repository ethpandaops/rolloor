package store

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/reconcile"
)

func TestHistoryIdentitySurvivesReopenAndChangesWithReplacement(t *testing.T) {
	ctx := context.Background()
	s, dir := open(t)
	snap, err := s.Load(ctx)
	require.NoError(t, err)

	identity := snap.HistoryID
	require.NotEmpty(t, identity)
	require.NoError(t, s.Close())

	again, err := Open(ctx, dir)
	require.NoError(t, err)
	snap, err = again.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, identity, snap.HistoryID)
	require.NoError(t, again.Close())
	require.NoError(t, os.Remove(filepath.Join(dir, "rolloor.db")))
	replacement, err := Open(ctx, dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = replacement.Close() })

	snap, err = replacement.Load(ctx)
	require.NoError(t, err)
	require.NotEqual(t, identity, snap.HistoryID)
}

func TestDecisionAndReplayRollBackWhenHistoryFails(t *testing.T) {
	ctx := context.Background()
	s, _ := open(t)
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	r := &reconcile.Rollout{ID: r1, Group: "a", State: reconcile.Running, CreatedAt: now}
	require.NoError(t, s.SaveRollout(ctx, r))

	e := reconcile.Event{ID: 1, At: now, Actor: reconcile.ControllerActor, Action: eventHalted, Rollout: r1, Group: "a"}
	r.State = reconcile.Halted
	_, err := s.db.ExecContext(ctx, `CREATE TRIGGER refuse_events BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT, 'refused'); END`)
	require.NoError(t, err)

	decision := &reconcile.Decision{Rollouts: []*reconcile.Rollout{r}, Degraded: map[string]string{t1: "bad"}, Events: []reconcile.Event{e}}
	require.ErrorContains(t, s.SaveDecision(ctx, decision), "refused")
	snap, err := s.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, reconcile.Running, snap.Rollouts[0].State)
	require.Empty(t, snap.Degraded)
	snap.Rollouts = []*reconcile.Rollout{r}
	snap.Degraded[t1] = "bad"
	snap.Events = []reconcile.Event{e}
	require.ErrorContains(t, s.ReplaceDecisions(ctx, snap), "refused")
	durable, err := s.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, reconcile.Running, durable.Rollouts[0].State)
	require.Empty(t, durable.Degraded)

	_, err = s.db.ExecContext(ctx, `DROP TRIGGER refuse_events`)
	require.NoError(t, err)
	require.NoError(t, s.ReplaceDecisions(ctx, snap))
	require.NoError(t, s.Close())
}

func TestCrashAfterDecisionWriteCannotLoseHistory(t *testing.T) {
	ctx := context.Background()
	if dir := os.Getenv("ROLLOOR_HISTORY_CRASH_DIR"); dir != "" {
		s, err := Open(ctx, dir)
		if err != nil {
			os.Exit(2)
		}

		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			os.Exit(3)
		}

		w := &SQLite{tx: tx}

		r := &reconcile.Rollout{ID: r1, Group: "a", State: reconcile.Halted, CreatedAt: time.Now()}
		if err := w.SaveRollout(ctx, r); err != nil {
			os.Exit(4)
		}

		os.Exit(23)
	}

	s, dir := open(t)
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	r := &reconcile.Rollout{ID: r1, Group: "a", State: reconcile.Running, CreatedAt: now}
	require.NoError(t, s.SaveRollout(ctx, r))

	childCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(childCtx, os.Args[0], "-test.run=^TestCrashAfterDecisionWriteCannotLoseHistory$")

	cmd.Env = append(os.Environ(), "ROLLOOR_HISTORY_CRASH_DIR="+dir)
	err := cmd.Run()

	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, 23, exit.ExitCode())

	snap, err := s.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, reconcile.Running, snap.Rollouts[0].State)

	events, err := s.Events(ctx, &reconcile.EventQuery{})
	require.NoError(t, err)
	require.Empty(t, events)

	r.State = reconcile.Halted
	require.NoError(t, s.SaveDecision(ctx, &reconcile.Decision{Rollouts: []*reconcile.Rollout{r}, Events: []reconcile.Event{{ID: 1, At: now, Actor: reconcile.ControllerActor, Action: eventHalted, Rollout: r1}}}))
	snap, err = s.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, reconcile.Halted, snap.Rollouts[0].State)

	events, err = s.Events(ctx, &reconcile.EventQuery{})
	require.NoError(t, err)
	require.Equal(t, eventHalted, events[0].Action)
}

func TestOperatorDecisionIsAllOrNothing(t *testing.T) {
	for _, action := range []string{"suspend", "resume", eventSync, "digest.changed", "rollout.aborted"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			s, _ := open(t)
			now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
			r := &reconcile.Rollout{ID: r1, Group: "a", State: reconcile.Running, CreatedAt: now}
			require.NoError(t, s.SaveRollout(ctx, r))
			require.NoError(t, s.SaveSuspension(ctx, &reconcile.Suspension{ID: s1}))
			require.NoError(t, s.SaveSuspension(ctx, &reconcile.Suspension{ID: s2}))
			require.NoError(t, s.SaveAborted(ctx, "a", "old"))
			require.NoError(t, s.SaveDegraded(ctx, t1, "bad"))
			before, err := s.Load(ctx)
			require.NoError(t, err)

			d := &reconcile.Decision{Events: []reconcile.Event{{ID: 1, At: now, Action: action, Group: "a"}}}
			switch action {
			case "suspend":
				d.Suspensions = []reconcile.Suspension{{ID: "s3", Reason: "maintenance"}}
			case "resume":
				d.DeleteSuspensions = []string{s1, s2}
			case eventSync:
				d.ClearAborted = []string{"a"}
				r.Human = true
				d.Rollouts = []*reconcile.Rollout{r}
			case "digest.changed":
				d.Desired = map[string]reconcile.Desired{img: {Digest: sha, ResolvedAt: now}}
			case "rollout.aborted":
				r.State = reconcile.Aborted
				d.Rollouts = []*reconcile.Rollout{r}
				d.Aborted = map[string]string{"a": newValue}
				d.ClearDegraded = []string{t1}
			}

			_, err = s.db.ExecContext(ctx, `CREATE TRIGGER refuse_events BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT, 'refused'); END`)
			require.NoError(t, err)
			require.ErrorContains(t, s.SaveDecision(ctx, d), "refused")
			after, err := s.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, before, after)

			_, err = s.db.ExecContext(ctx, `DROP TRIGGER refuse_events`)
			require.NoError(t, err)
			require.NoError(t, s.SaveDecision(ctx, d))
			events, err := s.Events(ctx, &reconcile.EventQuery{})
			require.NoError(t, err)
			require.Equal(t, d.Events, events)
		})
	}
}

func TestForgettingTargetObservationsIsAtomic(t *testing.T) {
	ctx := context.Background()
	s, _ := open(t)
	require.NoError(t, s.SaveLive(ctx, t1, &reconcile.Live{Digest: sha, Readiness: reconcile.Readiness{Ready: true}}))
	require.NoError(t, s.SaveDegraded(ctx, t1, "bad"))
	require.NoError(t, s.SaveHookRun(ctx, t1, &reconcile.HookRun{Hook: "ready"}))
	before, err := s.Load(ctx)
	require.NoError(t, err)
	_, err = s.db.ExecContext(ctx, `CREATE TRIGGER refuse_forget BEFORE DELETE ON hook_runs BEGIN SELECT RAISE(ABORT, 'refused'); END`)
	require.NoError(t, err)
	require.ErrorContains(t, s.SaveDecision(ctx, &reconcile.Decision{DeleteTargets: []string{t1}}), "refused")
	after, err := s.Load(ctx)
	require.NoError(t, err)
	require.Equal(t, before, after)

	_, err = s.db.ExecContext(ctx, `DROP TRIGGER refuse_forget`)
	require.NoError(t, err)
	require.NoError(t, s.SaveDecision(ctx, &reconcile.Decision{DeleteTargets: []string{t1}}))
	after, err = s.Load(ctx)
	require.NoError(t, err)
	require.NotContains(t, after.Live, t1)
	require.NotContains(t, after.Degraded, t1)
	require.NotContains(t, after.HookRuns, t1)
}
