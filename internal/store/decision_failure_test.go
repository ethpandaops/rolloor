package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/reconcile"
)

func TestDecisionRecordFailuresRollBackOtherRecordsAndHistory(t *testing.T) {
	cases := []struct{ name, table, operation string }{
		{"rollout", "rollouts", opInsert},
		{"suspension", tableSuspensions, opInsert},
		{"delete suspension", tableSuspensions, opDelete},
		{"delete live", "live", opDelete},
		{"delete target quarantine", tableDegraded, opDelete},
		{tableDesired, tableDesired, opInsert},
		{"quarantine", tableDegraded, opInsert},
		{"abort", tableAborted, opInsert},
		{"clear abort", tableAborted, opDelete},
		{"clear quarantine", tableDegraded, opDelete},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s, _ := open(t)
			now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
			r := &reconcile.Rollout{ID: r1, Group: "a", State: reconcile.Running, CreatedAt: now}
			require.NoError(t, s.SaveRollout(ctx, r))
			require.NoError(t, s.SaveSuspension(ctx, &reconcile.Suspension{ID: s1}))
			require.NoError(t, s.SaveLive(ctx, t1, &reconcile.Live{Digest: sha}))
			require.NoError(t, s.SaveDegraded(ctx, t1, "bad"))
			require.NoError(t, s.SaveDesired(ctx, img, reconcile.Desired{Digest: sha}))
			require.NoError(t, s.SaveAborted(ctx, "a", "old"))
			before, err := s.Load(ctx)
			require.NoError(t, err)

			r.State = reconcile.Halted
			d := &reconcile.Decision{
				Rollouts: []*reconcile.Rollout{{ID: newValue, Group: "b", State: reconcile.Running, CreatedAt: now}, r},
				Events:   []reconcile.Event{{ID: 1, At: now, Action: eventHalted, Rollout: r1}},
			}
			condition := ""

			switch tc.name {
			case "rollout":
				condition = "WHEN NEW.id = '" + r1 + "'"
			case "suspension":
				d.Suspensions = []reconcile.Suspension{{ID: s2}}
			case "delete suspension":
				d.DeleteSuspensions = []string{s1}
			case "delete live", "delete target quarantine":
				d.DeleteTargets = []string{t1}
			case tableDesired:
				d.Desired = map[string]reconcile.Desired{img: {Digest: "sha256:2"}}
			case "quarantine":
				d.Degraded = map[string]string{t1: newValue}
			case "abort":
				d.Aborted = map[string]string{"a": newValue}
			case "clear abort":
				d.ClearAborted = []string{"a"}
			case "clear quarantine":
				d.ClearDegraded = []string{t1}
			}

			_, err = s.db.ExecContext(ctx, fmt.Sprintf("CREATE TRIGGER refuse_record BEFORE %s ON %s %s BEGIN SELECT RAISE(ABORT, 'refused'); END", tc.operation, tc.table, condition))
			require.NoError(t, err)
			require.ErrorContains(t, s.SaveDecision(ctx, d), "refused")
			after, err := s.Load(ctx)
			require.NoError(t, err)
			require.Equal(t, before, after)

			events, err := s.Events(ctx, &reconcile.EventQuery{})
			require.NoError(t, err)
			require.Empty(t, events)
		})
	}
}
