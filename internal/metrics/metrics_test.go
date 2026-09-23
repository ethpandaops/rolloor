package metrics

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/hooks"
	"github.com/ethpandaops/rolloor/internal/reconcile"
	"github.com/ethpandaops/rolloor/internal/registry"
	"github.com/ethpandaops/rolloor/internal/targets"
)

type fake struct {
	digest string
	fail   bool
	err    error
}

func (f *fake) Resolve(context.Context, string) (registry.Resolved, error) {
	return registry.Resolved{Digest: f.digest}, nil
}

func (f *fake) Run(_ context.Context, program, hook, _ string, _ any) (hooks.Result, error) {
	if f.err != nil {
		return hooks.Result{}, f.err
	}

	if hook == config.HookInspect {
		return hooks.Result{Program: program, OK: true, Reason: "sha256:1"}, nil
	}

	return hooks.Result{Program: program, OK: !f.fail}, nil
}

func TestCollector(t *testing.T) {
	ctx := context.Background()

	cfg, err := config.Parse([]byte("environment: t\nlabels: {group: client, owner: owner}\nhooks: {dir: /tmp}\n"))
	require.NoError(t, err)

	set, err := targets.Parse([]byte(`
- {id: a, node: n1, weight: 10, image: org/a:t, labels: {client: a, owner: a}}
- {id: b, node: n2, weight: 10, image: org/a:t, labels: {client: b, owner: b}}
`), &targets.Rules{GroupLabel: "client", OwnerLabel: "owner", KnownHooks: config.TargetHooks})
	require.NoError(t, err)

	world := &fake{digest: "sha256:2"}

	c, err := reconcile.New(ctx, &reconcile.Options{Config: cfg, Targets: func() *targets.Set { return set }, Resolver: world, Runner: world,
		Store: reconcile.NewMemoryStore(), Log: logrus.New()})
	require.NoError(t, err)

	c.InspectAll(ctx)
	require.NoError(t, c.Tick(ctx))

	reg := prometheus.NewRegistry()
	reg.MustRegister(NewCollector(c))

	families, err := reg.Gather()
	require.NoError(t, err)

	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}

	require.Contains(t, names, "rolloor_target_info")
	require.Contains(t, names, "rolloor_rollout_state")
	require.Contains(t, names, "rolloor_disruption_budget_ratio")
	require.Contains(t, names, "rolloor_environment_check_passing")

	out, err := testutil.GatherAndCount(reg)
	require.NoError(t, err)
	require.Positive(t, out)

	require.InDelta(t, 0, syncValue(reconcile.Synced), 0)
	require.InDelta(t, 1, syncValue(reconcile.OutOfSync), 0)
	require.InDelta(t, 2, syncValue(reconcile.Unknown), 0)
	require.InDelta(t, 2, syncValue(reconcile.SyncState("x")), 0)
	require.InDelta(t, 0, healthValue(reconcile.Healthy), 0)
	require.InDelta(t, 1, healthValue(reconcile.Progressing), 0)
	require.InDelta(t, 2, healthValue(reconcile.Degraded), 0)
	require.InDelta(t, 3, healthValue(reconcile.Suspended), 0)
	require.InDelta(t, 0, healthValue(reconcile.Health("x")), 0)
}

func TestHookRunner(t *testing.T) {
	reg := prometheus.NewRegistry()
	world := &fake{}
	r := NewHookRunner(world, reg)

	res, err := r.Run(context.Background(), "p", "ready", "t", nil)
	require.NoError(t, err)
	require.True(t, res.OK)

	world.fail = true
	res, err = r.Run(context.Background(), "p", "ready", "t", nil)
	require.NoError(t, err)
	require.False(t, res.OK)

	world.err = errors.New("boom")
	_, err = r.Run(context.Background(), "p", "ready", "t", nil)
	require.Error(t, err)

	families, err := reg.Gather()
	require.NoError(t, err)

	var text strings.Builder

	for _, f := range families {
		for _, m := range f.GetMetric() {
			if f.GetName() == "rolloor_hook_runs_total" {
				text.WriteString(m.String())
			}
		}
	}

	require.Contains(t, text.String(), `value:"ok"`)
	require.Contains(t, text.String(), `value:"failed"`)
	require.Contains(t, text.String(), `value:"error"`)
}
