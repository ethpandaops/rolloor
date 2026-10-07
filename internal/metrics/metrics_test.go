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
	code   int
	err    error
}

func (f *fake) Resolve(context.Context, string) (registry.Resolved, error) {
	return registry.Resolved{Digest: f.digest}, nil
}

func (f *fake) Run(_ context.Context, program, hook, _ string, _ any) (hooks.Result, error) {
	if f.err != nil {
		return hooks.Result{ExitCode: -1}, f.err
	}

	if hook == config.HookInspect {
		return hooks.Result{Program: program, OK: true, Reason: "sha256:1"}, nil
	}

	code := f.code
	if f.fail && code == 0 {
		code = 1
	}

	return hooks.Result{Program: program, OK: code == 0, ExitCode: code}, nil
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
	c.ProbeAll(ctx)
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

	require.InDelta(t, 0, syncValue(reconcile.Synced), 0)
	require.InDelta(t, 1, syncValue(reconcile.OutOfSync), 0)
	require.InDelta(t, 2, syncValue(reconcile.Unknown), 0)
	require.InDelta(t, 0, healthValue(reconcile.Healthy), 0)
	require.InDelta(t, 1, healthValue(reconcile.Progressing), 0)
	require.InDelta(t, 2, healthValue(reconcile.Degraded), 0)
	require.InDelta(t, 3, healthValue(reconcile.Suspended), 0)
	require.InDelta(t, 4, healthValue(reconcile.HealthUnknown), 0)
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

	world.err, world.fail = nil, false
	for _, code := range []int{3, -1} {
		world.code = code
		_, err = r.Run(context.Background(), "p", "ready", "t", nil)
		require.NoError(t, err)
	}

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP rolloor_hook_runs_total Hook runs by outcome: ok, failed, error.
# TYPE rolloor_hook_runs_total counter
rolloor_hook_runs_total{hook="ready",outcome="error"} 3
rolloor_hook_runs_total{hook="ready",outcome="failed"} 1
rolloor_hook_runs_total{hook="ready",outcome="ok"} 1
# HELP rolloor_hook_failures_total Hook runs that did not pass, including ones that could not run.
# TYPE rolloor_hook_failures_total counter
rolloor_hook_failures_total{hook="ready"} 4
`), "rolloor_hook_runs_total", "rolloor_hook_failures_total"))
}
