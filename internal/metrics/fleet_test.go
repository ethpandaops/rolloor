package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/reconcile"
	"github.com/ethpandaops/rolloor/internal/registry"
	"github.com/ethpandaops/rolloor/internal/targets"
)

type metricClock struct {
	at time.Time
}

func (c *metricClock) Now() time.Time { return c.at }

type metricWorld struct {
	fake
	resolveError error
}

func (w *metricWorld) Resolve(ctx context.Context, ref string) (registry.Resolved, error) {
	if w.resolveError != nil {
		return registry.Resolved{}, w.resolveError
	}

	return w.fake.Resolve(ctx, ref)
}

func metricGauge(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()

	families, err := reg.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() == name {
			require.Len(t, family.GetMetric(), 1)

			return family.GetMetric()[0].GetGauge().GetValue()
		}
	}

	t.Fatalf("missing metric %s", name)

	return 0
}

func TestFleetMetricsWithoutRollouts(t *testing.T) {
	ctx := context.Background()
	cfg, err := config.Parse([]byte("environment: t\nhooks: {dir: /tmp}\nreadinessProbe: {failureThreshold: 1}\ndisruptionBudget: {maxUnavailable: 25%}\n"))
	require.NoError(t, err)
	set, err := targets.Parse([]byte("- {id: a, node: n1, weight: 10, image: org/a:t}\n- {id: b, node: n2, weight: 10, image: org/a:t}\n"), &targets.Rules{KnownHooks: config.TargetHooks})
	require.NoError(t, err)

	world := &metricWorld{fake: fake{digest: "sha256:1"}}
	clock := &metricClock{at: time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)}
	disk := reconcile.NewMemoryStore()
	c, err := reconcile.New(ctx, &reconcile.Options{Config: cfg, Targets: func() *targets.Set { return set }, Resolver: world, Runner: world, Store: disk, Clock: clock, Log: logrus.New()})
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	reg.MustRegister(NewCollector(c))

	require.Equal(t, float64(1), metricGauge(t, reg, "rolloor_fleet_unavailable_weight_ratio"))
	require.Zero(t, metricGauge(t, reg, "rolloor_last_inspect_timestamp_seconds"))
	require.Zero(t, metricGauge(t, reg, "rolloor_last_probe_timestamp_seconds"))

	c.InspectAll(ctx)
	c.ProbeAll(ctx)
	require.NoError(t, c.Tick(ctx))
	require.Empty(t, c.Rollouts())
	require.Zero(t, metricGauge(t, reg, "rolloor_fleet_unavailable_weight_ratio"))
	require.Equal(t, float64(clock.at.Unix()), metricGauge(t, reg, "rolloor_last_inspect_timestamp_seconds"))
	require.Equal(t, float64(clock.at.Unix()), metricGauge(t, reg, "rolloor_last_probe_timestamp_seconds"))

	world.fail = true

	c.ProbeAll(ctx)
	require.Empty(t, c.Rollouts())
	require.Equal(t, float64(1), metricGauge(t, reg, "rolloor_fleet_unavailable_weight_ratio"))
	require.Equal(t, float64(4), metricGauge(t, reg, "rolloor_disruption_budget_ratio"))

	disk.Fail = errors.New("storage unavailable")

	c.Refresh(ctx, "operator")
	require.Equal(t, float64(1), metricGauge(t, reg, "rolloor_store_writes_owed"))

	disk.Fail = nil

	require.NoError(t, c.Tick(ctx))
	require.Zero(t, metricGauge(t, reg, "rolloor_store_writes_owed"))

	c.ReportTargetsError(ctx, errors.New("invalid targets"))
	require.Equal(t, float64(1), metricGauge(t, reg, "rolloor_targets_load_failed"))
	c.ReportTargetsError(ctx, nil)
	require.Zero(t, metricGauge(t, reg, "rolloor_targets_load_failed"))

	world.resolveError = errors.New("registry unavailable")

	c.Refresh(ctx, "operator")
	require.NoError(t, c.Tick(ctx))
	require.Equal(t, float64(1), metricGauge(t, reg, "rolloor_registry_resolve_failed"))

	world.resolveError = nil
	world.fail = false

	c.Refresh(ctx, "operator")
	c.ProbeAll(ctx)
	require.NoError(t, c.Tick(ctx))
	require.Zero(t, metricGauge(t, reg, "rolloor_fleet_unavailable_weight_ratio"))
	require.Empty(t, c.Fleet().Resolve)
}

func TestConfigReloadAndEffectivePauseMetrics(t *testing.T) {
	ctx := context.Background()
	cfg, err := config.Parse([]byte("environment: t\nhooks: {dir: /tmp}\nlabels: {group: client}\n"))
	require.NoError(t, err)
	set, err := targets.Parse([]byte("- {id: a, node: n1, weight: 1, image: org/a:t, labels: {client: a}}\n"), &targets.Rules{GroupLabel: "client", KnownHooks: config.TargetHooks})
	require.NoError(t, err)

	world := &metricWorld{fake: fake{digest: "sha256:paused"}}
	c, err := reconcile.New(ctx, &reconcile.Options{Config: cfg, Targets: func() *targets.Set { return set }, Resolver: world, Runner: world, Store: reconcile.NewMemoryStore(), Log: logrus.New()})
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	reg.MustRegister(NewCollector(c))

	c.ReportConfigError(errors.New("load failed"))
	require.Equal(t, float64(1), metricGauge(t, reg, "rolloor_config_reload_failed"))
	require.Zero(t, metricGauge(t, reg, "rolloor_config_paused"))
	require.Zero(t, metricGauge(t, reg, "rolloor_group_paused"))
	require.NoError(t, c.ConfigureGroups(ctx, false, map[string]config.Group{"a": {Paused: true}}))
	require.Zero(t, metricGauge(t, reg, "rolloor_config_paused"))
	require.Equal(t, float64(1), metricGauge(t, reg, "rolloor_group_paused"))
	require.NoError(t, c.ConfigureGroups(ctx, true, nil))
	c.ReportConfigError(nil)
	require.Zero(t, metricGauge(t, reg, "rolloor_config_reload_failed"))
	require.Equal(t, float64(1), metricGauge(t, reg, "rolloor_config_paused"))
	require.Equal(t, float64(1), metricGauge(t, reg, "rolloor_group_paused"))
	require.NoError(t, c.ConfigureGroups(ctx, false, nil))
	require.Zero(t, metricGauge(t, reg, "rolloor_config_paused"))
	require.Zero(t, metricGauge(t, reg, "rolloor_group_paused"))
}
