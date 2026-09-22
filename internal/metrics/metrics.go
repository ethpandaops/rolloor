// Package metrics exposes the controller's state to Prometheus.
package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ethpandaops/rolloor/internal/hooks"
	"github.com/ethpandaops/rolloor/internal/reconcile"
)

const (
	namespace  = "rolloor"
	labelID    = "id"
	labelGroup = "group"
	labelHook  = "hook"
)

// Collector reads the controller's views on every scrape. Target ids are
// bounded by the targets file, so they are acceptable as labels.
type Collector struct {
	c *reconcile.Controller

	targetInfo   *prometheus.Desc
	targetSync   *prometheus.Desc
	targetHealth *prometheus.Desc
	rolloutState *prometheus.Desc
	budgetRatio  *prometheus.Desc
	envPassing   *prometheus.Desc
	envCheckedAt *prometheus.Desc
	lastTick     *prometheus.Desc
}

var _ prometheus.Collector = (*Collector)(nil)

// NewCollector builds the descriptors.
func NewCollector(c *reconcile.Controller) *Collector {
	return &Collector{
		c:            c,
		targetInfo:   prometheus.NewDesc(namespace+"_target_info", "One series per target with its current digests.", []string{labelID, "node", labelGroup, "owner", "image", "desired", "live"}, nil),
		targetSync:   prometheus.NewDesc(namespace+"_target_sync", "0 synced, 1 out of sync, 2 unknown.", []string{labelID, labelGroup}, nil),
		targetHealth: prometheus.NewDesc(namespace+"_target_health", "0 healthy, 1 progressing, 2 degraded, 3 suspended.", []string{labelID, labelGroup}, nil),
		rolloutState: prometheus.NewDesc(namespace+"_rollout_state", "1 for the state a rollout is in.", []string{"rollout", labelGroup, "state"}, nil),
		budgetRatio:  prometheus.NewDesc(namespace+"_budget_ratio", "In-flight weight over the budget.", nil, nil),
		envPassing:   prometheus.NewDesc(namespace+"_environment_check_passing", "1 when the environment check passes.", nil, nil),
		envCheckedAt: prometheus.NewDesc(namespace+"_environment_checked_at_seconds", "When the environment check last ran.", nil, nil),
		lastTick:     prometheus.NewDesc(namespace+"_last_tick_timestamp_seconds", "When the reconcile loop last completed a pass.", nil, nil),
	}
}

// Describe sends every descriptor.
func (m *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{m.targetInfo, m.targetSync, m.targetHealth, m.rolloutState, m.budgetRatio, m.envPassing, m.envCheckedAt, m.lastTick} {
		ch <- d
	}
}

// Collect reads the views.
func (m *Collector) Collect(ch chan<- prometheus.Metric) {
	views := m.c.Targets(nil)

	for i := range views {
		v := &views[i]

		for _, metric := range []prometheus.Metric{
			prometheus.MustNewConstMetric(m.targetInfo, prometheus.GaugeValue, 1, v.ID, v.Node, v.Group, v.Owner, v.Image, v.Desired, v.Live),
			prometheus.MustNewConstMetric(m.targetSync, prometheus.GaugeValue, syncValue(v.Sync), v.ID, v.Group),
			prometheus.MustNewConstMetric(m.targetHealth, prometheus.GaugeValue, healthValue(v.Health), v.ID, v.Group),
		} {
			ch <- metric
		}
	}

	var ratio float64

	rollouts := m.c.Rollouts()

	for i := range rollouts {
		r := &rollouts[i]
		if r.State.Active() {
			ch <- prometheus.MustNewConstMetric(m.rolloutState, prometheus.GaugeValue, 1, r.ID, r.Group, string(r.State))

			if r.BudgetTotal > 0 {
				ratio = r.BudgetInUse / r.BudgetTotal
			}
		}
	}

	ch <- prometheus.MustNewConstMetric(m.budgetRatio, prometheus.GaugeValue, ratio)

	ok, _, at := m.c.EnvironmentStatus()

	for _, metric := range []prometheus.Metric{
		prometheus.MustNewConstMetric(m.envPassing, prometheus.GaugeValue, boolValue(ok)),
		prometheus.MustNewConstMetric(m.envCheckedAt, prometheus.GaugeValue, float64(at.Unix())),
		prometheus.MustNewConstMetric(m.lastTick, prometheus.GaugeValue, float64(m.c.LastTick().Unix())),
	} {
		ch <- metric
	}
}

func syncValue(s reconcile.SyncState) float64 {
	switch s {
	case reconcile.Synced:
		return 0
	case reconcile.OutOfSync:
		return 1
	case reconcile.Unknown:
		return 2
	}

	return 2
}

func healthValue(h reconcile.Health) float64 {
	switch h {
	case reconcile.Healthy:
		return 0
	case reconcile.Progressing:
		return 1
	case reconcile.Degraded:
		return 2
	case reconcile.Suspended:
		return 3
	}

	return 0
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}

	return 0
}

// HookRunner wraps a runner and records how each hook does.
type HookRunner struct {
	inner    reconcile.Runner
	duration *prometheus.HistogramVec
	runs     *prometheus.CounterVec
	failures *prometheus.CounterVec
}

var _ reconcile.Runner = (*HookRunner)(nil)

// NewHookRunner registers the hook metrics on reg and wraps inner.
func NewHookRunner(inner reconcile.Runner, reg prometheus.Registerer) *HookRunner {
	h := &HookRunner{
		inner: inner,
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "hook_duration_seconds", Help: "How long each hook program took.",
			Buckets: []float64{.1, .5, 1, 2, 5, 10, 30, 60, 120},
		}, []string{labelHook}),
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "hook_runs_total", Help: "Hook runs by outcome: ok, failed, error.",
		}, []string{labelHook, "outcome"}),
		failures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "hook_failures_total", Help: "Hook runs that did not pass, including ones that could not run.",
		}, []string{labelHook}),
	}

	reg.MustRegister(h.duration, h.runs, h.failures)

	return h
}

// Run delegates and records.
func (h *HookRunner) Run(ctx context.Context, program, hook, targetID string, input any) (hooks.Result, error) {
	start := time.Now()
	res, err := h.inner.Run(ctx, program, hook, targetID, input)

	h.duration.WithLabelValues(hook).Observe(time.Since(start).Seconds())

	outcome := "ok"

	switch {
	case err != nil:
		outcome = "error"
	case !res.OK:
		outcome = "failed"
	}

	h.runs.WithLabelValues(hook, outcome).Inc()

	if outcome != "ok" {
		h.failures.WithLabelValues(hook).Inc()
	}

	return res, err
}
