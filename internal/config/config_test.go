package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const fastStrategy = "fast"

func TestParseMinimal(t *testing.T) {
	cfg, err := Parse([]byte("environment: env-1\n"))
	require.NoError(t, err)
	require.Equal(t, ":8080", cfg.Listen)
	require.Equal(t, 60*time.Second, cfg.Registry.Poll)
	require.Equal(t, "10%", cfg.DisruptionBudget.MaxUnavailable.String())
	require.Equal(t, "10%", cfg.Strategy.BatchSize.String())
	require.Equal(t, 10*time.Minute, cfg.Strategy.Soak.Duration)
	require.Equal(t, time.Minute, cfg.Strategy.Soak.Interval)
	require.Equal(t, 1, cfg.Strategy.Soak.FailureLimit)
	require.Equal(t, 10*time.Minute, cfg.Strategy.ProgressDeadline)
	require.Equal(t, Retry{Limit: 5, Backoff: Backoff{Duration: 10 * time.Second, Factor: 2, MaxDuration: 3 * time.Minute}}, cfg.Strategy.Retry)
	require.Equal(t, 3, cfg.Inspect.FailureThreshold)
	require.Equal(t, ReadinessProbe{Period: 30 * time.Second, FailureThreshold: 3, SuccessThreshold: 1}, cfg.ReadinessProbe)
	require.Empty(t, cfg.Strategies)
	require.Equal(t, "inspect", cfg.Hooks.Defaults[HookInspect])
	require.True(t, cfg.Strategy.UsesWaves())
	require.Equal(t, 3, cfg.Strategy.BatchFor(1, 30), "without firstBatch, batch 1 is batchSize")
}

func TestParseRejectsUnknownField(t *testing.T) {
	_, err := Parse([]byte("environment: x\nbogus: 1\n"))
	require.Error(t, err)
}

func TestNamedStrategiesStartFromTheDefault(t *testing.T) {
	cfg, err := Parse([]byte(`
environment: x
strategy:
  firstBatch: 1
  batchSize: 20%
  waves: false
  progressDeadline: 90s
  retry: {limit: 3, backoff: {duration: 5s, factor: 3, maxDuration: 45s}}
  soak:
    duration: 5m
    interval: 30s
    failureLimit: 0
strategies:
  fast:
    batchSize: 50%
    waves: true
    progressDeadline: 2m
    retry: {limit: 0, backoff: {factor: 1}}
    soak:
      duration: 0s
`))
	require.NoError(t, err)

	base := cfg.Strategy
	require.Equal(t, 1, base.BatchFor(1, 30))
	require.Equal(t, 6, base.BatchFor(2, 30))
	require.False(t, base.UsesWaves())
	require.Equal(t, 0, base.Soak.FailureLimit, "an explicit zero is kept")

	fast, ok := cfg.StrategyNamed(fastStrategy)
	require.True(t, ok)
	require.Equal(t, 15, fast.BatchFor(2, 30))
	require.Equal(t, 1, fast.BatchFor(1, 30), "firstBatch comes from the default")
	require.True(t, fast.UsesWaves())
	require.False(t, cfg.Strategy.UsesWaves(), "overriding waves leaves the default alone")
	require.Equal(t, time.Duration(0), fast.Soak.Duration)
	require.Equal(t, 30*time.Second, fast.Soak.Interval)
	require.Equal(t, 0, fast.Soak.FailureLimit, "the default's explicit zero is inherited")
	require.Equal(t, 90*time.Second, base.ProgressDeadline)
	require.Equal(t, 2*time.Minute, fast.ProgressDeadline)
	require.Equal(t, Retry{Limit: 0, Backoff: Backoff{Duration: 5 * time.Second, Factor: 1, MaxDuration: 45 * time.Second}}, fast.Retry)
	require.Equal(t, Retry{Limit: 3, Backoff: Backoff{Duration: 5 * time.Second, Factor: 3, MaxDuration: 45 * time.Second}}, base.Retry)
	require.Equal(t, []string{fastStrategy}, cfg.StrategyNames())

	_, ok = cfg.StrategyNamed("warp")
	require.False(t, ok)

	def, ok := cfg.StrategyNamed("")
	require.True(t, ok)
	require.Equal(t, base.BatchSize, def.BatchSize)

	zero, err := Parse([]byte("environment: x\ndisruptionBudget:\n  maxUnavailable: 0\n"))
	require.NoError(t, err)
	require.InDelta(t, 0, zero.DisruptionBudget.MaxUnavailable.OfWeight(1000), 0.001)
}

func TestGroupsNameStrategiesAndPause(t *testing.T) {
	cfg, err := Parse([]byte(`
environment: x
paused: true
strategies:
  fast: {batchSize: 50%}
groups:
  a: {paused: true, strategy: fast}
  b: {}
`))
	require.NoError(t, err)
	require.True(t, cfg.Paused)
	require.Equal(t, map[string]Group{
		"a": {Paused: true, Strategy: fastStrategy},
		"b": {},
	}, cfg.Groups)

	require.NoError(t, cfg.ValidateGroups(map[string]Group{"a": {Strategy: fastStrategy}, "b": {Paused: true}}))
	require.ErrorContains(t, cfg.ValidateGroups(map[string]Group{"a": {Strategy: "warp"}}), `groups.a.strategy "warp" is not a strategy (have [fast])`)
	require.ErrorContains(t, cfg.ValidateGroups(map[string]Group{"": {Paused: true}}), "groups need a name")
}

func TestParseRejectsRemovedSettings(t *testing.T) {
	cases := map[string]struct{ raw, field string }{
		"default policy":          {"environment: x\ndefaultPolicy: {mode: automated}\n", "defaultPolicy"},
		"pause after first batch": {"environment: x\nstrategy: {pauseAfterFirstBatch: true}\n", "pauseAfterFirstBatch"},
		"named pause after first": {"environment: x\nstrategies: {fast: {pauseAfterFirstBatch: false}}\n", "pauseAfterFirstBatch"},
		"soak error limit":        {"environment: x\nstrategy: {soak: {consecutiveErrorLimit: 4}}\n", "consecutiveErrorLimit"},
		"named soak error limit":  {"environment: x\nstrategies: {fast: {soak: {consecutiveErrorLimit: 0}}}\n", "consecutiveErrorLimit"},
		"group mode":              {"environment: x\ngroups: {a: {mode: manual}}\n", "mode"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(tc.raw))
			require.ErrorContains(t, err, "field "+tc.field+" not found")
		})
	}
}

func TestValidateErrors(t *testing.T) {
	cases := map[string]string{
		"missing environment":            "listen: ':1'\n",
		"unknown group strategy":         "environment: x\nstrategies: {fast: {batchSize: 1}}\ngroups: {a: {strategy: warp}}\n",
		"oidc without issuer":            "environment: x\nauth: {mode: oidc}\n",
		"unknown hook default":           "environment: x\nhooks: {defaults: {discover: d}}\n",
		"batch selects nothing":          "environment: x\nstrategy: {batchSize: 0}\n",
		"first batch nothing":            "environment: x\nstrategy: {firstBatch: 0%}\n",
		"soak interval too big":          "environment: x\nstrategy: {soak: {duration: 1m, interval: 2m}}\n",
		"negative failure limit":         "environment: x\nstrategy: {soak: {failureLimit: -1}}\n",
		"bad named strategy":             "environment: x\nstrategies: {fast: {batchSize: 0}}\n",
		"unknown strategy field":         "environment: x\nstrategies: {fast: {speed: 1}}\n",
		"unnamed strategy":               "environment: x\nstrategies: {\"\": {batchSize: 1}}\n",
		"trusted token without audience": "environment: x\nauth: {mode: oidc, issuer: i, clientId: c, redirectUrl: r, trustedTokens: [{issuer: j}]}\n",
		"strategy not a mapping":         "environment: x\nstrategies: {fast: [1]}\n",
		"zero probe period":              "environment: x\nreadinessProbe: {period: 0s}\n",
		"negative probe failures":        "environment: x\nreadinessProbe: {failureThreshold: -1}\n",
		"zero probe successes":           "environment: x\nreadinessProbe: {successThreshold: 0}\n",
		"unknown hook settings":          "environment: x\nhooks: {probe: {program: check}}\n",
		"zero progress deadline":         "environment: x\nstrategy: {progressDeadline: 0s}\n",
		"negative retry limit":           "environment: x\nstrategy: {retry: {limit: -1}}\n",
		"zero backoff duration":          "environment: x\nstrategy: {retry: {backoff: {duration: 0s}}}\n",
		"zero backoff factor":            "environment: x\nstrategy: {retry: {backoff: {factor: 0}}}\n",
		"negative maximum backoff":       "environment: x\nstrategy: {retry: {backoff: {maxDuration: -1s}}}\n",
	}

	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(raw))
			require.Error(t, err)
		})
	}
}

func TestFraction(t *testing.T) {
	f, err := ParseFraction("10%")
	require.NoError(t, err)
	require.Equal(t, 3, f.Of(30))
	require.Equal(t, 1, f.Of(3))
	require.Equal(t, 0, f.Of(0))
	require.InDelta(t, 100.0, f.OfWeight(1000), 0.001)

	n, err := ParseFraction("5")
	require.NoError(t, err)
	require.Equal(t, 5, n.Of(30))
	require.Equal(t, 3, n.Of(3))

	_, err = ParseFraction("101%")
	require.Error(t, err)
	_, err = ParseFraction("-1")
	require.Error(t, err)
}
