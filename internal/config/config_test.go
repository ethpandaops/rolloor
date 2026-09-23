package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
	require.Equal(t, 3, cfg.Inspect.FailureThreshold)
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
  pauseAfterFirstBatch: true
  waves: false
  soak:
    duration: 5m
    interval: 30s
    failureLimit: 0
strategies:
  fast:
    batchSize: 50%
    waves: true
    soak:
      duration: 0s
defaultPolicy:
  strategy: fast
`))
	require.NoError(t, err)

	base := cfg.Strategy
	require.Equal(t, 1, base.BatchFor(1, 30))
	require.Equal(t, 6, base.BatchFor(2, 30))
	require.False(t, base.UsesWaves())
	require.Equal(t, 0, base.Soak.FailureLimit, "an explicit zero is kept")

	fast, ok := cfg.StrategyNamed("fast")
	require.True(t, ok)
	require.Equal(t, 15, fast.BatchFor(2, 30))
	require.Equal(t, 1, fast.BatchFor(1, 30), "firstBatch comes from the default")
	require.True(t, fast.PauseAfterFirstBatch)
	require.True(t, fast.UsesWaves())
	require.False(t, cfg.Strategy.UsesWaves(), "overriding waves leaves the default alone")
	require.Equal(t, time.Duration(0), fast.Soak.Duration)
	require.Equal(t, 30*time.Second, fast.Soak.Interval)
	require.Equal(t, []string{"fast"}, cfg.StrategyNames())

	_, ok = cfg.StrategyNamed("warp")
	require.False(t, ok)

	def, ok := cfg.StrategyNamed("")
	require.True(t, ok)
	require.Equal(t, base.BatchSize, def.BatchSize)

	zero, err := Parse([]byte("environment: x\ndisruptionBudget:\n  maxUnavailable: 0\n"))
	require.NoError(t, err)
	require.InDelta(t, 0, zero.DisruptionBudget.MaxUnavailable.OfWeight(1000), 0.001)
}

func TestValidateErrors(t *testing.T) {
	cases := map[string]string{
		"missing environment":            "listen: ':1'\n",
		"bad default strategy":           "environment: x\ndefaultPolicy: {strategy: warp}\n",
		"bad mode":                       "environment: x\ndefaultPolicy: {mode: sometimes}\n",
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
