package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseMinimal(t *testing.T) {
	cfg, err := Parse([]byte("environment: devnet-1\n"))
	require.NoError(t, err)
	require.Equal(t, ":8080", cfg.Listen)
	require.Equal(t, 60*time.Second, cfg.Registry.Poll)
	require.Equal(t, "10%", cfg.Budget.String())
	require.Equal(t, "normal", cfg.DefaultPolicy.Speed)
	require.Len(t, cfg.Presets, 4)
	require.Equal(t, "inspect", cfg.Hooks.Defaults[HookInspect])
	require.False(t, cfg.Presets["all"].UsesWaves())
	require.True(t, cfg.Presets["normal"].UsesWaves())
}

func TestParseRejectsUnknownField(t *testing.T) {
	_, err := Parse([]byte("environment: x\nbogus: 1\n"))
	require.Error(t, err)
}

func TestParseCustomPresetsReplaceBuiltins(t *testing.T) {
	cfg, err := Parse([]byte(`
environment: x
defaultPolicy: {speed: slow}
presets:
  slow: {batch: [1], soak: {duration: 5m}}
`))
	require.NoError(t, err)
	require.Len(t, cfg.Presets, 1)
	require.Equal(t, 60*time.Second, cfg.Presets["slow"].Soak.Interval)
	require.Equal(t, 2, cfg.Presets["slow"].Soak.Passes)
	require.Equal(t, 1, cfg.Presets["slow"].Soak.Grace)
}

func TestValidateErrors(t *testing.T) {
	cases := map[string]string{
		"missing environment":   "listen: ':1'\n",
		"bad default speed":     "environment: x\ndefaultPolicy: {speed: warp}\n",
		"bad mode":              "environment: x\ndefaultPolicy: {mode: sometimes}\n",
		"oidc without issuer":   "environment: x\nauth: {mode: oidc}\n",
		"unknown hook default":  "environment: x\nhooks: {defaults: {discover: d}}\n",
		"empty batch":           "environment: x\npresets: {p: {batch: []}}\ndefaultPolicy: {speed: p}\n",
		"soak interval too big": "environment: x\npresets: {p: {batch: [1], soak: {duration: 1m, interval: 2m}}}\ndefaultPolicy: {speed: p}\n",
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
