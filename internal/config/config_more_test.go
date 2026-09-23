package config

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestLoadFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(path, []byte("environment: e\n"), 0o644))

	cfg, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, "e", cfg.Environment)

	_, err = Load(filepath.Join(t.TempDir(), "missing.yaml"))
	require.Error(t, err)
}

func TestValidateRemainingBranches(t *testing.T) {
	cases := map[string]string{
		"empty group label": "environment: x\nlabels: {group: \"\"}\n",
		"zero poll":         "environment: x\nregistry: {poll: 0s}\n",
		"zero hook timeout": "environment: x\nhooks: {timeout: 0s}\n",
		"zero inspect":      "environment: x\ninspect: {concurrency: 0}\n",
		"bad auth mode":     "environment: x\nauth: {mode: magic}\n",
		"budget not scalar": "environment: x\ndisruptionBudget: {maxUnavailable: [1]}\n",
		"bad budget":        "environment: x\ndisruptionBudget: {maxUnavailable: lots}\n",
	}

	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(raw))
			require.Error(t, err)
		})
	}

	_, err := Parse([]byte("environment: x\nauth: {mode: oidc, issuer: i, clientId: c, redirectUrl: r}\n"))
	require.NoError(t, err)
}

func TestDefaultsFailure(t *testing.T) {
	orig := applyDefaults
	applyDefaults = func(any) error { return errors.New("boom") }

	t.Cleanup(func() { applyDefaults = orig })

	_, err := Parse([]byte("environment: x\n"))
	require.ErrorContains(t, err, "boom")
}

func TestFractionMarshal(t *testing.T) {
	pct := Fraction{Percent: 12.5, IsPercent: true}
	cnt := Fraction{Count: 3}

	require.Equal(t, "12.5%", pct.String())
	require.Equal(t, "3", cnt.String())
	require.InDelta(t, 3.0, cnt.OfWeight(1000), 0.001)
	require.Equal(t, 0, Fraction{IsPercent: true}.Of(10))

	_, err := ParseFraction("")
	require.Error(t, err)
	_, err = ParseFraction("-5%")
	require.Error(t, err)

	y, err := yaml.Marshal(map[string]Fraction{"a": pct, "b": cnt})
	require.NoError(t, err)
	require.Equal(t, "a: 12.5%\nb: \"3\"\n", string(y))

	j, err := json.Marshal([]Fraction{pct, cnt})
	require.NoError(t, err)
	require.Equal(t, `["12.5%","3"]`, string(j))

	var back struct {
		F Fraction `yaml:"f"`
	}

	require.Error(t, yaml.Unmarshal([]byte("f: {a: 1}\n"), &back))
	require.Error(t, yaml.Unmarshal([]byte("f: 200%\n"), &back))
	require.NoError(t, yaml.Unmarshal([]byte("f: 7\n"), &back))
	require.Equal(t, 7, back.F.Count)
}
