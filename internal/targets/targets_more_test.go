package targets

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccessorsAndEdgeCases(t *testing.T) {
	s, err := Parse([]byte(good), &testRules)
	require.NoError(t, err)

	require.InDelta(t, 100.0, s.NodeWeight("a-1"), 0.001)
	require.InDelta(t, 0.0, s.NodeWeight("nope"), 0.001)
	require.Equal(t, labelClient, s.Rules().GroupLabel)
	require.Empty(t, s.Node("nope"))
	require.Empty(t, s.InGroup("nope"))

	_, ok := s.Get("nope")
	require.False(t, ok)

	// Wave falls back to 1 for an unparseable value on a target built by hand.
	odd := Target{Labels: map[string]string{labelWave: "x"}}
	require.Equal(t, 1, s.Wave(&odd))

	// Without a wave label configured every target is wave 1.
	noWave := testRules
	noWave.WaveLabel = ""
	s2, err := Parse([]byte(good), &noWave)
	require.NoError(t, err)
	require.Equal(t, 1, s2.Wave(&s2.Targets[2]))

	// Without group and owner labels required, a target with no labels is fine.
	loose := Rules{KnownHooks: testRules.KnownHooks}
	s3, err := Parse([]byte("- {id: q, node: n, weight: 1, image: a:b}\n"), &loose)
	require.NoError(t, err)
	require.NotNil(t, s3.Targets[0].Labels)
}

func TestValidationMore(t *testing.T) {
	cases := map[string]string{
		"no id":           "- {node: n, weight: 1, image: a:b, labels: {client: a, owner: a}}\n",
		"no node":         "- {id: q, weight: 1, image: a:b, labels: {client: a, owner: a}}\n",
		"negative weight": "- {id: q, node: n, weight: -1, image: a:b, labels: {client: a, owner: a}}\n",
		"no image":        "- {id: q, node: n, weight: 1, labels: {client: a, owner: a}}\n",
		"empty probe":     "- {id: q, node: n, weight: 1, image: a:b, labels: {client: a, owner: a}, hooks: {ready: \"\"}}\n",
		"not a list":      "id: q\n",
	}

	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(raw), &testRules)
			require.Error(t, err)
		})
	}
}

func TestProbeProgramMustBeExecutableFile(t *testing.T) {
	dir := t.TempDir()
	rules := testRules
	rules.HooksDir = dir

	require.NoError(t, os.Mkdir(filepath.Join(dir, "r"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "s"), []byte("#!/bin/sh\n"), 0o644))

	_, err := Parse([]byte(good), &rules)
	require.ErrorContains(t, err, "not executable")
}

func TestMultiDocumentAndLoadErrors(t *testing.T) {
	s, err := Parse([]byte(one+"---\n- {id: b, node: m, weight: 1, image: a:b, labels: {client: b, owner: b}}\n"), &testRules)
	require.NoError(t, err)
	require.Equal(t, 2, s.Len())

	_, err = Load(filepath.Join(t.TempDir(), "missing"), &testRules)
	require.Error(t, err)

	empty := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(empty, "sub.yaml"), 0o755))
	_, err = Load(empty, &testRules)
	require.ErrorContains(t, err, "no yaml files")

	broken := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(broken, "t.yaml"), []byte("nope: [\n"), 0o644))
	_, err = Load(broken, &testRules)
	require.Error(t, err)

	if os.Getuid() != 0 {
		unreadable := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(unreadable, "t.yaml"), []byte(one), 0o000))
		_, err = Load(unreadable, &testRules)
		require.Error(t, err)
	}
}

func TestSelectorMismatches(t *testing.T) {
	tgt := Target{ID: "a", Node: "n", Labels: map[string]string{labelClient: "x"}}
	require.False(t, Selector{KeyID: "b"}.Match(&tgt))
	require.False(t, Selector{KeyNode: "m"}.Match(&tgt))
	require.False(t, Selector{labelClient: "y"}.Match(&tgt))
	require.True(t, Selector{KeyID: "a", KeyNode: "n", labelClient: "x"}.Match(&tgt))
}
