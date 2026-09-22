package targets

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var testRules = Rules{
	GroupLabel: "client",
	OwnerLabel: "owner",
	WaveLabel:  "wave",
	KnownHooks: []string{"inspect", "update", "ready", "soak"},
}

const good = `
- id: a-1/cl
  node: a-1
  address: a-1.example
  weight: 100
  image: org/a:unstable
  labels: {client: a, owner: a, role: cl, wave: "1"}
  probes: {ready: r, soak: s}
- id: a-1/el
  node: a-1
  address: a-1.example
  weight: 100
  image: org/b:unstable
  labels: {client: b, owner: b, role: el}
- id: a-2/cl
  node: a-2
  address: a-2.example
  weight: 0
  image: org/a:unstable
  labels: {client: a, owner: a, role: cl, wave: "0"}
`

func TestParseAndIndexes(t *testing.T) {
	s, err := Parse([]byte(good), &testRules)
	require.NoError(t, err)
	require.Equal(t, 3, s.Len())
	require.InDelta(t, 100.0, s.TotalWeight(), 0.001)
	require.Equal(t, []string{"a", "b"}, s.Groups())
	require.Equal(t, []string{"org/a:unstable", "org/b:unstable"}, s.Images())
	require.Equal(t, []string{"a-1", "a-2"}, s.Nodes())
	require.Len(t, s.Node("a-1"), 2)

	tgt, ok := s.Get("a-2/cl")
	require.True(t, ok)
	require.Equal(t, 0, s.Wave(&tgt))
	require.Equal(t, "a", s.Group(&tgt))

	el, _ := s.Get("a-1/el")
	require.Equal(t, 1, s.Wave(&el))
	require.Len(t, s.InGroup("a"), 2)
}

func TestValidation(t *testing.T) {
	cases := map[string]string{
		"duplicate id":      good + "- {id: a-1/cl, node: z, weight: 1, image: x:y, labels: {client: a, owner: a}}\n",
		"node weight drift": good + "- {id: a-1/x, node: a-1, weight: 5, image: x:y, labels: {client: a, owner: a}}\n",
		"digest image":      "- {id: q, node: n, weight: 1, image: org/a@sha256:abc, labels: {client: a, owner: a}}\n",
		"no tag":            "- {id: q, node: n, weight: 1, image: org/a, labels: {client: a, owner: a}}\n",
		"missing group":     "- {id: q, node: n, weight: 1, image: a:b, labels: {owner: a}}\n",
		"bad wave":          "- {id: q, node: n, weight: 1, image: a:b, labels: {client: a, owner: a, wave: x}}\n",
		"unknown probe":     "- {id: q, node: n, weight: 1, image: a:b, labels: {client: a, owner: a}, probes: {discover: d}}\n",
		"probe with path":   "- {id: q, node: n, weight: 1, image: a:b, labels: {client: a, owner: a}, probes: {ready: ../x}}\n",
		"unknown field":     "- {id: q, node: n, weight: 1, image: a:b, labels: {client: a, owner: a}, bogus: 1}\n",
	}

	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(raw), &testRules)
			require.Error(t, err)
		})
	}
}

func TestProbeProgramsMustExist(t *testing.T) {
	dir := t.TempDir()
	rules := testRules
	rules.HooksDir = dir

	_, err := Parse([]byte(good), &rules)
	require.Error(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "r"), []byte("#!/bin/sh\n"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "s"), []byte("#!/bin/sh\n"), 0o755))

	_, err = Parse([]byte(good), &rules)
	require.NoError(t, err)
}

func TestLoadDirMergesFiles(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.yaml"), []byte(good), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.yml"), []byte("- {id: z, node: z, weight: 7, image: a:b, labels: {client: z, owner: z}}\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ignored.txt"), []byte("nope"), 0o644))

	s, err := Load(dir, &testRules)
	require.NoError(t, err)
	require.Equal(t, 4, s.Len())
	require.InDelta(t, 107.0, s.TotalWeight(), 0.001)
}

func TestSelector(t *testing.T) {
	s, err := Parse([]byte(good), &testRules)
	require.NoError(t, err)

	sel, err := ParseSelector("client=a, role=cl")
	require.NoError(t, err)
	require.Len(t, s.Select(sel), 2)
	require.Equal(t, "client=a,role=cl", sel.String())

	node, err := ParseSelector("node=a-1")
	require.NoError(t, err)
	require.Len(t, s.Select(node), 2)

	id, err := ParseSelector("id=a-1/el")
	require.NoError(t, err)
	require.Len(t, s.Select(id), 1)

	_, err = ParseSelector("client")
	require.Error(t, err)
	_, err = ParseSelector("")
	require.Error(t, err)
}
