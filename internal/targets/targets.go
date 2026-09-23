// Package targets loads the files that declare what the controller manages.
package targets

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Target is one managed container as declared in a targets file.
type Target struct {
	ID      string            `yaml:"id" json:"id"`
	Node    string            `yaml:"node" json:"node"`
	Address string            `yaml:"address" json:"address"`
	Weight  float64           `yaml:"weight" json:"weight"`
	Image   string            `yaml:"image" json:"image"`
	Labels  map[string]string `yaml:"labels" json:"labels"`
	Hooks   map[string]string `yaml:"hooks,omitempty" json:"hooks,omitempty"`
	Extra   map[string]any    `yaml:"extra,omitempty" json:"extra,omitempty"`
}

// Rules are the parts of the configuration that validation needs.
type Rules struct {
	GroupLabel string
	OwnerLabel string
	WaveLabel  string
	// HooksDir, when set, makes every named probe program have to exist there.
	HooksDir string
	// KnownHooks are the probe keys a target may set.
	KnownHooks []string
}

// Set is a validated collection of targets with the indexes the loop needs.
type Set struct {
	Targets []Target

	rules       Rules
	byID        map[string]int
	byNode      map[string][]int
	nodeWeight  map[string]float64
	totalWeight float64
}

// Load reads every *.yaml and *.yml file in dir.
func Load(dir string, rules *Rules) (*Set, error) {
	paths, err := listFiles(dir)
	if err != nil {
		return nil, err
	}

	if len(paths) == 0 {
		return nil, fmt.Errorf("targets: no yaml files in %s", dir)
	}

	var all []Target

	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("targets: read %s: %w", p, err)
		}

		ts, err := decode(raw)
		if err != nil {
			return nil, fmt.Errorf("targets: %s: %w", filepath.Base(p), err)
		}

		all = append(all, ts...)
	}

	return New(all, rules)
}

// Parse builds a Set from one document, for tests and validate.
func Parse(raw []byte, rules *Rules) (*Set, error) {
	ts, err := decode(raw)
	if err != nil {
		return nil, fmt.Errorf("targets: %w", err)
	}

	return New(ts, rules)
}

// New validates and indexes targets.
func New(ts []Target, rules *Rules) (*Set, error) {
	s := &Set{
		Targets:    make([]Target, 0, len(ts)),
		rules:      *rules,
		byID:       make(map[string]int, len(ts)),
		byNode:     make(map[string][]int),
		nodeWeight: make(map[string]float64),
	}

	for i := range ts {
		t := ts[i]

		if err := s.validate(&t); err != nil {
			return nil, err
		}

		if _, dup := s.byID[t.ID]; dup {
			return nil, fmt.Errorf("targets: duplicate id %q", t.ID)
		}

		if w, seen := s.nodeWeight[t.Node]; seen && w != t.Weight {
			return nil, fmt.Errorf("targets: node %q has weight %v on one target and %v on %q; a node has one weight", t.Node, w, t.Weight, t.ID)
		}

		if t.Labels == nil {
			t.Labels = map[string]string{}
		}

		s.byID[t.ID] = len(s.Targets)
		s.byNode[t.Node] = append(s.byNode[t.Node], len(s.Targets))
		s.nodeWeight[t.Node] = t.Weight
		s.Targets = append(s.Targets, t)
	}

	for _, w := range s.nodeWeight {
		s.totalWeight += w
	}

	return s, nil
}

func (s *Set) validate(t *Target) error {
	if t.ID == "" {
		return errors.New("targets: a target has no id")
	}

	if t.Node == "" {
		return fmt.Errorf("targets: %s: node is required", t.ID)
	}

	if t.Weight < 0 {
		return fmt.Errorf("targets: %s: weight must not be negative", t.ID)
	}

	if err := checkImage(t.Image); err != nil {
		return fmt.Errorf("targets: %s: %w", t.ID, err)
	}

	for _, l := range []string{s.rules.GroupLabel, s.rules.OwnerLabel} {
		if l != "" && t.Labels[l] == "" {
			return fmt.Errorf("targets: %s: label %q is required", t.ID, l)
		}
	}

	if s.rules.WaveLabel != "" {
		if v, ok := t.Labels[s.rules.WaveLabel]; ok {
			if n, err := strconv.Atoi(v); err != nil || n < 0 {
				return fmt.Errorf("targets: %s: label %s=%q must be a non-negative integer", t.ID, s.rules.WaveLabel, v)
			}
		}
	}

	for hook, prog := range t.Hooks {
		if !slices.Contains(s.rules.KnownHooks, hook) {
			return fmt.Errorf("targets: %s: hooks.%s is not a hook (want one of %s)", t.ID, hook, strings.Join(s.rules.KnownHooks, ", "))
		}

		if prog == "" || strings.ContainsAny(prog, `/\`) {
			return fmt.Errorf("targets: %s: hooks.%s=%q must be a bare program name", t.ID, hook, prog)
		}

		if s.rules.HooksDir != "" {
			if err := checkExecutable(filepath.Join(s.rules.HooksDir, prog)); err != nil {
				return fmt.Errorf("targets: %s: hooks.%s: %w", t.ID, hook, err)
			}
		}
	}

	return nil
}

// checkImage requires repository:tag and refuses digests.
func checkImage(image string) error {
	if image == "" {
		return errors.New("image is required")
	}

	if strings.Contains(image, "@") {
		return errors.New("image must be repository:tag, not a digest; pin through policy instead")
	}

	last := image[strings.LastIndex(image, "/")+1:]
	if !strings.Contains(last, ":") {
		return fmt.Errorf("image %q needs a tag", image)
	}

	return nil
}

func checkExecutable(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("program %s: %w", path, err)
	}

	if info.IsDir() || info.Mode()&0o111 == 0 {
		return fmt.Errorf("program %s is not executable", path)
	}

	return nil
}

// Get returns the target with this id.
func (s *Set) Get(id string) (Target, bool) {
	i, ok := s.byID[id]
	if !ok {
		return Target{}, false
	}

	return s.Targets[i], true
}

// Len is the number of targets.
func (s *Set) Len() int { return len(s.Targets) }

// Group is the value of the configured group label.
func (s *Set) Group(t *Target) string { return t.Labels[s.rules.GroupLabel] }

// Owner is the value of the configured owner label.
func (s *Set) Owner(t *Target) string { return t.Labels[s.rules.OwnerLabel] }

// Wave is the numeric wave; missing means 1.
func (s *Set) Wave(t *Target) int {
	if s.rules.WaveLabel == "" {
		return 1
	}

	v, ok := t.Labels[s.rules.WaveLabel]
	if !ok {
		return 1
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		return 1
	}

	return n
}

// NodeWeight is the weight of a node, zero if unknown.
func (s *Set) NodeWeight(node string) float64 { return s.nodeWeight[node] }

// TotalWeight is the sum of every node's weight.
func (s *Set) TotalWeight() float64 { return s.totalWeight }

// Node returns the targets on a node.
func (s *Set) Node(node string) []Target {
	idx := s.byNode[node]
	out := make([]Target, 0, len(idx))

	for _, i := range idx {
		out = append(out, s.Targets[i])
	}

	return out
}

// Nodes lists node names sorted.
func (s *Set) Nodes() []string {
	out := make([]string, 0, len(s.byNode))
	for n := range s.byNode {
		out = append(out, n)
	}

	sort.Strings(out)

	return out
}

// Groups lists group values sorted.
func (s *Set) Groups() []string {
	seen := map[string]struct{}{}
	for i := range s.Targets {
		seen[s.Group(&s.Targets[i])] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for g := range seen {
		out = append(out, g)
	}

	sort.Strings(out)

	return out
}

// InGroup returns the targets of a group.
func (s *Set) InGroup(group string) []Target {
	var out []Target

	for i := range s.Targets {
		if s.Group(&s.Targets[i]) == group {
			out = append(out, s.Targets[i])
		}
	}

	return out
}

// Images lists distinct image references sorted.
func (s *Set) Images() []string {
	seen := map[string]struct{}{}
	for _, t := range s.Targets {
		seen[t.Image] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for i := range seen {
		out = append(out, i)
	}

	sort.Strings(out)

	return out
}

// Select returns the targets a selector matches, in file order.
func (s *Set) Select(sel Selector) []Target {
	var out []Target

	for i := range s.Targets {
		if sel.Match(&s.Targets[i]) {
			out = append(out, s.Targets[i])
		}
	}

	return out
}

// Rules returns the rules the set was built with.
func (s *Set) Rules() Rules { return s.rules }

func decode(raw []byte) ([]Target, error) {
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)

	var all []Target

	for {
		var doc []Target

		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, err
		}

		all = append(all, doc...)
	}

	return all, nil
}

func listFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("targets: read dir %s: %w", dir, err)
	}

	var paths []string

	for _, e := range entries {
		if e.IsDir() {
			continue
		}

		ext := filepath.Ext(e.Name())
		if ext == ".yaml" || ext == ".yml" {
			paths = append(paths, filepath.Join(dir, e.Name()))
		}
	}

	sort.Strings(paths)

	return paths, nil
}
