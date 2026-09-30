package reconcile

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
)

// propGroups are the groups a random fleet draws from; each has one image.
var propGroups = []string{"a", "b", "c"}

func propImage(group string) string { return "org/" + group + ":t" }

// propTarget is one entry of the simulated targets file.
type propTarget struct {
	id, node, group, image, soak string
	wave                         int
}

// propFleet is the simulated targets file: nodes of integer weight (so the
// budget comparison is exact), each hosting one to three groups' targets.
// Entries taken out of the file are kept so they can come back.
type propFleet struct {
	targets []propTarget
	removed []propTarget
	weights map[string]int
}

func newPropFleet(rng *rand.Rand) *propFleet {
	f := &propFleet{weights: map[string]int{}}

	soaks := map[string]bool{}
	for _, g := range propGroups {
		soaks[g] = rng.IntN(2) == 0
	}

	for n := range 2 + rng.IntN(6) {
		node := fmt.Sprintf("n%d", n)

		f.weights[node] = 0
		if rng.IntN(4) != 0 {
			f.weights[node] = 1 + rng.IntN(100)
		}

		var groups []string

		for _, g := range propGroups {
			if rng.IntN(2) == 0 {
				groups = append(groups, g)
			}
		}

		if len(groups) == 0 {
			groups = append(groups, propGroups[rng.IntN(len(propGroups))])
		}

		for _, g := range groups {
			t := propTarget{id: node + "/" + g, node: node, group: g, image: propImage(g), wave: rng.IntN(3)}
			if soaks[g] {
				t.soak = "soak-" + g
			}

			f.targets = append(f.targets, t)
		}
	}

	return f
}

func (f *propFleet) yaml() string {
	var b strings.Builder

	for _, t := range f.targets {
		hooks := ""
		if t.soak != "" {
			hooks = ", hooks: {soak: " + t.soak + "}"
		}

		fmt.Fprintf(&b, "- {id: %s, node: %s, weight: %d, image: %s, labels: {client: %s, owner: %s, wave: \"%d\"}%s}\n",
			t.id, t.node, f.weights[t.node], t.image, t.group, t.group, t.wave, hooks)
	}

	return b.String()
}

func (f *propFleet) get(id string) (propTarget, bool) {
	for _, t := range f.targets {
		if t.id == id {
			return t, true
		}
	}

	return propTarget{}, false
}

func (f *propFleet) nodes() []string {
	nodes := make([]string, 0, len(f.weights))
	for n := range f.weights {
		nodes = append(nodes, n)
	}

	slices.Sort(nodes)

	return nodes
}

// edit changes the file the way a re-rendered inventory would: a target
// leaves or comes back, a node's weight changes, or a target moves to
// another node or group. It returns what it did, or "" for nothing.
func (f *propFleet) edit(rng *rand.Rand) string {
	switch rng.IntN(5) {
	case 0:
		if len(f.targets) < 2 {
			return ""
		}

		i := rng.IntN(len(f.targets))
		t := f.targets[i]
		f.targets = slices.Delete(f.targets, i, i+1)
		f.removed = append(f.removed, t)

		return "remove " + t.id
	case 1:
		if len(f.removed) == 0 {
			return ""
		}

		i := rng.IntN(len(f.removed))
		t := f.removed[i]
		f.removed = slices.Delete(f.removed, i, i+1)
		f.targets = append(f.targets, t)

		return "re-add " + t.id
	case 2:
		nodes := f.nodes()
		node := nodes[rng.IntN(len(nodes))]
		f.weights[node] = rng.IntN(101)

		return fmt.Sprintf("weight %s=%d", node, f.weights[node])
	case 3:
		nodes := f.nodes()
		i := rng.IntN(len(f.targets))
		f.targets[i].node = nodes[rng.IntN(len(nodes))]

		return "move " + f.targets[i].id + " to " + f.targets[i].node
	default:
		i := rng.IntN(len(f.targets))
		f.targets[i].group = propGroups[rng.IntN(len(propGroups))]

		return "relabel " + f.targets[i].id + " client=" + f.targets[i].group
	}
}

func propConfig(rng *rand.Rand) string {
	budget := []string{"10%", quarterBudget, "40%", "60%", "100"}[rng.IntN(5)]
	batch := []string{"1", "2", "3", "50%", "100%"}[rng.IntN(5)]
	soak := []string{"0s", "40s"}[rng.IntN(2)]

	return fmt.Sprintf(`
environment: test
disruptionBudget: {maxUnavailable: %s}
registry: {poll: 60s}
hooks: {dir: /tmp, timeout: 10s, defaults: {soak: ""}}
inspect: {interval: 30s, concurrency: 4, failureThreshold: 2}
labels: {group: client, owner: owner}
strategy: {batchSize: %s, soak: {duration: %s, interval: 20s, failureLimit: %d}}
strategies:
  careful: {firstBatch: 1, pauseAfterFirstBatch: true}
  flat:    {batchSize: 100%%, waves: false, soak: {duration: 0s}}
`, budget, batch, soak, rng.IntN(2))
}
