package reconcile

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/ethpandaops/rolloor/internal/targets"
)

// propGroups are the groups a random fleet draws from; each has one image.
var propGroups = []string{"a", "b", "c"}

func propImage(group string) string { return "org/" + group + ":t" }

// propFleet builds a random targets file: nodes of integer weight (so the
// budget comparison is exact), each hosting one to three groups' targets.
func propFleet(rng *rand.Rand) (yaml string, ids []string) {
	var b strings.Builder

	soaks := map[string]bool{}
	for _, g := range propGroups {
		soaks[g] = rng.IntN(2) == 0
	}

	nodes := 2 + rng.IntN(6)
	for n := range nodes {
		weight := 0
		if rng.IntN(4) != 0 {
			weight = 1 + rng.IntN(100)
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
			id := fmt.Sprintf("n%d/%s", n, g)

			hooks := ""
			if soaks[g] {
				hooks = ", hooks: {soak: soak-" + g + "}"
			}

			fmt.Fprintf(&b, "- {id: %s, node: n%d, weight: %d, image: %s, labels: {client: %s, owner: %s, wave: \"%d\"}%s}\n",
				id, n, weight, propImage(g), g, g, rng.IntN(3), hooks)

			ids = append(ids, id)
		}
	}

	return b.String(), ids
}

func propConfig(rng *rand.Rand) string {
	budget := []string{"10%", "25%", "40%", "60%", "100"}[rng.IntN(5)]
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
`, budget, batch, soak, rng.IntN(2))
}

// propCheck asserts what must hold after every step on a fleet whose targets
// file does not change: the controller's own count of unavailable weight
// stays within the budget, a group has at most one active rollout, and a
// finished rollout stays finished.
func propCheck(h *harness, terminal map[string]RolloutState) error {
	if used, allowed := h.unavailable(); used > allowed {
		return fmt.Errorf("unavailable weight %v exceeds the %v the budget allows", used, allowed)
	}

	active := map[string]string{}

	for _, r := range h.c.Rollouts() {
		if was, ok := terminal[r.ID]; ok && r.State != was {
			return fmt.Errorf("rollout %s left terminal state %s for %s", r.ID, was, r.State)
		}

		if !r.State.Active() {
			terminal[r.ID] = r.State

			continue
		}

		if other, ok := active[r.Group]; ok {
			return fmt.Errorf("group %s has two active rollouts: %s and %s", r.Group, other, r.ID)
		}

		active[r.Group] = r.ID
	}

	return nil
}

// propStep applies one random action: time passing, a new build, a failing or
// recovering program, or a person's verb. It returns a description.
func propStep(h *harness, rng *rand.Rand, ids []string, builds *int) string {
	ctx := h.ctx
	id := ids[rng.IntN(len(ids))]
	g := propGroups[rng.IntN(len(propGroups))]

	var rollouts []RolloutView

	for _, r := range h.c.Rollouts() {
		if r.State.Active() {
			rollouts = append(rollouts, r)
		}
	}

	pick := func() (RolloutView, bool) {
		if len(rollouts) == 0 {
			return RolloutView{}, false
		}

		return rollouts[rng.IntN(len(rollouts))], true
	}

	switch n := rng.IntN(100); {
	case n < 45:
		d := []time.Duration{0, 5 * time.Second, 20 * time.Second, 60 * time.Second}[rng.IntN(4)]
		h.clock.Advance(d)
		h.tick()

		return fmt.Sprintf("tick +%s", d)
	case n < 53:
		*builds++
		digest := fmt.Sprintf("sha256:%064x", *builds+1)

		h.world.set(func(w *world) { w.registry[propImage(g)] = digest })

		return "release " + g + " …" + digest[len(digest)-4:]
	case n < 60:
		fail := rng.IntN(2) == 0

		h.world.set(func(w *world) {
			if fail {
				w.updateFail[id] = "boom"
			} else {
				delete(w.updateFail, id)
			}
		})

		return fmt.Sprintf("updateFail %s=%v", id, fail)
	case n < 65:
		v := rng.IntN(2) == 0

		h.world.set(func(w *world) { w.notReady[id] = v })

		return fmt.Sprintf("notReady %s=%v", id, v)
	case n < 70:
		v := rng.IntN(2) == 0

		h.world.set(func(w *world) { w.soakFail["soak-"+g] = v })

		return fmt.Sprintf("soakFail soak-%s=%v", g, v)
	case n < 80:
		r, ok := pick()
		if !ok || r.State != Halted {
			return "retry: nothing halted"
		}

		err := h.c.Retry(ctx, actor, r.ID, "try again")

		return fmt.Sprintf("retry %s (%s): %v", r.ID, r.Group, err)
	case n < 83:
		r, ok := pick()
		if !ok {
			return "abort: nothing active"
		}

		return fmt.Sprintf("abort %s: %v", r.ID, h.c.Abort(ctx, actor, r.ID))
	case n < 87:
		r, ok := pick()
		if !ok {
			return "pause: nothing active"
		}

		return fmt.Sprintf("pause %s: %v", r.ID, h.c.Pause(ctx, actor, r.ID))
	case n < 91:
		r, ok := pick()
		if !ok {
			return "promote: nothing active"
		}

		return fmt.Sprintf("promote %s: %v", r.ID, h.c.Promote(ctx, actor, r.ID))
	case n < 95:
		force := rng.IntN(3) == 0
		_, err := h.c.Sync(ctx, SyncRequest{Actor: actor, Selector: targets.Selector{"client": g}, Force: force})

		return fmt.Sprintf("sync %s force=%v: %v", g, force, err)
	case n < 98:
		_, err := h.c.Suspend(ctx, SuspendRequest{Actor: actor, Selector: targets.Selector{"id": id}, Reason: "hold", Expires: time.Hour})

		return fmt.Sprintf("suspend %s: %v", id, err)
	default:
		_, err := h.c.Resume(ctx, actor, targets.Selector{"id": id})

		return fmt.Sprintf("resume %s: %v", id, err)
	}
}

// TestPropertyControllerInvariants drives random fleets through random
// sequences of builds, failures and verbs, checking the invariants after
// every step. A failure names the seed, the fleet and the last steps; the
// seed alone reproduces it.
func TestPropertyControllerInvariants(t *testing.T) {
	const steps, shown, maxFailures = 200, 40, 3

	seeds := 1000
	if testing.Short() {
		seeds = 100
	}

	failures := 0

	for seed := range uint64(seeds) {
		rng := rand.New(rand.NewPCG(seed, 0x5eed))
		fleet, ids := propFleet(rng)
		cfg := propConfig(rng)
		h := newHarness(t, cfg, fleet)
		h.prime()

		terminal := map[string]RolloutState{}
		builds := 0

		var trail []string

		for step := range steps {
			trail = append(trail, propStep(h, rng, ids, &builds))

			err := propCheck(h, terminal)
			if err == nil {
				continue
			}

			t.Errorf("seed %d, step %d: %v\nconfig:%s\ntargets:\n%s\nlast steps:\n  %s",
				seed, step, err, cfg, fleet, strings.Join(trail[max(0, len(trail)-shown):], "\n  "))

			failures++

			break
		}

		if failures == maxFailures {
			t.Fatalf("stopping after %d failing seeds", failures)
		}
	}
}
