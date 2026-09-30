package reconcile

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/targets"
)

// The usage tests run the controller the way an environment does: a fleet of
// nodes carrying several clients, builds landing on tags, people acting on
// rollouts, and the fleet changing underneath. They assert only what someone
// watching would see: which targets run which build, in what order, and the
// states, reasons and history the controller reports.

const usageConfig = `
environment: usage
disruptionBudget: {maxUnavailable: 25%}
registry: {poll: 60s}
hooks: {dir: /tmp, timeout: 30s, defaults: {soak: ""}}
inspect: {interval: 30s, concurrency: 8, failureThreshold: 2}
labels: {group: client, owner: owner, section: role}
strategy: {firstBatch: 1, batchSize: 2, soak: {duration: 5m, interval: 1m, failureLimit: 1}}
strategies:
  careful: {firstBatch: 1, pauseAfterFirstBatch: true}
  fast:    {batchSize: 50%, soak: {duration: 0s}}
`

// The fleet: eight nodes of weight 100 and two weightless observers, so the
// 25% budget allows two weighted nodes at once. Every weighted node runs a
// store (kilo or lima) and a server with its agent (alpha, bravo or
// charlie). alpha ships server and agent in one image, bravo in two. echo
// runs on one node; delta only on the observers.
const usageTargets = `
- {id: obs-1/store,   node: obs-1,  weight: 0,   image: org/kilo:stable,         labels: {client: kilo,    owner: kilo,    role: store,  wave: "0"}, hooks: {soak: soak-store}}
- {id: obs-1/server,  node: obs-1,  weight: 0,   image: org/alpha:stable,        labels: {client: alpha,   owner: alpha,   role: server, wave: "0"}, hooks: {soak: soak-server}}
- {id: obs-1/tool,    node: obs-1,  weight: 0,   image: org/delta:stable,        labels: {client: delta,   owner: delta,   role: tool,   wave: "0"}}
- {id: obs-2/store,   node: obs-2,  weight: 0,   image: org/kilo:stable,         labels: {client: kilo,    owner: kilo,    role: store,  wave: "0"}, hooks: {soak: soak-store}}
- {id: obs-2/server,  node: obs-2,  weight: 0,   image: org/bravo-server:stable, labels: {client: bravo,   owner: bravo,   role: server, wave: "0"}, hooks: {soak: soak-server}}
- {id: obs-2/tool,    node: obs-2,  weight: 0,   image: org/delta:stable,        labels: {client: delta,   owner: delta,   role: tool,   wave: "0"}}
- {id: node-1/store,  node: node-1, weight: 100, image: org/kilo:stable,         labels: {client: kilo,    owner: kilo,    role: store,  wave: "1"}, hooks: {soak: soak-store}}
- {id: node-1/server, node: node-1, weight: 100, image: org/alpha:stable,        labels: {client: alpha,   owner: alpha,   role: server, wave: "1"}, hooks: {soak: soak-server}}
- {id: node-1/agent,  node: node-1, weight: 100, image: org/alpha:stable,        labels: {client: alpha,   owner: alpha,   role: agent,  wave: "1"}}
- {id: node-2/store,  node: node-2, weight: 100, image: org/kilo:stable,         labels: {client: kilo,    owner: kilo,    role: store,  wave: "1"}, hooks: {soak: soak-store}}
- {id: node-2/server, node: node-2, weight: 100, image: org/alpha:stable,        labels: {client: alpha,   owner: alpha,   role: server, wave: "1"}, hooks: {soak: soak-server}}
- {id: node-2/agent,  node: node-2, weight: 100, image: org/alpha:stable,        labels: {client: alpha,   owner: alpha,   role: agent,  wave: "1"}}
- {id: node-3/store,  node: node-3, weight: 100, image: org/lima:stable,         labels: {client: lima,    owner: lima,    role: store,  wave: "1"}, hooks: {soak: soak-store}}
- {id: node-3/server, node: node-3, weight: 100, image: org/alpha:stable,        labels: {client: alpha,   owner: alpha,   role: server, wave: "1"}, hooks: {soak: soak-server}}
- {id: node-3/agent,  node: node-3, weight: 100, image: org/alpha:stable,        labels: {client: alpha,   owner: alpha,   role: agent,  wave: "1"}}
- {id: node-4/store,  node: node-4, weight: 100, image: org/kilo:stable,         labels: {client: kilo,    owner: kilo,    role: store,  wave: "1"}, hooks: {soak: soak-store}}
- {id: node-4/server, node: node-4, weight: 100, image: org/bravo-server:stable, labels: {client: bravo,   owner: bravo,   role: server, wave: "1"}, hooks: {soak: soak-server}}
- {id: node-4/agent,  node: node-4, weight: 100, image: org/bravo-agent:stable,  labels: {client: bravo,   owner: bravo,   role: agent,  wave: "1"}}
- {id: node-5/store,  node: node-5, weight: 100, image: org/kilo:stable,         labels: {client: kilo,    owner: kilo,    role: store,  wave: "1"}, hooks: {soak: soak-store}}
- {id: node-5/server, node: node-5, weight: 100, image: org/bravo-server:stable, labels: {client: bravo,   owner: bravo,   role: server, wave: "1"}, hooks: {soak: soak-server}}
- {id: node-5/agent,  node: node-5, weight: 100, image: org/bravo-agent:stable,  labels: {client: bravo,   owner: bravo,   role: agent,  wave: "1"}}
- {id: node-5/tool,   node: node-5, weight: 100, image: org/echo:stable,         labels: {client: echo,    owner: echo,    role: tool,   wave: "1"}}
- {id: node-6/store,  node: node-6, weight: 100, image: org/lima:stable,         labels: {client: lima,    owner: lima,    role: store,  wave: "1"}, hooks: {soak: soak-store}}
- {id: node-6/server, node: node-6, weight: 100, image: org/bravo-server:stable, labels: {client: bravo,   owner: bravo,   role: server, wave: "1"}, hooks: {soak: soak-server}}
- {id: node-6/agent,  node: node-6, weight: 100, image: org/bravo-agent:stable,  labels: {client: bravo,   owner: bravo,   role: agent,  wave: "1"}}
- {id: node-7/store,  node: node-7, weight: 100, image: org/kilo:stable,         labels: {client: kilo,    owner: kilo,    role: store,  wave: "1"}, hooks: {soak: soak-store}}
- {id: node-7/server, node: node-7, weight: 100, image: org/charlie:stable,      labels: {client: charlie, owner: charlie, role: server, wave: "1"}, hooks: {soak: soak-server}}
- {id: node-7/agent,  node: node-7, weight: 100, image: org/charlie:stable,      labels: {client: charlie, owner: charlie, role: agent,  wave: "1"}}
- {id: node-8/store,  node: node-8, weight: 100, image: org/lima:stable,         labels: {client: lima,    owner: lima,    role: store,  wave: "1"}, hooks: {soak: soak-store}}
- {id: node-8/server, node: node-8, weight: 100, image: org/charlie:stable,      labels: {client: charlie, owner: charlie, role: server, wave: "1"}, hooks: {soak: soak-server}}
- {id: node-8/agent,  node: node-8, weight: 100, image: org/charlie:stable,      labels: {client: charlie, owner: charlie, role: agent,  wave: "1"}}
`

const (
	imgAlpha       = "org/alpha:stable"
	imgBravoServer = "org/bravo-server:stable"
	imgBravoAgent  = "org/bravo-agent:stable"
	imgCharlie     = "org/charlie:stable"
	imgKilo        = "org/kilo:stable"
	imgEcho        = "org/echo:stable"
	imgDelta       = "org/delta:stable"
	soakServer     = "soak-server"
	clientAlpha    = "alpha"
	clientBravo    = "bravo"
	clientCharlie  = "charlie"
	clientKilo     = "kilo"
	observer       = "obs-1"
)

// newFleet starts the controller on the usage fleet, every target on d1.
func newFleet(t *testing.T, cfg string) *harness {
	t.Helper()

	h := newHarness(t, cfg, usageTargets)
	h.world.set(func(w *world) {
		for _, img := range []string{imgAlpha, imgBravoServer, imgBravoAgent, imgCharlie, imgKilo, "org/lima:stable", imgEcho, imgDelta} {
			w.registry[img] = d1
		}
	})
	h.prime()

	return h
}

// on lists the targets running a digest, sorted.
func (h *harness) on(digest string) []string {
	h.world.mu.Lock()
	defer h.world.mu.Unlock()

	var out []string

	for id, d := range h.world.running {
		if d == digest {
			out = append(out, id)
		}
	}

	slices.Sort(out)

	return out
}

// clientTargets lists a client's targets, sorted.
func (h *harness) clientTargets(client string) []string {
	in := h.current().InGroup(client)
	out := make([]string, 0, len(in))

	for _, t := range in {
		out = append(out, t.ID)
	}

	slices.Sort(out)

	return out
}

// settle runs the environment in 30s steps, the inspector included, until a
// whole step passes with no rollout active, and fails if the budget is ever
// broken on the way.
func (h *harness) settle(limit int) {
	h.t.Helper()

	quiet := false

	for i := 0; i < limit; i++ {
		h.step()

		if !h.anyActive() {
			if quiet {
				return
			}

			quiet = true

			continue
		}

		quiet = false
	}

	for _, r := range h.c.Rollouts() {
		if r.State.Active() {
			h.t.Fatalf("rollout %s of %s still %s after %d steps: %s", r.ID, r.Group, r.State, limit, r.Reason)
		}
	}
}

// until runs the environment in 30s steps until the rollout is in a state.
func (h *harness) until(id string, want RolloutState, limit int) RolloutView {
	h.t.Helper()

	for i := 0; i < limit && h.rollout(id).State != want; i++ {
		h.step()
	}

	r := h.rollout(id)
	require.Equal(h.t, want, r.State, r.Reason)

	return r
}

// step is 30 seconds of the environment: the inspector, then a tick.
func (h *harness) step() {
	h.t.Helper()
	h.c.InspectAll(h.ctx)
	h.clock.Advance(30 * time.Second)
	h.tick()
	h.requireBudget()
}

func (h *harness) anyActive() bool {
	for _, r := range h.c.Rollouts() {
		if r.State.Active() {
			return true
		}
	}

	return false
}

// requireBudget fails when more weight is unavailable than the budget
// allows, unless it is a single node going alone.
func (h *harness) requireBudget() {
	h.t.Helper()

	used, allowed := h.unavailable()
	if used <= allowed {
		return
	}

	if used <= h.admissionUsed {
		return
	}

	require.Empty(h.t, h.admission, "admission raised unavailable weight from %v to %v over %v", h.admissionUsed, used, allowed)

	weighted := 0

	for node := range h.oracleUnavailable() {
		if h.current().NodeWeight(node) > 0 {
			weighted++
		}
	}

	require.LessOrEqual(h.t, weighted, 1, "%v unavailable over the %v allowed", used, allowed)
}

// batchNodes lists the nodes of each batch of a rollout, in order.
func batchNodes(r *RolloutView) [][]string {
	out := make([][]string, 0, len(r.Batches))

	for _, b := range r.Batches {
		var nodes []string

		for _, id := range b.Targets {
			if n := r.target(id).Node; !slices.Contains(nodes, n) {
				nodes = append(nodes, n)
			}
		}

		out = append(out, nodes)
	}

	return out
}

// history is the actions recorded for one rollout, in order.
func (h *harness) history(rollout string) []string {
	h.notes.mu.Lock()
	defer h.notes.mu.Unlock()

	var out []string

	for _, e := range h.notes.events {
		if e.Rollout == rollout {
			out = append(out, e.Action)
		}
	}

	return out
}

// updates counts the update hook runs per target.
func (h *harness) updates() map[string]int {
	out := map[string]int{}

	for _, c := range h.world.callsFor("update") {
		out[c[len("update:"):len(c)-len(":update")]]++
	}

	return out
}

func TestUsageAClientShipsABuild(t *testing.T) {
	h := newFleet(t, usageConfig)
	h.release(imgAlpha, d2)
	r := h.active(clientAlpha)

	// The observer goes first and alone; the soak compares it with the
	// servers not yet reached.
	require.Equal(t, [][]string{{observer}}, batchNodes(&r))

	h.settle(100)

	r = h.rollout(r.ID)
	require.Equal(t, Complete, r.State)
	require.Equal(t, "All 7 targets on 2222222.", r.Reason)
	require.Equal(t, [][]string{{observer}, {"node-1", "node-2"}, {"node-3"}}, batchNodes(&r),
		"then two nodes at a time, all the budget allows")
	require.Equal(t, h.clientTargets(clientAlpha), h.on(d2), "alpha moved and nothing else did")
	require.Equal(t, "rollout.created "+strings.Repeat("batch.started rollout.soaking batch.passed ", 3)+"rollout.complete",
		strings.Join(h.history(r.ID), " "))
}

func TestUsageABadBuildStopsAtOneNodeAndTheFixGoesThereFirst(t *testing.T) {
	h := newFleet(t, usageConfig)

	// The build does worse than the servers it is compared with.
	h.world.set(func(w *world) { w.soakFail[soakServer] = true })
	h.release(imgAlpha, d2)
	bad := h.until(h.active(clientAlpha).ID, Halted, 20)

	require.Equal(t, []string{"obs-1/server"}, h.on(d2), "one observer took the bad build")
	require.Equal(t, Healthy, h.view("obs-1/server").Health)
	require.Equal(t, bad.ID, h.view("obs-1/server").Quarantine.Rollout)
	require.Contains(t, bad.Reason, "quarantined")

	// The team ships a fix. It supersedes the halted rollout and starts on
	// the quarantined observer.
	h.world.set(func(w *world) { w.soakFail[soakServer] = false })
	h.release(imgAlpha, d3)
	require.Equal(t, Superseded, h.rollout(bad.ID).State)

	fix := h.active(clientAlpha)
	require.Equal(t, "obs-1/server", fix.Targets[0].ID)
	require.False(t, fix.Targets[0].NotReadyBefore)

	h.settle(100)
	require.Equal(t, Complete, h.rollout(fix.ID).State)
	require.Equal(t, h.clientTargets(clientAlpha), h.on(d3))
	require.Equal(t, Healthy, h.view("obs-1/server").Health)
}

func TestUsageTwoClientsShipAtOnceAndShareTheBudget(t *testing.T) {
	h := newFleet(t, usageConfig)

	// kilo and alpha both run on node-1 and node-2; their builds land in the
	// same poll. settle checks the shared budget after every step.
	h.world.set(func(w *world) { w.registry[imgKilo] = d2 })
	h.release(imgAlpha, d2)

	kilo, alpha := h.active(clientKilo), h.active(clientAlpha)

	h.settle(200)
	require.Equal(t, Complete, h.rollout(kilo.ID).State)
	require.Equal(t, Complete, h.rollout(alpha.ID).State)
	require.Len(t, h.on(d2), len(h.clientTargets(clientKilo))+len(h.clientTargets(clientAlpha)))
}

func TestUsageATwoImageClientShipsBothImagesTogether(t *testing.T) {
	h := newFleet(t, usageConfig)

	// Both of bravo's images move in the same poll: one rollout moves both,
	// and each node is visited once.
	h.world.set(func(w *world) { w.registry[imgBravoAgent] = d2 })
	h.release(imgBravoServer, d2)

	r := h.active(clientBravo)
	require.Len(t, r.Desired, 2)

	h.settle(100)
	require.Equal(t, Complete, h.rollout(r.ID).State)
	require.Equal(t, h.clientTargets(clientBravo), h.on(d2))

	for id, n := range h.updates() {
		require.Equal(t, 1, n, "%s updated %d times", id, n)
	}
}

func TestUsageATwoImageClientShipsItsImagesMinutesApart(t *testing.T) {
	h := newFleet(t, usageConfig)

	// The server image lands first and its rollout starts; the agent image
	// lands two minutes later. The server rollout carries on, and the agent
	// build waits for it, then gets its own rollout.
	h.release(imgBravoServer, d2)
	first := h.active(clientBravo)
	h.ticks(4, 30*time.Second)

	h.release(imgBravoAgent, d2)
	require.Equal(t, first.ID, h.active(clientBravo).ID, "not superseded: the server image did not move again")

	h.settle(100)

	var rollouts []RolloutView

	for _, r := range h.c.Rollouts() {
		if r.Group == clientBravo {
			rollouts = append(rollouts, r)
		}
	}

	require.Len(t, rollouts, 2)
	require.Equal(t, Complete, rollouts[0].State)
	require.Equal(t, Complete, rollouts[1].State)
	require.Equal(t, h.clientTargets(clientBravo), h.on(d2))
}

// rolloutsOf lists a client's rollouts, oldest first.
func (h *harness) rolloutsOf(client string) []RolloutView {
	var out []RolloutView

	for _, r := range h.c.Rollouts() {
		if r.Group == client {
			out = append(out, r)
		}
	}

	slices.Reverse(out)

	return out
}

// fleetWith parses the usage fleet after an edit, for swapping in.
func (h *harness) fleetWith(edit func(doc string) string) *targets.Set {
	h.t.Helper()

	rules := h.current().Rules()
	set, err := targets.Parse([]byte(edit(usageTargets)), &rules)
	require.NoError(h.t, err)

	return set
}

func TestUsageRevertingATagRollsTheBuildBack(t *testing.T) {
	h := newFleet(t, usageConfig)
	h.release(imgAlpha, d2)
	h.settle(100)
	require.Equal(t, h.clientTargets(clientAlpha), h.on(d2))

	// The team moves the tag back to the build before; that is a build like
	// any other.
	h.release(imgAlpha, d1)
	back := h.active(clientAlpha)
	require.Equal(t, "1111111", back.Digest)

	h.settle(100)
	require.Equal(t, Complete, h.rollout(back.ID).State)
	require.Empty(t, h.on(d2))
}

func TestUsageRevertingMidRolloutMovesBackOnlyWhatMoved(t *testing.T) {
	h := newFleet(t, usageConfig)
	h.release(imgAlpha, d2)
	first := h.active(clientAlpha)

	for i := 0; i < 40 && len(h.rollout(first.ID).Batches) < 2; i++ {
		h.step()
	}

	moved := h.on(d2)
	before := h.updates()

	require.NotEmpty(t, moved)

	h.release(imgAlpha, d1)
	require.Equal(t, Superseded, h.rollout(first.ID).State)

	back := h.active(clientAlpha)
	h.settle(100)
	require.Equal(t, Complete, h.rollout(back.ID).State)
	require.Empty(t, h.on(d2))

	after := h.updates()

	for _, id := range h.clientTargets(clientAlpha) {
		if slices.Contains(moved, id) {
			require.Equal(t, before[id]+1, after[id], "%s moved back once", id)
		} else {
			require.Equal(t, before[id], after[id], "%s never moved, so it is left alone", id)
		}
	}
}

func TestUsageASuspendedNodeCatchesUpWhenItsSuspensionEnds(t *testing.T) {
	h := newFleet(t, usageConfig)

	_, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("node=node-2"), Reason: "debugging a disk", Expires: time.Hour})
	require.NoError(t, err)

	// Two builds land while node-2 is held; both skip it.
	h.release(imgAlpha, d2)
	h.settle(100)
	h.release(imgAlpha, d3)
	h.settle(100)
	require.Equal(t, []string{"node-2/agent", "node-2/server"}, intersect(h.on(d1), h.clientTargets(clientAlpha)))

	// The suspension runs out: node-2 goes straight to the latest build in a
	// rollout of its own.
	h.clock.Advance(time.Hour)
	h.settle(100)
	require.Equal(t, h.clientTargets(clientAlpha), h.on(d3)[:len(h.clientTargets(clientAlpha))])

	last := h.rolloutsOf(clientAlpha)[2]
	require.Equal(t, Complete, last.State)
	require.Len(t, last.Targets, 2)
	require.Equal(t, 1, h.updates()["node-2/server"], "d2 was never deployed on node-2")
}

func TestUsageAManualClientWaitsForASyncAndPausesAfterItsFirstNode(t *testing.T) {
	h := newFleet(t, usageConfig)
	require.NoError(t, h.c.SetPolicy(h.ctx, actor, clientCharlie, Policy{Mode: ModeManual, Strategy: careful}))

	h.release(imgCharlie, d2)
	r := h.active(clientCharlie)

	for range 5 {
		h.step()
	}

	require.Equal(t, WaitingForSync, h.rollout(r.ID).State)
	require.Empty(t, h.on(d2), "nothing moves until someone syncs")

	_, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=charlie")})
	require.NoError(t, err)

	r = h.until(r.ID, Paused, 40)
	require.Equal(t, "Paused after batch 1. Run promote to continue.", r.Reason)
	require.Equal(t, []string{"node-7/agent", "node-7/server"}, h.on(d2))

	require.NoError(t, h.c.Promote(h.ctx, actor, r.ID))
	h.settle(100)
	require.Equal(t, Complete, h.rollout(r.ID).State)
	require.Equal(t, h.clientTargets(clientCharlie), h.on(d2))
}

func TestUsageForceSkipsTheChecksButNotTheBudget(t *testing.T) {
	// A 10% budget allows 80 of 800: less than one node, so one node at a
	// time.
	h := newFleet(t, replaceLine(usageConfig, "maxUnavailable: 25%", "maxUnavailable: 10%"))

	// The whole network is failing its checks. A fix is forced through:
	// readiness and soak are skipped, the budget is not.
	h.world.set(func(w *world) {
		w.soakFail[soakServer] = true

		for _, id := range h.clientTargets(clientAlpha) {
			w.notReady[id] = true
		}
	})
	h.release(imgAlpha, d2)

	_, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=alpha"), Force: true})
	require.NoError(t, err)

	h.settle(100)

	r := h.rolloutsOf(clientAlpha)[0]
	require.Equal(t, Complete, r.State)
	require.Equal(t, [][]string{{observer}, {"node-1", "node-2"}, {"node-3"}}, batchNodes(&r))
	require.Equal(t, h.clientTargets(clientAlpha), h.on(d2))
}

func TestUsageANodeThatDropsOffIsSkippedAndCaughtUpLater(t *testing.T) {
	h := newFleet(t, usageConfig)
	h.release(imgAlpha, d2)
	r := h.active(clientAlpha)

	// node-3 stops answering before its batch.
	h.world.set(func(w *world) { w.inspectFail["node-3/server"], w.inspectFail["node-3/agent"] = true, true })
	h.settle(100)

	r = h.rollout(r.ID)
	require.Equal(t, Complete, r.State)
	require.Equal(t, "5 of 7 targets on 2222222; 2 skipped and left for the next rollout.", r.Reason)
	require.Equal(t, PhaseSkipped, h.phases(r)["node-3/server"])
	require.Equal(t, Unknown, h.view("node-3/server").Sync)

	// It comes back and catches up in a rollout of its own.
	h.world.set(func(w *world) { clear(w.inspectFail) })
	h.settle(100)
	require.Equal(t, h.clientTargets(clientAlpha), h.on(d2))
	require.Len(t, h.rolloutsOf(clientAlpha), 2)
}

func TestUsageANodeJoinsMidRollout(t *testing.T) {
	h := newFleet(t, usageConfig)
	h.release(imgAlpha, d2)
	r := h.active(clientAlpha)
	h.step()

	// A new node joins on the old build while the rollout runs.
	joined := h.fleetWith(func(doc string) string {
		return doc + "- {id: node-9/server, node: node-9, weight: 100, image: org/alpha:stable, labels: {client: alpha, owner: alpha, role: server, wave: \"1\"}, hooks: {soak: soak-server}}\n"
	})
	h.world.set(func(w *world) { w.running["node-9/server"] = d1 })
	h.swap(joined)
	h.settle(200)

	// The running rollout finishes what it started; the next one takes the
	// new node.
	require.NotContains(t, h.phases(h.rollout(r.ID)), "node-9/server")
	require.Len(t, h.rolloutsOf(clientAlpha), 2)
	require.Contains(t, h.on(d2), "node-9/server")
}

func TestUsageANodeLeavesMidRollout(t *testing.T) {
	h := newFleet(t, usageConfig)
	h.release(imgAlpha, d2)
	r := h.active(clientAlpha)
	h.step()

	// node-3 is taken out of the fleet before its batch.
	gone := h.fleetWith(func(doc string) string { return dropLines(doc, "node: node-3,") })
	h.swap(gone)
	h.c.Forget(h.ctx, []string{"node-3/store", "node-3/server", "node-3/agent"})
	h.settle(100)

	r = h.rollout(r.ID)
	require.Equal(t, Complete, r.State)
	require.Equal(t, PhaseSkipped, h.phases(r)["node-3/server"])
	require.Equal(t, "removed from the targets file", r.target("node-3/server").Reason)
	require.Len(t, h.rolloutsOf(clientAlpha), 1)
}

func TestUsageWeightMovesBetweenNodesMidRollout(t *testing.T) {
	h := newFleet(t, usageConfig)
	h.release(imgAlpha, d2)
	r := h.active(clientAlpha)
	h.step()

	// node-3's weight moves to node-4 before node-3's batch; settle checks
	// the budget against the new weights after every step.
	moved := h.fleetWith(func(doc string) string {
		doc = strings.ReplaceAll(doc, "node: node-3, weight: 100", "node: node-3, weight: 0  ")

		return strings.ReplaceAll(doc, "node: node-4, weight: 100", "node: node-4, weight: 200")
	})
	h.swap(moved)
	h.settle(100)

	require.Equal(t, Complete, h.rollout(r.ID).State)
	require.Equal(t, h.clientTargets(clientAlpha), intersect(h.on(d2), h.clientTargets(clientAlpha)))
}

func TestUsageARestartMidBatchResumesTheSameRollout(t *testing.T) {
	h := newFleet(t, usageConfig)
	h.release(imgAlpha, d2)
	r := h.active(clientAlpha)

	for i := 0; i < 40 && len(h.rollout(r.ID).Batches) < 2; i++ {
		h.step()
	}

	// The process restarts on the same store.
	h.c = h.newController()
	h.settle(100)

	require.Equal(t, Complete, h.rollout(r.ID).State)
	require.Len(t, h.rolloutsOf(clientAlpha), 1)

	for _, id := range h.clientTargets(clientAlpha) {
		require.Equal(t, 1, h.updates()[id], "%s updated once", id)
	}
}

func TestUsageARegistryOutageDoesNotDisturbARollout(t *testing.T) {
	h := newFleet(t, usageConfig)
	h.release(imgAlpha, d2)
	r := h.active(clientAlpha)

	h.world.set(func(w *world) { w.registryErr[imgAlpha] = errors.New("503 service unavailable") })
	h.step()
	h.step()
	require.Contains(t, h.c.Fleet().Resolve[imgAlpha], "503")

	// The rollout knows where it is going and finishes.
	h.settle(100)
	require.Equal(t, Complete, h.rollout(r.ID).State)

	// The registry comes back on the same build: no new rollout.
	h.world.set(func(w *world) { clear(w.registryErr) })
	h.step()
	h.step()
	require.Empty(t, h.c.Fleet().Resolve)
	require.Len(t, h.rolloutsOf(clientAlpha), 1)
	require.Contains(t, h.notes.actions(), "registry.recovered")
}

func TestUsageAnObserverOnlyClientNeverWaitsForTheBudget(t *testing.T) {
	h := newFleet(t, replaceLine(usageConfig, "duration: 5m", "duration: 30m"))

	// alpha's second batch soaks on node-1 and node-2 for half an hour,
	// holding the whole budget.
	h.release(imgAlpha, d2)
	alpha := h.active(clientAlpha)

	for i := 0; i < 200 && (len(h.rollout(alpha.ID).Batches) != 2 || h.rollout(alpha.ID).State != Soaking); i++ {
		h.step()
	}

	used, _ := h.unavailable()
	require.InDelta(t, 0, used, 0)

	// delta runs only on the weightless observers, so it goes anyway.
	h.release(imgDelta, d2)
	delta := h.active("delta")
	h.until(delta.ID, Complete, 10)
	require.Equal(t, Soaking, h.rollout(alpha.ID).State, "alpha still holds the budget")
}

func TestUsageAZeroBudgetMovesOnlyWeightlessNodes(t *testing.T) {
	h := newFleet(t, replaceLine(usageConfig, "maxUnavailable: 25%", "maxUnavailable: 0"))
	h.release(imgAlpha, d2)
	r := h.active(clientAlpha)

	for range 20 {
		h.step()
	}

	r = h.rollout(r.ID)
	require.Equal(t, WaitingForBudget, r.State)
	require.Contains(t, r.Reason, "Waiting for the disruption budget")
	require.Equal(t, []string{"obs-1/server"}, h.on(d2))
}

func TestUsageAClientOnASingleNode(t *testing.T) {
	h := newFleet(t, usageConfig)
	h.release(imgEcho, d2)
	r := h.active("echo")
	h.settle(20)

	r = h.rollout(r.ID)
	require.Equal(t, Complete, r.State)
	require.Equal(t, [][]string{{"node-5"}}, batchNodes(&r))
	require.Equal(t, []string{"node-5/tool"}, h.on(d2))
}

func TestUsageOneNodesTargetsInDifferentWavesMoveInSeparatePasses(t *testing.T) {
	h := newHarness(t, replaceLine(usageConfig, "maxUnavailable: 25%", "maxUnavailable: 50%"), `
- {id: n1/server, node: n1, weight: 100, image: org/a:t, labels: {client: a, owner: a, wave: "0"}}
- {id: n1/agent,  node: n1, weight: 100, image: org/a:t, labels: {client: a, owner: a, wave: "1"}}
- {id: n2/server, node: n2, weight: 100, image: org/a:t, labels: {client: a, owner: a, wave: "0"}}
- {id: n2/agent,  node: n2, weight: 100, image: org/a:t, labels: {client: a, owner: a, wave: "1"}}
`)
	h.prime()
	h.release(imgA, d2)
	r := h.active("a")
	h.settle(100)

	// Servers first, one node at a time under the budget; then the agents,
	// visiting each node again.
	r = h.rollout(r.ID)
	require.Equal(t, Complete, r.State)

	batches := make([][]string, 0, len(r.Batches))
	for _, b := range r.Batches {
		batches = append(batches, b.Targets)
	}

	require.Equal(t, [][]string{{"n1/server"}, {"n2/server"}, {"n1/agent"}, {"n2/agent"}}, batches)
}

func TestUsageASlowUpdaterPassesInsideTheDeadlineAndHaltsPastIt(t *testing.T) {
	h := newFleet(t, usageConfig)

	// obs-1's updater takes a minute to land the build, inside the 2m30s
	// the update may take (5 hook timeouts).
	h.world.set(func(w *world) { w.updateStuck["obs-1/server"] = true })
	h.release(imgAlpha, d2)
	h.step()
	h.step()
	h.world.set(func(w *world) {
		w.running["obs-1/server"] = d2
		delete(w.updateStuck, "obs-1/server")
	})
	h.settle(100)
	require.Equal(t, h.clientTargets(clientAlpha), h.on(d2))

	// On the next build it never lands.
	h.world.set(func(w *world) { w.updateStuck["obs-1/server"] = true })
	h.release(imgAlpha, d3)
	r := h.until(h.active(clientAlpha).ID, Halted, 20)
	require.Contains(t, r.Reason, "obs-1/server did not reach 3333333 within 2m30s")
	require.Empty(t, intersect(h.on(d3), h.clientTargets(clientAlpha)))
}

func TestUsageOneFailedSoakCheckIsToleratedAndTwoHalt(t *testing.T) {
	h := newFleet(t, usageConfig)
	h.release(imgAlpha, d2)
	r := h.active(clientAlpha)

	// One check fails in the first batch's soak: within failureLimit 1.
	h.until(r.ID, Soaking, 10)
	h.world.set(func(w *world) { w.soakFail[soakServer] = true })

	for i := 0; i < 10 && h.rollout(r.ID).Soak.Failures == 0; i++ {
		h.step()
	}

	h.world.set(func(w *world) { w.soakFail[soakServer] = false })
	h.settle(100)
	require.Equal(t, Complete, h.rollout(r.ID).State)

	// On the next build two checks fail, and it halts.
	h.release(imgAlpha, d3)
	next := h.active(clientAlpha)
	h.until(next.ID, Soaking, 10)
	h.world.set(func(w *world) { w.soakFail[soakServer] = true })

	halted := h.until(next.ID, Halted, 10)
	require.Contains(t, halted.Reason, "soak soak-server failed 2 times")
}

func TestUsageAnAbortHoldsUntilASyncOrANewerBuild(t *testing.T) {
	h := newFleet(t, usageConfig)
	d4 := "sha256:" + strings.Repeat("4", 64)

	h.release(imgAlpha, d2)
	r := h.active(clientAlpha)
	h.step()
	require.NoError(t, h.c.Abort(h.ctx, actor, r.ID))

	// The group stays behind; nothing reopens on its own.
	for range 5 {
		h.step()
	}

	require.False(t, h.anyActive())

	// A person syncs the same build: a new rollout finishes the job.
	_, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=alpha")})
	require.NoError(t, err)
	h.settle(100)
	require.Equal(t, h.clientTargets(clientAlpha), intersect(h.on(d2), h.clientTargets(clientAlpha)))

	// Aborted again on the next build; a newer build rolls without a sync.
	h.release(imgAlpha, d3)
	require.NoError(t, h.c.Abort(h.ctx, actor, h.active(clientAlpha).ID))
	h.release(imgAlpha, d4)
	h.settle(100)
	require.Equal(t, h.clientTargets(clientAlpha), intersect(h.on(d4), h.clientTargets(clientAlpha)))
}

// intersect lists the ids in both, sorted.
func intersect(a, b []string) []string {
	var out []string

	for _, id := range a {
		if slices.Contains(b, id) {
			out = append(out, id)
		}
	}

	slices.Sort(out)

	return out
}

// dropLines removes every line containing a fragment.
func dropLines(doc, fragment string) string {
	lines := strings.Split(doc, "\n")

	return strings.Join(slices.DeleteFunc(lines, func(l string) bool { return strings.Contains(l, fragment) }), "\n")
}

func TestUsageUnrelatedOutageConsumesBudget(t *testing.T) {
	h := newFleet(t, replaceLine(usageConfig, "maxUnavailable: 25%", "maxUnavailable: 10%"))
	h.world.set(func(w *world) { w.notReady["node-8/store"] = true })

	for range h.cfg.ReadinessProbe.FailureThreshold {
		h.c.ProbeAll(h.ctx)
	}

	h.release(imgEcho, d2)
	r := h.active("echo")
	require.Equal(t, WaitingForBudget, r.State)
	require.Empty(t, h.world.callsFor("update"))
	require.Equal(t, Degraded, h.view("node-8/store").Health)
	used, _ := h.unavailable()
	require.InDelta(t, 100, used, 0)

	h.world.set(func(w *world) { delete(w.notReady, "node-8/store") })
	h.settle(30)
	require.Equal(t, Complete, h.rollout(r.ID).State)
	require.Equal(t, []string{"node-5/tool"}, h.on(d2))
}

func TestUsageFleetWideFailureRollsItsFixWithoutBudgetWait(t *testing.T) {
	h := newFleet(t, replaceLine(usageConfig, "duration: 5m", "duration: 0s"))
	h.world.set(func(w *world) {
		for _, target := range h.current().Targets {
			w.notReady[target.ID] = true
			w.recoverOnUpdate[target.ID] = true
			w.registry[target.Image] = d2
		}
	})

	for range h.cfg.ReadinessProbe.FailureThreshold {
		h.c.ProbeAll(h.ctx)
	}

	used, allowed := h.unavailable()
	require.Greater(t, used, allowed)

	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()

	for _, r := range h.c.Rollouts() {
		require.Equal(t, Running, r.State)
		require.NotEmpty(t, r.Batches)

		for _, rt := range r.Targets {
			if rt.Batch > 0 {
				require.True(t, rt.Free)
			}
		}
	}

	h.settle(100)

	for _, v := range h.c.Targets(nil) {
		require.Equal(t, d2, v.Live)
		require.Equal(t, Healthy, v.Health)
	}
}

func TestUsageTargetRecoversWithoutRollout(t *testing.T) {
	h := newFleet(t, usageConfig)
	h.world.set(func(w *world) { w.notReady["node-1/server"] = true })

	for range h.cfg.ReadinessProbe.FailureThreshold {
		h.clock.Advance(h.cfg.ReadinessProbe.Period)
		h.tick()
	}

	failed := h.view("node-1/server")
	require.Equal(t, Degraded, failed.Health)
	require.Equal(t, Synced, failed.Sync)
	require.Equal(t, Degraded, h.c.Fleet().Health)

	h.world.set(func(w *world) { delete(w.notReady, "node-1/server") })
	h.clock.Advance(h.cfg.ReadinessProbe.Period)
	h.tick()
	recovered := h.view("node-1/server")
	require.Equal(t, Healthy, recovered.Health)
	require.True(t, recovered.Readiness.Since.After(failed.Readiness.Since))
	require.Equal(t, Healthy, h.c.Fleet().Health)
	require.Empty(t, h.c.Rollouts())
	require.Empty(t, h.world.callsFor("update"))
}

func TestUsageFlappingBelowFailureThresholdStaysHealthy(t *testing.T) {
	h := newFleet(t, usageConfig)
	since := h.view("node-1/server").Readiness.Since

	for range 4 {
		h.world.set(func(w *world) { w.notReady["node-1/server"] = true })

		for range h.cfg.ReadinessProbe.FailureThreshold - 1 {
			h.clock.Advance(h.cfg.ReadinessProbe.Period)
			h.c.ProbeAll(h.ctx)
			require.Equal(t, Healthy, h.view("node-1/server").Health)
		}

		h.world.set(func(w *world) { delete(w.notReady, "node-1/server") })
		h.clock.Advance(h.cfg.ReadinessProbe.Period)
		h.c.ProbeAll(h.ctx)
		require.Equal(t, since, h.view("node-1/server").Readiness.Since)
	}

	require.Equal(t, Healthy, h.c.Fleet().Health)
	require.Empty(t, h.c.Rollouts())
}
