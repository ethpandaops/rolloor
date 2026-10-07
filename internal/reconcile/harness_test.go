package reconcile

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/hooks"
	"github.com/ethpandaops/rolloor/internal/registry"
	"github.com/ethpandaops/rolloor/internal/targets"
)

const (
	actor     = "sam"
	other     = "robin"
	speedTest = "test"
	speedAll  = "all"
	labelWave = "wave"
	tA1       = "a-1/cl"
	tA2       = "a-2/cl"
	tA3       = "a-3/cl"
	tA4       = "a-4/cl"
	tA5       = "a-5/cl"
	tA6       = "a-6/cl"
	refused   = "no"
	mine      = "mine"
	tA3el     = "a-3/el"
	tA3VC     = "a-3/vc"
	tN0A      = "n0/a"
	tN1A      = "n1/a"
	tN2A      = "n2/a"

	imgA  = "org/a:t"
	imgB  = "org/b:t"
	imgS  = "org/s:t"
	d1    = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	d2    = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	d3    = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
	soakA = "soak-a"
	soakB = "soak-b"

	groupLabel        = "client"
	careful           = "careful"
	boom              = "boom"
	debugReason       = "debugging"
	maintenanceReason = "maintenance"
)

const testConfig = `
environment: test
disruptionBudget: {maxUnavailable: 50%}
registry: {poll: 60s}
hooks:
  dir: /tmp
  timeout: 10s
  defaults: {soak: ""}
inspect: {interval: 30s, concurrency: 4, failureThreshold: 2}
labels: {group: client, owner: owner, section: role, hiddenGroups: [side]}
strategy: {batchSize: 2, progressDeadline: 50s, retry: {limit: 1}, soak: {duration: 60s, interval: 20s, failureLimit: 1}}
strategies:
  nosoak:  {batchSize: 50%, soak: {duration: 0s}}
  careful: {firstBatch: 1, batchSize: 50%, soak: {duration: 0s}, pauseAfterFirstBatch: true}
  all:     {batchSize: 100%, soak: {duration: 0s}, waves: false}
`

// The standard fleet. Weight lives on nodes a-3..a-6 and b-1 (100 each), so
// the total is 500 and a 50% budget allows two weighted nodes at once.
const testTargets = `
- {id: a-1/cl, node: a-1, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: "0"}, hooks: {soak: soak-a}}
- {id: a-2/cl, node: a-2, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: "0"}, hooks: {soak: soak-a}}
- {id: a-3/cl, node: a-3, weight: 100, image: org/a:t, labels: {client: a, owner: a, role: cl, wave: "1"}, hooks: {soak: soak-a}}
- {id: a-4/cl, node: a-4, weight: 100, image: org/a:t, labels: {client: a, owner: a, role: cl, wave: "1"}, hooks: {soak: soak-a}}
- {id: a-5/cl, node: a-5, weight: 100, image: org/a:t, labels: {client: a, owner: a, role: cl, wave: "1"}, hooks: {soak: soak-a}}
- {id: a-6/cl, node: a-6, weight: 100, image: org/a:t, labels: {client: a, owner: a, role: cl, wave: "2"}, hooks: {soak: soak-a}}
- {id: a-3/el, node: a-3, weight: 100, image: org/b:t, labels: {client: b, owner: b, role: el, wave: "1"}, hooks: {soak: soak-b}}
- {id: b-1/el, node: b-1, weight: 100, image: org/b:t, labels: {client: b, owner: b, role: el, wave: "1"}, hooks: {soak: soak-b}}
- {id: a-3/side, node: a-3, weight: 100, image: org/s:t, labels: {client: side, owner: operators, role: sidecar}}
`

var testRules = targets.Rules{GroupLabel: groupLabel, OwnerLabel: "owner", WaveLabel: labelWave, KnownHooks: config.TargetHooks}

// world is the fake registry, fleet and hook behaviour behind a test.
type world struct {
	mu sync.Mutex

	registry    map[string]string
	revisions   map[string]string
	registryErr map[string]error

	running         map[string]string
	inspectFail     map[string]bool
	updateFail      map[string]string
	updateStuck     map[string]bool
	notReady        map[string]bool
	recoverOnUpdate map[string]bool
	soakFail        map[string]bool
	soakStdout      string
	runErr          map[string]error
	runResult       map[string]hooks.Result
	gates           map[string]chan struct{}
	entered         chan struct{}
	resolved        int

	calls []string
}

func newWorld() *world {
	return &world{
		registry:        map[string]string{imgA: d1, imgB: d1, imgS: d1},
		revisions:       map[string]string{imgA: "reva", imgB: "revb"},
		registryErr:     map[string]error{},
		running:         map[string]string{},
		inspectFail:     map[string]bool{},
		updateFail:      map[string]string{},
		updateStuck:     map[string]bool{},
		notReady:        map[string]bool{},
		recoverOnUpdate: map[string]bool{},
		soakFail:        map[string]bool{},
		runErr:          map[string]error{},
		runResult:       map[string]hooks.Result{},
		gates:           map[string]chan struct{}{},
	}
}

// gate makes the next call of kind ("resolve", a hook name, or
// "hook:target" for one target's) block until
// the returned function is called, so a test can overlap two operations.
func (w *world) gate(kind string) (entered <-chan struct{}, release func()) {
	in := make(chan struct{})
	out := make(chan struct{})

	w.mu.Lock()
	w.gates[kind] = out
	w.entered = in
	w.mu.Unlock()

	return in, func() { close(out) }
}

// wait blocks on the gate for kind if one is set, once.
func (w *world) wait(kind string) {
	w.mu.Lock()
	g, ok := w.gates[kind]
	entered := w.entered

	if ok {
		delete(w.gates, kind)
		w.entered = nil
	}

	w.mu.Unlock()

	if ok {
		close(entered)
		<-g
	}
}

func (w *world) Resolve(_ context.Context, ref string) (registry.Resolved, error) {
	w.wait("resolve")

	w.mu.Lock()
	defer w.mu.Unlock()

	w.resolved++

	if err, ok := w.registryErr[ref]; ok {
		return registry.Resolved{}, err
	}

	d, ok := w.registry[ref]
	if !ok {
		return registry.Resolved{}, errors.New("unknown image")
	}

	return registry.Resolved{Digest: d, Revision: w.revisions[ref], At: time.Now()}, nil
}

func (w *world) Run(_ context.Context, program, hook, targetID string, input any) (hooks.Result, error) {
	w.wait(hook)
	w.wait(hook + ":" + targetID)

	w.mu.Lock()
	defer w.mu.Unlock()

	w.calls = append(w.calls, hook+":"+targetID+":"+program)

	if err, ok := w.runErr[program]; ok {
		return hooks.Result{}, err
	}

	if res, ok := w.runResult[program]; ok {
		return res, nil
	}

	res := hooks.Result{Program: program, OK: true, RanAt: time.Now()}

	switch hook {
	case config.HookInspect:
		if w.inspectFail[targetID] {
			res.OK, res.Reason = false, "connection refused"

			return res, nil
		}

		res.Reason = w.running[targetID]
		res.Stdout = res.Reason
	case config.HookUpdate:
		if why, ok := w.updateFail[targetID]; ok {
			res.OK, res.Reason = false, why

			return res, nil
		}

		// A faithful updater deploys the digest it is told, not the tag head.
		if !w.updateStuck[targetID] {
			in, _ := input.(HookInput)
			if in.Desired != "" {
				w.running[targetID] = in.Desired
			} else {
				w.running[targetID] = w.registry[in.Image]
			}

			if w.recoverOnUpdate[targetID] {
				delete(w.notReady, targetID)
			}
		}

		res.Reason = "update started"
	case config.HookReady:
		if w.notReady[targetID] {
			res.OK, res.Reason = false, "still syncing"
		} else {
			res.Reason = "following"
		}
	case config.HookSoak:
		if w.soakFail[program] {
			res.OK, res.Reason = false, "61% vs 98%"
		} else {
			res.Reason = "fine"
		}

		res.Stdout = res.Reason + "\n" + w.soakStdout
	}

	return res, nil
}

func (w *world) set(fn func(w *world)) {
	w.mu.Lock()
	defer w.mu.Unlock()

	fn(w)
}

// resolves is how many registry lookups have been made.
func (w *world) resolves() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.resolved
}

func (w *world) callsFor(hook string) []string {
	w.mu.Lock()
	defer w.mu.Unlock()

	var out []string

	for _, c := range w.calls {
		if strings.HasPrefix(c, hook+":") {
			out = append(out, c)
		}
	}

	return out
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.now = f.now.Add(d)
}

type notes struct {
	mu     sync.Mutex
	events []Event
}

func (n *notes) Publish(e *Event) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.events = append(n.events, *e)
}

func (n *notes) actions() []string {
	n.mu.Lock()
	defer n.mu.Unlock()

	out := make([]string, 0, len(n.events))
	for _, e := range n.events {
		out = append(out, e.Action)
	}

	return out
}

// harness ties a controller to its fakes.
type harness struct {
	t             *testing.T
	ctx           context.Context //nolint:containedctx // a test harness carries the test's context
	cfg           *config.Config
	set           *targets.Set
	world         *world
	clock         *fakeClock
	store         *MemoryStore
	notes         *notes
	c             *Controller
	ids           int
	admission     map[string]float64
	admissionUsed float64
	// setMu guards set for tests that swap it while programs are running.
	setMu sync.Mutex
}

// current is the controller's view of the targets.
func (h *harness) current() *targets.Set {
	h.setMu.Lock()
	defer h.setMu.Unlock()

	return h.set
}

// swap replaces the targets while the controller may be reading them.
func (h *harness) swap(set *targets.Set) {
	h.setMu.Lock()
	defer h.setMu.Unlock()

	h.set = set
}

func newHarness(t *testing.T, cfgYAML, targetsYAML string) *harness {
	t.Helper()

	cfg, err := config.Parse([]byte(cfgYAML))
	require.NoError(t, err)

	set, err := targets.Parse([]byte(targetsYAML), &testRules)
	require.NoError(t, err)

	h := &harness{
		t: t, ctx: context.Background(), cfg: cfg, set: set,
		world: newWorld(), clock: &fakeClock{now: time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)},
		store: NewMemoryStore(), notes: &notes{},
	}

	for _, tg := range set.Targets {
		h.world.running[tg.ID] = d1
	}

	h.c = h.newController()

	return h
}

func (h *harness) newController() *Controller {
	h.t.Helper()

	c, err := New(h.ctx, &Options{
		Config: h.cfg, Targets: h.current, Resolver: h.world, Runner: h.world,
		Store: h.store, Notifier: h.notes, Clock: h.clock, Log: logrus.New(),
		NewID: h.nextID,
	})
	require.NoError(h.t, err)

	return c
}

// nextID hands out rollout and suspension ids that stay unique across
// controllers built on the same harness.
func (h *harness) nextID() string {
	h.ids++

	return fmt.Sprintf("id-%d", h.ids)
}

// prime inspects everything and resolves once, so every target is Synced.
func (h *harness) prime() {
	h.t.Helper()
	h.c.InspectAll(h.ctx)
	h.c.ProbeAll(h.ctx)
	require.NoError(h.t, h.c.Tick(h.ctx))
}

func (h *harness) tick() {
	h.t.Helper()
	h.c.InspectAll(h.ctx)
	h.clock.Advance(time.Nanosecond)
	h.c.ProbeAll(h.ctx)
	h.admission = h.oracleUnavailable()
	h.admissionUsed, _ = h.unavailable()

	require.NoError(h.t, h.c.Tick(h.ctx))
}

// ticks advances the clock by step between n ticks.
func (h *harness) ticks(n int, step time.Duration) {
	h.t.Helper()

	for i := 0; i < n; i++ {
		h.tick()
		h.clock.Advance(step)
	}
}

// release moves the registry to a new digest and lets the poll notice it.
func (h *harness) release(image, digest string) {
	h.t.Helper()
	h.world.set(func(w *world) { w.registry[image] = digest })
	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()
}

// drive ticks until the rollout is no longer active or the limit is reached.
func (h *harness) drive(id string, limit int, step time.Duration) RolloutView {
	h.t.Helper()

	for i := 0; i < limit && h.rollout(id).State.Active(); i++ {
		h.ticks(1, step)
	}

	return h.rollout(id)
}

func (h *harness) rollout(id string) RolloutView {
	h.t.Helper()

	r, ok := h.c.Rollout(id)
	require.True(h.t, ok, "rollout %s", id)

	return r
}

func (h *harness) rolloutTarget(rolloutID, targetID string) *RolloutTarget {
	h.t.Helper()
	r := h.rollout(rolloutID)
	rt := r.target(targetID)
	require.NotNil(h.t, rt)

	return rt
}

func (h *harness) active(group string) RolloutView {
	h.t.Helper()

	for _, r := range h.c.Rollouts() {
		if r.Group == group && r.State.Active() {
			return r
		}
	}

	h.t.Fatalf("no active rollout for %s", group)

	return RolloutView{}
}

func (h *harness) phases(r RolloutView) map[string]TargetPhase {
	out := map[string]TargetPhase{}
	for _, rt := range r.Targets {
		out[rt.ID] = rt.Phase
	}

	return out
}

// unavailable sums the independent oracle's unavailable nodes, each at least
// the weight its rollout targets recorded at admission, and the weight the
// configured budget allows.
func (h *harness) unavailable() (used, allowed float64) {
	set := h.current()

	for node, least := range h.oracleUnavailable() {
		used += max(set.NodeWeight(node), least)
	}

	return used, h.cfg.DisruptionBudget.MaxUnavailable.OfWeight(set.TotalWeight())
}

func (h *harness) oracleUnavailable() map[string]float64 {
	set, now := h.current(), h.clock.Now()

	h.c.mu.RLock()
	defer h.c.mu.RUnlock()

	return h.oracleUnavailableLocked(set, now, "", nil)
}

func (h *harness) oracleUnavailableLocked(set *targets.Set, now time.Time, changed string, before *Rollout) map[string]float64 {
	nodes := map[string]float64{}

	for _, t := range set.Targets {
		probe := h.c.live[t.ID].Readiness
		if !probe.Ready || probe.ProbedAt.IsZero() {
			nodes[t.Node] = 0
		}
	}

	for id, r := range h.c.rollouts {
		if id == changed {
			r = before
		}

		if r == nil {
			continue
		}

		h.oracleRolloutUnavailableLocked(set, now, r, nodes)
	}

	return nodes
}

func (h *harness) oracleRolloutUnavailableLocked(set *targets.Set, now time.Time, r *Rollout, nodes map[string]float64) {
	for _, rt := range r.Targets {
		t, present := set.Get(rt.ID)
		live := h.c.live[rt.ID]
		probe := live.Readiness
		known := !live.SeenAt.IsZero() && live.Failures < h.cfg.Inspect.FailureThreshold
		sameTarget := present && t.Node == rt.Node && t.Image == rt.Image
		observedReady := sameTarget && known && live.Digest == r.Desired[rt.Image] && probe.Ready && probe.LastSuccessAt.After(live.DigestSince) && probe.LastSuccessAt.After(rt.UpdatedAt)
		forced := sameTarget && r.Force && rt.Updated && (rt.Phase == PhaseReady || rt.Phase == PhasePassed) && (!probe.Ready || rt.DigestSeenAt.IsZero() || !probe.LastSuccessAt.After(rt.DigestSeenAt))

		if (now.Before(rt.HoldUntil) || forced) && !observedReady {
			nodes[rt.Node] = max(nodes[rt.Node], rt.NodeWeight)
		}

		b := r.CurrentBatch()
		if !r.State.Active() || b == nil || rt.Batch != b.Number || (rt.Phase != PhasePending && rt.Phase != PhaseUpdating && rt.Phase != PhaseReady) {
			continue
		}

		if !present || !rt.Updated || !probe.Ready || !probe.LastSuccessAt.After(rt.DigestSeenAt) {
			nodes[rt.Node] = max(nodes[rt.Node], rt.NodeWeight)
		}
	}
}

func (h *harness) view(id string) TargetView {
	h.t.Helper()

	v, ok := h.c.Target(id)
	require.True(h.t, ok)

	return v
}
