package reconcile

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/rolloor/internal/targets"
)

// The property test drives random fleets through random builds, program
// failures, verbs, targets-file edits, restarts, crashes and store outages,
// and checks after every step what must always hold. Once the steps run out
// every failure is healed, and every target must end on its desired build.
//
// Reproduce a failure with ROLLOOR_SIM_SEED=<seed>; run more seeds with
// ROLLOOR_SIM_SEEDS=<n>.

// propSuspension is a suspension the controller accepted.
type propSuspension struct {
	sel     targets.Selector
	expires time.Time
}

func (s *propSuspension) matches(t *propTarget) bool {
	for k, v := range s.sel {
		switch k {
		case targets.KeyID:
			if t.id != v {
				return false
			}
		case groupLabel:
			if t.group != v {
				return false
			}
		default:
			return false
		}
	}

	return true
}

// sim is one simulated environment. Besides the harness it keeps its own
// record of what it did to the controller and the world, so the checks rest
// on evidence the controller did not produce.
type sim struct {
	h     *harness
	rng   *rand.Rand
	fleet *propFleet
	proc  *process
	log   *logrus.Logger

	// down is set when the store refused to load and no process runs.
	down   bool
	outage bool

	digests int
	builds  map[string][]string
	modes   map[string]string
	pins    map[string]map[string]string

	suspended []propSuspension
	terminal  map[string]RolloutState
	manual    map[string]bool
	strategy  map[string]string
	before    map[string]*RolloutView
	trail     []string

	mu         sync.Mutex
	violations []string
	dispatches []propDispatch
}

func newSim(t *testing.T, rng *rand.Rand) (*sim, string) {
	t.Helper()

	cfg := propConfig(rng)
	fleet := newPropFleet(rng)

	log := logrus.New()
	log.SetOutput(io.Discard)

	s := &sim{
		h: newHarness(t, cfg, fleet.yaml()), rng: rng, fleet: fleet, log: log,
		builds: map[string][]string{}, modes: map[string]string{}, pins: map[string]map[string]string{},
		terminal: map[string]RolloutState{}, manual: map[string]bool{}, strategy: map[string]string{},
	}

	s.h.world.set(func(w *world) { w.registry[propImage("c")] = d1 })
	s.start()
	s.h.prime()

	return s, cfg
}

// start runs a new process on whatever the store holds.
func (s *sim) start() {
	h := s.h
	s.proc = &process{MemoryStore: h.store, sim: s, writes: -1}

	c, err := New(h.ctx, &Options{
		Config: h.cfg, Targets: h.current, Resolver: h.world, Runner: s.proc, Store: s.proc,
		Notifier: h.notes, Clock: h.clock, Log: s.log, NewID: h.nextID,
	})
	if err != nil {
		s.down = true

		return
	}

	h.c, s.down = c, false
}

func (s *sim) violation(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.violations = append(s.violations, fmt.Sprintf(format, args...))
}

// want is the digest a target should end on.
func (s *sim) want(t *targets.Target) string {
	if pin, ok := s.pins[s.h.current().Group(t)][t.Image]; ok {
		return pin
	}

	s.h.world.mu.Lock()
	defer s.h.world.mu.Unlock()

	return s.h.world.registry[t.Image]
}

// converge heals every failure, acts as a patient operator (resume, retry,
// promote, sync), and requires every target to end on its desired build.
func (s *sim) converge() error {
	h := s.h

	h.world.set(func(w *world) {
		clear(w.updateFail)
		clear(w.updateStuck)
		clear(w.notReady)
		clear(w.soakFail)
		clear(w.inspectFail)
		clear(w.runErr)
		clear(w.registryErr)
	})

	s.outage = false
	s.setOutage(false)

	if s.down {
		s.start()

		if s.down {
			return errors.New("no process starts on a healthy store")
		}
	}

	var err error

	for range 60 {
		s.operate()

		for range 5 {
			h.c.InspectAll(h.ctx)
			h.clock.Advance(20 * time.Second)
			h.tick()
		}

		if err = s.converged(); err == nil {
			return nil
		}
	}

	return err
}

func (s *sim) operate() {
	h := s.h

	for len(s.suspended) > 0 {
		before := len(s.suspended)
		s.lift(s.suspended[0].sel)

		if len(s.suspended) == before {
			s.suspended = s.suspended[1:]
		}
	}

	busy := map[string]bool{}

	for _, r := range h.c.Rollouts() {
		if !r.State.Active() {
			continue
		}

		busy[r.Group] = true

		switch r.State {
		case Halted:
			if r.RetryPending {
				_ = h.c.Abort(h.ctx, actor, r.ID)
			} else {
				_ = h.c.Retry(h.ctx, actor, r.ID, "healed")
			}
		case Paused:
			_ = h.c.Promote(h.ctx, actor, r.ID)
		case WaitingForSync:
			_, _ = h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: targets.Selector{groupLabel: r.Group}})
		case Running, Soaking, WaitingForBudget, Aborted, Superseded, Complete:
		}
	}

	// A group whose abort still holds sits out of sync with no rollout.
	for _, g := range h.current().Groups() {
		if !busy[g] {
			_, _ = h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: targets.Selector{groupLabel: g}})
		}
	}
}

// converged reports the first target not on its desired build, or an
// active rollout left over.
func (s *sim) converged() error {
	h := s.h
	set := h.current()

	for i := range set.Targets {
		t := &set.Targets[i]
		want := s.want(t)

		h.world.mu.Lock()
		running := h.world.running[t.ID]
		h.world.mu.Unlock()

		v := h.view(t.ID)
		if running != want {
			return fmt.Errorf("%s runs %s, want %s: %s", t.ID, shortDigest(running), shortDigest(want), v.Reason)
		}

		if v.Sync != Synced || v.Health != Healthy {
			return fmt.Errorf("%s runs its desired build but reads %s and %s: %s", t.ID, v.Sync, v.Health, v.Reason)
		}
	}

	for _, r := range h.c.Rollouts() {
		if r.State.Active() {
			return fmt.Errorf("rollout %s is still %s: %s", r.ID, r.State, r.Reason)
		}
	}

	return nil
}

// runSim runs one simulation and returns a report of the first failure.
func runSim(t *testing.T, rng *rand.Rand, steps int) error {
	t.Helper()

	s, cfg := newSim(t, rng)

	fail := func(err error) error {
		const shown = 40

		return fmt.Errorf("%w\nconfig:%s\ntargets:\n%s\nlast steps:\n  %s",
			err, cfg, s.fleet.yaml(), strings.Join(s.trail[max(0, len(s.trail)-shown):], "\n  "))
	}

	for step := range steps {
		s.snapshot()
		s.trail = append(s.trail, s.act())

		if err := s.check(); err != nil {
			return fail(fmt.Errorf("step %d: %w", step, err))
		}
	}

	if err := s.converge(); err != nil {
		var rollouts []string
		for _, r := range s.h.c.Rollouts() {
			rollouts = append(rollouts, fmt.Sprintf("%s %s %s: %s", r.ID, r.Group, r.State, r.Reason))
		}

		return fail(fmt.Errorf("after healing every failure: %w\nrollouts:\n  %s", err, strings.Join(rollouts, "\n  ")))
	}

	return nil
}

// propSeeds is which seeds to run: one from ROLLOOR_SIM_SEED, a count from
// ROLLOOR_SIM_SEEDS, else 1000, or 100 in short mode.
func propSeeds(t *testing.T) (first, n uint64) {
	t.Helper()

	if v := os.Getenv("ROLLOOR_SIM_SEED"); v != "" {
		seed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("ROLLOOR_SIM_SEED: %v", err)
		}

		return seed, 1
	}

	if v := os.Getenv("ROLLOOR_SIM_SEEDS"); v != "" {
		count, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			t.Fatalf("ROLLOOR_SIM_SEEDS: %v", err)
		}

		return 0, count
	}

	if testing.Short() {
		return 0, 100
	}

	return 0, 1000
}

// TestPropertyControllerInvariants runs the simulation for many seeds. A
// failure names the seed, the config, the fleet and the last steps.
func TestPropertyControllerInvariants(t *testing.T) {
	const steps, maxFailures = 200, 3

	first, n := propSeeds(t)
	failures := 0

	for seed := first; seed < first+n; seed++ {
		if err := runSim(t, rand.New(rand.NewPCG(seed, 0x5eed)), steps); err != nil {
			t.Errorf("seed %d: %v", seed, err)

			failures++
		}

		if failures == maxFailures {
			t.Fatalf("stopping after %d failing seeds", failures)
		}
	}
}
