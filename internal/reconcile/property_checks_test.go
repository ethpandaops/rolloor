package reconcile

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// dispatched checks an update at the moment the process runs it: the batch
// that admitted it is already on disk, the target is not suspended, its group
// is not paused by config, and the rollout was free to move when the step began.
func (s *sim) dispatched(disk *MemoryStore, id, digest string) {
	if !onDisk(disk, id, digest) {
		s.violation("update %s to %s ran before its batch and dispatch reached the store", id, shortDigest(digest))
	}

	t, ok := s.fleet.get(id)
	if !ok {
		s.violation("update ran for %s, which is not in the targets file", id)

		return
	}

	now := s.h.clock.Now()

	for i := range s.suspended {
		if sp := &s.suspended[i]; now.Before(sp.expires) && sp.matches(&t) {
			s.violation("update %s ran while suspended by %s", id, sp.sel)
		}
	}

	if s.configPaused(t.group) {
		s.violation("update %s ran while group %s is paused by config", id, t.group)
	}

	s.mu.Lock()
	s.dispatches = append(s.dispatches, propDispatch{id: id, group: t.group, digest: digest, at: now})
	s.mu.Unlock()
}

// configPaused reports whether the config file pauses a group.
func (s *sim) configPaused(group string) bool {
	return s.h.cfg.Paused || s.h.cfg.Groups[group].Paused
}

// admissionProblem checks a durable reservation before its hooks run, using
// current observations and the previous reservation rather than Free flags.
// The caller holds the controller lock while publishing the decision.
func (s *sim) admissionProblem(r, before *Rollout) error {
	b := r.CurrentBatch()
	if !r.State.Active() || b == nil {
		return nil
	}

	var prior *Batch
	if before != nil {
		prior = before.CurrentBatch()
	}

	if (prior == nil || prior.Number != b.Number) && s.configPaused(r.Group) {
		return fmt.Errorf("rollout %s admitted batch %d while group %s is paused by config", r.ID, b.Number, r.Group)
	}

	var nodes map[string]bool

	for _, id := range b.Targets {
		rt := r.target(id)
		if rt.Phase != PhasePending && rt.Phase != PhaseUpdating && rt.Phase != PhaseReady {
			continue
		}

		if prior != nil && prior.Number == b.Number {
			old := before.target(id)
			if old != nil && old.Batch == b.Number && (old.Phase == PhasePending || old.Phase == PhaseUpdating || old.Phase == PhaseReady) {
				continue
			}
		}

		if nodes == nil {
			nodes = map[string]bool{}
		}

		nodes[rt.Node] = true
	}

	if len(nodes) == 0 {
		return nil
	}

	h := s.h
	set := h.current()
	busy := h.oracleUnavailableLocked(set, h.clock.Now(), r.ID, before)

	var used, cost float64

	for node, least := range busy {
		used += max(set.NodeWeight(node), least)
	}

	for node := range nodes {
		weight := set.NodeWeight(node)
		if _, held := busy[node]; held || weight == 0 {
			delete(nodes, node)

			continue
		}

		cost += weight
	}

	allowed := h.cfg.DisruptionBudget.MaxUnavailable.OfWeight(set.TotalWeight())
	if cost == 0 || used+cost <= allowed || (allowed > 0 && used == 0 && len(nodes) == 1) {
		return nil
	}

	return fmt.Errorf("rollout %s batch %d admitted %v available weight with %v already unavailable, over the %v the budget allows; charged nodes=%v",
		r.ID, b.Number, cost, used, allowed, nodes)
}

// propDispatch is one update the process ran during a step.
type propDispatch struct {
	id, group, digest string
	at                time.Time
}

// checkHeld asserts that a rollout halted or paused by an operator at the
// start of the step, and still its group's rollout at the end, ran no update
// unless its pause had expired.
func (s *sim) checkHeld(dispatches []propDispatch) error {
	for _, d := range dispatches {
		r, had := s.before[d.group]
		if !had {
			continue
		}

		expired := r.State == Paused && !r.PauseExpiresAt.IsZero() && !d.at.Before(r.PauseExpiresAt)
		if held := r.State == Paused || r.State == Halted; !held || expired {
			continue
		}

		if now, ok := s.h.c.Rollout(r.ID); ok && now.State.Active() && slices.Contains(sortedValues(r.Desired), d.digest) {
			return fmt.Errorf("update %s ran for rollout %s while it was %s", d.id, r.ID, r.State)
		}
	}

	return nil
}

// snapshot records the held rollouts to compare against the next step.
func (s *sim) snapshot() {
	if s.down {
		return
	}

	before := map[string]*RolloutView{}

	for _, r := range s.h.c.Rollouts() {
		if r.State.Active() {
			before[r.Group] = &r
		}
	}

	s.before = before
}

// check asserts what must hold after every step.
func (s *sim) check() error {
	s.mu.Lock()
	problems, dispatches := s.violations, s.dispatches
	s.violations, s.dispatches = nil, nil
	s.mu.Unlock()

	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}

	if s.down {
		return nil
	}

	if err := s.checkHeld(dispatches); err != nil {
		return err
	}

	h := s.h

	active := map[string]string{}

	for _, r := range h.c.Rollouts() {
		if was, ok := s.terminal[r.ID]; ok && r.State != was {
			return fmt.Errorf("rollout %s left terminal state %s for %s", r.ID, was, r.State)
		}

		if err := s.checkRollout(&r); err != nil {
			return err
		}

		// A state the store has not taken yet may be lost to a crash; only
		// a terminal state on disk must hold.
		if !r.State.Active() {
			if durableState(h.store, r.ID) == r.State {
				s.terminal[r.ID] = r.State
			}

			continue
		}

		if other, ok := active[r.Group]; ok {
			return fmt.Errorf("group %s has two active rollouts: %s and %s", r.Group, other, r.ID)
		}

		active[r.Group] = r.ID
	}

	return nil
}

// checkRollout asserts the shape of one rollout: batch sizes, wave order,
// retry isolation and what completion means.
func (s *sim) checkRollout(r *RolloutView) error {
	total := len(rolloutNodes(&r.Rollout))

	for i := range r.Batches {
		b := &r.Batches[i]

		key := r.ID + "/" + strconv.Itoa(b.Number)
		if _, seen := s.strategy[key]; !seen {
			s.strategy[key] = r.Strategy
		}

		// A retry batch is sized by the budget alone.
		if b.Retried {
			continue
		}

		st := s.h.c.strategy(s.strategy[key])

		nodes := map[string]struct{}{}
		for _, id := range b.Targets {
			nodes[r.target(id).Node] = struct{}{}
		}

		if want := st.BatchFor(b.Number, total); len(nodes) > want {
			return fmt.Errorf("rollout %s batch %d has %d nodes; the strategy allows %d", r.ID, b.Number, len(nodes), want)
		}
	}

	if err := checkRetries(r); err != nil {
		return err
	}

	if err := s.checkWaves(r); err != nil {
		return err
	}

	for _, rt := range r.Targets {
		if r.State == Complete && rt.Phase != PhasePassed && rt.Phase != PhaseSkipped {
			return fmt.Errorf("rollout %s is Complete with %s %s", r.ID, rt.ID, rt.Phase)
		}
	}

	return nil
}

// checkWaves asserts that a batch went ahead of a lower wave only if the
// strategy it was cut under ignores waves. A retry repeats targets already
// ordered, so each target counts where it was first batched.
func (s *sim) checkWaves(r *RolloutView) error {
	first := map[string]int{}

	for i := range r.Batches {
		for _, id := range r.Batches[i].Targets {
			if _, seen := first[id]; !seen {
				first[id] = r.Batches[i].Number
			}
		}
	}

	for _, a := range r.Targets {
		for _, b := range r.Targets {
			fa, fb := first[a.ID], first[b.ID]
			if a.NotReadyBefore || b.NotReadyBefore || fa == 0 || fb == 0 {
				continue
			}

			if st := s.h.c.strategy(s.strategy[r.ID+"/"+strconv.Itoa(fb)]); a.Wave < b.Wave && fa > fb && st.UsesWaves() {
				return fmt.Errorf("rollout %s moved %s (wave %d) in batch %d, after %s (wave %d) in batch %d",
					r.ID, a.ID, a.Wave, fa, b.ID, b.Wave, fb)
			}
		}
	}

	return nil
}

// checkRetries asserts from batch history that a retry appends batches: a
// retry batch repeats only retried targets, an ordinary batch repeats none, a
// target's batch is the last to take it, and none is cut past a waiting retry.
func checkRetries(r *RolloutView) error {
	last := map[string]int{}
	ordinary := 0

	for i := range r.Batches {
		b := &r.Batches[i]
		if !b.Retried {
			ordinary = b.Number
		}

		for _, id := range b.Targets {
			_, repeat := last[id]
			if b.Retried && (!repeat || !r.target(id).Retried) {
				return fmt.Errorf("rollout %s retry batch %d took %s, which no retry sent back", r.ID, b.Number, id)
			}

			if !b.Retried && repeat {
				return fmt.Errorf("rollout %s batch %d took %s again without a retry", r.ID, b.Number, id)
			}

			last[id] = b.Number
		}
	}

	for _, rt := range r.Targets {
		if rt.Batch != 0 && rt.Batch != last[rt.ID] {
			return fmt.Errorf("rollout %s puts %s in batch %d, but batch %d last took it", r.ID, rt.ID, rt.Batch, last[rt.ID])
		}

		if rt.Retried && last[rt.ID] == 0 {
			return fmt.Errorf("rollout %s retried %s, which no batch took", r.ID, rt.ID)
		}

		if rt.Retried && rt.Batch == 0 && rt.Phase == PhasePending && ordinary > last[rt.ID] {
			return fmt.Errorf("rollout %s cut ordinary batch %d while retried %s waited", r.ID, ordinary, rt.ID)
		}
	}

	return nil
}

func TestPropertyControllerInvariantsAdmissionBudget(t *testing.T) {
	const fleet = `
- {id: n0/a, node: n0, weight: 0, image: org/a:t, labels: {client: a, owner: a}}
- {id: n1/a, node: n1, weight: 100, image: org/a:t, labels: {client: a, owner: a}}
- {id: n1/b, node: n1, weight: 100, image: org/a:t, labels: {client: a, owner: a}}
- {id: n2/a, node: n2, weight: 100, image: org/a:t, labels: {client: a, owner: a}}
`

	cases := []struct {
		name        string
		budget      string
		unavailable []string
		targets     []string
		retry       bool
		reserved    bool
		rejected    bool
	}{
		{name: "fits exactly", budget: "100", targets: []string{tN1A}},
		{name: "lone heavy node with weightless outage", budget: "1", unavailable: []string{tN0A}, targets: []string{tN1A}},
		{name: "two available nodes despite free flags", budget: "1", targets: []string{tN1A, tN2A}, rejected: true},
		{name: "weighted outage blocks another heavy node", budget: "100", unavailable: []string{tN2A}, targets: []string{tN1A}, rejected: true},
		{name: "zero cost above budget", budget: "0", unavailable: []string{tN2A}, targets: []string{tN2A, tN0A}},
		{name: "two targets charge one node", budget: "1", targets: []string{tN1A, "n1/b"}},
		{name: "zero budget rejects available weight", budget: "0", targets: []string{tN1A}, rejected: true},
		{name: "retry charges recovered nodes", budget: "100", targets: []string{tN1A, tN2A}, retry: true, rejected: true},
		{name: "retry adds no unavailable weight", budget: "0", unavailable: []string{tN2A}, targets: []string{tN2A, tN0A}, retry: true},
		{name: "persisted reservation is not a new admission", budget: "0", targets: []string{tN1A}, reserved: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := replaceLine(testConfig, "maxUnavailable: 50%", "maxUnavailable: "+tc.budget)
			h := newHarness(t, cfg, fleet)
			h.cfg.ReadinessProbe.FailureThreshold = 1
			h.prime()
			h.world.set(func(w *world) {
				for _, id := range tc.unavailable {
					w.notReady[id] = true
				}
			})
			h.c.ProbeAll(h.ctx)

			r := &Rollout{ID: "proposed", Group: "a", State: Running,
				Desired: map[string]string{imgA: d2}, Batches: []Batch{{Number: 1, Targets: tc.targets}}}

			for _, id := range tc.targets {
				target, ok := h.current().Get(id)
				require.True(t, ok)

				r.Targets = append(r.Targets, RolloutTarget{ID: id, Node: target.Node, Batch: 1, Phase: PhasePending, Free: true})
			}

			var before *Rollout
			if tc.retry || tc.reserved {
				before = cloneRollout(r)
			}

			// A retry leaves batch 1 closed, saves its targets waiting to be
			// retried, then cuts them into batch 2.
			if tc.retry {
				before.Batches[0].EndedAt = h.clock.Now()
				r.Batches = []Batch{before.Batches[0], {Number: 2, Targets: tc.targets, Retried: true}}

				for i := range before.Targets {
					before.Targets[i] = RolloutTarget{ID: r.Targets[i].ID, Node: r.Targets[i].Node, Phase: PhasePending, Retried: true}
					r.Targets[i].Batch, r.Targets[i].Retried = 2, true
				}
			}

			if before != nil {
				require.NoError(t, h.store.SaveRollout(h.ctx, before))
			}

			s := &sim{h: h, terminal: map[string]RolloutState{}, strategy: map[string]string{}}
			p := &process{MemoryStore: h.store, sim: s, writes: -1}
			h.c.mu.Lock()
			h.c.rollouts[r.ID] = r
			writeErr := p.SaveRollout(h.ctx, r)
			h.c.mu.Unlock()
			require.NoError(t, writeErr)

			err := s.check()

			if tc.rejected {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestPropertyControllerInvariantsRetryIsolation(t *testing.T) {
	in := func(id string, batch int, retried bool) RolloutTarget {
		return RolloutTarget{ID: id, Batch: batch, Phase: PhaseUpdating, Retried: retried}
	}
	waiting := RolloutTarget{ID: tN2A, Phase: PhasePending, Retried: true}
	untried := RolloutTarget{ID: tN0A, Phase: PhasePending}
	halted := Batch{Number: 1, Targets: []string{tN1A, tN2A}}

	cases := []struct {
		name     string
		batches  []Batch
		targets  []RolloutTarget
		rejected bool
	}{
		{name: "retry batch takes the retried targets that fit", batches: []Batch{halted, {Number: 2, Targets: []string{tN1A}, Retried: true}},
			targets: []RolloutTarget{in(tN1A, 2, true), waiting, untried}},
		{name: "ordinary batches resume after the retries", batches: []Batch{halted, {Number: 2, Targets: []string{tN1A, tN2A}, Retried: true}, {Number: 3, Targets: []string{tN0A}}},
			targets: []RolloutTarget{in(tN1A, 2, true), in(tN2A, 2, true), in(tN0A, 3, false)}},
		{name: "retry batch mixes in an untried target", batches: []Batch{halted, {Number: 2, Targets: []string{tN1A, tN0A}, Retried: true}},
			targets: []RolloutTarget{in(tN1A, 2, true), waiting, in(tN0A, 2, true)}, rejected: true},
		{name: "ordinary batch goes ahead of a waiting retry", batches: []Batch{halted, {Number: 2, Targets: []string{tN0A}}},
			targets: []RolloutTarget{{ID: tN1A, Phase: PhasePending, Retried: true}, waiting, in(tN0A, 2, false)}, rejected: true},
		{name: "ordinary batch repeats a target", batches: []Batch{halted, {Number: 2, Targets: []string{tN1A}}},
			targets: []RolloutTarget{in(tN1A, 2, true), {ID: tN2A, Phase: PhaseSkipped, Retried: true}, untried}, rejected: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkRetries(&RolloutView{Rollout: Rollout{ID: "r", Batches: tc.batches, Targets: tc.targets}})
			require.Equal(t, tc.rejected, err != nil, "%v", err)
		})
	}
}

func TestPropertyControllerInvariantsInspectionCanRestoreAnEndedHoldWithoutAdmission(t *testing.T) {
	h := newHarness(t, testConfig, `
- {id: n1/a, node: n1, weight: 100, image: org/a:t, labels: {client: a, owner: a}}
- {id: n2/a, node: n2, weight: 100, image: org/a:t, labels: {client: a, owner: a}}
`)
	h.cfg.ReadinessProbe.FailureThreshold = 1
	h.prime()
	h.release(imgA, d2)
	require.NoError(t, h.c.Abort(h.ctx, actor, h.active("a").ID))
	h.world.set(func(w *world) { w.notReady[tN2A] = true })
	h.clock.Advance(time.Nanosecond)
	h.c.ProbeAll(h.ctx)
	used, allowed := h.unavailable()
	require.Equal(t, float64(100), used)
	require.Equal(t, float64(100), allowed)

	s := &sim{h: h, terminal: map[string]RolloutState{}, strategy: map[string]string{}}
	s.snapshot()
	h.world.set(func(w *world) { w.runErr["inspect"] = errFake })
	h.c.InspectAll(h.ctx)
	h.c.InspectAll(h.ctx)
	used, _ = h.unavailable()
	require.Equal(t, float64(200), used)
	require.Len(t, h.world.callsFor("update"), 1)
	require.NoError(t, s.check())
}
