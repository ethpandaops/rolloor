package reconcile

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// dispatched checks an update at the moment the process runs it: the batch
// that admitted it is already on disk, the target is not suspended, and the
// rollout was free to move when the step began.
func (s *sim) dispatched(disk *MemoryStore, id, digest string) {
	if !onDisk(disk, id, digest) {
		s.violation("update %s to %s ran before its batch reached the store", id, shortDigest(digest))
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

	s.checkAdmission(id)

	s.mu.Lock()
	s.dispatches = append(s.dispatches, propDispatch{id: id, group: t.group, digest: digest})
	s.mu.Unlock()
}

// checkAdmission asserts the budget when a newly admitted target's update
// runs, after every batch of the tick was cut: the weight unavailable is
// within the budget, or the target's node is the only weighted node
// unavailable. A node already counted, free or weightless adds nothing.
func (s *sim) checkAdmission(id string) {
	h := s.h
	set := h.current()

	for _, r := range h.c.Rollouts() {
		b := r.CurrentBatch()
		if !r.State.Active() || b == nil || !slices.Contains(b.Targets, id) {
			continue
		}

		rt := r.target(id)
		if s.before.open[r.ID+"/"+id] || s.admission[rt.Node] || set.NodeWeight(rt.Node) == 0 {
			return
		}

		if used, allowed := h.unavailable(); used > allowed && s.weightedUnavailable() > 1 {
			s.violation("%s was admitted with %v unavailable, over the %v the budget allows", id, used, allowed)
		}

		return
	}
}

// propDispatch is one update the process ran during a step.
type propDispatch struct {
	id, group, digest string
}

// checkHeld asserts that a rollout held at the start of the step (paused,
// halted without a retry, or waiting for a sync) and still its group's
// rollout at the end ran no update.
func (s *sim) checkHeld(dispatches []propDispatch) error {
	for _, d := range dispatches {
		r, had := s.before.active[d.group]
		if !had {
			continue
		}

		if held := r.State == Paused || r.State == WaitingForSync || (r.State == Halted && !r.RetryPending); !held {
			continue
		}

		if now, ok := s.h.c.Rollout(r.ID); ok && now.State.Active() && slices.Contains(sortedValues(r.Desired), d.digest) {
			return fmt.Errorf("update %s ran for rollout %s while it was %s", d.id, r.ID, r.State)
		}
	}

	return nil
}

// snapshot records what the next step's checks compare against. While no
// process runs, the last one's record stands, and an edit counts until a
// process checks after it.
func (s *sim) snapshot() {
	if s.down {
		return
	}

	s.edited, s.started = false, false

	b := propBefore{active: map[string]*RolloutView{}, open: map[string]bool{}}

	h := s.h
	b.used, _ = h.unavailable()
	s.admission = h.oracleUnavailable()
	s.admissionUsed = b.used

	for _, r := range h.c.Rollouts() {
		if r.State.Active() {
			b.active[r.Group] = &r
		}

		if cur := r.CurrentBatch(); r.State.Active() && cur != nil {
			for _, id := range cur.Targets {
				b.open[r.ID+"/"+id] = true
			}
		}
	}

	s.before = b
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

	// Probe observations may increase outages without any controller admission.
	// New admissions must stay within budget or add only the lone rounded node.
	if used, allowed := h.unavailable(); used > allowed && used > s.admissionUsed && !s.edited && !s.started && s.weightedUnavailable() > 1 {
		return fmt.Errorf("unavailable weight rose from %v to %v, over the %v the budget allows", s.before.used, used, allowed)
	}

	// Readiness and admissions, not quarantine, establish zero-cost grants.
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

// weightedUnavailable counts the unavailable nodes that carry weight.
func (s *sim) weightedUnavailable() int {
	set := s.h.current()

	n := 0

	for node := range s.h.oracleUnavailable() {
		if set.NodeWeight(node) > 0 {
			n++
		}
	}

	return n
}

// checkRollout asserts the shape of one rollout: batch sizes, wave order,
// manual policy, what completion means, and that a node was admitted free
// only when its node was already unavailable.
func (s *sim) checkRollout(r *RolloutView) error {
	if _, seen := s.manual[r.ID]; !seen {
		s.manual[r.ID] = s.modes[r.Group] == ModeManual
	}

	if s.manual[r.ID] && !r.Human && len(r.Batches) > 0 {
		return fmt.Errorf("rollout %s started a batch under a manual policy without a sync", r.ID)
	}

	total := len(rolloutNodes(&r.Rollout))

	for i := range r.Batches {
		b := &r.Batches[i]

		key := r.ID + "/" + strconv.Itoa(b.Number)
		if _, seen := s.strategy[key]; !seen {
			s.strategy[key] = r.Strategy
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

	// A batch that went ahead of a lower wave breaks the order only if the
	// strategy it was cut under follows waves.
	for _, a := range r.Targets {
		for _, b := range r.Targets {
			if a.NotReadyBefore || b.NotReadyBefore || a.Batch == 0 || b.Batch == 0 {
				continue
			}

			if st := s.h.c.strategy(s.strategy[r.ID+"/"+strconv.Itoa(b.Batch)]); a.Wave < b.Wave && a.Batch > b.Batch && st.UsesWaves() {
				return fmt.Errorf("rollout %s moved %s (wave %d) in batch %d, after %s (wave %d) in batch %d",
					r.ID, a.ID, a.Wave, a.Batch, b.ID, b.Wave, b.Batch)
			}
		}
	}

	for _, rt := range r.Targets {
		if r.State == Complete && rt.Phase != PhasePassed && rt.Phase != PhaseSkipped {
			return fmt.Errorf("rollout %s is Complete with %s %s", r.ID, rt.ID, rt.Phase)
		}
	}

	return nil
}
