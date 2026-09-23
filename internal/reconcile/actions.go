package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethpandaops/rolloor/internal/targets"
)

// ErrNotFound is returned for an unknown rollout, group or selector match.
var ErrNotFound = errors.New("not found")

// ErrState is returned when a verb does not apply to the rollout's state.
var ErrState = errors.New("not in a state that allows this")

// SyncRequest is what sync carries.
type SyncRequest struct {
	Actor    string
	Selector targets.Selector
	Force    bool
	// Strategy names a configured strategy for a rollout this creates.
	Strategy string
}

// Sync asks for the selected groups to converge now. Tags are re-resolved
// first so the request acts on the current digests. A manual rollout waiting
// for a sync starts; a group with no rollout gets one; an active rollout is
// marked as a person's, so the environment check no longer holds it.
func (c *Controller) Sync(ctx context.Context, req SyncRequest) ([]string, error) {
	set := c.targets()

	if req.Strategy != "" {
		if _, ok := c.cfg.Strategies[req.Strategy]; !ok {
			return nil, fmt.Errorf("strategy %q is not configured", req.Strategy)
		}
	}

	matched := set.Select(req.Selector)
	if len(matched) == 0 {
		return nil, fmt.Errorf("selector %s: %w", req.Selector, ErrNotFound)
	}

	groups := map[string]struct{}{}
	for i := range matched {
		groups[set.Group(&matched[i])] = struct{}{}
	}

	if err := c.resolveAll(ctx, c.clock.Now()); err != nil {
		return nil, err
	}

	now := c.clock.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	c.supersedeChangedRollouts(ctx, now)

	var started []string

	for group := range groups {
		if _, aborted := c.aborted[group]; aborted {
			delete(c.aborted, group)

			if err := c.persist(ctx, c.store.ClearAborted(ctx, group)); err != nil {
				return started, err
			}
		}

		r := c.activeRollout(group)
		created := r == nil

		if created {
			eligible, desired, from := c.outOfSync(group, set)
			if len(eligible) == 0 {
				continue
			}

			policy := c.policyFor(group)
			if req.Strategy != "" {
				policy.Strategy = req.Strategy
			}

			r = c.newRollout(now, group, policy, eligible, desired, from, set)
		}

		// Nothing reaches controller memory before the store has it.
		err := c.commit(ctx, r, func() {
			r.Human = true
			r.Force = r.Force || req.Force

			if req.Strategy != "" {
				r.Strategy = req.Strategy
			}

			if r.State == WaitingForSync || r.State == WaitingForEnvironment {
				r.State, r.Reason, r.UpdatedAt = Running, "Started by "+req.Actor, now
			}
		})
		if err != nil {
			return started, err
		}

		if created {
			c.rollouts[r.ID] = r
			c.event(ctx, now, &Event{Actor: req.Actor, Action: "rollout.created", Group: group, Rollout: r.ID,
				Reason: fmt.Sprintf("%d targets to %s (%s)", len(r.Targets), r.DigestShort(), strategyLabel(r.Strategy))})
		}

		c.event(ctx, now, &Event{Actor: req.Actor, Action: "sync", Group: group, Rollout: r.ID, Selector: req.Selector.String(),
			Reason: syncReason(req)})

		started = append(started, r.ID)
	}

	c.Nudge()

	return started, nil
}

func syncReason(req SyncRequest) string {
	s := "sync"
	if req.Force {
		s += " --force (readiness and soak skipped; budget still applies)"
	}

	if req.Strategy != "" {
		s += " --strategy " + req.Strategy
	}

	return s
}

// Refresh re-resolves every tag on the next tick.
func (c *Controller) Refresh(ctx context.Context, actor string) {
	now := c.clock.Now()

	c.mu.Lock()
	c.refreshWanted = true
	c.event(ctx, now, &Event{Actor: actor, Action: "refresh"})
	c.mu.Unlock()

	c.Nudge()
}

// SuspendRequest is what suspend carries.
type SuspendRequest struct {
	Actor    string
	Selector targets.Selector
	Reason   string
	Expires  time.Duration
}

// Suspend keeps the selected targets out of every rollout until the expiry.
func (c *Controller) Suspend(ctx context.Context, req SuspendRequest) (Suspension, error) {
	if req.Reason == "" {
		return Suspension{}, errors.New("a reason is required")
	}

	if req.Expires <= 0 {
		req.Expires = 24 * time.Hour
	}

	if len(c.targets().Select(req.Selector)) == 0 {
		return Suspension{}, fmt.Errorf("selector %s: %w", req.Selector, ErrNotFound)
	}

	now := c.clock.Now()
	s := Suspension{ID: c.newID(), Selector: req.Selector, Reason: req.Reason, Actor: req.Actor, CreatedAt: now, ExpiresAt: now.Add(req.Expires)}

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.persist(ctx, c.store.SaveSuspension(ctx, &s)); err != nil {
		return Suspension{}, err
	}

	c.suspensions[s.ID] = s
	c.event(ctx, now, &Event{Actor: req.Actor, Action: "suspend", Selector: req.Selector.String(),
		Reason: fmt.Sprintf("%s (until %s)", req.Reason, s.ExpiresAt.UTC().Format(time.RFC3339))})

	return s, nil
}

// Resume lifts every suspension with exactly this selector.
func (c *Controller) Resume(ctx context.Context, actor string, sel targets.Selector) (int, error) {
	now := c.clock.Now()
	key := sel.String()

	c.mu.Lock()
	defer c.mu.Unlock()

	n := 0

	for id, s := range c.suspensions {
		if s.Selector.String() != key {
			continue
		}

		if err := c.persist(ctx, c.store.DeleteSuspension(ctx, id)); err != nil {
			return n, err
		}

		delete(c.suspensions, id)

		n++
	}

	if n == 0 {
		return 0, fmt.Errorf("no suspension for %s: %w", key, ErrNotFound)
	}

	c.event(ctx, now, &Event{Actor: actor, Action: "resume", Selector: key, Reason: fmt.Sprintf("%d suspensions lifted", n)})
	c.Nudge()

	return n, nil
}

// Pause stops a rollout from starting another batch. The batch in flight
// finishes its soak.
func (c *Controller) Pause(ctx context.Context, actor, rolloutID string) error {
	now := c.clock.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	r, ok := c.rollouts[rolloutID]
	if !ok {
		return fmt.Errorf("rollout %s: %w", rolloutID, ErrNotFound)
	}

	switch r.State {
	case Running, Soaking, WaitingForBudget, WaitingForEnvironment:
	case WaitingForSync, Paused, Halted, Aborted, Superseded, Complete:
		return fmt.Errorf("rollout is %s: %w", r.State, ErrState)
	}

	err := c.commit(ctx, r, func() {
		r.PausePending = true

		if r.CurrentBatch() == nil {
			r.PausePending = false
			r.State, r.Reason, r.UpdatedAt = Paused, "Paused by "+actor+". Run promote to continue.", now
		}
	})
	if err != nil {
		return err
	}

	c.event(ctx, now, &Event{Actor: actor, Action: "pause", Group: r.Group, Rollout: r.ID, Reason: r.Reason})

	return nil
}

// Promote continues a paused rollout.
func (c *Controller) Promote(ctx context.Context, actor, rolloutID string) error {
	now := c.clock.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	r, ok := c.rollouts[rolloutID]
	if !ok {
		return fmt.Errorf("rollout %s: %w", rolloutID, ErrNotFound)
	}

	if r.State != Paused && !r.PausePending {
		return fmt.Errorf("rollout is %s and not pausing: %w", r.State, ErrState)
	}

	err := c.commit(ctx, r, func() {
		r.PausePending = false
		r.Human = true

		if r.State == Paused {
			r.State, r.Reason, r.UpdatedAt = Running, "Promoted by "+actor, now
		}
	})
	if err != nil {
		return err
	}

	c.event(ctx, now, &Event{Actor: actor, Action: "promote", Group: r.Group, Rollout: r.ID})
	c.Nudge()

	return nil
}

// Abort closes a rollout. The group stays out of sync and the controller will
// not open another rollout for the same digests until a sync or a new build.
func (c *Controller) Abort(ctx context.Context, actor, rolloutID string) error {
	now := c.clock.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	r, ok := c.rollouts[rolloutID]
	if !ok {
		return fmt.Errorf("rollout %s: %w", rolloutID, ErrNotFound)
	}

	if !r.State.Active() {
		return fmt.Errorf("rollout is %s: %w", r.State, ErrState)
	}

	// The marker goes first; if the rollout itself cannot be saved the marker
	// is taken back, so the two never disagree on disk. A restart with the
	// marker but an active rollout resolves in the marker's favour.
	key := c.groupDesiredKey(r.Group, c.targets())
	if err := c.persist(ctx, c.store.SaveAborted(ctx, r.Group, key)); err != nil {
		return err
	}

	reason := "Aborted by " + actor

	err := c.commit(ctx, r, func() {
		if b := r.CurrentBatch(); b != nil {
			b.EndedAt = now
			b.Soak = cloneSoak(&r.Soak)
		}

		c.end(now, r, Aborted, reason)
	})
	if err != nil {
		_ = c.persist(ctx, c.store.ClearAborted(ctx, r.Group))

		return err
	}

	c.aborted[r.Group] = key
	c.event(ctx, now, &Event{Actor: ControllerActor, Action: "rollout.aborted", Group: r.Group, Rollout: r.ID, Reason: reason})

	return nil
}

// Retry reopens a halted rollout's batch; see retryBatch.
func (c *Controller) Retry(ctx context.Context, actor, rolloutID, reason string) error {
	if reason == "" {
		return errors.New("a reason is required")
	}

	now := c.clock.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	r, ok := c.rollouts[rolloutID]
	if !ok {
		return fmt.Errorf("rollout %s: %w", rolloutID, ErrNotFound)
	}

	if r.State != Halted {
		return fmt.Errorf("rollout is %s: %w", r.State, ErrState)
	}

	err := c.commit(ctx, r, func() {
		r.RetryPending = true
		r.Human = true
	})
	if err != nil {
		return err
	}

	c.event(ctx, now, &Event{Actor: actor, Action: "retry", Group: r.Group, Rollout: r.ID, Reason: reason})
	c.Nudge()

	return nil
}

// SetPolicy replaces a group's policy.
func (c *Controller) SetPolicy(ctx context.Context, actor, group string, p Policy) error {
	if p.Mode != ModeAutomated && p.Mode != ModeManual {
		return fmt.Errorf("mode must be %s or %s", ModeAutomated, ModeManual)
	}

	if _, ok := c.cfg.StrategyNamed(p.Strategy); !ok {
		return fmt.Errorf("strategy %q is not configured", p.Strategy)
	}

	set := c.targets()
	if len(set.InGroup(group)) == 0 {
		return fmt.Errorf("group %s: %w", group, ErrNotFound)
	}

	now := c.clock.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.persist(ctx, c.store.SavePolicy(ctx, group, p)); err != nil {
		return err
	}

	c.policies[group] = p
	c.event(ctx, now, &Event{Actor: actor, Action: "policy", Group: group,
		Reason: fmt.Sprintf("mode=%s strategy=%s pins=%d", p.Mode, strategyLabel(p.Strategy), len(p.Pins))})
	c.Nudge()

	return nil
}

// Events returns history from the store.
func (c *Controller) Events(ctx context.Context, q EventQuery) ([]Event, error) {
	return c.store.Events(ctx, q)
}

// EventsAbout returns the history that concerns some targets: events naming
// one of them, events whose selector matches one, and group or rollout
// events for the groups they belong to. It filters before it cuts to the
// limit, so an old match is not hidden by unrelated recent events.
func (c *Controller) EventsAbout(ctx context.Context, about []targets.Target, q EventQuery) ([]Event, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 200
	}

	set := c.targets()
	ids := map[string]struct{}{}
	groups := map[string]struct{}{}

	for i := range about {
		ids[about[i].ID] = struct{}{}
		groups[set.Group(&about[i])] = struct{}{}
	}

	c.mu.RLock()

	rolloutGroups := make(map[string]string, len(c.rollouts))
	for id, r := range c.rollouts {
		rolloutGroups[id] = r.Group
	}

	c.mu.RUnlock()

	out := make([]Event, 0, limit)
	q.Limit = eventPage

	for scanned := 0; scanned < eventScanLimit; {
		page, err := c.store.Events(ctx, q)
		if err != nil {
			return nil, err
		}

		for i := range page {
			e := &page[i]
			if eventConcerns(e, about, ids, groups, rolloutGroups) {
				out = append(out, *e)
			}

			if len(out) >= limit {
				return out, nil
			}
		}

		scanned += len(page)

		if len(page) < eventPage {
			break
		}

		q.Before = page[len(page)-1].ID
	}

	return out, nil
}

// History is read back a page at a time, and a search gives up after
// eventScanLimit events so one query cannot read the whole table.
const (
	eventPage      = 1000
	eventScanLimit = 50 * eventPage
)

func eventConcerns(e *Event, about []targets.Target, ids, groups map[string]struct{}, rolloutGroups map[string]string) bool {
	if e.Target != "" {
		_, ok := ids[e.Target]

		return ok
	}

	if e.Selector != "" {
		sel, err := targets.ParseSelector(e.Selector)
		if err != nil {
			return false
		}

		for i := range about {
			if sel.Match(&about[i]) {
				return true
			}
		}

		return false
	}

	group := e.Group
	if group == "" && e.Rollout != "" {
		group = rolloutGroups[e.Rollout]
	}

	_, ok := groups[group]

	return ok
}
