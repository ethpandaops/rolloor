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
	Speed    string
}

// Sync asks for the selected groups to converge now. A manual rollout waiting
// for a sync starts; a group with no rollout gets one; an active rollout is
// marked as a person's, so the environment check no longer holds it.
func (c *Controller) Sync(ctx context.Context, req SyncRequest) ([]string, error) {
	now := c.clock.Now()
	set := c.targets()

	if req.Speed != "" {
		if _, ok := c.cfg.Presets[req.Speed]; !ok {
			return nil, fmt.Errorf("speed %q is not a preset", req.Speed)
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

	c.mu.Lock()
	defer c.mu.Unlock()

	c.refreshWanted = true

	var started []string

	for group := range groups {
		delete(c.aborted, group)

		r := c.activeRollout(group)
		if r == nil {
			eligible, desired, from := c.outOfSync(group, set)
			if len(eligible) == 0 {
				continue
			}

			policy := c.policyFor(group)
			if req.Speed != "" {
				policy.Speed = req.Speed
			}

			r = c.newRollout(now, group, policy, eligible, desired, from, set)
			c.rollouts[r.ID] = r
			c.event(ctx, now, &Event{Actor: req.Actor, Action: "rollout.created", Group: group, Rollout: r.ID,
				Reason: fmt.Sprintf("%d targets to %s (%s)", len(eligible), r.DigestShort(), policy.Speed)})
		}

		r.Human = true
		r.Force = r.Force || req.Force

		if req.Speed != "" {
			r.Speed = req.Speed
		}

		if r.State == WaitingForSync || r.State == WaitingForEnvironment {
			c.setState(ctx, now, r, Running, "Started by "+req.Actor)
		}

		c.saveRollout(ctx, r)
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

	if req.Speed != "" {
		s += " --speed " + req.Speed
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

	c.suspensions[s.ID] = s
	c.persist(ctx, c.store.SaveSuspension(ctx, &s))
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

		delete(c.suspensions, id)
		c.persist(ctx, c.store.DeleteSuspension(ctx, id))

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

	r.PausePending = true

	if r.CurrentBatch() == nil {
		r.PausePending = false
		c.setState(ctx, now, r, Paused, "Paused by "+actor+". Run promote to continue.")
	} else {
		c.saveRollout(ctx, r)
	}

	c.event(ctx, now, &Event{Actor: actor, Action: "pause", Group: r.Group, Rollout: r.ID})

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

	r.PausePending = false
	r.Human = true

	if r.State == Paused {
		c.setState(ctx, now, r, Running, "Promoted by "+actor)
	} else {
		c.saveRollout(ctx, r)
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

	c.aborted[r.Group] = desiredKey(r.Desired)

	if b := r.CurrentBatch(); b != nil {
		b.EndedAt = now
	}

	c.finish(ctx, now, r, Aborted, "Aborted by "+actor)

	return nil
}

// Retry re-runs the soak of a halted rollout's batch.
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

	r.RetryPending = true
	r.Human = true
	c.saveRollout(ctx, r)
	c.event(ctx, now, &Event{Actor: actor, Action: "retry", Group: r.Group, Rollout: r.ID, Reason: reason})
	c.Nudge()

	return nil
}

// SetPolicy replaces a group's policy.
func (c *Controller) SetPolicy(ctx context.Context, actor, group string, p Policy) error {
	if p.Mode != ModeAutomated && p.Mode != ModeManual {
		return fmt.Errorf("mode must be %s or %s", ModeAutomated, ModeManual)
	}

	if _, ok := c.cfg.Presets[p.Speed]; !ok {
		return fmt.Errorf("speed %q is not a preset", p.Speed)
	}

	set := c.targets()
	if len(set.InGroup(group)) == 0 {
		return fmt.Errorf("group %s: %w", group, ErrNotFound)
	}

	now := c.clock.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	c.policies[group] = p
	c.persist(ctx, c.store.SavePolicy(ctx, group, p))
	c.event(ctx, now, &Event{Actor: actor, Action: "policy", Group: group,
		Reason: fmt.Sprintf("mode=%s speed=%s pins=%d", p.Mode, p.Speed, len(p.Pins))})
	c.Nudge()

	return nil
}

// Events returns history from the store.
func (c *Controller) Events(ctx context.Context, q EventQuery) ([]Event, error) {
	return c.store.Events(ctx, q)
}
