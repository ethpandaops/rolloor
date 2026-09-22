package reconcile

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/targets"
)

// supersedeChangedRollouts closes any active rollout whose desired digests
// have moved on; openRollouts will start a fresh one in the same tick.
func (c *Controller) supersedeChangedRollouts(ctx context.Context, now time.Time) {
	set := c.targets()

	for _, r := range c.rollouts {
		if !r.State.Active() {
			continue
		}

		for img, want := range r.Desired {
			cur, ok := c.desiredForImage(r.Group, img, set)
			if ok && cur != want {
				c.finish(ctx, now, r, Superseded, fmt.Sprintf("%s moved to %s", img, shortDigest(cur)))

				break
			}
		}
	}
}

// desiredForImage is the current desired digest for an image within a group,
// honouring pins.
func (c *Controller) desiredForImage(group, image string, set *targets.Set) (string, bool) {
	for i := range set.Targets {
		t := &set.Targets[i]
		if set.Group(t) == group && t.Image == image {
			d, ok := c.desiredFor(group, t)

			return d.Digest, ok
		}
	}

	return "", false
}

// finish moves a rollout to a terminal state and records it.
func (c *Controller) finish(ctx context.Context, now time.Time, r *Rollout, state RolloutState, reason string) {
	c.end(now, r, state, reason)
	_ = c.saveRollout(ctx, r)
	c.event(ctx, now, &Event{Actor: ControllerActor, Action: "rollout." + lower(state), Group: r.Group, Rollout: r.ID, Reason: reason})
}

// end is the in-memory part of finishing. A target whose update was
// dispatched keeps its node counted against the budget for as long as that
// update could still be landing.
func (c *Controller) end(now time.Time, r *Rollout, state RolloutState, reason string) {
	r.State = state
	r.Reason = reason
	r.UpdatedAt = now
	r.EndedAt = now

	for i := range r.Targets {
		rt := &r.Targets[i]
		if rt.Phase != PhaseUpdating && rt.Phase != PhaseReady {
			continue
		}

		if rt.Phase == PhaseUpdating && !rt.Updated {
			rt.HoldUntil = now.Add(c.updateDeadline())
		}

		rt.Phase = PhaseSkipped
		rt.Reason = string(state)
	}
}

// updateDeadline is how long an update may take to show its digest.
func (c *Controller) updateDeadline() time.Duration {
	return c.cfg.Hooks.Timeout * 5
}

// openRollouts creates a rollout for every group with out-of-sync targets and
// no active rollout.
func (c *Controller) openRollouts(ctx context.Context, now time.Time) {
	set := c.targets()

	for _, group := range set.Groups() {
		if c.activeRollout(group) != nil {
			continue
		}

		// An abort holds until the group's desired digests change or a person
		// syncs; a temporary lack of eligible targets does not lift it.
		if key, aborted := c.aborted[group]; aborted {
			if key == c.groupDesiredKey(group, set) {
				continue
			}

			delete(c.aborted, group)
			_ = c.persist(ctx, c.store.ClearAborted(ctx, group))
		}

		eligible, desired, from := c.outOfSync(group, set)
		if len(eligible) == 0 {
			continue
		}

		policy := c.policyFor(group)
		r := c.newRollout(now, group, policy, eligible, desired, from, set)

		if policy.Mode == ModeManual {
			r.State = WaitingForSync
			r.Reason = fmt.Sprintf("Manual policy. Build %s is waiting for a sync.", r.DigestShort())
		}

		c.rollouts[r.ID] = r
		_ = c.saveRollout(ctx, r)
		c.event(ctx, now, &Event{Actor: ControllerActor, Action: "rollout.created", Group: group, Rollout: r.ID,
			Reason: fmt.Sprintf("%d targets to %s (%s)", len(eligible), r.DigestShort(), policy.Speed)})
	}
}

// groupDesiredKey identifies what every image in a group currently points at,
// whether or not any target is out of sync.
func (c *Controller) groupDesiredKey(group string, set *targets.Set) string {
	desired := map[string]string{}

	for i := range set.Targets {
		t := &set.Targets[i]
		if set.Group(t) != group {
			continue
		}

		if d, ok := c.desiredFor(group, t); ok && d.Digest != "" {
			desired[t.Image] = d.Digest
		}
	}

	return desiredKey(desired)
}

// outOfSync lists the group's targets that should move: desired and live both
// known, different, and not suspended.
func (c *Controller) outOfSync(group string, set *targets.Set) (eligible []*targets.Target, desired, from map[string]string) {
	desired = map[string]string{}
	fromCount := map[string]map[string]int{}

	for i := range set.Targets {
		t := &set.Targets[i]
		if set.Group(t) != group {
			continue
		}

		d, ok := c.desiredFor(group, t)
		if !ok || d.Digest == "" {
			continue
		}

		live, known := c.liveKnown(t.ID)
		if !known || live == d.Digest || c.suspensionFor(t) != nil {
			continue
		}

		eligible = append(eligible, t)
		desired[t.Image] = d.Digest

		if fromCount[t.Image] == nil {
			fromCount[t.Image] = map[string]int{}
		}

		fromCount[t.Image][live]++
	}

	from = map[string]string{}

	for img, counts := range fromCount {
		best, n := "", 0

		for d, k := range counts {
			if k > n || (k == n && d < best) {
				best, n = d, k
			}
		}

		from[img] = best
	}

	return eligible, desired, from
}

// newRollout builds a rollout with its targets in update order: already
// degraded first, then by wave, then node, then id.
func (c *Controller) newRollout(now time.Time, group string, policy Policy, eligible []*targets.Target, desired, from map[string]string, set *targets.Set) *Rollout {
	r := &Rollout{
		ID:        c.newID(),
		Group:     group,
		Speed:     policy.Speed,
		Desired:   desired,
		Revisions: map[string]string{},
		From:      from,
		State:     Running,
		Reason:    "Starting",
		CreatedAt: now,
		UpdatedAt: now,
	}

	for img := range desired {
		if d, ok := c.desired[img]; ok && d.Revision != "" {
			r.Revisions[img] = d.Revision
		}
	}

	preset := c.preset(policy.Speed)

	for _, t := range eligible {
		wave := set.Wave(t)
		if !preset.UsesWaves() {
			wave = 0
		}

		_, degraded := c.degraded[t.ID]

		r.Targets = append(r.Targets, RolloutTarget{
			ID: t.ID, Node: t.Node, Wave: wave, Phase: PhasePending, DegradedBefore: degraded,
		})
	}

	sort.SliceStable(r.Targets, func(i, j int) bool {
		a, b := r.Targets[i], r.Targets[j]
		if a.DegradedBefore != b.DegradedBefore {
			return a.DegradedBefore
		}

		if a.Wave != b.Wave {
			return a.Wave < b.Wave
		}

		if a.Node != b.Node {
			return a.Node < b.Node
		}

		return a.ID < b.ID
	})

	return r
}

// inFlightNodes lists every node with a target mid-update across all
// rollouts, plus nodes a finished rollout may still be changing, excluding
// nodes that are already degraded.
func (c *Controller) inFlightNodes(set *targets.Set, now time.Time) map[string]struct{} {
	nodes := map[string]struct{}{}

	for _, r := range c.rollouts {
		if !r.State.Active() {
			for i := range r.Targets {
				rt := &r.Targets[i]
				if now.Before(rt.HoldUntil) && !c.nodeDegraded(set, r, rt.Node) {
					nodes[rt.Node] = struct{}{}
				}
			}

			continue
		}

		b := r.CurrentBatch()
		if b == nil {
			continue
		}

		for _, id := range b.Targets {
			rt := r.target(id)
			if rt != nil && rt.Phase != PhaseSkipped && !c.nodeDegraded(set, r, rt.Node) {
				nodes[rt.Node] = struct{}{}
			}
		}
	}

	return nodes
}

// nodeDegraded reports whether a node is already disrupted: any target on it
// is quarantined, or was degraded when the rollout began. Such a node costs
// nothing against the budget, whichever group is moving it.
func (c *Controller) nodeDegraded(set *targets.Set, r *Rollout, node string) bool {
	for i := range r.Targets {
		if r.Targets[i].Node == node && r.Targets[i].DegradedBefore {
			return true
		}
	}

	for _, t := range set.Node(node) {
		if _, degraded := c.degraded[t.ID]; degraded {
			return true
		}
	}

	return false
}

// inFlightWeight is the weight of the in-flight nodes.
func (c *Controller) inFlightWeight(set *targets.Set) float64 {
	var w float64
	for n := range c.inFlightNodes(set, c.clock.Now()) {
		w += set.NodeWeight(n)
	}

	return w
}

// startBatch cuts and opens the next batch, or explains why it cannot. A
// pending pause is honoured where the batch ends, in passBatch, never here.
func (c *Controller) startBatch(ctx context.Context, now time.Time, r *Rollout, set *targets.Set) {
	remaining := c.remaining(r, set, now)
	if len(remaining) == 0 {
		c.finish(ctx, now, r, Complete, fmt.Sprintf("All %d targets on %s.", len(r.Targets), r.DigestShort()))

		return
	}

	if !c.envOK && !r.Human {
		c.setState(ctx, now, r, WaitingForEnvironment, "Environment check failing: "+c.envReason+". Automated rollouts are paused; a person may still sync.")

		return
	}

	preset := c.preset(r.Speed)
	wave := remaining[0].Wave

	// A wave is a contiguous run of the sorted order; degraded targets sort
	// first and may carry a later wave number, which must not pull the rest
	// of that wave ahead of the ones between.
	var candidates []*RolloutTarget

	for _, rt := range remaining {
		if rt.Wave != wave && preset.UsesWaves() {
			break
		}

		candidates = append(candidates, rt)
	}

	idx := min(len(r.Batches), len(preset.Batch)-1)
	size := preset.Batch[idx].Of(len(r.Targets))
	budget := c.cfg.Budget.OfWeight(set.TotalWeight())
	busy := c.inFlightNodes(set, now)
	inflight := c.inFlightWeight(set)

	var (
		picked []*RolloutTarget
		nodes  = map[string]struct{}{}
		cost   float64
	)

	for _, rt := range candidates {
		if len(picked) >= size {
			break
		}

		w := set.NodeWeight(rt.Node)
		if _, already := busy[rt.Node]; already || c.nodeDegraded(set, r, rt.Node) {
			w = 0
		}

		if _, seen := nodes[rt.Node]; !seen && inflight+cost+w > budget && w > 0 {
			continue
		}

		if _, seen := nodes[rt.Node]; !seen {
			nodes[rt.Node] = struct{}{}
			cost += w
		}

		picked = append(picked, rt)
	}

	if len(picked) == 0 {
		need := set.NodeWeight(candidates[0].Node)
		c.setState(ctx, now, r, WaitingForBudget, fmt.Sprintf("Waiting for budget: %s of %s in use; next batch needs %s.",
			pct(inflight, set.TotalWeight()), c.cfg.Budget.String(), pct(need, set.TotalWeight())))

		return
	}

	b := Batch{Number: len(r.Batches) + 1, Wave: wave, StartedAt: now}
	for _, rt := range picked {
		rt.Batch = b.Number
		b.Targets = append(b.Targets, rt.ID)
	}

	r.Batches = append(r.Batches, b)
	c.setState(ctx, now, r, Running, fmt.Sprintf("Wave %d, batch %d of about %d: updating %d targets.", wave, b.Number, c.estimateBatches(r, &preset), len(picked)))
	c.event(ctx, now, &Event{Actor: ControllerActor, Action: "batch.started", Group: r.Group, Rollout: r.ID,
		Reason: fmt.Sprintf("batch %d: %d targets in wave %d", b.Number, len(picked), wave)})
}

// remaining lists targets not yet batched, skipping ones that have since been
// suspended or lost, in update order.
func (c *Controller) remaining(r *Rollout, set *targets.Set, now time.Time) []*RolloutTarget {
	var out []*RolloutTarget

	for i := range r.Targets {
		rt := &r.Targets[i]
		if rt.Batch != 0 || rt.Phase != PhasePending {
			continue
		}

		t, ok := set.Get(rt.ID)
		if reason, skip := c.skipReason(r, set, &t, ok); skip {
			rt.Phase, rt.Reason = PhaseSkipped, reason

			continue
		}

		if _, known := c.liveKnown(rt.ID); !known {
			rt.Phase, rt.Reason = PhaseSkipped, "unreachable"

			continue
		}

		if live, _ := c.liveKnown(rt.ID); live == r.Desired[t.Image] {
			rt.Phase, rt.Reason, rt.UpdatedAt = PhasePassed, "already on the new build", now

			continue
		}

		out = append(out, rt)
	}

	return out
}

// estimateBatches guesses how many batches the rollout will have in total:
// the ones already cut, plus what is left in each wave at the last batch
// fraction, assuming the budget never bites.
func (c *Controller) estimateBatches(r *Rollout, preset *config.Preset) int {
	last := max(preset.Batch[len(preset.Batch)-1].Of(len(r.Targets)), 1)
	left := map[int]int{}

	for i := range r.Targets {
		rt := &r.Targets[i]
		if rt.Batch == 0 && rt.Phase == PhasePending {
			wave := rt.Wave
			if !preset.UsesWaves() {
				wave = 0
			}

			left[wave]++
		}
	}

	total := len(r.Batches)
	for _, n := range left {
		total += (n + last - 1) / last
	}

	return total
}

func (c *Controller) setState(ctx context.Context, now time.Time, r *Rollout, state RolloutState, reason string) {
	changed := r.State != state || r.Reason != reason
	r.State = state
	r.Reason = reason
	r.UpdatedAt = now

	_ = c.saveRollout(ctx, r)

	if changed && state != Running {
		c.event(ctx, now, &Event{Actor: ControllerActor, Action: "rollout." + lower(state), Group: r.Group, Rollout: r.ID, Reason: reason})
	}
}

func lower(s RolloutState) string {
	b := []byte(string(s))
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}

	return string(b)
}
