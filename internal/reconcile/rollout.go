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
// have moved on, or whose group has no targets left; openRollouts will start
// a fresh one in the same tick where there is anything to move.
func (c *Controller) supersedeChangedRollouts(ctx context.Context, now time.Time) {
	set := c.targets()

	for _, r := range c.rollouts {
		if !r.State.Active() {
			continue
		}

		if len(set.InGroup(r.Group)) == 0 {
			c.finish(ctx, now, r, Superseded, "no targets left in group "+r.Group)

			continue
		}

		if r.GroupDesiredKey != c.groupDesiredKey(r.Group, set) {
			c.finish(ctx, now, r, Superseded, "group desired images changed")
		}
	}
}

// finish moves a rollout to a terminal state and records it.
func (c *Controller) finish(ctx context.Context, now time.Time, r *Rollout, state RolloutState, reason string) {
	c.end(now, r, state, reason)
	_ = c.saveRollout(ctx, r)
	c.clearRolloutQuarantine(ctx, r)
	c.event(ctx, now, &Event{Actor: ControllerActor, Action: "rollout." + lower(state), Group: r.Group, Rollout: r.ID, Reason: reason})
}

func (c *Controller) clearRolloutQuarantine(ctx context.Context, r *Rollout) {
	for i := range r.Targets {
		rt := &r.Targets[i]
		if q := c.quarantineFor(rt.ID); q != nil && q.Rollout == r.ID {
			delete(c.degraded, rt.ID)
			_ = c.persist(ctx, c.store.ClearDegraded(ctx, rt.ID))
		}
	}
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

		if !c.readyOnBuild(rt) {
			rt.HoldUntil = now.Add(c.strategy(r.Strategy).ProgressDeadline)
		}

		rt.Phase = PhaseSkipped
		rt.Reason = string(state)
	}
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
			Reason: fmt.Sprintf("%d targets to %s (%s)", len(eligible), r.DigestShort(), strategyLabel(policy.Strategy))})
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

// outOfSync lists targets with observed digest drift, excluding suspensions.
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

// newRollout sorts targets not ready at creation first, then wave, node and id.
func (c *Controller) newRollout(now time.Time, group string, policy Policy, eligible []*targets.Target, desired, from map[string]string, set *targets.Set) *Rollout {
	r := &Rollout{
		ID:              c.newID(),
		Group:           group,
		Strategy:        policy.Strategy,
		Desired:         desired,
		GroupDesiredKey: c.groupDesiredKey(group, set),
		Revisions:       map[string]string{},
		From:            from,
		State:           Running,
		Reason:          "Starting",
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	for img := range desired {
		if d, ok := c.desired[img]; ok && d.Revision != "" {
			r.Revisions[img] = d.Revision
		}
	}

	st := c.strategy(policy.Strategy)

	for _, t := range eligible {
		wave := set.Wave(t)
		if !st.UsesWaves() {
			wave = 0
		}

		r.Targets = append(r.Targets, RolloutTarget{
			ID: t.ID, Node: t.Node, Wave: wave, Phase: PhasePending, NotReadyBefore: !c.targetReady(t.ID),
		})
	}

	sort.SliceStable(r.Targets, func(i, j int) bool {
		a, b := r.Targets[i], r.Targets[j]
		if a.NotReadyBefore != b.NotReadyBefore {
			return a.NotReadyBefore
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

// unavailableNodes includes observed outages, admitted updates awaiting readiness,
// and updates that may still land after their operation stopped.
func (c *Controller) unavailableNodes(now time.Time) map[string]struct{} {
	nodes := map[string]struct{}{}
	set := c.targets()

	for _, t := range set.Targets {
		if !c.targetReady(t.ID) {
			nodes[t.Node] = struct{}{}
		}
	}

	for _, r := range c.rollouts {
		for i := range r.Targets {
			rt := &r.Targets[i]
			_, present := set.Get(rt.ID)
			forced := present && r.Force && rt.Updated && (rt.Phase == PhaseReady || rt.Phase == PhasePassed) && !c.readyOnBuild(rt)

			if (now.Before(rt.HoldUntil) || forced) && !c.heldReady(r, rt) {
				nodes[rt.Node] = struct{}{}
			}
		}

		b := r.CurrentBatch()
		if !r.State.Active() || b == nil {
			continue
		}

		for _, id := range b.Targets {
			rt := r.target(id)
			if (rt.Phase == PhasePending || rt.Phase == PhaseUpdating || rt.Phase == PhaseReady) && !c.readyOnBuild(rt) {
				nodes[rt.Node] = struct{}{}
			}
		}
	}

	return nodes
}

// unavailableWeight counts each unavailable node's weight once.
func (c *Controller) unavailableWeight(set *targets.Set) float64 {
	var w float64
	for n := range c.unavailableNodes(c.clock.Now()) {
		w += set.NodeWeight(n)
	}

	return w
}

// startBatch cuts and opens the next batch, or explains why it cannot. A
// pending pause is honoured where the batch ends, in passBatch, never here.
func (c *Controller) startBatch(ctx context.Context, now time.Time, r *Rollout, set *targets.Set) {
	remaining := c.remaining(r, set, now)
	if len(remaining) == 0 {
		c.finish(ctx, now, r, Complete, completeReason(r))

		return
	}

	st := c.strategy(r.Strategy)
	wave := remaining[0].Wave

	// Readiness priority may place a later wave first; waves remain contiguous
	// in that sorted order.
	var candidates []*RolloutTarget

	for _, rt := range remaining {
		if rt.Wave != wave && st.UsesWaves() {
			break
		}

		candidates = append(candidates, rt)
	}

	// Batch sizes count nodes: a batch takes every target the group has on
	// each node it picks.
	size := st.BatchFor(len(r.Batches)+1, len(rolloutNodes(r)))
	budget := c.cfg.DisruptionBudget.MaxUnavailable.OfWeight(set.TotalWeight())
	busy := c.unavailableNodes(now)
	inflight := c.unavailableWeight(set)

	// Each admission records whether it added an unavailable node.
	var (
		picked []*RolloutTarget
		nodes  = map[string]bool{}
		cost   float64
	)

	for _, rt := range candidates {
		if _, seen := nodes[rt.Node]; !seen {
			if len(nodes) >= size {
				continue
			}

			_, free := busy[rt.Node]
			w := set.NodeWeight(rt.Node)

			if free {
				w = 0
			}

			// As maxUnavailable rounds up to one pod on a DaemonSet, a budget
			// above zero lets one node go when nothing else is unavailable,
			// however heavy; otherwise such a node could never be updated.
			alone := budget > 0 && inflight == 0 && cost == 0
			if inflight+cost+w > budget && w > 0 && !alone {
				continue
			}

			nodes[rt.Node] = free
			cost += w
		}

		picked = append(picked, rt)
	}

	if len(picked) == 0 {
		need := set.NodeWeight(candidates[0].Node)
		c.setState(ctx, now, r, WaitingForBudget, fmt.Sprintf("Waiting for the disruption budget: %s of %s unavailable; the next batch needs %s.",
			pct(inflight, set.TotalWeight()), c.cfg.DisruptionBudget.MaxUnavailable.String(), pct(need, set.TotalWeight())))

		return
	}

	b := Batch{Number: len(r.Batches) + 1, Wave: wave, StartedAt: now}
	for _, rt := range picked {
		rt.Batch, rt.Free = b.Number, nodes[rt.Node]
		b.Targets = append(b.Targets, rt.ID)
	}

	r.Batches = append(r.Batches, b)
	c.setState(ctx, now, r, Running, fmt.Sprintf("Wave %d, batch %d of about %d: updating %d targets on %d nodes.", wave, b.Number, c.estimateBatches(r, &st), len(picked), len(nodes)))
	c.event(ctx, now, &Event{Actor: ControllerActor, Action: "batch.started", Group: r.Group, Rollout: r.ID,
		Reason: fmt.Sprintf("batch %d: %d targets on %d nodes in wave %d", b.Number, len(picked), len(nodes), wave)})
}

// remaining lists unbatched eligible targets in update order. Observed digest
// convergence needs no operation, regardless of any earlier quarantine.
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
func (c *Controller) estimateBatches(r *Rollout, st *config.Strategy) int {
	last := max(st.BatchSize.Of(len(rolloutNodes(r))), 1)
	left := map[int]map[string]struct{}{}

	for i := range r.Targets {
		rt := &r.Targets[i]
		if rt.Batch == 0 && rt.Phase == PhasePending {
			wave := rt.Wave
			if !st.UsesWaves() {
				wave = 0
			}

			if left[wave] == nil {
				left[wave] = map[string]struct{}{}
			}

			left[wave][rt.Node] = struct{}{}
		}
	}

	total := len(r.Batches)
	for _, nodes := range left {
		total += (len(nodes) + last - 1) / last
	}

	return total
}

// rolloutNodes is the set of nodes a rollout's targets are on.
func rolloutNodes(r *Rollout) map[string]struct{} {
	nodes := map[string]struct{}{}
	for i := range r.Targets {
		nodes[r.Targets[i].Node] = struct{}{}
	}

	return nodes
}

// completeReason says how a rollout ended, counting only targets it moved.
func completeReason(r *Rollout) string {
	moved, skipped := 0, 0

	for i := range r.Targets {
		switch r.Targets[i].Phase {
		case PhasePassed:
			moved++
		case PhaseSkipped:
			skipped++
		case PhasePending, PhaseUpdating, PhaseReady, PhaseFailed:
		}
	}

	if skipped == 0 {
		return fmt.Sprintf("All %d targets on %s.", moved, r.DigestShort())
	}

	return fmt.Sprintf("%d of %d targets on %s; %d skipped and left for the next rollout.", moved, len(r.Targets), r.DigestShort(), skipped)
}

func (c *Controller) setState(ctx context.Context, now time.Time, r *Rollout, state RolloutState, reason string) {
	prev := r.State
	changed := r.State != state || r.Reason != reason
	r.State = state
	r.Reason = reason
	r.UpdatedAt = now

	_ = c.saveRollout(ctx, r)

	// History records moves between states; a soak's running progress is
	// only shown on the rollout itself.
	progress := state == Soaking && prev == Soaking
	if changed && state != Running && !progress {
		c.event(ctx, now, &Event{Actor: ControllerActor, Action: "rollout." + lower(state), Group: r.Group, Rollout: r.ID, Reason: reason})
	}
}

// strategyLabel names a strategy for people; the default has no name.
func strategyLabel(name string) string {
	if name == "" {
		return "default strategy"
	}

	return name + " strategy"
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
