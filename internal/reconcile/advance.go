package reconcile

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/hooks"
	"github.com/ethpandaops/rolloor/internal/targets"
)

// job is one hook invocation the plan wants run this tick.
type job struct {
	rollout string
	target  string
	hook    string
	program string
	input   any
	retryAt time.Time
	// soakIDs are the batch targets a soak job speaks for.
	soakIDs []string
}

// soakInput is what the soak hook receives on stdin.
type soakInput struct {
	Updated   []targets.Target `json:"updated"`
	Remaining []targets.Target `json:"remaining"`
	Rollout   soakRollout      `json:"rollout"`
}

type soakRollout struct {
	ID    string `json:"id"`
	Group string `json:"group"`
	Batch int    `json:"batch"`
	Wave  int    `json:"wave"`
}

// outcome is a job with its result and when it started.
type outcome struct {
	job
	res     hooks.Result
	err     error
	started time.Time
}

// planRollouts decides, under the lock, which hooks to run for every active
// rollout. It also makes the transitions that need no hook at all.
func (c *Controller) planRollouts(ctx context.Context, now time.Time) []job {
	set := c.targets()

	var jobs []job

	ids := make([]string, 0, len(c.rollouts))
	for id := range c.rollouts {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	for _, id := range ids {
		r := c.rollouts[id]
		if !r.State.Active() {
			continue
		}

		jobs = append(jobs, c.planRollout(ctx, now, r, set)...)
	}

	return jobs
}

// planRollout is only called for active rollouts; WaitingForSync and Paused
// have nothing to do until a person acts.
func (c *Controller) planRollout(ctx context.Context, now time.Time, r *Rollout, set *targets.Set) []job {
	switch r.State {
	case Halted:
		if !r.RetryPending {
			return nil
		}

		// A retry waiting for room is not a new halt, so history and anyone
		// following it hear nothing until the batch reopens.
		if why, fits := c.retryFits(set, r, now); !fits {
			if r.Reason != why {
				r.Reason, r.UpdatedAt = why, now
				_ = c.saveRollout(ctx, r)
			}

			return nil
		}

		c.retryBatch(ctx, now, r)

		return c.planBatch(ctx, now, r, set)
	case Soaking:
		return c.planSoak(ctx, now, r, set)
	case Running, WaitingForBudget:
		if r.CurrentBatch() == nil {
			c.startBatch(ctx, now, r, set)

			if r.CurrentBatch() == nil {
				return nil
			}
		}

		return c.planBatch(ctx, now, r, set)
	default:
		return nil
	}
}

// retryFits checks the same observed-unavailability budget as a new admission.
func (c *Controller) retryFits(set *targets.Set, r *Rollout, now time.Time) (string, bool) {
	b := &r.Batches[len(r.Batches)-1]
	busy := c.unavailableNodes(now)
	nodes := map[string]struct{}{}

	var cost float64

	for _, id := range b.Targets {
		rt := r.target(id)
		if rt.Phase != PhaseFailed {
			continue
		}

		t, ok := set.Get(id)
		if _, skip := c.skipReason(r, set, &t, ok); skip {
			continue
		}

		_, counted := busy[rt.Node]
		_, seen := nodes[rt.Node]

		if counted || seen || set.NodeWeight(rt.Node) == 0 {
			continue
		}

		nodes[rt.Node] = struct{}{}
		cost += set.NodeWeight(rt.Node)
	}

	budget := c.cfg.DisruptionBudget.MaxUnavailable.OfWeight(set.TotalWeight())
	inflight := c.unavailableWeight(set)
	alone := budget > 0 && inflight == 0 && len(nodes) == 1

	if cost == 0 || inflight+cost <= budget || alone {
		return "", true
	}

	return fmt.Sprintf("Retry waiting for the disruption budget: %s of %s unavailable; reopening batch %d needs %s.",
		pct(inflight, set.TotalWeight()), c.cfg.DisruptionBudget.MaxUnavailable.String(), b.Number, pct(cost, set.TotalWeight())), false
}

// retryBatch reopens failed targets using the current observations and preserves
// earlier soak attempts.
func (c *Controller) retryBatch(ctx context.Context, now time.Time, r *Rollout) {
	busy := c.unavailableNodes(now)
	set := c.targets()
	r.RetryPending = false
	r.Soak = SoakProgress{}

	b := &r.Batches[len(r.Batches)-1]
	b.EndedAt = time.Time{}

	if b.Soak != nil {
		b.PriorSoaks = append(b.PriorSoaks, b.Soak)
		b.Soak = nil
	}

	for _, id := range b.Targets {
		rt := r.target(id)
		if rt.Phase == PhaseFailed {
			t, present := set.Get(id)
			if reason, skip := c.skipReason(r, set, &t, present); skip {
				rt.Phase, rt.Reason = PhaseSkipped, reason

				continue
			}

			if c.heldReady(r, rt) {
				rt.HoldUntil = time.Time{}
			}

			rt.Phase, rt.Reason, rt.UpdatedAt = PhaseUpdating, "retrying: waiting for the digest", now
			rt.DigestSeenAt = time.Time{}
			rt.UpdateDone, rt.Updated = true, false
			rt.UpdateAttempts = 0
			rt.RetryAt = time.Time{}
			rt.UpdateError = ""

			if live, known := c.liveKnown(id); !known || live != r.Desired[t.Image] {
				rt.Phase, rt.Reason, rt.UpdateDone = PhasePending, "retrying: running the update again", false
			}

			_, rt.Free = busy[rt.Node]
		}
	}

	c.setState(ctx, now, r, Running, fmt.Sprintf("Retrying batch %d.", b.Number))
}

// planBatch dispatches updates and reads the independent observations.
// Ineligible targets are skipped where they are.
func (c *Controller) planBatch(ctx context.Context, now time.Time, r *Rollout, set *targets.Set) []job {
	b := r.CurrentBatch()
	st := c.strategy(r.Strategy)

	var jobs []job

	allReady, skipped := true, false
	changed := false

	for _, id := range b.Targets {
		rt := r.target(id)

		t, ok := set.Get(id)
		if reason, skip := c.skipReason(r, set, &t, ok); skip && (rt.Phase == PhasePending || rt.Phase == PhaseUpdating) {
			rt.Phase, rt.Reason = PhaseSkipped, reason
			skipped = true

			continue
		}

		switch rt.Phase {
		case PhasePending:
			allReady = false
			changed = true

			rt.Phase, rt.Reason = PhaseUpdating, "update running"
			if rt.UpdatedAt.IsZero() {
				rt.UpdatedAt = now
			}

			// An already-observed digest needs verification, not another update
			// that could collide with one still finishing.
			if live, known := c.liveKnown(id); known && live == r.Desired[t.Image] {
				rt.UpdateDone, rt.Reason = true, "already on the new build, waiting for inspect to confirm"

				continue
			}

			if why := updateDeadlineReason(now, rt, &st, r.Desired[t.Image]); why != "" {
				c.withdraw(jobs)
				c.halt(ctx, now, r, id, why)

				return nil
			}

			if rt.UpdateAttempts >= max(st.Retry.Limit, 1) {
				c.withdraw(jobs)
				c.halt(ctx, now, r, id, "update retry limit reached after an interrupted update")

				return nil
			}

			jobs = append(jobs, c.updateJob(now, r, rt, set, &t))
		case PhaseUpdating:
			live, known := c.liveKnown(id)
			if known && live == r.Desired[t.Image] && rt.UpdateDone {
				if !rt.Updated {
					changed = true
					rt.Updated, rt.DigestSeenAt = true, c.live[id].DigestSince
					rt.Reason = "on the new build, waiting for readiness"
				}

				if r.Force || c.readyOnBuild(rt) {
					changed = true
					rt.Phase, rt.Reason = PhaseReady, "ready"

					continue
				}
			} else if rt.Updated {
				changed = true
				rt.Updated = false
				rt.DigestSeenAt = time.Time{}
			}

			allReady = false

			if why := updateDeadlineReason(now, rt, &st, r.Desired[t.Image]); why != "" {
				c.withdraw(jobs)
				c.halt(ctx, now, r, id, why)

				return nil
			}

			if !rt.RetryAt.IsZero() {
				changed = true

				if now.Before(rt.RetryAt) {
					rt.Reason = fmt.Sprintf("%s; retrying in %s", updateFailure(rt, &st), rt.RetryAt.Sub(now).Round(time.Second))
				} else {
					jobs = append(jobs, c.updateJob(now, r, rt, set, &t))
				}

				continue
			}

			reason := "waiting for the digest"
			if rt.Updated {
				reason = "not ready: " + c.live[id].Readiness.Reason
			}

			if rt.Reason != reason {
				changed = true
				rt.Reason = reason
			}
		case PhaseReady, PhasePassed, PhaseSkipped, PhaseFailed:
		}
	}

	c.saveSkips(ctx, r, skipped || changed)

	if allReady && len(jobs) == 0 {
		c.batchReady(ctx, now, r, set)
	}

	return jobs
}

// skipReason says why a rollout target can no longer be worked on, if so.
func (c *Controller) skipReason(r *Rollout, set *targets.Set, t *targets.Target, present bool) (string, bool) {
	if !present {
		return "removed from the targets file", true
	}

	if set.Group(t) != r.Group {
		return "moved to group " + set.Group(t), true
	}

	if rt := r.target(t.ID); rt != nil && rt.Node != t.Node {
		return "moved to node " + t.Node, true
	}

	if _, known := r.Desired[t.Image]; !known {
		return "image changed to " + t.Image, true
	}

	if s := c.suspensionFor(t); s != nil {
		return "suspended by " + s.Actor + ": " + s.Reason, true
	}

	return "", false
}

// saveSkips writes a rollout whose open batch just skipped targets. A skip
// frees its node for batches cut later in the same tick, so it must reach the
// store before they do, or a crash between the two writes would restart with
// both counted.
func (c *Controller) saveSkips(ctx context.Context, r *Rollout, skipped bool) {
	if skipped {
		_ = c.saveRollout(ctx, r)
	}
}

// batchReady is called once every target in the batch is ready: either the
// soak begins or, with no soak, the batch passes.
func (c *Controller) batchReady(ctx context.Context, now time.Time, r *Rollout, set *targets.Set) {
	st := c.strategy(r.Strategy)

	if st.Soak.Duration == 0 || r.Force || !c.batchHasSoak(r, set) {
		c.passBatch(ctx, now, r, "no soak")

		return
	}

	r.Soak = SoakProgress{StartedAt: now}
	c.setState(ctx, now, r, Soaking, fmt.Sprintf("Batch %d in soak for %s.", r.CurrentBatch().Number, st.Soak.Duration))
}

// batchHasSoak reports whether any target still active in the open batch
// names a soak program.
func (c *Controller) batchHasSoak(r *Rollout, set *targets.Set) bool {
	for _, id := range r.CurrentBatch().Targets {
		rt := r.target(id)
		if rt.Phase == PhaseSkipped {
			continue
		}

		if t, ok := set.Get(id); ok && c.programFor(&t, config.HookSoak) != "" {
			return true
		}
	}

	return false
}

// planSoak runs the soak programs when a check is due, and passes the batch
// once the streak and duration are met on evidence no older than two
// intervals. A forced rollout, or a batch with no soak programs left, passes
// at once.
func (c *Controller) planSoak(ctx context.Context, now time.Time, r *Rollout, set *targets.Set) []job {
	st := c.strategy(r.Strategy)
	b := r.CurrentBatch()

	if r.Force {
		c.passBatch(ctx, now, r, "forced")

		return nil
	}

	if !c.batchHasSoak(r, set) {
		c.passBatch(ctx, now, r, "no soak programs left in the batch")

		return nil
	}

	if !r.Soak.LastCheckStartedAt.IsZero() && now.Sub(r.Soak.LastCheckStartedAt) < st.Soak.Interval {
		return nil
	}

	fresh := r.Soak.ConsecutiveErrors == 0 && !r.Soak.LastCheckAt.IsZero() && now.Sub(r.Soak.LastCheckAt) <= 2*st.Soak.Interval
	if fresh && r.Soak.Streak > 0 && now.Sub(r.Soak.StartedAt) >= st.Soak.Duration {
		c.passBatch(ctx, now, r, soakPassed(&r.Soak))

		return nil
	}

	byProgram := map[string][]targets.Target{}
	byProgramIDs := map[string][]string{}
	skipped := false

	for _, id := range b.Targets {
		rt := r.target(id)
		if rt.Phase == PhaseSkipped {
			continue
		}

		t, ok := set.Get(id)
		if reason, skip := c.skipReason(r, set, &t, ok); skip {
			rt.Phase, rt.Reason = PhaseSkipped, reason
			skipped = true

			continue
		}

		prog := c.programFor(&t, config.HookSoak)
		if prog == "" {
			continue
		}

		byProgram[prog] = append(byProgram[prog], t)
		byProgramIDs[prog] = append(byProgramIDs[prog], id)
	}

	c.saveSkips(ctx, r, skipped)

	programs := make([]string, 0, len(byProgram))
	for p := range byProgram {
		programs = append(programs, p)
	}

	sort.Strings(programs)

	jobs := make([]job, 0, len(programs))

	for _, prog := range programs {
		remaining := c.remainingForSoak(r, set, prog)
		jobs = append(jobs, job{
			rollout: r.ID, hook: config.HookSoak, program: prog, soakIDs: byProgramIDs[prog],
			input: soakInput{
				Updated:   byProgram[prog],
				Remaining: remaining,
				Rollout:   soakRollout{ID: r.ID, Group: r.Group, Batch: b.Number, Wave: b.Wave},
			},
		})
	}

	r.Soak.LastCheckStartedAt = now

	return jobs
}

// remainingForSoak lists the rollout targets not yet reached that use the same
// soak program, excluding suspended and unknown ones.
func (c *Controller) remainingForSoak(r *Rollout, set *targets.Set, prog string) []targets.Target {
	out := []targets.Target{}

	for i := range r.Targets {
		rt := &r.Targets[i]
		if rt.Batch != 0 || rt.Phase != PhasePending {
			continue
		}

		t, ok := set.Get(rt.ID)
		if !ok || c.programFor(&t, config.HookSoak) != prog || c.suspensionFor(&t) != nil {
			continue
		}

		if _, known := c.liveKnown(rt.ID); !known {
			continue
		}

		out = append(out, t)
	}

	return out
}

// passBatch closes the open batch as passed, keeps its soak with it, and
// decides what comes next.
func (c *Controller) passBatch(ctx context.Context, now time.Time, r *Rollout, why string) {
	b := r.CurrentBatch()
	b.EndedAt = now
	b.Passed = true
	b.Soak = cloneSoak(&r.Soak)

	for _, id := range b.Targets {
		rt := r.target(id)
		if rt.Phase == PhaseReady {
			rt.Phase, rt.Reason = PhasePassed, why
		}

		if _, was := c.degraded[id]; was && rt.Phase == PhasePassed {
			delete(c.degraded, id)
			_ = c.persist(ctx, c.store.ClearDegraded(ctx, id))
		}
	}

	c.event(ctx, now, &Event{Actor: ControllerActor, Action: "batch.passed", Group: r.Group, Rollout: r.ID,
		Reason: fmt.Sprintf("batch %d: %s", b.Number, why)})

	st := c.strategy(r.Strategy)

	if r.PausePending || (st.PauseAfterFirstBatch && b.Number == 1 && c.hasRemaining(r)) {
		r.PausePending = false
		c.setState(ctx, now, r, Paused, fmt.Sprintf("Paused after batch %d. Run promote to continue.", b.Number))

		return
	}

	c.setState(ctx, now, r, Running, fmt.Sprintf("Batch %d passed.", b.Number))
}

func cloneSoak(s *SoakProgress) *SoakProgress {
	cp := *s
	cp.Checks = append([]SoakCheck(nil), s.Checks...)

	return &cp
}

func (c *Controller) hasRemaining(r *Rollout) bool {
	for i := range r.Targets {
		if rt := &r.Targets[i]; rt.Batch == 0 && rt.Phase == PhasePending {
			return true
		}
	}

	return false
}

// halt stops the rollout at the open batch and quarantines its targets.
func (c *Controller) halt(ctx context.Context, now time.Time, r *Rollout, culprit, why string) {
	b := r.CurrentBatch()
	b.EndedAt = now
	b.Soak = cloneSoak(&r.Soak)

	held := 0

	for _, id := range b.Targets {
		rt := r.target(id)
		if rt.Phase == PhaseSkipped || rt.Phase == PhasePassed {
			continue
		}

		held++

		reason := why
		if culprit != "" && id != culprit {
			reason = "in the halted batch"
		}

		rt.Phase, rt.Reason = PhaseFailed, reason
		if !c.readyOnBuild(rt) {
			rt.HoldUntil = now.Add(c.strategy(r.Strategy).ProgressDeadline)
		}

		c.degraded[id] = reason
		_ = c.persist(ctx, c.store.SaveDegraded(ctx, id, reason))
	}

	msg := fmt.Sprintf("Halted at batch %d: %s. %d targets quarantined on %s for debugging.", b.Number, why, held, r.DigestShort())
	if culprit != "" {
		msg = fmt.Sprintf("Halted at batch %d: %s %s. %d targets quarantined on %s for debugging.", b.Number, culprit, why, held, r.DigestShort())
	}

	c.setState(ctx, now, r, Halted, msg)
}

// withdraw takes back jobs that will not run: a target whose update was
// about to be dispatched waits for it again, as after a restart.
func (c *Controller) withdraw(jobs []job) {
	for i := range jobs {
		j := &jobs[i]
		if j.hook != config.HookUpdate {
			continue
		}

		if rt := c.rollouts[j.rollout].target(j.target); rt.Phase == PhaseUpdating && !rt.UpdateDone {
			rt.Phase, rt.Reason = PhasePending, "waiting for the store before running the update"
			rt.UpdateAttempts--
			rt.RetryAt = j.retryAt

			if rt.UpdateAttempts == 0 {
				rt.UpdatedAt = time.Time{}
			}
		}
	}
}

// execute runs jobs concurrently outside the lock.
func (c *Controller) execute(ctx context.Context, jobs []job) []outcome {
	out := make([]outcome, len(jobs))

	var mu sync.Mutex

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(c.limit)

	for i := range jobs {
		j := &jobs[i]

		g.Go(func() error {
			started := c.clock.Now()
			res, err := c.runner.Run(gctx, j.program, j.hook, j.target, j.input)

			mu.Lock()
			out[i] = outcome{job: *j, res: res, err: err, started: started}
			mu.Unlock()

			return nil
		})
	}

	_ = g.Wait()

	return out
}

// applyResults folds hook outcomes back into rollout state. A result for a
// rollout that has since closed, or a target that has since been skipped, is
// dropped.
func (c *Controller) applyResults(ctx context.Context, now time.Time, results []outcome) {
	touched := map[string]*Rollout{}
	soaks := map[string][]outcome{}

	for i := range results {
		o := &results[i]

		r, ok := c.rollouts[o.rollout]
		if !ok || !r.State.Active() {
			continue
		}

		if o.err != nil {
			o.res = hooks.Result{Program: o.program, Reason: o.err.Error(), ExitCode: -1, RanAt: now}
		}

		if o.hook == config.HookSoak {
			soaks[o.rollout] = append(soaks[o.rollout], *o)

			continue
		}

		c.recordHookRun(ctx, o.target, o.hook, &o.res)

		touched[r.ID] = r
		c.applyTargetResult(ctx, now, r, o)
	}

	for id, outs := range soaks {
		r := c.rollouts[id]
		touched[id] = r
		c.applySoak(ctx, now, r, outs)
	}

	for _, r := range touched {
		if r.State.Active() {
			_ = c.saveRollout(ctx, r)
		}
	}
}
func (c *Controller) applyTargetResult(ctx context.Context, now time.Time, r *Rollout, o *outcome) {
	if r.State != Running {
		return
	}

	rt := r.target(o.target)
	if rt == nil || rt.Phase != PhaseUpdating {
		return
	}

	// A target that stopped being eligible while its program ran is skipped;
	// its result neither advances nor halts the batch.
	t, present := c.targets().Get(o.target)
	if reason, skip := c.skipReason(r, c.targets(), &t, present); skip {
		rt.Phase, rt.Reason = PhaseSkipped, reason

		return
	}

	if !o.res.OK {
		c.retryUpdate(ctx, now, r, rt, o.res.Reason)

		return
	}

	rt.UpdateDone, rt.Reason = true, "update started, waiting for the digest"
	rt.RetryAt = time.Time{}
	rt.UpdateError = ""
}

// applySoak folds one check's program results into the soak progress.
func (c *Controller) applySoak(ctx context.Context, now time.Time, r *Rollout, outs []outcome) {
	if r.State != Soaking {
		return
	}

	st := c.strategy(r.Strategy)
	failed, errored := c.recordSoak(ctx, now, r, outs)

	if errored >= 0 {
		r.Soak.ConsecutiveErrors++
	} else {
		r.Soak.ConsecutiveErrors = 0
		r.Soak.LastCheckAt = now
	}

	if failed >= 0 {
		r.Soak.Streak = 0
		r.Soak.Failures++
	} else if errored < 0 {
		r.Soak.Streak++
	}

	if r.Force {
		c.passBatch(ctx, now, r, "forced")

		return
	}

	if r.Soak.Failures > st.Soak.FailureLimit {
		last := &r.Soak.Checks[len(r.Soak.Checks)-1]

		for i := len(r.Soak.Checks) - 1; i >= 0; i-- {
			check := &r.Soak.Checks[i]
			if !check.OK && !check.Error {
				last = check

				break
			}
		}

		c.halt(ctx, now, r, "", fmt.Sprintf("soak %s failed %d times: %s", last.Program, r.Soak.Failures, last.Reason))

		return
	}

	if errored >= 0 {
		last := r.Soak.Checks[errored]
		why := fmt.Sprintf("soak %s could not run: %s; %d consecutive errors of %d allowed", last.Program, last.Reason, r.Soak.ConsecutiveErrors, st.Soak.ConsecutiveErrorLimit)

		if r.Soak.ConsecutiveErrors > st.Soak.ConsecutiveErrorLimit {
			c.halt(ctx, now, r, "", why)
		} else {
			c.setState(ctx, now, r, Soaking, why)
		}

		return
	}

	if r.Soak.Streak > 0 && now.Sub(r.Soak.StartedAt) >= st.Soak.Duration {
		c.passBatch(ctx, now, r, soakPassed(&r.Soak))

		return
	}

	left := st.Soak.Duration - now.Sub(r.Soak.StartedAt)
	if left < 0 {
		left = 0
	}

	c.setState(ctx, now, r, Soaking, fmt.Sprintf("Batch %d in soak, %s left; %d checks failed of %d allowed.",
		r.CurrentBatch().Number, left.Round(time.Second), r.Soak.Failures, st.Soak.FailureLimit))
}

func (c *Controller) recordSoak(ctx context.Context, now time.Time, r *Rollout, outs []outcome) (failed, errored int) {
	failed, errored = -1, -1

	for i := range outs {
		o := &outs[i]
		check := SoakCheck{At: now, Program: o.program, OK: o.res.OK, Error: o.err != nil || o.res.TimedOut || o.res.ExitCode < 0, Reason: o.res.Reason}
		check.Updated, check.Remaining, check.Unit = parseSoakNumbers(o.res.Stdout)
		r.Soak.Checks = append(r.Soak.Checks, check)

		for _, id := range o.soakIDs {
			c.recordHookRun(ctx, id, config.HookSoak, &o.res)
		}

		if check.Error {
			errored = len(r.Soak.Checks) - 1
		} else if !check.OK {
			failed = len(r.Soak.Checks) - 1
		}
	}

	return failed, errored
}

// soakPassed says how a soak ended well.
func soakPassed(s *SoakProgress) string {
	return fmt.Sprintf("soak passed: %d checks, %d failed", len(s.Checks), s.Failures)
}

// parseSoakNumbers reads the optional second stdout line
// "updated=<num> remaining=<num> unit=<text>".
func parseSoakNumbers(stdout string) (updated, remaining, unit string) {
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if len(lines) < 2 {
		return "", "", ""
	}

	for field := range strings.FieldsSeq(lines[1]) {
		k, v, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}

		switch k {
		case "updated":
			updated = v
		case "remaining":
			remaining = v
		case "unit":
			unit = v
		}
	}

	return updated, remaining, unit
}
