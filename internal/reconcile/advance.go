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

// outcome is a job with its result.
type outcome struct {
	job
	res hooks.Result
	err error
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

		c.retryBatch(ctx, now, r)

		return c.planSoak(ctx, now, r, set)
	case Soaking:
		return c.planSoak(ctx, now, r, set)
	case Running, WaitingForBudget, WaitingForEnvironment:
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

// retryBatch reopens a halted batch for another soak.
func (c *Controller) retryBatch(ctx context.Context, now time.Time, r *Rollout) {
	r.RetryPending = false
	r.Soak = SoakProgress{StartedAt: now}

	b := &r.Batches[len(r.Batches)-1]
	b.EndedAt = time.Time{}

	for _, id := range b.Targets {
		rt := r.target(id)
		if rt.Phase == PhaseFailed {
			rt.Phase, rt.Reason = PhaseReady, "retrying"

			delete(c.degraded, id)
			c.persist(ctx, c.store.ClearDegraded(ctx, id))
		}
	}

	c.setState(ctx, now, r, Soaking, fmt.Sprintf("Retrying the soak for batch %d.", b.Number))
}

// planBatch moves each target in the open batch one step: update, then wait
// for the digest to land, then ready.
func (c *Controller) planBatch(ctx context.Context, now time.Time, r *Rollout, set *targets.Set) []job {
	b := r.CurrentBatch()

	var jobs []job

	allReady := true

	for _, id := range b.Targets {
		rt := r.target(id)
		t, ok := set.Get(id)

		if !ok {
			rt.Phase, rt.Reason = PhaseSkipped, "removed from the targets file"

			continue
		}

		switch rt.Phase {
		case PhasePending:
			allReady = false

			jobs = append(jobs, job{rollout: r.ID, target: id, hook: config.HookUpdate, program: c.programFor(&t, config.HookUpdate), input: t})
		case PhaseUpdating:
			allReady = false

			if deadline := c.cfg.Hooks.Timeout * 5; now.Sub(rt.UpdatedAt) > deadline {
				why := fmt.Sprintf("did not reach %s within %s", shortDigest(r.Desired[t.Image]), deadline)
				if rt.Updated {
					why = fmt.Sprintf("%s within %s", rt.Reason, deadline)
				}

				c.halt(ctx, now, r, id, why)

				return nil
			}

			if !rt.Updated {
				jobs = append(jobs, job{rollout: r.ID, target: id, hook: config.HookInspect, program: c.programFor(&t, config.HookInspect), input: t})
			} else {
				jobs = append(jobs, job{rollout: r.ID, target: id, hook: config.HookReady, program: c.programFor(&t, config.HookReady), input: t})
			}
		case PhaseReady, PhasePassed, PhaseSkipped, PhaseFailed:
		}
	}

	if allReady && len(jobs) == 0 {
		c.batchReady(ctx, now, r, set)
	}

	return jobs
}

// batchReady is called once every target in the batch is ready: either the
// soak begins or, with no soak, the batch passes.
func (c *Controller) batchReady(ctx context.Context, now time.Time, r *Rollout, set *targets.Set) {
	preset := c.preset(r.Speed)

	if preset.Soak.Duration == 0 || r.Force || !c.batchHasSoak(r, set) {
		c.passBatch(ctx, now, r, "no soak")

		return
	}

	r.Soak = SoakProgress{StartedAt: now}
	c.setState(ctx, now, r, Soaking, fmt.Sprintf("Batch %d in soak for %s.", r.CurrentBatch().Number, preset.Soak.Duration))
}

// batchHasSoak reports whether any target in the open batch names a soak program.
func (c *Controller) batchHasSoak(r *Rollout, set *targets.Set) bool {
	for _, id := range r.CurrentBatch().Targets {
		if t, ok := set.Get(id); ok && c.programFor(&t, config.HookSoak) != "" {
			return true
		}
	}

	return false
}

// planSoak runs the soak programs when a check is due, and passes the batch
// once the streak and duration are met.
func (c *Controller) planSoak(ctx context.Context, now time.Time, r *Rollout, set *targets.Set) []job {
	preset := c.preset(r.Speed)
	b := r.CurrentBatch()

	if !r.Soak.LastCheckAt.IsZero() && now.Sub(r.Soak.LastCheckAt) < preset.Soak.Interval {
		return nil
	}

	if r.Soak.Streak >= preset.Soak.Passes && now.Sub(r.Soak.StartedAt) >= preset.Soak.Duration {
		c.passBatch(ctx, now, r, fmt.Sprintf("%d checks passed", r.Soak.Streak))

		return nil
	}

	byProgram := map[string][]targets.Target{}
	byProgramIDs := map[string][]string{}

	for _, id := range b.Targets {
		t, ok := set.Get(id)
		if !ok {
			continue
		}

		prog := c.programFor(&t, config.HookSoak)
		if prog == "" {
			continue
		}

		byProgram[prog] = append(byProgram[prog], t)
		byProgramIDs[prog] = append(byProgramIDs[prog], id)
	}

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

	r.Soak.LastCheckAt = now

	return jobs
}

// remainingForSoak lists the rollout targets not yet reached that use the same
// soak program, excluding suspended and unknown ones.
func (c *Controller) remainingForSoak(r *Rollout, set *targets.Set, prog string) []targets.Target {
	out := []targets.Target{}

	for _, rt := range r.Targets {
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

// passBatch closes the open batch as passed and decides what comes next.
func (c *Controller) passBatch(ctx context.Context, now time.Time, r *Rollout, why string) {
	b := r.CurrentBatch()
	b.EndedAt = now
	b.Passed = true

	for _, id := range b.Targets {
		rt := r.target(id)
		if rt.Phase == PhaseReady {
			rt.Phase, rt.Reason = PhasePassed, why
		}

		if _, was := c.degraded[id]; was {
			delete(c.degraded, id)
			c.persist(ctx, c.store.ClearDegraded(ctx, id))
		}
	}

	c.event(ctx, now, &Event{Actor: ControllerActor, Action: "batch.passed", Group: r.Group, Rollout: r.ID,
		Reason: fmt.Sprintf("batch %d: %s", b.Number, why)})

	preset := c.preset(r.Speed)

	if r.PausePending || (preset.PauseAfterFirst && b.Number == 1 && c.hasRemaining(r)) {
		r.PausePending = false
		c.setState(ctx, now, r, Paused, fmt.Sprintf("Paused after batch %d. Run promote to continue.", b.Number))

		return
	}

	c.setState(ctx, now, r, Running, fmt.Sprintf("Batch %d passed.", b.Number))
}

func (c *Controller) hasRemaining(r *Rollout) bool {
	for _, rt := range r.Targets {
		if rt.Batch == 0 && rt.Phase == PhasePending {
			return true
		}
	}

	return false
}

// halt stops the rollout at the open batch and quarantines its targets.
func (c *Controller) halt(ctx context.Context, now time.Time, r *Rollout, culprit, why string) {
	b := r.CurrentBatch()
	b.EndedAt = now

	for _, id := range b.Targets {
		rt := r.target(id)
		if rt.Phase == PhaseSkipped || rt.Phase == PhasePassed {
			continue
		}

		reason := why
		if culprit != "" && id != culprit {
			reason = "in the halted batch"
		}

		rt.Phase, rt.Reason = PhaseFailed, reason
		c.degraded[id] = reason
		c.persist(ctx, c.store.SaveDegraded(ctx, id, reason))
	}

	msg := fmt.Sprintf("Halted at batch %d: %s. %d targets left running %s for debugging.", b.Number, why, len(b.Targets), r.DigestShort())
	if culprit != "" {
		msg = fmt.Sprintf("Halted at batch %d: %s %s. %d targets left running %s for debugging.", b.Number, culprit, why, len(b.Targets), r.DigestShort())
	}

	c.setState(ctx, now, r, Halted, msg)
}

// execute runs jobs concurrently outside the lock.
func (c *Controller) execute(ctx context.Context, jobs []job) []outcome {
	out := make([]outcome, len(jobs))

	var mu sync.Mutex

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(c.limit)

	for i, j := range jobs {
		g.Go(func() error {
			res, err := c.runner.Run(gctx, j.program, j.hook, j.target, j.input)

			mu.Lock()
			out[i] = outcome{job: j, res: res, err: err}
			mu.Unlock()

			return nil
		})
	}

	_ = g.Wait()

	return out
}

// applyResults folds hook outcomes back into rollout state.
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

		c.recordHookRun(o.target, o.hook, &o.res)

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
			c.saveRollout(ctx, r)
		}
	}
}

func (c *Controller) applyTargetResult(ctx context.Context, now time.Time, r *Rollout, o *outcome) {
	if r.State != Running {
		return
	}

	rt := r.target(o.target)
	if rt == nil {
		return
	}

	switch o.hook {
	case config.HookUpdate:
		if !o.res.OK {
			c.halt(ctx, now, r, o.target, "update failed: "+o.res.Reason)

			return
		}

		rt.Phase, rt.UpdatedAt, rt.Reason = PhaseUpdating, now, "update started"
	case config.HookInspect:
		digest := strings.TrimSpace(o.res.Reason)
		c.setLiveLocked(ctx, now, o.target, digest, o.res.OK, o.res.Reason)

		t, _ := c.targets().Get(o.target)
		if o.res.OK && digest == r.Desired[t.Image] {
			rt.Updated, rt.Reason = true, "on the new build, checking readiness"

			if r.Force {
				rt.Phase, rt.Reason = PhaseReady, "ready (forced)"
			}
		}
	case config.HookReady:
		if o.res.OK {
			rt.Phase, rt.Reason = PhaseReady, "ready"
		} else {
			rt.Reason = "not ready: " + o.res.Reason
		}
	}
}

// applySoak folds one check's program results into the soak progress.
func (c *Controller) applySoak(ctx context.Context, now time.Time, r *Rollout, outs []outcome) {
	if r.State != Soaking {
		return
	}

	preset := c.preset(r.Speed)
	allOK := true

	for i := range outs {
		o := &outs[i]
		check := SoakCheck{At: now, Program: o.program, OK: o.res.OK, Reason: o.res.Reason}
		check.Updated, check.Remaining, check.Unit = parseSoakNumbers(o.res.Stdout)
		r.Soak.Checks = append(r.Soak.Checks, check)

		for _, id := range o.soakIDs {
			c.recordHookRun(id, config.HookSoak, &o.res)
		}

		if !o.res.OK {
			allOK = false
		}
	}

	if allOK {
		r.Soak.Streak++
	} else {
		r.Soak.Streak = 0
		r.Soak.Failures++
	}

	if r.Soak.Failures > preset.Soak.Grace {
		last := r.Soak.Checks[len(r.Soak.Checks)-1]
		c.halt(ctx, now, r, "", fmt.Sprintf("soak %s failed %d times: %s", last.Program, r.Soak.Failures, last.Reason))

		return
	}

	if r.Soak.Streak >= preset.Soak.Passes && now.Sub(r.Soak.StartedAt) >= preset.Soak.Duration {
		c.passBatch(ctx, now, r, fmt.Sprintf("%d checks passed", r.Soak.Streak))

		return
	}

	left := preset.Soak.Duration - now.Sub(r.Soak.StartedAt)
	if left < 0 {
		left = 0
	}

	c.setState(ctx, now, r, Soaking, fmt.Sprintf("Batch %d in soak, %s left, %d of %d checks passed.", r.CurrentBatch().Number, left.Round(time.Second), r.Soak.Streak, preset.Soak.Passes))
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
