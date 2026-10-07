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
	// batch and attempts identify the update the plan wanted, so dispatch can
	// tell whether it is still wanted.
	batch    int
	attempts int
}

// soakInput is what the soak hook receives on stdin.
type soakInput struct {
	Updated   []soakTarget     `json:"updated"`
	Remaining []targets.Target `json:"remaining"`
	Rollout   soakRollout      `json:"rollout"`
}

type soakTarget struct {
	targets.Target
	UpdatedAt time.Time `json:"updatedAt"`
}

type soakRollout struct {
	ID    string `json:"id"`
	Group string `json:"group"`
	Batch int    `json:"batch"`
	Wave  int    `json:"wave"`
}

// outcome is a job with its result and when it started. A withdrawn job never
// ran.
type outcome struct {
	job
	res       hooks.Result
	err       error
	started   time.Time
	withdrawn bool
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
		if c.dirty {
			break
		}

		r := c.rollouts[id]
		if !r.State.Active() {
			continue
		}

		jobs = append(jobs, c.planRollout(ctx, now, r, set)...)
	}

	return jobs
}

// planRollout advances an active operation from observed state.
func (c *Controller) planRollout(ctx context.Context, now time.Time, r *Rollout, set *targets.Set) []job {
	switch r.State {
	case Soaking:
		return c.planSoak(ctx, now, r, set)
	case Running, WaitingForBudget:
		if r.CurrentBatch() == nil {
			c.startBatch(ctx, now, r, set)

			if c.dirty || r.CurrentBatch() == nil {
				return nil
			}
		}

		return c.planBatch(ctx, now, r, set)
	default:
		return nil
	}
}

// planBatch plans updates and reads the independent observations. Ineligible
// targets leave the batch where they are.
func (c *Controller) planBatch(ctx context.Context, now time.Time, r *Rollout, set *targets.Set) []job {
	b := r.CurrentBatch()
	st := c.strategy(r.Strategy)
	paused := c.groupPaused(r.Group)

	var jobs []job

	allReady, skipped := true, false
	changed := false

	for _, id := range b.Targets {
		rt := r.target(id)

		t, ok := set.Get(id)
		if reason, skip := c.skipReason(r, set, &t, ok); skip && (rt.Phase == PhasePending || rt.Phase == PhaseUpdating) {
			c.leave(now, r, rt, PhaseSkipped, reason)

			skipped = true

			continue
		}

		switch rt.Phase {
		case PhasePending:
			allReady = false
			changed = true

			if c.pendingUpdate(now, r, rt, r.Desired[t.Image], &st, paused) {
				jobs = append(jobs, c.updateJob(r, rt))
			}
		case PhaseUpdating:
			live, known := c.liveKnown(id)
			if known && live == r.Desired[t.Image] {
				if !rt.UpdateDone {
					changed = true
					rt.UpdateDone, rt.UpdateError, rt.RetryAt = true, "", time.Time{}
				}

				if !rt.Updated {
					changed = true
					rt.Updated, rt.DigestSeenAt = true, c.live[id].DigestSince

					if rt.UpdatedAt.IsZero() {
						rt.UpdatedAt = rt.DigestSeenAt
					}

					rt.Reason = "on the new build, waiting for readiness"
				}

				if r.Force || c.readyOnBuild(rt) {
					changed = true
					rt.Phase, rt.Reason = PhaseReady, "ready"

					continue
				}
			} else if known && rt.Updated {
				changed = true
				rt.Updated = false
				rt.DigestSeenAt = time.Time{}
			}

			allReady = false

			if why := updateDeadlineReason(now, rt, &st, r.Desired[t.Image]); why != "" {
				changed = true

				if !rt.Updated {
					c.leave(now, r, rt, PhaseSkipped, "update skipped: "+why)

					continue
				}

				if c.readinessCheckMissing(rt) {
					rt.Reason = "readiness could not be checked; waiting for observations to recover"

					continue
				}

				c.withdraw(jobs)
				c.halt(ctx, now, r, id, why)

				return nil
			}

			if paused && !rt.UpdateDone {
				changed = true
				rt.Reason = c.configPauseReason(r.Group)

				continue
			}

			if !rt.RetryAt.IsZero() {
				changed = true

				if now.Before(rt.RetryAt) {
					rt.Reason = fmt.Sprintf("%s; retrying in %s", updateFailure(rt, &st), rt.RetryAt.Sub(now).Round(time.Second))
				} else {
					jobs = append(jobs, c.updateJob(r, rt))
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

// pendingUpdate prepares an unstarted or interrupted target for dispatch.
func (c *Controller) pendingUpdate(now time.Time, r *Rollout, rt *RolloutTarget, desired string, st *config.Strategy, paused bool) bool {
	if paused && rt.UpdateAttempts == 0 {
		rt.Reason = c.configPauseReason(r.Group)

		return false
	}

	rt.Phase, rt.Reason = PhaseUpdating, "update running"

	// An already-observed digest needs verification, not another update
	// that could collide with one still finishing.
	if live, known := c.liveKnown(rt.ID); known && live == desired {
		if rt.UpdatedAt.IsZero() {
			rt.UpdatedAt = c.live[rt.ID].DigestSince
		}

		rt.UpdateDone, rt.Reason = true, "already on the new build, waiting for inspect to confirm"

		return false
	}

	if why := updateDeadlineReason(now, rt, st, desired); why != "" {
		c.leave(now, r, rt, PhaseSkipped, "update skipped: "+why)

		return false
	}

	if rt.UpdateAttempts >= max(st.Retry.Limit, 1) {
		c.leave(now, r, rt, PhaseSkipped, "update skipped: retry limit reached after an interrupted update")

		return false
	}

	if paused {
		rt.Phase, rt.Reason = PhasePending, c.configPauseReason(r.Group)

		return false
	}

	return true
}

// skipReason says why a rollout target can no longer be worked on, if so.
func (c *Controller) skipReason(r *Rollout, set *targets.Set, t *targets.Target, present bool) (string, bool) {
	if !present {
		return "removed from the targets file", true
	}

	if set.Group(t) != r.Group {
		return "moved to group " + set.Group(t), true
	}

	rt := r.target(t.ID)
	if rt != nil && rt.Node != t.Node {
		return "moved to node " + t.Node, true
	}

	if _, known := r.Desired[t.Image]; !known || (rt != nil && rt.Image != t.Image) {
		return "image changed to " + t.Image, true
	}

	if s := c.suspensionFor(t); s != nil {
		return "suspended by " + s.Actor + ": " + s.Reason, true
	}

	return "", false
}

// saveSkips stores changed reservations before later batch admissions.
// Dispatched targets retain holds; undispatched skips may release weight.
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

	byProgram := map[string][]soakTarget{}
	byProgramIDs := map[string][]string{}
	skipped := false

	for _, id := range b.Targets {
		rt := r.target(id)
		if rt.Phase == PhaseSkipped {
			continue
		}

		t, ok := set.Get(id)
		if reason, skip := c.skipReason(r, set, &t, ok); skip {
			c.leave(now, r, rt, PhaseSkipped, reason)

			skipped = true

			continue
		}

		prog := c.programFor(&t, config.HookSoak)
		if prog == "" {
			continue
		}

		byProgram[prog] = append(byProgram[prog], soakTarget{Target: t, UpdatedAt: rt.DigestSeenAt})
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
// soak program, excluding suspended and unknown ones. A retried target was
// reached by the batch that halted, so it is no baseline.
func (c *Controller) remainingForSoak(r *Rollout, set *targets.Set, prog string) []targets.Target {
	out := []targets.Target{}

	for i := range r.Targets {
		rt := &r.Targets[i]
		if rt.Batch != 0 || rt.Phase != PhasePending || rt.Retried {
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
			c.leave(now, r, rt, PhasePassed, why)
		}

		if _, was := c.degraded[id]; was && rt.Phase == PhasePassed {
			delete(c.degraded, id)
			_ = c.persist(ctx, c.store.ClearDegraded(ctx, id))
		}
	}

	c.event(ctx, now, &Event{Actor: ControllerActor, Action: "batch.passed", Group: r.Group, Rollout: r.ID,
		Reason: fmt.Sprintf("batch %d: %s", b.Number, why)})

	if r.PausePending {
		r.PausePending = false
		c.setState(ctx, now, r, Paused, pauseReason(r))

		return
	}

	c.setState(ctx, now, r, Running, fmt.Sprintf("Batch %d passed.", b.Number))
}

func cloneSoak(s *SoakProgress) *SoakProgress {
	cp := *s
	cp.Checks = append([]SoakCheck(nil), s.Checks...)

	return &cp
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

		c.leave(now, r, rt, PhaseFailed, reason)

		c.degraded[id] = reason
		_ = c.persist(ctx, c.store.SaveDegraded(ctx, id, reason))
	}

	msg := fmt.Sprintf("Halted at batch %d: %s. %d targets quarantined on %s for debugging.", b.Number, why, held, r.DigestShort())
	if culprit != "" {
		msg = fmt.Sprintf("Halted at batch %d: %s %s. %d targets quarantined on %s for debugging.", b.Number, culprit, why, held, r.DigestShort())
	}

	c.setState(ctx, now, r, Halted, msg)
}

// storeWait is why an update waits after the store refused a decision.
const storeWait = "waiting for the store before running the update"

// withdraw takes back the updates of jobs that will not run.
func (c *Controller) withdraw(jobs []job) {
	for i := range jobs {
		if jobs[i].hook == config.HookUpdate {
			c.withdrawUpdate(&jobs[i], storeWait)
		}
	}
}

// withdrawUpdate puts a target whose planned update will not run back to
// waiting for it, as after a restart; attempts already dispatched still count.
// It returns the rollout it changed, if any.
func (c *Controller) withdrawUpdate(j *job, why string) *Rollout {
	r := c.rollouts[j.rollout]

	rt := r.target(j.target)
	if rt.Phase != PhaseUpdating || rt.UpdateDone || rt.UpdateAttempts != j.attempts {
		return nil
	}

	rt.Phase, rt.Reason = PhasePending, why
	rt.RetryAt = j.retryAt

	if rt.UpdateAttempts == 0 {
		rt.UpdatedAt = time.Time{}
	}

	return r
}

// dispatch decides under the lock, just before the program runs, whether a
// planned update is still wanted, and stores the attempt first so a restart
// knows it may have landed. An unwanted update is withdrawn instead.
func (c *Controller) dispatch(ctx context.Context, j *job) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	set := c.targets()

	why := c.staleUpdate(set, j)
	if why == "" && c.dirty {
		why = storeWait
	}

	if why == "" {
		r := c.rollouts[j.rollout]
		rt := r.target(j.target)
		t, _ := set.Get(j.target)

		if rt.UpdateAttempts == 0 {
			rt.UpdatedAt = c.clock.Now()
		}

		rt.UpdateAttempts++

		if c.saveRollout(ctx, r) == nil {
			j.program, j.input = c.programFor(&t, config.HookUpdate), c.hookInput(&t)

			return true
		}

		rt.UpdateAttempts--
		why = storeWait
	}

	if r := c.withdrawUpdate(j, why); r != nil && !c.dirty {
		_ = c.saveRollout(ctx, r)
	}

	return false
}

// staleUpdate says why a planned update no longer belongs to its rollout's
// open batch, or "" while it does.
func (c *Controller) staleUpdate(set *targets.Set, j *job) string {
	r := c.rollouts[j.rollout]
	if r.State != Running {
		return "update withdrawn: the rollout stopped running"
	}

	if c.groupPaused(r.Group) {
		return "update withdrawn: " + c.configPauseReason(r.Group)
	}

	// Only the attempt the plan made may run, and only while its batch is open.
	b, rt := r.CurrentBatch(), r.target(j.target)
	if b == nil || b.Number != j.batch || rt.Phase != PhaseUpdating || rt.UpdateDone || rt.UpdateAttempts != j.attempts {
		return "update withdrawn: no longer waiting for it"
	}

	t, present := set.Get(j.target)
	if reason, skip := c.skipReason(r, set, &t, present); skip {
		return "update withdrawn: " + reason
	}

	if r.GroupDesiredKey != c.groupDesiredKey(r.Group, set) {
		return "update withdrawn: group desired images changed"
	}

	if live, known := c.liveKnown(j.target); known && live == r.Desired[t.Image] {
		return "update withdrawn: already observed on the desired digest"
	}

	return ""
}

// execute runs jobs concurrently outside the lock; an update runs only once
// dispatch has validated and stored it.
func (c *Controller) execute(ctx context.Context, jobs []job) []outcome {
	out := make([]outcome, len(jobs))

	var mu sync.Mutex

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(c.limit)

	for i := range jobs {
		j := &jobs[i]

		g.Go(func() error {
			if j.hook == config.HookUpdate && !c.dispatch(gctx, j) {
				mu.Lock()
				out[i] = outcome{job: *j, withdrawn: true}
				mu.Unlock()

				return nil
			}

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

// applyResults folds hook outcomes back into rollout state. A withdrawn job,
// a result for a rollout that has since closed, or one for a target that has
// since been skipped, is dropped.
func (c *Controller) applyResults(ctx context.Context, now time.Time, results []outcome) {
	touched := map[string]*Rollout{}
	soaks := map[string][]outcome{}

	for i := range results {
		o := &results[i]
		if o.withdrawn {
			continue
		}

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
		c.leave(now, r, rt, PhaseSkipped, reason)

		return
	}

	if !o.res.OK {
		c.retryUpdate(ctx, now, r, rt, o.res.Reason)

		return
	}

	rt.UpdateDone, rt.Reason = true, "update started, waiting for the digest"
	rt.RetryAt = time.Time{}
	rt.UpdateError = ""

	// The update may land at once, so it earns one observation that does not
	// wait for the probe spacing.
	c.probes.kick(rt.ID)
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
		why := fmt.Sprintf("soak %s could not check: %s; waiting for checks to recover (%d consecutive errors)", last.Program, last.Reason, r.Soak.ConsecutiveErrors)
		c.setState(ctx, now, r, Soaking, why)

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
		check := SoakCheck{At: now, Program: o.program, OK: o.res.OK, Error: o.err != nil || o.res.CouldNotCheck(), Reason: o.res.Reason}
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
