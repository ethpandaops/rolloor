package reconcile

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/hooks"
	"github.com/ethpandaops/rolloor/internal/observability"
	"github.com/ethpandaops/rolloor/internal/registry"
	"github.com/ethpandaops/rolloor/internal/targets"
)

// Resolver turns an image reference into the digest its tag points at.
type Resolver interface {
	Resolve(ctx context.Context, ref string) (registry.Resolved, error)
}

// Runner executes hook programs.
type Runner interface {
	Run(ctx context.Context, program, hook, targetID string, input any) (hooks.Result, error)
}

// Clock is the source of time, replaceable in tests.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Options wire a Controller.
type Options struct {
	Config *config.Config
	// Targets returns the current set; it is called on every tick.
	Targets  func() *targets.Set
	Resolver Resolver
	Runner   Runner
	Store    Store
	Notifier Notifier
	Clock    Clock
	Log      observability.ContextualLogger
	// NewID generates rollout and suspension ids; random when nil.
	NewID func() string
	// Concurrency bounds hook and registry calls made in one tick.
	Concurrency int
}

// Controller owns all rollout state for one environment.
type Controller struct {
	cfg      *config.Config
	targets  func() *targets.Set
	resolver Resolver
	runner   Runner
	store    Store
	notifier Notifier
	clock    Clock
	log      observability.ContextualLogger
	newID    func() string
	limit    int

	mu          sync.RWMutex
	desired     map[string]Desired
	live        map[string]Live
	degraded    map[string]string
	policies    map[string]Policy
	suspensions map[string]Suspension
	rollouts    map[string]*Rollout
	aborted     map[string]string
	hookRuns    map[string]map[string]HookRun

	envOK        bool
	envReason    string
	envCheckedAt time.Time

	lastResolve   time.Time
	lastTick      time.Time
	refreshWanted bool
	// dirty is set when a store write failed; no hook runs until every
	// decision has been written again.
	dirty bool
	// resolveMu serializes registry scans so an older scan cannot publish
	// over a newer one.
	resolveMu     sync.Mutex
	resolveErrors map[string]string
	targetsError  string
	nextEventID   int64

	nudge chan struct{}
}

// New restores state from the store and returns a controller ready to tick.
func New(ctx context.Context, opts *Options) (*Controller, error) {
	if opts == nil || opts.Config == nil || opts.Targets == nil || opts.Resolver == nil || opts.Runner == nil || opts.Store == nil {
		return nil, errors.New("reconcile: config, targets, resolver, runner and store are required")
	}

	c := &Controller{
		cfg:           opts.Config,
		targets:       opts.Targets,
		resolver:      opts.Resolver,
		runner:        opts.Runner,
		store:         opts.Store,
		notifier:      opts.Notifier,
		clock:         opts.Clock,
		newID:         opts.NewID,
		limit:         opts.Concurrency,
		desired:       map[string]Desired{},
		live:          map[string]Live{},
		degraded:      map[string]string{},
		policies:      map[string]Policy{},
		suspensions:   map[string]Suspension{},
		rollouts:      map[string]*Rollout{},
		aborted:       map[string]string{},
		hookRuns:      map[string]map[string]HookRun{},
		resolveErrors: map[string]string{},
		envOK:         true,
		nudge:         make(chan struct{}, 1),
	}

	if c.clock == nil {
		c.clock = realClock{}
	}

	if c.newID == nil {
		c.newID = randomID
	}

	if c.limit <= 0 {
		c.limit = 16
	}

	if opts.Log == nil {
		return nil, errors.New("reconcile: logger is required")
	}

	c.log = opts.Log.WithField("component", "reconcile")

	snap, err := c.store.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("reconcile: load state: %w", err)
	}

	c.restore(snap)

	return c, nil
}

func (c *Controller) restore(snap *Snapshot) {
	for _, r := range snap.Rollouts {
		c.rollouts[r.ID] = r
	}

	for g, p := range snap.Policies {
		c.policies[g] = p
	}

	for _, s := range snap.Suspensions {
		c.suspensions[s.ID] = s
	}

	for id, l := range snap.Live {
		c.live[id] = l
	}

	for id, reason := range snap.Degraded {
		c.degraded[id] = reason
	}

	for img, d := range snap.Desired {
		c.desired[img] = d
	}

	for g, key := range snap.Aborted {
		c.aborted[g] = key
	}

	for id, runs := range snap.HookRuns {
		c.hookRuns[id] = runs
	}

	// An abort whose rollout save never landed: the marker wins.
	for _, r := range c.rollouts {
		if _, aborted := c.aborted[r.Group]; aborted && r.State.Active() {
			c.end(c.clock.Now(), r, Aborted, "Aborted before the last restart.")
		}
	}

	// An update whose hook was running when the process stopped never
	// reported back; the hook is idempotent, so it runs again.
	for _, r := range c.rollouts {
		for i := range r.Targets {
			rt := &r.Targets[i]
			if rt.Phase == PhaseUpdating && !rt.UpdateDone {
				rt.Phase, rt.Reason = PhasePending, "update interrupted by a restart; running again"
			}
		}
	}

	c.nextEventID = max(snap.NextEventID, 1)
}

func randomID() string {
	var b [6]byte

	_, _ = rand.Read(b[:])

	return hex.EncodeToString(b[:])
}

// Run ticks on a schedule until ctx ends. It blocks.
func (c *Controller) Run(ctx context.Context, every time.Duration) error {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		if err := c.Tick(ctx); err != nil {
			c.log.WithError(err).Warn("tick failed")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-c.nudge:
		}
	}
}

// Nudge asks Run to tick now.
func (c *Controller) Nudge() {
	select {
	case c.nudge <- struct{}{}:
	default:
	}
}

// Tick advances everything as far as the present moment allows: resolve tags
// if due, run the environment check if due, expire suspensions, open rollouts
// for groups that need one, and move every active rollout one step.
func (c *Controller) Tick(ctx context.Context) error {
	now := c.clock.Now()

	if err := c.resolveIfDue(ctx, now); err != nil {
		return err
	}

	if err := c.checkEnvironmentIfDue(ctx, now); err != nil {
		return err
	}

	c.mu.Lock()

	var jobs []job

	// Decisions the store refused are written again before any new ones are
	// made or any program runs on their behalf.
	if c.flush(ctx) {
		c.expireSuspensions(ctx, now)
		c.supersedeChangedRollouts(ctx, now)
		c.openRollouts(ctx, now)
		jobs = c.planRollouts(ctx, now)
	}

	c.mu.Unlock()

	results := c.execute(ctx, jobs)

	c.mu.Lock()
	c.applyResults(ctx, now, results)
	c.lastTick = now
	c.mu.Unlock()

	return ctx.Err()
}

// LastTick is when the loop last completed a pass.
func (c *Controller) LastTick() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.lastTick
}

// resolveIfDue re-resolves every image reference on the registry poll interval
// or when a refresh was asked for. The refresh flag is consumed before the
// network work starts, so a request arriving during it stays pending.
func (c *Controller) resolveIfDue(ctx context.Context, now time.Time) error {
	c.mu.Lock()
	due := c.refreshWanted || c.lastResolve.IsZero() || now.Sub(c.lastResolve) >= c.cfg.Registry.Poll
	c.refreshWanted = false
	c.mu.Unlock()

	if !due {
		return nil
	}

	return c.resolveAll(ctx, now)
}

// resolveAll resolves every image now and records the results.
func (c *Controller) resolveAll(ctx context.Context, now time.Time) error {
	c.resolveMu.Lock()
	defer c.resolveMu.Unlock()

	images := c.targets().Images()
	resolved := make(map[string]registry.Resolved, len(images))
	failed := make(map[string]string)

	var rmu sync.Mutex

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(c.limit)

	for _, img := range images {
		g.Go(func() error {
			res, err := c.resolver.Resolve(gctx, img)

			rmu.Lock()
			defer rmu.Unlock()

			if err != nil {
				failed[img] = err.Error()

				return nil
			}

			resolved[img] = res

			return nil
		})
	}

	_ = g.Wait()

	c.mu.Lock()
	defer c.mu.Unlock()

	c.lastResolve = now

	for img, res := range resolved {
		prev, had := c.desired[img]
		d := Desired{Digest: res.Digest, Revision: res.Revision, ResolvedAt: now}
		c.desired[img] = d

		if _, wasFailing := c.resolveErrors[img]; wasFailing {
			delete(c.resolveErrors, img)
			c.event(ctx, now, &Event{Actor: ControllerActor, Action: "registry.recovered", Target: img})
		}

		if !had || prev.Digest != res.Digest {
			_ = c.persist(ctx, c.store.SaveDesired(ctx, img, d))
			c.event(ctx, now, &Event{Actor: ControllerActor, Action: "digest.changed", Target: img,
				Reason: fmt.Sprintf("%s → %s %s", shortDigest(prev.Digest), shortDigest(res.Digest), res.Revision)})
		}
	}

	// One event per failure episode; the latest text stays visible in views.
	for img, msg := range failed {
		if _, already := c.resolveErrors[img]; !already {
			c.event(ctx, now, &Event{Actor: ControllerActor, Action: "registry.error", Target: img, Reason: msg})
		}

		c.resolveErrors[img] = msg
	}

	return ctx.Err()
}

// ReportTargetsError records that the targets files failed to reload, once
// per distinct message; the previous set stays in use.
func (c *Controller) ReportTargetsError(ctx context.Context, err error) {
	now := c.clock.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	msg := ""
	if err != nil {
		msg = err.Error()
	}

	if msg == c.targetsError {
		return
	}

	c.targetsError = msg

	if msg == "" {
		c.event(ctx, now, &Event{Actor: ControllerActor, Action: "targets.reloaded"})

		return
	}

	c.event(ctx, now, &Event{Actor: ControllerActor, Action: "targets.invalid", Reason: msg})
}

// checkEnvironmentIfDue runs the environment hook on its interval.
func (c *Controller) checkEnvironmentIfDue(ctx context.Context, now time.Time) error {
	prog := c.cfg.Hooks.Environment.Program
	if prog == "" {
		return nil
	}

	c.mu.Lock()
	due := c.envCheckedAt.IsZero() || now.Sub(c.envCheckedAt) >= c.cfg.Hooks.Environment.Interval
	c.mu.Unlock()

	if !due {
		return nil
	}

	res, err := c.runner.Run(ctx, prog, config.HookEnvironment, "", map[string]string{"environment": c.cfg.Environment})

	c.mu.Lock()
	defer c.mu.Unlock()

	c.envCheckedAt = now

	ok := err == nil && res.OK
	reason := res.Reason

	if err != nil {
		reason = err.Error()
	}

	if ok != c.envOK {
		action := "environment.passing"
		if !ok {
			action = "environment.failing"
		}

		c.event(ctx, now, &Event{Actor: ControllerActor, Action: action, Reason: reason})
	}

	c.envOK, c.envReason = ok, reason

	return ctx.Err()
}

func (c *Controller) expireSuspensions(ctx context.Context, now time.Time) {
	for id, s := range c.suspensions {
		if !now.Before(s.ExpiresAt) {
			delete(c.suspensions, id)
			_ = c.persist(ctx, c.store.DeleteSuspension(ctx, id))
			c.event(ctx, now, &Event{Actor: ControllerActor, Action: "suspend.expired", Selector: s.Selector.String(), Reason: s.Reason})
		}
	}
}

// event records one line of history and tells listeners.
func (c *Controller) event(ctx context.Context, now time.Time, e *Event) {
	e.ID = c.nextEventID
	e.At = now
	c.nextEventID++

	_ = c.persist(ctx, c.store.AppendEvent(ctx, e))

	if c.notifier != nil {
		c.notifier.Publish(e)
	}
}

// persist logs a store failure, marks the state dirty and returns it. A
// person's action reports it; the controller's own decisions stay in memory
// and are written again by flush before anything else happens.
func (c *Controller) persist(ctx context.Context, err error) error {
	if err != nil {
		c.dirty = true

		c.log.WithContext(ctx).WithError(err).Error("store write failed")
	}

	return err
}

// flush writes every decision again after a store failure. It reports
// whether the store is clean.
func (c *Controller) flush(ctx context.Context) bool {
	if !c.dirty {
		return true
	}

	c.dirty = false

	snap := &Snapshot{Degraded: c.degraded, Aborted: c.aborted, Desired: c.desired}

	for _, r := range c.rollouts {
		snap.Rollouts = append(snap.Rollouts, r)
	}

	for _, sp := range c.suspensions {
		snap.Suspensions = append(snap.Suspensions, sp)
	}

	// Deletions are decisions too: a lifted quarantine or abort marker must
	// not come back after a restart, so the tables are replaced whole.
	_ = c.persist(ctx, c.store.ReplaceDecisions(ctx, snap))

	if c.dirty {
		c.log.WithContext(ctx).Warn("store still failing; no programs run this tick")
	}

	return !c.dirty
}

// commit applies change to r and saves it. When the store refuses, the
// rollout is put back so memory never runs ahead of disk.
func (c *Controller) commit(ctx context.Context, r *Rollout, change func()) error {
	before := cloneRollout(r)

	change()

	if err := c.saveRollout(ctx, r); err != nil {
		*r = *before

		return err
	}

	return nil
}

func (c *Controller) saveRollout(ctx context.Context, r *Rollout) error {
	return c.persist(ctx, c.store.SaveRollout(ctx, r))
}

// SetLive records what inspect reported for a target, for an observation
// that began at observedAt. ok false counts a failure; after enough
// consecutive failures the target reads Unknown.
func (c *Controller) SetLive(ctx context.Context, id, digest string, ok bool, reason string, observedAt time.Time) {
	now := c.clock.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	c.setLiveLocked(ctx, now, observedAt, id, digest, ok, reason)
}

// setLiveLocked applies an observation unless a newer one is already
// recorded or the target has left the files. It reports whether it applied.
func (c *Controller) setLiveLocked(ctx context.Context, now, observedAt time.Time, id, digest string, ok bool, reason string) bool {
	if _, present := c.targets().Get(id); !present {
		return false
	}

	l := c.live[id]
	if observedAt.Before(l.ObservedAt) {
		return false
	}

	if ok {
		l = Live{Digest: digest, SeenAt: now}
	} else {
		l.Failures++
		l.Reason = reason
	}

	l.ObservedAt = observedAt
	c.live[id] = l
	_ = c.persist(ctx, c.store.SaveLive(ctx, id, &l))

	return true
}

// InspectAll runs the inspect hook for every target and records the result.
func (c *Controller) InspectAll(ctx context.Context) {
	set := c.targets()

	// Hook input reads the desired digests, so it is built under the lock
	// before the programs run.
	c.mu.Lock()

	inputs := make([]HookInput, len(set.Targets))
	for i := range set.Targets {
		inputs[i] = c.hookInput(set, &set.Targets[i])
	}

	c.mu.Unlock()

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(c.cfg.Inspect.Concurrency)

	for i := range set.Targets {
		t := set.Targets[i]
		in := inputs[i]

		g.Go(func() error {
			started := c.clock.Now()
			res, err := c.runner.Run(gctx, c.programFor(&t, config.HookInspect), config.HookInspect, t.ID, in)

			c.mu.Lock()
			defer c.mu.Unlock()

			if err != nil {
				c.setLiveLocked(gctx, c.clock.Now(), started, t.ID, "", false, err.Error())

				return nil
			}

			c.recordHookRun(gctx, t.ID, config.HookInspect, &res)
			c.setLiveLocked(gctx, c.clock.Now(), started, t.ID, strings.TrimSpace(res.Reason), res.OK, res.Reason)

			return nil
		})
	}

	_ = g.Wait()
}

// RunInspector observes every target on the inspect interval until ctx ends.
// It blocks. The first pass runs before it returns control to the ticker.
func (c *Controller) RunInspector(ctx context.Context) error {
	ticker := time.NewTicker(c.cfg.Inspect.Interval)
	defer ticker.Stop()

	for {
		c.InspectAll(ctx)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Forget drops live state and quarantine for targets that left the files.
func (c *Controller) Forget(ctx context.Context, ids []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, id := range ids {
		delete(c.live, id)
		delete(c.degraded, id)
		delete(c.hookRuns, id)
		_ = c.persist(ctx, c.store.DeleteLive(ctx, id))
		_ = c.persist(ctx, c.store.ClearDegraded(ctx, id))
		_ = c.persist(ctx, c.store.DeleteHookRuns(ctx, id))
	}
}

// HookInput is what target hooks receive on stdin: the target plus the
// digest it should be running, so update can deploy exactly that.
type HookInput struct {
	targets.Target

	Desired    string `json:"desired,omitempty"`
	DesiredRef string `json:"desiredRef,omitempty"`
}

func (c *Controller) hookInput(set *targets.Set, t *targets.Target) HookInput {
	in := HookInput{Target: *t}

	if d, ok := c.desiredFor(set.Group(t), t); ok && d.Digest != "" {
		in.Desired = d.Digest
		in.DesiredRef = DesiredRef(t.Image, d.Digest)
	}

	return in
}

// DesiredRef is the image reference a runtime can pull for exactly one
// digest: the repository without its tag, at the digest.
func DesiredRef(image, digest string) string {
	return imageRepo(image) + "@" + digest
}

// imageRepo strips the tag from an image reference.
func imageRepo(image string) string {
	slash := strings.LastIndex(image, "/")
	if colon := strings.LastIndex(image, ":"); colon > slash {
		return image[:colon]
	}

	return image
}

// hookRunsFor copies a target's last hook results for a view.
func (c *Controller) hookRunsFor(id string) map[string]HookRun {
	runs, ok := c.hookRuns[id]
	if !ok {
		return nil
	}

	out := make(map[string]HookRun, len(runs))
	for k, v := range runs {
		out[k] = v
	}

	return out
}

// liveKnown reports whether a target's running digest can be trusted.
func (c *Controller) liveKnown(id string) (string, bool) {
	l, ok := c.live[id]
	if !ok || l.SeenAt.IsZero() || l.Failures >= c.cfg.Inspect.UnknownAfter {
		return "", false
	}

	return l.Digest, true
}

// desiredFor is the digest a target should run: a pin, else the tag's head.
func (c *Controller) desiredFor(group string, t *targets.Target) (Desired, bool) {
	if p, ok := c.policies[group]; ok {
		if pin, pinned := p.Pins[t.Image]; pinned {
			return Desired{Digest: pin}, true
		}
	}

	d, ok := c.desired[t.Image]

	return d, ok
}

// suspensionFor returns the suspension covering a target, if any.
func (c *Controller) suspensionFor(t *targets.Target) *Suspension {
	var found *Suspension

	for id := range c.suspensions {
		s := c.suspensions[id]
		if s.Selector.Match(t) && (found == nil || s.ID < found.ID) {
			found = &s
		}
	}

	return found
}

func (c *Controller) policyFor(group string) Policy {
	if p, ok := c.policies[group]; ok {
		return p
	}

	return Policy{Mode: c.cfg.DefaultPolicy.Mode, Speed: c.cfg.DefaultPolicy.Speed}
}

func (c *Controller) preset(name string) config.Preset {
	if p, ok := c.cfg.Presets[name]; ok {
		return p
	}

	return c.cfg.Presets[c.cfg.DefaultPolicy.Speed]
}

func (c *Controller) programFor(t *targets.Target, hook string) string {
	if p, ok := t.Probes[hook]; ok && p != "" {
		return p
	}

	return c.cfg.Hooks.Defaults[hook]
}

// recordHookRun keeps the last result per target and hook, for people
// looking at a target. A result for a target that has left the files is
// dropped.
func (c *Controller) recordHookRun(ctx context.Context, id, hook string, res *hooks.Result) {
	if _, present := c.targets().Get(id); !present {
		return
	}

	if c.hookRuns[id] == nil {
		c.hookRuns[id] = map[string]HookRun{}
	}

	run := HookRun{Hook: hook, Result: *res}
	c.hookRuns[id][hook] = run
	_ = c.persist(ctx, c.store.SaveHookRun(ctx, id, &run))
}

// activeRollout returns the group's active rollout, if any.
func (c *Controller) activeRollout(group string) *Rollout {
	for _, r := range c.rollouts {
		if r.Group == group && r.State.Active() {
			return r
		}
	}

	return nil
}

// desiredKey identifies a set of image → digest choices.
func desiredKey(m map[string]string) string {
	parts := make([]string, 0, len(m))
	for img, d := range m {
		parts = append(parts, img+"="+d)
	}

	sort.Strings(parts)

	return strings.Join(parts, ",")
}

func sortedValues(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, m[k])
	}

	return out
}

func shortDigest(d string) string {
	d = strings.TrimPrefix(d, "sha256:")
	if len(d) > 7 {
		return d[:7]
	}

	if d == "" {
		return "none"
	}

	return d
}

func pct(part, whole float64) string {
	if whole <= 0 {
		return "0%"
	}

	return fmt.Sprintf("%.1f%%", 100*part/whole)
}
