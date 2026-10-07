package reconcile

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
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
	paused      bool
	groups      map[string]config.Group
	suspensions map[string]Suspension
	rollouts    map[string]*Rollout
	aborted     map[string]string
	hookRuns    map[string]map[string]HookRun

	lastResolve   time.Time
	lastTick      time.Time
	refreshWanted bool
	// dirty is set when a store write failed; no hook runs until every
	// decision has been written again.
	dirty bool
	// resolveMu serializes registry scans so an older scan cannot publish
	// over a newer one.
	resolveMu     sync.Mutex
	probeMu       sync.Mutex
	resolveErrors map[string]string
	targetsError  string
	nextEventID   int64
	pendingEvents []Event
	historyID     string
	configError   bool
	lastInspect   time.Time
	lastProbe     time.Time
	probes        probePacer
	// generations hold each observed target's current generation; forgetting
	// a target deletes its entry, so results begun before are refused.
	generations    map[string]generation
	lastGeneration uint64

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
		paused:        opts.Config.Paused,
		groups:        maps.Clone(opts.Config.Groups),
		suspensions:   map[string]Suspension{},
		rollouts:      map[string]*Rollout{},
		aborted:       map[string]string{},
		hookRuns:      map[string]map[string]HookRun{},
		resolveErrors: map[string]string{},
		generations:   map[string]generation{},
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
	c.flush(ctx)

	// Observations restored for a target whose definition changed while the
	// process was down describe a target that no longer exists.
	var stale []string

	set := c.targets()

	for id := range c.live {
		t, present := set.Get(id)
		if d := c.live[id].Definition; !present || d == "" || d != c.generationFor(&t).definition {
			stale = append(stale, id)
		}
	}

	c.Forget(ctx, stale)

	for _, r := range c.rollouts {
		if !r.State.Active() {
			c.clearRolloutQuarantine(ctx, r)
		}
	}

	return c, nil
}

func (c *Controller) restore(snap *Snapshot) {
	c.nextEventID = max(snap.NextEventID, 1)
	c.historyID = snap.HistoryID

	for _, r := range snap.Rollouts {
		c.rollouts[r.ID] = r
	}

	for _, s := range snap.Suspensions {
		c.suspensions[s.ID] = s
	}

	for id := range snap.Live {
		c.live[id] = snap.Live[id]
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
			c.pendingEvents = append(c.pendingEvents, Event{ID: c.nextEventID, At: c.clock.Now(),
				Actor: ControllerActor, Action: "rollout.aborted", Group: r.Group, Rollout: r.ID, Reason: r.Reason})
			c.nextEventID++
			c.dirty = true
		}
	}

	// A dispatched update might have landed without a response. Planning checks
	// its digest and remaining dispatch allowance before sending another attempt.
	for _, r := range c.rollouts {
		for i := range r.Targets {
			rt := &r.Targets[i]
			if rt.Phase == PhaseUpdating && !rt.UpdateDone && rt.RetryAt.IsZero() {
				rt.Phase, rt.Reason = PhasePending, "update interrupted by a restart; checking whether it landed"
			}
		}
	}
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

// Tick resolves tags, expires suspensions, opens rollouts and advances active
// operations from their observations.
func (c *Controller) Tick(ctx context.Context) error {
	now := c.clock.Now()

	c.mu.Lock()
	stored := c.flush(ctx)
	c.mu.Unlock()

	if !stored {
		return ctx.Err()
	}

	if err := c.resolveIfDue(ctx, now); err != nil {
		return err
	}

	c.mu.Lock()

	var jobs []job

	// Decisions the store refused are written again before any new ones are
	// made or any program runs on their behalf.
	if c.flush(ctx) {
		c.expireSuspensions(ctx, now)
		c.expirePauses(ctx, now)
		c.supersedeChangedRollouts(ctx, now)
		c.openRollouts(ctx, now)
		jobs = c.planRollouts(ctx, now)

		// A batch whose save failed must not move anything: after a restart
		// nobody would know its nodes are changing.
		if c.dirty {
			c.withdraw(jobs)
			jobs = nil
		}
	}

	c.mu.Unlock()

	results := c.execute(ctx, jobs)
	now = c.clock.Now()

	c.mu.Lock()
	c.applyResults(ctx, now, results)
	c.lastTick = now
	c.mu.Unlock()

	c.observeBatch(ctx)

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

	if err := c.writeOwed(ctx); err != nil {
		return err
	}

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
			c.recordDecision(ctx, now, &Decision{Desired: map[string]Desired{img: d}}, &Event{Actor: ControllerActor, Action: "digest.changed", Target: img,
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

func (c *Controller) expireSuspensions(ctx context.Context, now time.Time) {
	for id, s := range c.suspensions {
		if c.dirty {
			break
		}

		if !now.Before(s.ExpiresAt) {
			delete(c.suspensions, id)
			c.recordDecision(ctx, now, &Decision{DeleteSuspensions: []string{id}},
				&Event{Actor: ControllerActor, Action: "suspend.expired", Selector: s.Selector.String(), Reason: s.Reason})
		}
	}
}

// event queues history until its write succeeds; listeners see durable events.
func (c *Controller) event(ctx context.Context, now time.Time, e *Event) {
	e.ID, e.At = c.nextEventID, now
	c.nextEventID++

	if !c.dirty && len(c.pendingEvents) == 0 {
		if err := c.persist(ctx, c.store.AppendEvent(ctx, e)); err == nil {
			if c.notifier != nil {
				c.notifier.Publish(e)
			}

			return
		}
	}

	c.pendingEvents = append(c.pendingEvents, *e)
}

func (c *Controller) prepareEvents(now time.Time, events []*Event) []Event {
	out := make([]Event, len(events))
	for i, e := range events {
		e.ID, e.At = c.nextEventID+int64(i), now
		out[i] = *e
	}

	return out
}

func (c *Controller) recordDecision(ctx context.Context, now time.Time, d *Decision, events ...*Event) {
	d.Events = c.prepareEvents(now, events)
	c.nextEventID += int64(len(d.Events))

	if !c.dirty && len(c.pendingEvents) == 0 {
		if err := c.persist(ctx, c.store.SaveDecision(ctx, d)); err == nil {
			c.publishEvents(d.Events)

			return
		}
	}

	c.pendingEvents = append(c.pendingEvents, d.Events...)
}

func (c *Controller) publishEvents(events []Event) {
	if c.notifier != nil {
		for i := range events {
			c.notifier.Publish(&events[i])
		}
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

	_ = c.persist(ctx, c.store.ReplaceDecisions(ctx, c.decisions()))
	if !c.dirty {
		c.publishEvents(c.pendingEvents)
		c.pendingEvents = nil
	}

	if c.dirty {
		c.log.WithContext(ctx).Warn("store still failing; no programs run this tick")
	}

	return !c.dirty
}

// writeOwed writes the decisions the store refused earlier before a person's
// action adds new ones, so disk never holds a new decision without the older
// ones it rests on (a new rollout beside the unsaved end of the last one).
func (c *Controller) writeOwed(ctx context.Context) error {
	if !c.dirty {
		return nil
	}

	c.dirty = false

	if err := c.persist(ctx, c.store.ReplaceDecisions(ctx, c.decisions())); err != nil {
		return fmt.Errorf("earlier decisions are still not stored, so nothing was changed: %w", err)
	}

	c.publishEvents(c.pendingEvents)
	c.pendingEvents = nil

	return nil
}

// decisions is everything flush writes. Deletions are decisions too: a lifted
// quarantine or abort marker must not come back after a restart, so the
// tables are replaced whole.
func (c *Controller) decisions() *Snapshot {
	snap := &Snapshot{Degraded: c.degraded, Aborted: c.aborted, Desired: c.desired, Live: c.live, HookRuns: c.hookRuns, Events: c.pendingEvents}

	for _, r := range c.rollouts {
		snap.Rollouts = append(snap.Rollouts, r)
	}

	for _, sp := range c.suspensions {
		snap.Suspensions = append(snap.Suspensions, sp)
	}

	return snap
}

// commit applies change to r and saves it. When the store refuses, the
// rollout is put back so memory never runs ahead of disk.
func (c *Controller) commit(ctx context.Context, r *Rollout, change func(), events ...*Event) error {
	return c.commitDecision(ctx, r, &Decision{}, change, events...)
}

func (c *Controller) commitDecision(ctx context.Context, r *Rollout, d *Decision, change func(), events ...*Event) error {
	before := cloneRollout(r)

	change()

	d.Rollouts, d.Events = []*Rollout{r}, c.prepareEvents(c.clock.Now(), events)
	if err := c.persist(ctx, c.store.SaveDecision(ctx, d)); err != nil {
		*r = *before

		return err
	}

	c.nextEventID += int64(len(d.Events))
	c.publishEvents(d.Events)

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

	if t, present := c.targets().Get(id); present {
		c.setLiveLocked(ctx, now, &observation{id: id, started: observedAt, generation: c.generationFor(&t)}, digest, ok, reason)
	}
}

// setLiveLocked applies an observation unless a newer one is already recorded.
func (c *Controller) setLiveLocked(ctx context.Context, now time.Time, o *observation, digest string, ok bool, reason string) bool {
	l := c.live[o.id]
	if o.started.Before(l.ObservedAt) {
		return false
	}

	if ok {
		since := l.DigestSince
		if l.Digest != digest || since.IsZero() {
			since = now
		}

		l = Live{Digest: digest, SeenAt: now, DigestSince: since, Readiness: l.Readiness}
	} else {
		l.Failures++
		l.Reason = reason
	}

	l.ObservedAt, l.Definition = o.started, o.generation.definition
	c.live[o.id] = l
	_ = c.persist(ctx, c.store.SaveLive(ctx, o.id, &l))

	return true
}

// InspectAll runs the inspect hook for every target now and records the result.
func (c *Controller) InspectAll(ctx context.Context) {
	c.observeTargets(ctx, c.targets().Targets, config.HookInspect, false)
}

// observation is one inspect or readiness run as its worker began it: the
// target's program and input then, and the generation its result must match.
type observation struct {
	id         string
	program    string
	input      HookInput
	started    time.Time
	generation generation
}

// generation is a target's observations since it was last forgotten, with a
// stamp of the definition they describe.
type generation struct {
	n          uint64
	definition string
}

// observeTargets runs hook for the targets ts names, each with its definition
// and input as its worker starts; a result is refused once the target is
// forgotten. A paced pass skips a target the hook began observing too recently.
func (c *Controller) observeTargets(ctx context.Context, ts []targets.Target, hook string, paced bool) {
	if hook == config.HookReady {
		c.probeMu.Lock()
		defer c.probeMu.Unlock()
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(c.cfg.Inspect.Concurrency)

	for i := range ts {
		id := ts[i].ID

		g.Go(func() error {
			o, run := c.beginObservation(hook, id, paced)
			if !run {
				return nil
			}

			res, err := c.runner.Run(gctx, o.program, hook, id, o.input)

			if hook == config.HookReady && gctx.Err() != nil {
				return nil
			}

			if err != nil {
				res = hooks.Result{Program: o.program, Reason: err.Error(), ExitCode: -1, RanAt: c.clock.Now()}
			}

			if res.CouldNotCheck() {
				res.Reason = "could not check: " + res.Reason
			}

			c.mu.Lock()
			defer c.mu.Unlock()

			if _, present := c.targets().Get(id); !present || c.generations[id].n != o.generation.n {
				return nil
			}

			accepted := false

			if hook == config.HookInspect {
				accepted = c.setLiveLocked(gctx, c.clock.Now(), &o, strings.TrimSpace(res.Reason), res.OK, res.Reason)
			} else {
				accepted = c.setReadinessLocked(gctx, &o, res.OK, res.Reason)
			}

			if accepted {
				c.recordHookRun(gctx, id, hook, &res)
			}

			return nil
		})
	}

	_ = g.Wait()

	c.mu.Lock()

	if hook == config.HookInspect {
		c.lastInspect = c.clock.Now()
	} else {
		c.lastProbe = c.clock.Now()
	}

	c.mu.Unlock()
}

// beginObservation decides as a worker starts whether its target is still
// listed and due, taking the input from what is known at that moment.
func (c *Controller) beginObservation(hook, id string, paced bool) (observation, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	t, present := c.targets().Get(id)
	started := c.clock.Now()

	if !present || (paced && !c.probes.due(hook, id, started)) {
		return observation{}, false
	}

	c.probes.begin(hook, id, started)

	return observation{
		id: id, program: c.programFor(&t, hook), input: c.hookInput(&t), started: started, generation: c.generationFor(&t),
	}, true
}

// generationFor returns the target's generation, opening one stamped with
// its definition if it has none.
func (c *Controller) generationFor(t *targets.Target) generation {
	g, ok := c.generations[t.ID]
	if !ok {
		c.lastGeneration++
		g = generation{n: c.lastGeneration, definition: c.definition(t)}
		c.generations[t.ID] = g
	}

	return g
}

// definition stamps hook inputs and program selection across process restarts.
func (c *Controller) definition(t *targets.Target) string {
	raw, err := json.Marshal(struct {
		Target      *targets.Target
		Environment string
		Dir         string
		Programs    [4]string
	}{t, c.cfg.Environment, c.cfg.Hooks.Dir, [4]string{
		c.programFor(t, config.HookInspect), c.programFor(t, config.HookReady),
		c.programFor(t, config.HookUpdate), c.programFor(t, config.HookSoak),
	}})
	if err != nil {
		return ""
	}

	sum := sha256.Sum256(raw)

	return hex.EncodeToString(sum[:])
}

// observeBatch looks again at targets waiting on an update between the
// background passes, with each hook paced per target like those passes.
func (c *Controller) observeBatch(ctx context.Context) {
	set := c.targets()
	c.mu.RLock()

	now := c.clock.Now()

	var inspect, ready []targets.Target

	seen := map[string]struct{}{}

	for _, r := range c.rollouts {
		b := r.CurrentBatch()
		if r.State != Running || b == nil {
			continue
		}

		for _, id := range b.Targets {
			if r.target(id).Phase != PhaseUpdating {
				continue
			}

			t, present := set.Get(id)
			_, duplicate := seen[id]
			seen[id] = struct{}{}

			if present && !duplicate && c.probes.due(config.HookInspect, id, now) {
				inspect = append(inspect, t)
			}

			if present && !duplicate && c.probes.due(config.HookReady, id, now) {
				ready = append(ready, t)
			}
		}
	}

	c.mu.RUnlock()

	if len(inspect) > 0 {
		c.observeTargets(ctx, inspect, config.HookInspect, true)
	}

	if len(ready) > 0 {
		c.observeTargets(ctx, ready, config.HookReady, true)
	}
}

// RunInspector observes every target on the inspect interval until ctx ends.
// It blocks. The first pass runs before it returns control to the ticker; a
// target observed within earlyProbeSpacing waits for a later pass.
func (c *Controller) RunInspector(ctx context.Context) error {
	ticker := time.NewTicker(c.cfg.Inspect.Interval)
	defer ticker.Stop()

	for {
		c.observeTargets(ctx, c.targets().Targets, config.HookInspect, true)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Forget drops everything observed about targets, in memory and the store,
// and refuses their results still in flight, so each reads unobserved and
// unavailable until observed again.
func (c *Controller) Forget(ctx context.Context, ids []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, id := range ids {
		c.forgetLocked(ctx, id)
	}
}

// ReplaceTargets runs publish, which makes Options.Targets return the new set,
// and forgets every target that left or changed under the same lock, so no
// reader pairs a new definition with observations of the old one.
func (c *Controller) ReplaceTargets(ctx context.Context, publish func() (old, current *targets.Set)) {
	c.mu.Lock()
	defer c.mu.Unlock()

	old, current := publish()

	for i := range old.Targets {
		t := &old.Targets[i]
		if kept, present := current.Get(t.ID); !present || !reflect.DeepEqual(*t, kept) {
			c.forgetLocked(ctx, t.ID)
		}
	}
}

func (c *Controller) forgetLocked(ctx context.Context, id string) {
	delete(c.live, id)
	delete(c.degraded, id)
	delete(c.hookRuns, id)
	delete(c.generations, id)

	for _, hook := range []string{config.HookInspect, config.HookReady} {
		key := probeKey{hook: hook, target: id}
		delete(c.probes.began, key)
		delete(c.probes.kicked, key)
	}

	_ = c.persist(ctx, c.store.SaveDecision(ctx, &Decision{DeleteTargets: []string{id}}))
}

// HookInput is what target hooks receive on stdin: the target plus the
// digest it should be running, so update can deploy exactly that.
type HookInput struct {
	targets.Target

	Desired    string `json:"desired,omitempty"`
	DesiredRef string `json:"desiredRef,omitempty"`
	// DigestSince is when the target was first seen running the digest it
	// runs now, so a program can tell a fresh restart from steady state.
	DigestSince time.Time `json:"digestSince,omitzero"`
}

func (c *Controller) hookInput(t *targets.Target) HookInput {
	in := HookInput{Target: *t, DigestSince: c.live[t.ID].DigestSince}

	if d, ok := c.desiredFor(t); ok && d.Digest != "" {
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
	if !ok || l.SeenAt.IsZero() || l.Failures >= c.cfg.Inspect.FailureThreshold {
		return "", false
	}

	return l.Digest, true
}

// desiredFor is the current head digest of a target's image tag.
func (c *Controller) desiredFor(t *targets.Target) (Desired, bool) {
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

// strategy is a named strategy, or the default when the name is empty or no
// longer configured.
func (c *Controller) strategy(name string) config.Strategy {
	if st, ok := c.cfg.StrategyNamed(name); ok {
		return st
	}

	return c.cfg.Strategy
}

func (c *Controller) programFor(t *targets.Target, hook string) string {
	if p, ok := t.Hooks[hook]; ok && p != "" {
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
