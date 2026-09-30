package reconcile

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/targets"
)

// propAction is one kind of step and how often it is taken.
type propAction struct {
	weight int
	run    func(s *sim) string
}

var propActions = []propAction{
	{36, (*sim).tick},
	{6, (*sim).inspect},
	{6, (*sim).release},
	{5, (*sim).updateFail},
	{3, func(s *sim) string {
		return s.toggle("updateStuck", func(w *world) map[string]bool { return w.updateStuck })
	}},
	{4, func(s *sim) string { return s.toggle("notReady", func(w *world) map[string]bool { return w.notReady }) }},
	{2, func(s *sim) string {
		return s.toggle("inspectFail", func(w *world) map[string]bool { return w.inspectFail })
	}},
	{4, (*sim).soakFail},
	{2, (*sim).programCrash},
	{2, (*sim).registryDown},
	{6, (*sim).retry},
	{2, (*sim).abort},
	{3, (*sim).pause},
	{3, (*sim).promote},
	{3, (*sim).sync},
	{2, (*sim).suspend},
	{2, (*sim).resume},
	{2, (*sim).policy},
	{2, (*sim).edit},
	{2, (*sim).restart},
	{1, (*sim).crash},
	{2, (*sim).storeOutage},
}

func (s *sim) act() string {
	total := 0
	for _, a := range propActions {
		total += a.weight
	}

	n := s.rng.IntN(total)

	for _, a := range propActions {
		if n < a.weight {
			return a.run(s)
		}

		n -= a.weight
	}

	return ""
}

func (s *sim) randomTarget() propTarget {
	return s.fleet.targets[s.rng.IntN(len(s.fleet.targets))]
}

func (s *sim) randomGroup() string {
	return propGroups[s.rng.IntN(len(propGroups))]
}

func (s *sim) tick() string {
	d := []time.Duration{0, 5 * time.Second, 20 * time.Second, 60 * time.Second}[s.rng.IntN(4)]
	s.h.clock.Advance(d)

	if s.down {
		return fmt.Sprintf("tick +%s (no process)", d)
	}

	s.h.tick()

	return fmt.Sprintf("tick +%s", d)
}

func (s *sim) inspect() string {
	if s.down {
		return "inspect (no process)"
	}

	s.h.c.InspectAll(s.h.ctx)

	return "inspect"
}

func (s *sim) release() string {
	g := s.randomGroup()
	s.digests++
	digest := fmt.Sprintf("sha256:%04x%060x", s.digests, s.digests)
	s.builds[g] = append(s.builds[g], digest)

	s.h.world.set(func(w *world) { w.registry[propImage(g)] = digest })

	return "release " + g + " " + shortDigest(digest)
}

// toggle flips one target's entry in one of the world's failure sets.
func (s *sim) toggle(name string, set func(w *world) map[string]bool) string {
	id := s.randomTarget().id
	on := s.rng.IntN(2) == 0

	s.h.world.set(func(w *world) {
		m := set(w)
		if on {
			m[id] = true
		} else {
			delete(m, id)
		}
	})

	return fmt.Sprintf("%s %s=%v", name, id, on)
}

func (s *sim) updateFail() string {
	id := s.randomTarget().id
	on := s.rng.IntN(2) == 0

	s.h.world.set(func(w *world) {
		if on {
			w.updateFail[id] = boom
		} else {
			delete(w.updateFail, id)
		}
	})

	return fmt.Sprintf("updateFail %s=%v", id, on)
}

func (s *sim) soakFail() string {
	prog := "soak-" + s.randomGroup()
	on := s.rng.IntN(2) == 0

	s.h.world.set(func(w *world) { w.soakFail[prog] = on })

	return fmt.Sprintf("soakFail %s=%v", prog, on)
}

// programCrash makes one program fail to run at all, or run again.
func (s *sim) programCrash() string {
	prog := []string{config.HookUpdate, config.HookReady, config.HookInspect, "soak-" + s.randomGroup()}[s.rng.IntN(4)]
	on := s.rng.IntN(2) == 0

	s.h.world.set(func(w *world) {
		if on {
			w.runErr[prog] = errors.New("exec: program crashed")
		} else {
			delete(w.runErr, prog)
		}
	})

	return fmt.Sprintf("programCrash %s=%v", prog, on)
}

func (s *sim) registryDown() string {
	img := propImage(s.randomGroup())
	on := s.rng.IntN(2) == 0

	s.h.world.set(func(w *world) {
		if on {
			w.registryErr[img] = errors.New("registry unavailable")
		} else {
			delete(w.registryErr, img)
		}
	})

	return fmt.Sprintf("registryDown %s=%v", img, on)
}

// pick returns a random rollout in one of the states, if any.
func (s *sim) pick(states ...RolloutState) (RolloutView, bool) {
	var found []RolloutView

	for _, r := range s.h.c.Rollouts() {
		if slices.Contains(states, r.State) {
			found = append(found, r)
		}
	}

	if len(found) == 0 {
		return RolloutView{}, false
	}

	return found[s.rng.IntN(len(found))], true
}

var activeStates = []RolloutState{WaitingForSync, Running, Soaking, Paused, WaitingForBudget, Halted}

func (s *sim) retry() string {
	if s.down {
		return "retry (no process)"
	}

	r, ok := s.pick(Halted)
	if !ok {
		return "retry: nothing halted"
	}

	return fmt.Sprintf("retry %s: %v", r.ID, s.h.c.Retry(s.h.ctx, actor, r.ID, "try again"))
}

func (s *sim) abort() string {
	if s.down {
		return "abort (no process)"
	}

	r, ok := s.pick(activeStates...)
	if !ok {
		return "abort: nothing active"
	}

	return fmt.Sprintf("abort %s: %v", r.ID, s.h.c.Abort(s.h.ctx, actor, r.ID))
}

func (s *sim) pause() string {
	if s.down {
		return "pause (no process)"
	}

	r, ok := s.pick(activeStates...)
	if !ok {
		return "pause: nothing active"
	}

	return fmt.Sprintf("pause %s: %v", r.ID, s.h.c.Pause(s.h.ctx, actor, r.ID))
}

func (s *sim) promote() string {
	if s.down {
		return "promote (no process)"
	}

	r, ok := s.pick(activeStates...)
	if !ok {
		return "promote: nothing active"
	}

	return fmt.Sprintf("promote %s: %v", r.ID, s.h.c.Promote(s.h.ctx, actor, r.ID))
}

func (s *sim) sync() string {
	if s.down {
		return "sync (no process)"
	}

	g := s.randomGroup()
	req := SyncRequest{Actor: actor, Selector: targets.Selector{groupLabel: g}, Force: s.rng.IntN(3) == 0}

	if s.rng.IntN(4) == 0 {
		req.Strategy = []string{careful, "flat"}[s.rng.IntN(2)]
	}

	_, err := s.h.c.Sync(s.h.ctx, req)

	return fmt.Sprintf("sync %s force=%v strategy=%q: %v", g, req.Force, req.Strategy, err)
}

func (s *sim) suspend() string {
	if s.down {
		return "suspend (no process)"
	}

	sel := targets.Selector{targets.KeyID: s.randomTarget().id}
	if s.rng.IntN(3) == 0 {
		sel = targets.Selector{groupLabel: s.randomGroup()}
	}

	expires := []time.Duration{2 * time.Minute, time.Hour}[s.rng.IntN(2)]

	sp, err := s.h.c.Suspend(s.h.ctx, SuspendRequest{Actor: actor, Selector: sel, Reason: "hold", Expires: expires})
	if err == nil {
		s.suspended = append(s.suspended, propSuspension{sel: sel, expires: sp.ExpiresAt})
	}

	return fmt.Sprintf("suspend %s for %s: %v", sel, expires, err)
}

func (s *sim) resume() string {
	if s.down {
		return "resume (no process)"
	}

	if len(s.suspended) == 0 {
		return "resume: nothing suspended"
	}

	sel := s.suspended[s.rng.IntN(len(s.suspended))].sel
	s.lift(sel)

	return "resume " + sel.String()
}

// lift resumes a selector and forgets the suspensions the controller lifted.
func (s *sim) lift(sel targets.Selector) {
	if _, err := s.h.c.Resume(s.h.ctx, actor, sel); err != nil {
		return
	}

	s.suspended = slices.DeleteFunc(s.suspended, func(sp propSuspension) bool { return sp.sel.String() == sel.String() })
}

func (s *sim) policy() string {
	if s.down {
		return "policy (no process)"
	}

	g := s.randomGroup()
	p := Policy{Mode: []string{ModeAutomated, ModeManual}[s.rng.IntN(2)]}

	if builds := s.builds[g]; len(builds) > 0 && s.rng.IntN(3) == 0 {
		p.Pins = map[string]string{propImage(g): builds[s.rng.IntN(len(builds))]}
	}

	err := s.h.c.SetPolicy(s.h.ctx, actor, g, p)
	if err == nil {
		s.modes[g], s.pins[g] = p.Mode, p.Pins
	}

	return fmt.Sprintf("policy %s mode=%s pins=%v: %v", g, p.Mode, p.Pins, err)
}

func (s *sim) edit() string {
	what := s.fleet.edit(s.rng)
	if what == "" {
		return "edit: nothing"
	}

	s.edited = true

	old := s.h.current()

	set, err := targets.Parse([]byte(s.fleet.yaml()), &testRules)
	if err != nil {
		s.violation("the simulated targets file is invalid: %v", err)

		return what
	}

	s.h.swap(set)

	var gone []string

	for i := range old.Targets {
		if _, ok := set.Get(old.Targets[i].ID); !ok {
			gone = append(gone, old.Targets[i].ID)
		}
	}

	if len(gone) > 0 && !s.down {
		s.h.c.Forget(s.h.ctx, gone)
	}

	return "edit: " + what
}

// restart stops the process between ticks and starts another. Whatever the
// old one decided must come back, unless a store write was still owed.
func (s *sim) restart() string {
	if s.down {
		return "restart (no process)"
	}

	old := s.h.c
	before := old.Rollouts()

	old.mu.RLock()
	owed := old.dirty
	old.mu.RUnlock()

	s.start()

	if s.down {
		return "restart: the store refused to load"
	}

	if owed {
		return "restart with writes owed"
	}

	for _, r := range before {
		now, ok := s.h.c.Rollout(r.ID)
		if !ok || now.State != r.State || len(now.Batches) != len(r.Batches) {
			s.violation("rollout %s was %s with %d batches before a restart and %s with %d after",
				r.ID, r.State, len(r.Batches), now.State, len(now.Batches))
		}
	}

	return "restart"
}

// crash kills the process partway through a tick, after a random number of
// store writes, and starts another from what reached the disk.
func (s *sim) crash() string {
	if s.down {
		return "crash (no process)"
	}

	writes := s.rng.IntN(12)

	s.proc.mu.Lock()
	s.proc.writes = writes
	s.proc.mu.Unlock()

	s.h.clock.Advance(20 * time.Second)
	s.h.tick()
	s.start()

	return fmt.Sprintf("crash after %d writes", writes)
}

// storeOutage starts or ends a spell of every store call failing. A process
// that could not start during one starts when it ends.
func (s *sim) storeOutage() string {
	s.outage = !s.outage
	s.setOutage(s.outage)

	if !s.outage && s.down {
		s.start()
	}

	return fmt.Sprintf("storeOutage=%v", s.outage)
}

func (s *sim) setOutage(on bool) {
	disk := s.h.store

	disk.mu.Lock()
	defer disk.mu.Unlock()

	disk.Fail = nil
	if on {
		disk.Fail = errDiskFull
	}
}
