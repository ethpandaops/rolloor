package reconcile

import (
	"context"
	"time"

	"github.com/ethpandaops/rolloor/internal/config"
)

// earlyProbeSpacing is the least time between two observations of a target
// by one hook that the background loops or a rollout batch may begin.
const earlyProbeSpacing = 10 * time.Second

type probeKey struct {
	hook   string
	target string
}

// probePacer remembers when each hook last began observing each target. It
// is guarded by the controller's mu and ready to use at its zero value.
type probePacer struct {
	began  map[probeKey]time.Time
	kicked map[probeKey]bool
}

// due reports whether a paced observation may begin now: the spacing has
// passed since the last one of any kind, or an update has since succeeded.
func (p *probePacer) due(hook, id string, now time.Time) bool {
	k := probeKey{hook: hook, target: id}
	last, seen := p.began[k]

	return p.kicked[k] || !seen || now.Sub(last) >= earlyProbeSpacing
}

// begin records an observation starting now, which uses up any kick.
func (p *probePacer) begin(hook, id string, now time.Time) {
	if p.began == nil {
		p.began = map[probeKey]time.Time{}
	}

	k := probeKey{hook: hook, target: id}
	p.began[k] = now
	delete(p.kicked, k)
}

// kick lets the next inspect and readiness observation of a target begin
// without waiting for the spacing, once each.
func (p *probePacer) kick(id string) {
	if p.kicked == nil {
		p.kicked = map[probeKey]bool{}
	}

	p.kicked[probeKey{hook: config.HookInspect, target: id}] = true
	p.kicked[probeKey{hook: config.HookReady, target: id}] = true
}

// ProbeAll assesses every target now, independently of any rollout.
func (c *Controller) ProbeAll(ctx context.Context) {
	c.observeTargets(ctx, c.targets().Targets, config.HookReady, false)
	c.Nudge()
}

// RunProber observes readiness on its own period until ctx ends. It blocks.
// A target observed within earlyProbeSpacing waits for a later pass.
func (c *Controller) RunProber(ctx context.Context) error {
	ticker := time.NewTicker(c.cfg.ReadinessProbe.Period)
	defer ticker.Stop()

	for {
		c.observeTargets(ctx, c.targets().Targets, config.HookReady, true)
		c.Nudge()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *Controller) setReadinessLocked(ctx context.Context, id string, started time.Time, ok bool, reason string) bool {
	if _, present := c.targets().Get(id); !present {
		return false
	}

	l := c.live[id]
	r := &l.Readiness

	if started.Before(r.ObservedAt) {
		return false
	}

	now := c.clock.Now()
	wasReady := r.Ready

	if ok {
		r.Failures = 0
		r.Successes++
		r.LastSuccessAt = started

		if r.Successes >= c.cfg.ReadinessProbe.SuccessThreshold {
			r.Ready = true
		}
	} else {
		r.Successes = 0
		r.Failures++

		if r.Failures >= c.cfg.ReadinessProbe.FailureThreshold {
			r.Ready = false
		}
	}

	if r.ProbedAt.IsZero() || wasReady != r.Ready {
		r.Since = now
	}

	r.ObservedAt, r.ProbedAt, r.Reason = started, now, reason
	c.live[id] = l
	_ = c.persist(ctx, c.store.SaveLive(ctx, id, &l))

	return true
}

func (c *Controller) targetReady(id string) bool {
	r := c.live[id].Readiness

	return !r.ProbedAt.IsZero() && r.Ready
}

func (c *Controller) readyOnBuild(rt *RolloutTarget) bool {
	r := c.live[rt.ID].Readiness

	return rt.Updated && !rt.DigestSeenAt.IsZero() && r.Ready && r.LastSuccessAt.After(rt.DigestSeenAt)
}

// heldReady reports whether a target is seen ready on its rollout's build at
// the node and image it was admitted with, after its update was planned.
func (c *Controller) heldReady(r *Rollout, rt *RolloutTarget) bool {
	t, present := c.targets().Get(rt.ID)
	if !present {
		return false
	}

	l := c.live[rt.ID]
	digest, known := c.liveKnown(rt.ID)

	return known && t.Node == rt.Node && t.Image == rt.Image && digest == r.Desired[rt.Image] && l.Readiness.Ready && l.Readiness.LastSuccessAt.After(l.DigestSince) && l.Readiness.LastSuccessAt.After(rt.UpdatedAt)
}
