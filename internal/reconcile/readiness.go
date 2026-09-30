package reconcile

import (
	"context"
	"time"

	"github.com/ethpandaops/rolloor/internal/config"
)

// ProbeAll assesses every target independently of any rollout.
func (c *Controller) ProbeAll(ctx context.Context) {
	c.observeTargets(ctx, c.targets().Targets, config.HookReady)
	c.Nudge()
}

// RunProber observes readiness on its own period until ctx ends. It blocks.
func (c *Controller) RunProber(ctx context.Context) error {
	ticker := time.NewTicker(c.cfg.ReadinessProbe.Period)
	defer ticker.Stop()

	for {
		c.ProbeAll(ctx)

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

func (c *Controller) heldReady(r *Rollout, rt *RolloutTarget) bool {
	t, present := c.targets().Get(rt.ID)
	if !present {
		return false
	}

	l := c.live[rt.ID]
	digest, known := c.liveKnown(rt.ID)

	return known && digest == r.Desired[t.Image] && l.Readiness.Ready && l.Readiness.LastSuccessAt.After(l.DigestSince) && l.Readiness.LastSuccessAt.After(rt.UpdatedAt)
}
