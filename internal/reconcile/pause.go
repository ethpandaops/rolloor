package reconcile

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/ethpandaops/rolloor/internal/config"
)

// ConfigureGroups applies declarative pause and strategy choices without
// changing the strategy of an operation already in progress.
func (c *Controller) ConfigureGroups(ctx context.Context, paused bool, groups map[string]config.Group) error {
	if err := c.cfg.ValidateGroups(groups); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.paused == paused && maps.Equal(c.groups, groups) {
		return nil
	}

	c.paused, c.groups = paused, maps.Clone(groups)
	c.event(ctx, c.clock.Now(), &Event{Actor: ControllerActor, Action: "config.changed",
		Reason: fmt.Sprintf("paused=%t, %d group overrides", paused, len(groups))})
	c.Nudge()

	return nil
}

// GroupPaused reports whether config disables updates for this group.
func (c *Controller) GroupPaused(group string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.groupPaused(group)
}

func (c *Controller) groupPaused(group string) bool {
	return c.paused || c.groups[group].Paused
}

func (c *Controller) configPauseReason(group string) string {
	if c.paused {
		return "Paused by config: paused is true; observation continues."
	}

	return "Paused by config: groups." + group + ".paused is true; observation continues."
}

func pauseReason(r *Rollout) string {
	return "Paused until " + r.PauseExpiresAt.UTC().Format(time.RFC3339) + "; resumes automatically."
}

func (c *Controller) expirePauses(ctx context.Context, now time.Time) {
	for _, r := range c.rollouts {
		if c.dirty {
			break
		}

		if !r.State.Active() || r.PauseExpiresAt.IsZero() || now.Before(r.PauseExpiresAt) {
			continue
		}

		r.PausePending = false
		r.PauseExpiresAt = time.Time{}
		r.UpdatedAt = now

		if r.State == Paused {
			r.State, r.Reason = Running, "Operator pause expired; resuming automatically."
		}

		c.recordDecision(ctx, now, &Decision{Rollouts: []*Rollout{r}}, &Event{Actor: ControllerActor, Action: "pause.expired", Group: r.Group, Rollout: r.ID})
	}
}
