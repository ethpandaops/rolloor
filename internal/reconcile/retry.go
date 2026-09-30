package reconcile

import (
	"context"
	"fmt"
	"time"

	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/targets"
)

func retryBackoff(b config.Backoff, attempt int) time.Duration {
	delay := min(b.Duration, b.MaxDuration)
	if b.Factor == 1 {
		return delay
	}

	factor := int64(b.Factor)

	for i := 1; i < attempt && delay < b.MaxDuration; i++ {
		if int64(delay) > int64(b.MaxDuration)/factor {
			return b.MaxDuration
		}

		delay = time.Duration(int64(delay) * factor)
	}

	return delay
}

func updateFailure(rt *RolloutTarget, st *config.Strategy) string {
	return fmt.Sprintf("update failed (attempt %d of %d): %s", rt.UpdateAttempts, max(st.Retry.Limit, 1), rt.UpdateError)
}

func updateDeadlineReason(now time.Time, rt *RolloutTarget, st *config.Strategy, desired string) string {
	if rt.UpdatedAt.IsZero() || now.Sub(rt.UpdatedAt) < st.ProgressDeadline {
		return ""
	}

	if rt.UpdateError != "" {
		return updateFailure(rt, st) + "; progress deadline exceeded after " + st.ProgressDeadline.String()
	}

	if rt.Updated {
		return fmt.Sprintf("%s within %s", rt.Reason, st.ProgressDeadline)
	}

	return fmt.Sprintf("did not reach %s within %s", shortDigest(desired), st.ProgressDeadline)
}

func (c *Controller) updateJob(now time.Time, r *Rollout, rt *RolloutTarget, set *targets.Set, t *targets.Target) job {
	j := job{rollout: r.ID, target: rt.ID, hook: config.HookUpdate, program: c.programFor(t, config.HookUpdate), input: c.hookInput(set, t), retryAt: rt.RetryAt}
	if rt.UpdateAttempts == 0 {
		rt.UpdatedAt = now
	}

	rt.UpdateAttempts++
	rt.RetryAt = time.Time{}
	rt.Reason = "update running"

	return j
}

func (c *Controller) retryUpdate(ctx context.Context, now time.Time, r *Rollout, rt *RolloutTarget, why string) {
	st := c.strategy(r.Strategy)
	rt.UpdateError = why
	reason := updateFailure(rt, &st)

	if deadline := updateDeadlineReason(now, rt, &st, ""); deadline != "" {
		c.halt(ctx, now, r, rt.ID, deadline)

		return
	}

	if rt.UpdateAttempts >= max(st.Retry.Limit, 1) {
		c.halt(ctx, now, r, rt.ID, reason+"; retry limit reached")

		return
	}

	delay := retryBackoff(st.Retry.Backoff, rt.UpdateAttempts)
	rt.RetryAt = now.Add(delay)
	rt.Reason = fmt.Sprintf("%s; retrying in %s", reason, delay)
}
