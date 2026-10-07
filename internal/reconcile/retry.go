package reconcile

import (
	"context"
	"fmt"
	"time"

	"github.com/ethpandaops/rolloor/internal/config"
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

// updateJob plans an update; durable dispatch starts its clock and counts the attempt.
func (c *Controller) updateJob(r *Rollout, rt *RolloutTarget) job {
	j := job{rollout: r.ID, target: rt.ID, hook: config.HookUpdate, retryAt: rt.RetryAt, batch: rt.Batch, attempts: rt.UpdateAttempts}
	rt.RetryAt = time.Time{}
	rt.Reason = "update running"

	return j
}

func (c *Controller) retryUpdate(ctx context.Context, now time.Time, r *Rollout, rt *RolloutTarget, why string) {
	st := c.strategy(r.Strategy)
	rt.UpdateError = why
	reason := updateFailure(rt, &st)

	if deadline := updateDeadlineReason(now, rt, &st, ""); deadline != "" {
		c.leave(now, r, rt, PhaseSkipped, "update skipped: "+deadline)

		return
	}

	if rt.UpdateAttempts >= max(st.Retry.Limit, 1) {
		c.leave(now, r, rt, PhaseSkipped, reason+"; target skipped after retry limit")

		return
	}

	delay := retryBackoff(st.Retry.Backoff, rt.UpdateAttempts)
	rt.RetryAt = now.Add(delay)
	rt.Reason = fmt.Sprintf("%s; retrying in %s", reason, delay)
}
