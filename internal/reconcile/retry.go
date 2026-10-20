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

// updateDeadlineReason says why a target in its batch ran out of time, or "":
// its digest must appear within progressDeadline of the last dispatch, then be
// ready within progressDeadline of the later of first dispatch and first sighting.
func updateDeadlineReason(now time.Time, rt *RolloutTarget, st *config.Strategy, desired string) string {
	if rt.UpdatedAt.IsZero() {
		return ""
	}

	if rt.Updated {
		start := rt.UpdatedAt
		if rt.DigestSeenAt.After(start) {
			start = rt.DigestSeenAt
		}

		if now.Sub(start) < st.ProgressDeadline {
			return ""
		}

		return fmt.Sprintf("%s within %s", rt.Reason, st.ProgressDeadline)
	}

	if now.Sub(lastDispatch(rt)) < st.ProgressDeadline {
		return ""
	}

	why := fmt.Sprintf("did not reach %s within %s", shortDigest(desired), st.ProgressDeadline)
	if rt.UpdateError != "" {
		return updateFailure(rt, st) + "; " + why
	}

	return why
}

// lastDispatch is when the latest update was sent, or when the build was first
// seen on a target that needed none.
func lastDispatch(rt *RolloutTarget) time.Time {
	if rt.DispatchedAt.After(rt.UpdatedAt) {
		return rt.DispatchedAt
	}

	return rt.UpdatedAt
}

// recoverDispatch dates the last of several attempts stored without its time
// to now, so the watch runs a full deadline from this process, never shorter.
func recoverDispatch(now time.Time, rt *RolloutTarget) bool {
	if rt.DispatchedAt.IsZero() && rt.UpdateAttempts > 1 {
		rt.DispatchedAt = now

		return true
	}

	return false
}

// retriesOver reports whether a dispatched update may not be sent again: its
// attempts are spent or its first dispatch is a progress deadline old.
func retriesOver(now time.Time, rt *RolloutTarget, st *config.Strategy) bool {
	return rt.UpdateAttempts > 0 && (rt.UpdateAttempts >= max(st.Retry.Limit, 1) || now.Sub(rt.UpdatedAt) >= st.ProgressDeadline)
}

func watchReason(rt *RolloutTarget, st *config.Strategy) string {
	cause := fmt.Sprintf("no result from update attempt %d of %d", rt.UpdateAttempts, max(st.Retry.Limit, 1))
	if rt.UpdateError != "" {
		cause = updateFailure(rt, st)
	}

	stop := "progress deadline passed"
	if rt.UpdateAttempts >= max(st.Retry.Limit, 1) {
		stop = "retry limit reached"
	}

	return fmt.Sprintf("%s; %s, waiting up to %s after the last dispatch for a possibly accepted update", cause, stop, st.ProgressDeadline)
}

// watchUpdate keeps a target whose last allowed attempt may have been accepted
// in its batch without another attempt; it reports whether it did.
func watchUpdate(now time.Time, rt *RolloutTarget, st *config.Strategy) bool {
	if rt.UpdateDone || !retriesOver(now, rt, st) {
		return false
	}

	rt.Reason, rt.RetryAt = watchReason(rt, st), time.Time{}

	return true
}

// updateJob plans an update; durable dispatch starts its clock and counts the attempt.
func (c *Controller) updateJob(r *Rollout, rt *RolloutTarget) job {
	j := job{rollout: r.ID, target: rt.ID, hook: config.HookUpdate, retryAt: rt.RetryAt, batch: rt.Batch, attempts: rt.UpdateAttempts}
	rt.RetryAt = time.Time{}
	rt.Reason = "update running"

	return j
}

func (c *Controller) retryUpdate(ctx context.Context, now time.Time, r *Rollout, rt *RolloutTarget, why string) {
	// Inspect already sees the desired build, whatever the update reported and
	// however late: it is verified like any landed update, never retried or skipped.
	if live, known := c.liveKnown(rt.ID); known && live == r.Desired[rt.Image] {
		rt.UpdateDone, rt.RetryAt = true, time.Time{}
		rt.Reason = "update reported a failure, but the new build is running; waiting for inspect to confirm"

		return
	}

	st := c.strategy(r.Strategy)
	rt.UpdateError = why

	if deadline := updateDeadlineReason(now, rt, &st, r.Desired[rt.Image]); deadline != "" {
		c.leave(now, r, rt, PhaseSkipped, "update skipped: "+deadline)

		return
	}

	// A failure may be a lost response to an accepted update, so a target out
	// of attempts stays in its batch until that update could no longer land.
	if watchUpdate(now, rt, &st) {
		return
	}

	delay := retryBackoff(st.Retry.Backoff, rt.UpdateAttempts)
	rt.RetryAt = now.Add(delay)
	rt.Reason = fmt.Sprintf("%s; retrying in %s", updateFailure(rt, &st), delay)
}
