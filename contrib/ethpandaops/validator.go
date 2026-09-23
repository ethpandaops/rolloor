package ethpandaops

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// indices samples up to n validator indices, evenly spread, from a
// "start-end" range with end exclusive.
func indices(r string, n int) ([]string, error) {
	if r == "" {
		return nil, nil
	}

	a, b, ok := strings.Cut(r, "-")
	if !ok {
		return nil, fmt.Errorf("validator range %q is not start-end", r)
	}

	start, err := strconv.ParseUint(strings.TrimSpace(a), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("validator range %q: %w", r, err)
	}

	end, err := strconv.ParseUint(strings.TrimSpace(b), 10, 64)
	if err != nil || end < start {
		return nil, fmt.Errorf("validator range %q is not start-end", r)
	}

	size := end - start
	count := min(size, uint64(n)) //nolint:gosec // n is a positive setting

	out := make([]string, 0, count)
	for i := range count {
		out = append(out, strconv.FormatUint(start+i*size/count, 10))
	}

	return out, nil
}

// sampleAll gathers the sampled indices of every target.
func (h *Hooks) sampleAll(ts []Target) ([]string, error) {
	var out []string

	for i := range ts {
		idx, err := indices(ts[i].Extra.Validators, h.Sample)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", ts[i].ID, err)
		}

		out = append(out, idx...)
	}

	return out, nil
}

// timelyTarget asks the reference beacon node for the attestation rewards of
// some validators in an epoch and returns the fraction that earned the
// target reward, and how many were counted.
func (h *Hooks) timelyTarget(ctx context.Context, epoch uint64, idx []string) (rate float64, counted int, err error) {
	var out struct {
		Data struct {
			TotalRewards []struct {
				Target string `json:"target"`
			} `json:"total_rewards"`
		} `json:"data"`
	}

	url := fmt.Sprintf("%s/eth/v1/beacon/rewards/attestations/%d", h.Beacon, epoch)
	if _, _, err := h.call(ctx, http.MethodPost, url, idx, h.nodeAuth, &out); err != nil {
		return 0, 0, err
	}

	hit := 0

	for _, r := range out.Data.TotalRewards {
		if v, perr := strconv.ParseInt(r.Target, 10, 64); perr == nil && v > 0 {
			hit++
		}
	}

	counted = len(out.Data.TotalRewards)
	if counted == 0 {
		return 0, 0, nil
	}

	return float64(hit) / float64(counted), counted, nil
}

// SoakValidator compares the share of validators on the updated nodes that
// attested to the right target, two epochs back, against the remaining
// nodes' share. With nothing remaining, the updated share must reach the
// floor.
func SoakValidator(ctx context.Context, h *Hooks, stdin []byte) (Result, error) {
	in, err := decodeSoak(stdin)
	if err != nil {
		return Result{}, err
	}

	updated, err := h.sampleAll(in.Updated)
	if err != nil {
		return Result{}, err
	}

	if len(updated) == 0 {
		return pass("no validators on the updated targets"), nil
	}

	remaining, err := h.sampleAll(in.Remaining)
	if err != nil {
		return Result{}, err
	}

	n, err := h.network(ctx)
	if err != nil {
		return fail("reference beacon node: %v", err), nil
	}

	if n.epoch < 2 {
		return fail("the chain is younger than two epochs"), nil
	}

	epoch := n.epoch - 2

	uRate, uCount, err := h.timelyTarget(ctx, epoch, updated)
	if err != nil {
		return fail("rewards for the updated validators: %v", err), nil
	}

	if uCount == 0 {
		return fail("no rewards reported for the updated validators in epoch %d", epoch), nil
	}

	want, against := h.Floor, fmt.Sprintf("the floor of %.1f%%", 100*h.Floor)
	numbers := fmt.Sprintf("updated=%.1f remaining=- unit=%%", 100*uRate)

	if len(remaining) > 0 {
		rRate, rCount, rerr := h.timelyTarget(ctx, epoch, remaining)
		if rerr != nil {
			return fail("rewards for the remaining validators: %v", rerr), nil
		}

		if rCount > 0 {
			want = rRate - h.Tolerance
			against = fmt.Sprintf("%.1f%% on the remaining nodes", 100*rRate)
			numbers = fmt.Sprintf("updated=%.1f remaining=%.1f unit=%%", 100*uRate, 100*rRate)
		}
	}

	res := Result{OK: uRate >= want, Numbers: numbers,
		Reason: fmt.Sprintf("%.1f%% of %d updated validators hit the target in epoch %d, against %s", 100*uRate, uCount, epoch, against)}

	return res, nil
}
