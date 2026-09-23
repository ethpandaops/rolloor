package ethpandaops

import (
	"fmt"
	"slices"
	"sync"
)

// health is one node's answer to a soak question: how far behind it is and
// how many peers it has.
type health struct {
	id    string
	lag   uint64
	peers uint64
	err   error
}

// probeAll asks fn about every target, a few at a time.
func probeAll(ts []Target, fn func(t *Target) health) []health {
	out := make([]health, len(ts))
	sem := make(chan struct{}, 8)

	var wg sync.WaitGroup

	for i := range ts {
		wg.Add(1)

		sem <- struct{}{}

		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			out[i] = fn(&ts[i])
			out[i].id = ts[i].ID
		}()
	}

	wg.Wait()

	return out
}

// compare passes when the updated nodes are no further behind than the
// remaining ones (or slack, whichever is more) and keep at least half the
// remaining ones' median peer count. A remaining node that does not answer
// is left out of the comparison; an updated one that does not answer fails.
func compare(unit string, slack uint64, updated, remaining []health) Result {
	for _, u := range updated {
		if u.err != nil {
			return fail("%s: %v", u.id, u.err)
		}
	}

	var rem []health

	for _, r := range remaining {
		if r.err == nil {
			rem = append(rem, r)
		}
	}

	uWorst, uWorstID := uint64(0), updated[0].id
	uMin, uMinID := updated[0].peers, updated[0].id

	for _, u := range updated {
		if u.lag > uWorst {
			uWorst, uWorstID = u.lag, u.id
		}

		if u.peers < uMin {
			uMin, uMinID = u.peers, u.id
		}
	}

	var rWorst uint64

	peers := make([]uint64, 0, len(rem))

	for _, r := range rem {
		rWorst = max(rWorst, r.lag)
		peers = append(peers, r.peers)
	}

	numbers := fmt.Sprintf("updated=%d remaining=%d unit=%s", uWorst, rWorst, unit)

	if allowed := max(rWorst, slack); uWorst > allowed {
		res := fail("%s is %d %s behind; the remaining nodes are at most %d", uWorstID, uWorst, unit, rWorst)
		res.Numbers = numbers

		return res
	}

	if len(rem) == 0 {
		if uMin == 0 {
			return Result{Reason: uMinID + " has no peers", Numbers: numbers}
		}

		return Result{OK: true, Reason: fmt.Sprintf("updated at most %d %s behind with at least %d peers; nothing left to compare against", uWorst, unit, uMin), Numbers: numbers}
	}

	slices.Sort(peers)
	median := peers[len(peers)/2]

	if uMin*2 < median {
		return Result{Reason: fmt.Sprintf("%s has %d peers; the remaining nodes' median is %d", uMinID, uMin, median), Numbers: numbers}
	}

	return Result{OK: true, Numbers: numbers, Reason: fmt.Sprintf("updated at most %d %s behind with at least %d peers; remaining at most %d behind, median %d peers",
		uWorst, unit, uMin, rWorst, median)}
}
