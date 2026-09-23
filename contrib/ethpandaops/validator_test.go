package ethpandaops

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndices(t *testing.T) {
	got, err := indices("100-110", 5)
	require.NoError(t, err)
	require.Equal(t, []string{"100", "102", "104", "106", "108"}, got)

	got, err = indices("0-3", 10)
	require.NoError(t, err)
	require.Equal(t, []string{"0", "1", "2"}, got)

	got, err = indices("", 10)
	require.NoError(t, err)
	require.Empty(t, got)

	for _, bad := range []string{"12", "a-3", "3-b", "5-2"} {
		_, err = indices(bad, 3)
		require.Error(t, err, bad)
	}
}

func TestSoakValidator(t *testing.T) {
	c := newChain(t)
	h := testHooks()
	h.Beacon = c.srv.URL
	h.Sample = 10

	with := func(id, r string) Target { return Target{ID: id, Extra: Extra{Validators: r}} }
	in := SoakInput{Updated: []Target{with("up/vc", rangeOf(0, 100))}, Remaining: []Target{with("r/vc", rangeOf(100, 200))}}

	code, out := runProgram(t, h, "soak-validator", in)
	require.Equal(t, 0, code, out)
	require.Equal(t, "100.0% of 10 updated validators hit the target in epoch 98, against 100.0% on the remaining nodes\nupdated=100.0 remaining=100.0 unit=%", out)

	// One of ten missing is within 5% of the remaining nodes? No: 90% < 95%.
	c.set(func(c *chain) { c.missed["0"] = true })

	code, out = runProgram(t, h, "soak-validator", in)
	require.Equal(t, 1, code)
	require.Contains(t, out, "90.0% of 10 updated validators")

	// The remaining nodes doing as badly makes it a pass.
	c.set(func(c *chain) { c.missed["100"] = true })

	code, _ = runProgram(t, h, "soak-validator", in)
	require.Equal(t, 0, code)

	// Nothing remaining: the floor applies.
	solo := SoakInput{Updated: in.Updated}
	code, out = runProgram(t, h, "soak-validator", solo)
	require.Equal(t, 0, code)
	require.Contains(t, out, "against the floor of 80.0%")
	require.Contains(t, out, "remaining=- unit=%")

	// No validators on the updated targets.
	code, out = runProgram(t, h, "soak-validator", SoakInput{Updated: []Target{with("up/cl", "")}})
	require.Equal(t, 0, code)
	require.Equal(t, "no validators on the updated targets", out)

	// Remaining targets with no validators count as nothing remaining.
	code, out = runProgram(t, h, "soak-validator", SoakInput{Updated: in.Updated, Remaining: []Target{with("r/cl", "")}})
	require.Equal(t, 0, code)
	require.Contains(t, out, "the floor")

	code, out = runProgram(t, h, "soak-validator", SoakInput{Updated: []Target{with("up/vc", "bad")}})
	require.Equal(t, 1, code)
	require.Contains(t, out, "up/vc: validator range")

	code, _ = runProgram(t, h, "soak-validator", SoakInput{Updated: in.Updated, Remaining: []Target{with("r/vc", "bad")}})
	require.Equal(t, 1, code)

	c.set(func(c *chain) { c.slot = 40 })

	code, out = runProgram(t, h, "soak-validator", in)
	require.Equal(t, 1, code)
	require.Equal(t, "the chain is younger than two epochs", out)

	c.set(func(c *chain) { c.slot = 3200 })

	path := "/eth/v1/beacon/rewards/attestations/98"

	c.set(func(c *chain) { c.fail[path] = 1 })

	code, out = runProgram(t, h, "soak-validator", in)
	require.Equal(t, 1, code)
	require.Contains(t, out, "rewards for the updated validators")

	c.set(func(c *chain) { c.fail[path] = 2 })

	code, out = runProgram(t, h, "soak-validator", in)
	require.Equal(t, 1, code)
	require.Contains(t, out, "rewards for the remaining validators")

	c.set(func(c *chain) { c.fail[pathSpec] = 1 })

	code, out = runProgram(t, h, "soak-validator", in)
	require.Equal(t, 1, code)
	require.Contains(t, out, "reference beacon node")

	// A node that reports no rewards at all.
	h.Beacon = jsonServer(t, map[string]string{
		pathSpec:                                 specBody,
		pathHead:                                 `{"data":{"header":{"message":{"slot":"3200"}}}}`,
		"/eth/v1/beacon/rewards/attestations/98": `{"data":{"total_rewards":[]}}`,
	})
	code, out = runProgram(t, h, "soak-validator", in)
	require.Equal(t, 1, code)
	require.Equal(t, "no rewards reported for the updated validators in epoch 98", out)

	code, _ = runProgram(t, h, "soak-validator", "bad")
	require.Equal(t, 1, code)

	_ = http.StatusOK
}
