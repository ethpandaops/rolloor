package ethpandaops

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadyBeacon(t *testing.T) {
	n := newNode(t)
	h := testHooks()
	tg := n.target("n/beacon", beacon)

	code, out := runProgram(t, h, "ready-beacon", tg)
	require.Equal(t, 0, code)
	require.Equal(t, "synced at slot 100", out)

	n.set(func(n *node) { n.syncing, n.syncDistance = true, "40" })

	code, out = runProgram(t, h, "ready-beacon", tg)
	require.Equal(t, 1, code)
	require.Equal(t, "syncing, 40 slots behind", out)

	n.set(func(n *node) { n.syncing, n.elOffline = false, true })

	code, out = runProgram(t, h, "ready-beacon", tg)
	require.Equal(t, 1, code)
	require.Equal(t, "execution client offline", out)

	n.set(func(n *node) { n.statusFor["/eth/v1/node/syncing"] = http.StatusServiceUnavailable })

	code, out = runProgram(t, h, "ready-beacon", tg)
	require.Equal(t, 1, code)
	require.Contains(t, out, "beacon API: GET")

	code, out = runProgram(t, h, "ready-beacon", Target{ID: "x"})
	require.Equal(t, 1, code)
	require.Equal(t, "beacon API: x: extra.beacon is required", out)

	code, _ = runProgram(t, h, "ready-beacon", "bad")
	require.Equal(t, 1, code)
}

func TestSoakBeacon(t *testing.T) {
	up, rem1, rem2 := newNode(t), newNode(t), newNode(t)
	h := testHooks()
	in := SoakInput{
		Updated:   []Target{up.target("up/beacon", beacon)},
		Remaining: []Target{rem1.target("r1/beacon", beacon), rem2.target("r2/beacon", beacon)},
	}

	code, out := runProgram(t, h, "soak-beacon", in)
	require.Equal(t, 0, code, out)
	require.Equal(t, "updated at most 0 slots behind with at least 50 peers; remaining at most 0 behind, median 50 peers\nupdated=0 remaining=0 unit=slots", out)

	// Within slack, or no worse than the remaining nodes, passes.
	up.set(func(n *node) { n.syncDistance = "2" })

	code, _ = runProgram(t, h, "soak-beacon", in)
	require.Equal(t, 0, code)

	up.set(func(n *node) { n.syncDistance = "6" })

	code, out = runProgram(t, h, "soak-beacon", in)
	require.Equal(t, 1, code)
	require.Equal(t, "up/beacon is 6 slots behind; the remaining nodes are at most 0\nupdated=6 remaining=0 unit=slots", out)

	rem1.set(func(n *node) { n.syncDistance = "7" })

	code, _ = runProgram(t, h, "soak-beacon", in)
	require.Equal(t, 0, code)

	// Half the remaining median peer count is the floor.
	up.set(func(n *node) { n.syncDistance, n.peers = "0", "20" })

	code, out = runProgram(t, h, "soak-beacon", in)
	require.Equal(t, 1, code)
	require.Contains(t, out, "up/beacon has 20 peers; the remaining nodes' median is 50")

	// A remaining node that does not answer is left out; an updated one fails.
	up.set(func(n *node) { n.peers = "50" })
	rem2.set(func(n *node) { n.statusFor["/eth/v1/node/syncing"] = http.StatusBadGateway })

	code, _ = runProgram(t, h, "soak-beacon", in)
	require.Equal(t, 0, code)

	up.set(func(n *node) { n.elOffline = true })

	code, out = runProgram(t, h, "soak-beacon", in)
	require.Equal(t, 1, code)
	require.Equal(t, "up/beacon: execution client offline", out)

	up.set(func(n *node) { n.elOffline, n.syncDistance = false, "x" })

	code, out = runProgram(t, h, "soak-beacon", in)
	require.Equal(t, 1, code)
	require.Contains(t, out, `sync distance "x"`)

	up.set(func(n *node) { n.syncDistance, n.peers = "0", "y" })

	code, out = runProgram(t, h, "soak-beacon", in)
	require.Equal(t, 1, code)
	require.Contains(t, out, `peer count "y"`)

	up.set(func(n *node) { n.peers = "50"; n.statusFor["/eth/v1/node/peer_count"] = http.StatusBadGateway })

	code, _ = runProgram(t, h, "soak-beacon", in)
	require.Equal(t, 1, code)

	up.set(func(n *node) { n.statusFor["/eth/v1/node/syncing"] = http.StatusBadGateway })

	code, _ = runProgram(t, h, "soak-beacon", in)
	require.Equal(t, 1, code)

	// The least peered of several updated nodes is the one judged.
	up.set(func(n *node) {
		delete(n.statusFor, "/eth/v1/node/syncing")
		delete(n.statusFor, "/eth/v1/node/peer_count")
	})

	second := newNode(t)
	second.set(func(n *node) { n.peers = "10" })
	two := SoakInput{Updated: []Target{up.target("up/beacon", beacon), second.target("up2/beacon", beacon)}, Remaining: in.Remaining}
	code, out = runProgram(t, h, "soak-beacon", two)
	require.Equal(t, 1, code, out)
	require.Contains(t, out, "up2/beacon has 10 peers")

	// Nothing left to compare against: the updated nodes must have peers.
	alone := newNode(t)
	solo := SoakInput{Updated: []Target{alone.target("a/beacon", beacon)}}
	code, out = runProgram(t, h, "soak-beacon", solo)
	require.Equal(t, 0, code)
	require.Contains(t, out, "nothing left to compare against")

	alone.set(func(n *node) { n.peers = "0" })

	code, out = runProgram(t, h, "soak-beacon", solo)
	require.Equal(t, 1, code)
	require.Contains(t, out, "a/beacon has no peers")

	code, out = runProgram(t, h, "soak-beacon", SoakInput{})
	require.Equal(t, 1, code)
	require.Equal(t, "soak document has no updated targets", out)

	code, _ = runProgram(t, h, "soak-beacon", "bad")
	require.Equal(t, 1, code)
}

func TestEnvironment(t *testing.T) {
	c := newChain(t)
	h := testHooks()
	h.Beacon = c.srv.URL

	code, out := runProgram(t, h, "environment", map[string]string{"environment": "devnet-11"})
	require.Equal(t, 0, code, out)
	require.Equal(t, "head epoch 100, finalized 98", out)

	c.set(func(c *chain) { c.finalized = 90 })

	code, out = runProgram(t, h, "environment", nil)
	require.Equal(t, 1, code)
	require.Equal(t, "finality is 10 epochs behind the head at epoch 100 (limit 4)", out)

	c.set(func(c *chain) { c.finalized = 1123; c.slot = 1122 * 32 })

	code, out = runProgram(t, h, "environment", nil)
	require.Equal(t, 1, code)
	require.Equal(t, "GLOAS_FORK_EPOCH is epoch 1125, 3 epochs from the head at 1122", out)

	c.set(func(c *chain) { c.slot = 1200 * 32; c.finalized = 1198 })

	h.QuietEpochs = []uint64{1203}
	code, out = runProgram(t, h, "environment", nil)
	require.Equal(t, 1, code)
	require.Equal(t, "quiet epoch 1203 is 3 epochs from the head at 1200", out)

	h.QuietEpochs = nil

	for _, path := range []string{pathSpec, pathHead, pathFinality} {
		c.set(func(c *chain) { c.fail[path] = 1 })

		code, out = runProgram(t, h, "environment", nil)
		require.Equal(t, 1, code, path)
		require.Contains(t, out, "reference beacon node: GET", path)
	}

	c.set(func(c *chain) { c.forks["BAD_FORK_EPOCH"] = "soon" })

	code, out = runProgram(t, h, "environment", nil)
	require.Equal(t, 1, code)
	require.Contains(t, out, "spec BAD_FORK_EPOCH soon")

	h.Beacon = ""
	code, out = runProgram(t, h, "environment", nil)
	require.Equal(t, 1, code)
	require.Equal(t, "reference beacon node: ETHPANDAOPS_BEACON is not set", out)
}

func TestNetworkRejectsOddAnswers(t *testing.T) {
	h := testHooks()

	for name, body := range map[string]string{
		"no slots per epoch": `{"data":{}}`,
	} {
		srv := jsonServer(t, map[string]string{pathSpec: body})
		h.Beacon = srv
		_, err := h.network(t.Context())
		require.Error(t, err, name)
	}

	srv := jsonServer(t, map[string]string{
		pathSpec: specBody,
		pathHead: `{"data":{"header":{"message":{"slot":"x"}}}}`,
	})
	h.Beacon = srv
	_, err := h.network(t.Context())
	require.ErrorContains(t, err, `head slot "x"`)

	srv = jsonServer(t, map[string]string{
		pathSpec:     specBody,
		pathHead:     `{"data":{"header":{"message":{"slot":"64"}}}}`,
		pathFinality: `{"data":{"finalized":{"epoch":"x"}}}`,
	})
	h.Beacon = srv
	code, out := runProgram(t, h, "environment", nil)
	require.Equal(t, 1, code)
	require.Contains(t, out, `finalized epoch "x"`)

	require.Equal(t, uint64(3), distance(5, 2))
	require.Equal(t, uint64(3), distance(2, 5))
}
