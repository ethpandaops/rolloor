package ethpandaops

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadyExecution(t *testing.T) {
	n := newNode(t)
	h := testHooks()
	tg := n.target("n/execution", "execution")

	code, out := runProgram(t, h, "ready-execution", tg)
	require.Equal(t, 0, code)
	require.Equal(t, "synced at block 100 with 32 peers", out)

	n.set(func(n *node) { n.elSyncing = `{"currentBlock":"0x1"}` })

	code, out = runProgram(t, h, "ready-execution", tg)
	require.Equal(t, 1, code)
	require.Equal(t, "syncing", out)

	n.set(func(n *node) { n.elSyncing, n.elPeers = rpcFalse, "0x0" })

	code, out = runProgram(t, h, "ready-execution", tg)
	require.Equal(t, 1, code)
	require.Equal(t, "no peers", out)

	n.set(func(n *node) { n.elPeers = "0x5"; n.rpcErr = methodBlockNumber })

	code, out = runProgram(t, h, "ready-execution", tg)
	require.Equal(t, 1, code)
	require.Equal(t, "execution API: eth_blockNumber: boom", out)

	n.set(func(n *node) { n.rpcErr = methodPeerCount })

	code, _ = runProgram(t, h, "ready-execution", tg)
	require.Equal(t, 1, code)

	n.set(func(n *node) { n.rpcErr = methodSyncing })

	code, _ = runProgram(t, h, "ready-execution", tg)
	require.Equal(t, 1, code)

	n.set(func(n *node) { n.rpcErr = ""; n.elPeers = "0xzz" })

	code, out = runProgram(t, h, "ready-execution", tg)
	require.Equal(t, 1, code)
	require.Contains(t, out, `net_peerCount "0xzz"`)

	n.set(func(n *node) { n.elSyncing = "not json" })

	code, _ = runProgram(t, h, "ready-execution", tg)
	require.Equal(t, 1, code)

	n.set(func(n *node) { n.statusFor["/rpc"] = http.StatusBadGateway })

	code, _ = runProgram(t, h, "ready-execution", tg)
	require.Equal(t, 1, code)

	code, out = runProgram(t, h, "ready-execution", Target{ID: "x"})
	require.Equal(t, 1, code)
	require.Equal(t, "execution API: x: extra.rpc is required", out)

	code, _ = runProgram(t, h, "ready-execution", "bad")
	require.Equal(t, 1, code)
}

func TestSoakExecution(t *testing.T) {
	up, rem := newNode(t), newNode(t)
	h := testHooks()
	in := SoakInput{Updated: []Target{up.target("up/el", "execution")}, Remaining: []Target{rem.target("r/el", "execution")}}

	code, out := runProgram(t, h, "soak-execution", in)
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "updated=0 remaining=0 unit=blocks")

	rem.set(func(n *node) { n.block = "0x6e" })

	code, out = runProgram(t, h, "soak-execution", in)
	require.Equal(t, 1, code)
	require.Equal(t, "up/el is 10 blocks behind; the remaining nodes are at most 0\nupdated=10 remaining=0 unit=blocks", out)

	for _, method := range []string{methodSyncing, methodBlockNumber, methodPeerCount} {
		up.set(func(n *node) { n.rpcErr = method })

		code, out = runProgram(t, h, "soak-execution", in)
		require.Equal(t, 1, code, method)
		require.Contains(t, out, "boom", method)
	}

	up.set(func(n *node) { n.rpcErr = ""; n.elSyncing = "{}" })

	code, out = runProgram(t, h, "soak-execution", in)
	require.Equal(t, 1, code)
	require.Equal(t, "up/el: syncing", out)

	code, _ = runProgram(t, h, "soak-execution", "bad")
	require.Equal(t, 1, code)
}
