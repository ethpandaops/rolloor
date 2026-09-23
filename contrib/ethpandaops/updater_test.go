package ethpandaops

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInspect(t *testing.T) {
	n := newNode(t)
	h := testHooks()

	code, out := runProgram(t, h, "inspect", n.target("n/beacon", beacon))
	require.Equal(t, 0, code)
	require.Equal(t, dOld, out)

	code, out = runProgram(t, h, "inspect", n.target("n/gone", "gone"))
	require.Equal(t, 0, code)
	require.Equal(t, "none", out)

	n.set(func(n *node) { n.containers[beacon] = container{Name: beacon, Image: "local:dev"} })
	code, out = runProgram(t, h, "inspect", n.target("n/beacon", beacon))
	require.Equal(t, 1, code)
	require.Equal(t, "beacon runs local:dev, which has no registry digest", out)

	// A wrong token, a missing updater and a bad document are errors.
	h.UpdaterToken = "wrong"
	code, out = runProgram(t, h, "inspect", n.target("n/beacon", beacon))
	require.Equal(t, 1, code)
	require.Contains(t, out, "401")

	code, out = runProgram(t, h, "inspect", Target{ID: "x"})
	require.Equal(t, 1, code)
	require.Equal(t, "x: extra.updater and extra.container are required", out)

	code, out = runProgram(t, h, "inspect", map[string]any{})
	require.Equal(t, 1, code)
	require.Equal(t, "target document has no id", out)

	code, _ = runProgram(t, h, "inspect", "not an object")
	require.Equal(t, 1, code)
}

func TestUpdate(t *testing.T) {
	n := newNode(t)
	h := testHooks()
	tg := n.target("n/beacon", beacon)

	code, out := runProgram(t, h, "update", tg)
	require.Equal(t, 0, code, out)
	require.Equal(t, "update to 222222222222 started", out)
	require.Equal(t, []string{"beacon async=true"}, n.updates)

	// Already there: nothing is asked of the updater.
	n.set(func(n *node) { n.containers[beacon] = container{Name: beacon, Digest: dNew, LatestDigest: dNew} })

	code, out = runProgram(t, h, "update", tg)
	require.Equal(t, 0, code)
	require.Equal(t, "already running 222222222222", out)
	require.Len(t, n.updates, 1)

	// The tag has moved past the desired digest: left as is.
	n.set(func(n *node) { n.containers[beacon] = container{Name: beacon, Digest: dOld, LatestDigest: dNewer} })

	code, out = runProgram(t, h, "update", tg)
	require.Equal(t, 0, code)
	require.Contains(t, out, "tag head is 333333333333, not 222222222222; left as is")
	require.Len(t, n.updates, 1)

	// The updater cannot read the registry.
	n.set(func(n *node) { n.checkErr = "unauthorized" })

	code, out = runProgram(t, h, "update", tg)
	require.Equal(t, 1, code)
	require.Equal(t, "updater could not read the registry: unauthorized", out)

	n.set(func(n *node) {
		n.checkErr = ""
		n.containers[beacon] = container{Name: beacon, Digest: dOld, LatestDigest: dNew}
	})

	// A busy updater is waited out.
	n.set(func(n *node) { n.busy = 2 })

	code, out = runProgram(t, h, "update", tg)
	require.Equal(t, 0, code, out)
	require.Len(t, n.updates, 2)

	// One that stays busy past the wait fails the update.
	h.UpdateWait = time.Millisecond
	h.Sleep = sleep

	n.set(func(n *node) { n.busy = 1000 })

	code, out = runProgram(t, h, "update", tg)
	require.Equal(t, 1, code)
	require.Equal(t, "the updater stayed busy for 1ms", out)

	// A cancelled run reports the cancellation.
	h.UpdateWait = time.Minute
	h.Sleep = func(context.Context, time.Duration) error { return context.Canceled }
	code, out = runProgram(t, h, "update", tg)
	require.Equal(t, 1, code)
	require.Equal(t, "context canceled", out)

	n.set(func(n *node) { n.busy = 0; n.statusFor["/v1/update"] = http.StatusInternalServerError })

	code, out = runProgram(t, h, "update", tg)
	require.Equal(t, 1, code)
	require.Contains(t, out, "updater: POST")

	// Unwatched container, no desired digest, broken listing and check.
	code, out = runProgram(t, h, "update", n.target("n/x", "x"))
	require.Equal(t, 1, code)
	require.Equal(t, "the updater on n does not watch a container named x", out)

	code, out = runProgram(t, h, "update", Target{ID: "x"})
	require.Equal(t, 1, code)
	require.Equal(t, "no desired digest in the target document", out)

	code, _ = runProgram(t, h, "update", Target{ID: "x", Desired: dNew})
	require.Equal(t, 1, code)

	n.set(func(n *node) { n.statusFor["/v1/check"] = http.StatusBadGateway })

	code, out = runProgram(t, h, "update", tg)
	require.Equal(t, 1, code)
	require.Contains(t, out, "502 Bad Gateway: nope")

	code, _ = runProgram(t, h, "update", "bad")
	require.Equal(t, 1, code)

	_, err := h.startUpdate(context.Background(), &Target{ID: "x"})
	require.Error(t, err)
}

func TestReadyRunning(t *testing.T) {
	n := newNode(t)
	h := testHooks()
	tg := n.target("n/beacon", beacon)

	code, out := runProgram(t, h, "ready-running", tg)
	require.Equal(t, 1, code)
	require.Equal(t, "beacon runs 111111111111, not 222222222222", out)

	tg.Desired = dOld
	code, out = runProgram(t, h, "ready-running", tg)
	require.Equal(t, 0, code)
	require.Equal(t, "beacon running 111111111111", out)

	n.set(func(n *node) { n.containers[beacon] = container{Name: beacon, Digest: dOld} })

	code, out = runProgram(t, h, "ready-running", tg)
	require.Equal(t, 1, code)
	require.Equal(t, "beacon is not running", out)

	code, out = runProgram(t, h, "ready-running", n.target("n/x", "x"))
	require.Equal(t, 1, code)
	require.Equal(t, "x is not on n", out)

	code, _ = runProgram(t, h, "ready-running", Target{ID: "x"})
	require.Equal(t, 1, code)

	code, _ = runProgram(t, h, "ready-running", "bad")
	require.Equal(t, 1, code)
}
