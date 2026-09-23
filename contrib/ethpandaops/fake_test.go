package ethpandaops

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	token    = "tok"
	dOld     = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	dNew     = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	dNewer   = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
	beacon   = "beacon"
	running  = "running"
	keyData  = "data"
	specBody = `{"data":{"SLOTS_PER_EPOCH":"32"}}`
)

// node fakes one devnet host: its updater, beacon and execution APIs.
type node struct {
	mu sync.Mutex

	containers map[string]container
	checkErr   string
	busy       int
	updates    []string
	statusFor  map[string]int

	syncDistance string
	syncing      bool
	elOffline    bool
	peers        string

	elSyncing string
	block     string
	elPeers   string
	rpcErr    string

	srv *httptest.Server
}

func newNode(t *testing.T) *node {
	t.Helper()

	n := &node{
		containers: map[string]container{
			beacon: {Name: "/" + beacon, Image: "org/cl:t", Digest: dOld, Running: true, LatestDigest: dNew},
		},
		statusFor:    map[string]int{},
		syncDistance: "0",
		peers:        "50",
		elSyncing:    rpcFalse,
		block:        "0x64",
		elPeers:      "0x20",
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/containers", n.list(false))
	mux.HandleFunc("GET /v1/containers/details", n.list(false))
	mux.HandleFunc("POST /v1/check", n.list(true))
	mux.HandleFunc("POST /v1/update", n.update)
	mux.HandleFunc("GET /eth/v1/node/syncing", n.beaconSyncing)
	mux.HandleFunc("GET /eth/v1/node/peer_count", func(w http.ResponseWriter, r *http.Request) {
		n.reply(w, r, map[string]any{keyData: map[string]string{"connected": n.peers}})
	})
	mux.HandleFunc("POST /rpc", n.rpc)

	n.srv = httptest.NewServer(mux)
	t.Cleanup(n.srv.Close)

	return n
}

// reply answers with v, unless a status override is set for the path.
func (n *node) reply(w http.ResponseWriter, r *http.Request, v any) {
	n.mu.Lock()
	code, override := n.statusFor[r.URL.Path]
	n.mu.Unlock()

	if override {
		w.WriteHeader(code)
		_, _ = w.Write([]byte("nope\nsecond line"))

		return
	}

	_ = json.NewEncoder(w).Encode(v)
}

func (n *node) authorized(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("Authorization") != "Bearer "+token {
		w.WriteHeader(http.StatusUnauthorized)

		return false
	}

	return true
}

func (n *node) list(check bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !n.authorized(w, r) {
			return
		}

		n.mu.Lock()

		var out []container

		for name, c := range n.containers {
			match := r.URL.Query().Get("name") == name
			if check {
				match = r.URL.Query().Get("container") == name
				c.Error = n.checkErr
			}

			if match {
				out = append(out, c)
			}
		}

		n.mu.Unlock()

		n.reply(w, r, map[string]any{"containers": out, "count": len(out)})
	}
}

func (n *node) update(w http.ResponseWriter, r *http.Request) {
	if !n.authorized(w, r) {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.busy > 0 {
		n.busy--

		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)

		return
	}

	if code, ok := n.statusFor[r.URL.Path]; ok {
		w.WriteHeader(code)

		return
	}

	n.updates = append(n.updates, r.URL.Query().Get("container")+" async="+r.URL.Query().Get("async"))

	w.WriteHeader(http.StatusAccepted)
}

func (n *node) beaconSyncing(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	body := map[string]any{keyData: map[string]any{
		"head_slot": "100", "sync_distance": n.syncDistance, "is_syncing": n.syncing, "el_offline": n.elOffline,
	}}
	n.mu.Unlock()

	n.reply(w, r, body)
}

func (n *node) rpc(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Method string `json:"method"`
	}

	_ = json.NewDecoder(r.Body).Decode(&req)

	n.mu.Lock()
	defer n.mu.Unlock()

	if code, ok := n.statusFor[r.URL.Path]; ok {
		w.WriteHeader(code)

		return
	}

	if n.rpcErr == req.Method {
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"message": "boom"}})

		return
	}

	var result json.RawMessage

	switch req.Method {
	case methodSyncing:
		result = json.RawMessage(n.elSyncing)
	case methodBlockNumber:
		result = json.RawMessage(strconv.Quote(n.block))
	case methodPeerCount:
		result = json.RawMessage(strconv.Quote(n.elPeers))
	}

	_ = json.NewEncoder(w).Encode(map[string]any{"result": result})
}

func (n *node) set(fn func(n *node)) {
	n.mu.Lock()
	defer n.mu.Unlock()

	fn(n)
}

func (n *node) target(id, name string) Target {
	return Target{
		ID: id, Node: "n", Image: "org/cl:t", Desired: dNew,
		Extra: Extra{Container: name, Updater: n.srv.URL + "/", Beacon: n.srv.URL, RPC: n.srv.URL + "/rpc"},
	}
}

func testHooks() *Hooks {
	s, err := SettingsFromEnv(func(string) string { return "" })
	if err != nil {
		panic(err)
	}

	s.UpdaterToken = token
	s.NodeAuth = "user:pass"
	h := New(&s)
	h.Sleep = func(context.Context, time.Duration) error { return nil }

	return h
}

func doc(t *testing.T, v any) []byte {
	t.Helper()

	raw, err := json.Marshal(v)
	require.NoError(t, err)

	return raw
}

// runProgram runs a program through Run and returns its exit code and output.
func runProgram(t *testing.T, h *Hooks, name string, v any) (int, string) {
	t.Helper()

	var out strings.Builder

	code := Run(context.Background(), h, name, strings.NewReader(string(doc(t, v))), &out)

	return code, strings.TrimSpace(out.String())
}

// chain fakes the reference beacon node: spec, head, finality and rewards.
type chain struct {
	mu        sync.Mutex
	slot      uint64
	finalized uint64
	forks     map[string]string
	missed    map[string]bool
	fail      map[string]int
	srv       *httptest.Server
}

func newChain(t *testing.T) *chain {
	t.Helper()

	c := &chain{slot: 3200, finalized: 98, forks: map[string]string{"GLOAS_FORK_EPOCH": "1125", "OLD_FORK_EPOCH": "0", "NEVER_FORK_EPOCH": "18446744073709551615"}, missed: map[string]bool{}, fail: map[string]int{}}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /eth/v1/config/spec", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()

		data := map[string]any{slotsPerEpoch: "32", "CONFIG_NAME": "devnet", "SECONDS_PER_SLOT": 12.0}
		for k, v := range c.forks {
			data[k] = v
		}
		c.mu.Unlock()
		c.reply(w, r, map[string]any{keyData: data})
	})
	mux.HandleFunc("GET /eth/v1/beacon/headers/head", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		slot := strconv.FormatUint(c.slot, 10)
		c.mu.Unlock()
		c.reply(w, r, map[string]any{keyData: map[string]any{"header": map[string]any{"message": map[string]string{"slot": slot}}}})
	})
	mux.HandleFunc("GET /eth/v1/beacon/states/head/finality_checkpoints", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		fin := strconv.FormatUint(c.finalized, 10)
		c.mu.Unlock()
		c.reply(w, r, map[string]any{keyData: map[string]any{"finalized": map[string]string{"epoch": fin}}})
	})
	mux.HandleFunc("POST /eth/v1/beacon/rewards/attestations/{epoch}", func(w http.ResponseWriter, r *http.Request) {
		var idx []string

		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &idx)

		c.mu.Lock()

		rewards := make([]map[string]string, 0, len(idx))
		for _, i := range idx {
			v := "100"
			if c.missed[i] {
				v = "-100"
			}

			rewards = append(rewards, map[string]string{"validator_index": i, "target": v})
		}
		c.mu.Unlock()
		c.reply(w, r, map[string]any{keyData: map[string]any{"total_rewards": rewards}})
	})

	c.srv = httptest.NewServer(mux)
	t.Cleanup(c.srv.Close)

	return c
}

// reply fails the nth call to a path when asked to, counting down.
func (c *chain) reply(w http.ResponseWriter, r *http.Request, v any) {
	c.mu.Lock()

	n, failing := c.fail[r.URL.Path]
	if failing {
		if n <= 1 {
			delete(c.fail, r.URL.Path)
		} else {
			c.fail[r.URL.Path] = n - 1
		}
	}
	c.mu.Unlock()

	if failing && n <= 1 {
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	_ = json.NewEncoder(w).Encode(v)
}

func (c *chain) set(fn func(c *chain)) {
	c.mu.Lock()
	defer c.mu.Unlock()

	fn(c)
}

func rangeOf(start, end int) string { return fmt.Sprintf("%d-%d", start, end) }
