package ethpandaops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// JSON-RPC methods asked of execution clients.
const (
	methodSyncing     = "eth_syncing"
	methodBlockNumber = "eth_blockNumber"
	methodPeerCount   = "net_peerCount"
	rpcFalse          = "false"
)

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// rpc calls one JSON-RPC method on the target's execution client.
func (h *Hooks) rpc(ctx context.Context, t *Target, method string, out any) error {
	if t.Extra.RPC == "" {
		return fmt.Errorf("%s: extra.rpc is required", t.ID)
	}

	body := map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": []any{}}

	var resp rpcResponse
	if _, _, err := h.call(ctx, http.MethodPost, strings.TrimRight(t.Extra.RPC, "/"), body, h.nodeAuth, &resp); err != nil {
		return err
	}

	if resp.Error != nil {
		return fmt.Errorf("%s: %s", method, resp.Error.Message)
	}

	if err := json.Unmarshal(resp.Result, out); err != nil {
		return fmt.Errorf("%s: %w", method, err)
	}

	return nil
}

func (h *Hooks) rpcUint(ctx context.Context, t *Target, method string) (uint64, error) {
	var hex string
	if err := h.rpc(ctx, t, method, &hex); err != nil {
		return 0, err
	}

	v, err := strconv.ParseUint(strings.TrimPrefix(hex, "0x"), 16, 64)
	if err != nil {
		return 0, fmt.Errorf("%s %q: %w", method, hex, err)
	}

	return v, nil
}

// syncingEL reports whether eth_syncing says the client is still syncing:
// it answers false when done, an object while syncing.
func (h *Hooks) syncingEL(ctx context.Context, t *Target) (bool, error) {
	var raw json.RawMessage
	if err := h.rpc(ctx, t, methodSyncing, &raw); err != nil {
		return false, err
	}

	return string(raw) != rpcFalse, nil
}

// ReadyExecution passes when the execution client has finished syncing and
// has peers.
func ReadyExecution(ctx context.Context, h *Hooks, stdin []byte) (Result, error) {
	t, err := decodeTarget(stdin)
	if err != nil {
		return Result{}, err
	}

	syncing, err := h.syncingEL(ctx, t)
	if err != nil {
		return fail("execution API: %v", err), nil
	}

	if syncing {
		return fail("syncing"), nil
	}

	peers, err := h.rpcUint(ctx, t, methodPeerCount)
	if err != nil {
		return fail("execution API: %v", err), nil
	}

	if peers == 0 {
		return fail("no peers"), nil
	}

	block, err := h.rpcUint(ctx, t, methodBlockNumber)
	if err != nil {
		return fail("execution API: %v", err), nil
	}

	return pass("synced at block %d with %d peers", block, peers), nil
}

// SoakExecution compares how many blocks behind the highest head seen and
// how well peered the updated execution clients are against the remaining.
func SoakExecution(ctx context.Context, h *Hooks, stdin []byte) (Result, error) {
	in, err := decodeSoak(stdin)
	if err != nil {
		return Result{}, err
	}

	// The probe records each head block in lag; once every head is known,
	// lag becomes the distance from the highest.
	probe := func(t *Target) health {
		syncing, err := h.syncingEL(ctx, t)
		if err != nil {
			return health{err: err}
		}

		if syncing {
			return health{err: errors.New("syncing")}
		}

		block, err := h.rpcUint(ctx, t, methodBlockNumber)
		if err != nil {
			return health{err: err}
		}

		peers, err := h.rpcUint(ctx, t, methodPeerCount)
		if err != nil {
			return health{err: err}
		}

		return health{lag: block, peers: peers}
	}

	updated, remaining := probeAll(in.Updated, probe), probeAll(in.Remaining, probe)

	var top uint64

	for _, hs := range [][]health{updated, remaining} {
		for i := range hs {
			if hs[i].err == nil {
				top = max(top, hs[i].lag)
			}
		}
	}

	for _, hs := range [][]health{updated, remaining} {
		for i := range hs {
			hs[i].lag = top - min(hs[i].lag, top)
		}
	}

	return compare("blocks", 2, updated, remaining), nil
}
