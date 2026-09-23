package ethpandaops

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// farFuture is the epoch of a fork that is not scheduled.
const farFuture = math.MaxUint64

// Beacon API paths and spec keys read by more than one program.
const (
	pathSpec      = "/eth/v1/config/spec"
	pathHead      = "/eth/v1/beacon/headers/head"
	pathFinality  = "/eth/v1/beacon/states/head/finality_checkpoints"
	slotsPerEpoch = "SLOTS_PER_EPOCH"
)

type beaconSyncing struct {
	Data struct {
		HeadSlot     string `json:"head_slot"`
		SyncDistance string `json:"sync_distance"`
		IsSyncing    bool   `json:"is_syncing"`
		ELOffline    bool   `json:"el_offline"`
	} `json:"data"`
}

type beaconPeers struct {
	Data struct {
		Connected string `json:"connected"`
	} `json:"data"`
}

func beaconBase(t *Target) (string, error) {
	if t.Extra.Beacon == "" {
		return "", fmt.Errorf("%s: extra.beacon is required", t.ID)
	}

	return strings.TrimRight(t.Extra.Beacon, "/"), nil
}

func (h *Hooks) beaconGet(ctx context.Context, url string, out any) error {
	_, _, err := h.call(ctx, http.MethodGet, url, nil, h.nodeAuth, out)

	return err
}

func (h *Hooks) syncing(ctx context.Context, t *Target) (*beaconSyncing, error) {
	base, err := beaconBase(t)
	if err != nil {
		return nil, err
	}

	var s beaconSyncing
	if err := h.beaconGet(ctx, base+"/eth/v1/node/syncing", &s); err != nil {
		return nil, err
	}

	return &s, nil
}

// ReadyBeacon passes when the beacon node is synced and its execution client
// is online.
func ReadyBeacon(ctx context.Context, h *Hooks, stdin []byte) (Result, error) {
	t, err := decodeTarget(stdin)
	if err != nil {
		return Result{}, err
	}

	s, err := h.syncing(ctx, t)
	if err != nil {
		return fail("beacon API: %v", err), nil
	}

	switch {
	case s.Data.IsSyncing:
		return fail("syncing, %s slots behind", s.Data.SyncDistance), nil
	case s.Data.ELOffline:
		return fail("execution client offline"), nil
	}

	return pass("synced at slot %s", s.Data.HeadSlot), nil
}

// SoakBeacon compares how far behind and how well peered the updated beacon
// nodes are against the remaining ones.
func SoakBeacon(ctx context.Context, h *Hooks, stdin []byte) (Result, error) {
	in, err := decodeSoak(stdin)
	if err != nil {
		return Result{}, err
	}

	probe := func(t *Target) health {
		s, err := h.syncing(ctx, t)
		if err != nil {
			return health{err: err}
		}

		if s.Data.ELOffline {
			return health{err: errors.New("execution client offline")}
		}

		lag, err := strconv.ParseUint(s.Data.SyncDistance, 10, 64)
		if err != nil {
			return health{err: fmt.Errorf("sync distance %q: %w", s.Data.SyncDistance, err)}
		}

		base, _ := beaconBase(t)

		var p beaconPeers
		if perr := h.beaconGet(ctx, base+"/eth/v1/node/peer_count", &p); perr != nil {
			return health{err: perr}
		}

		peers, err := strconv.ParseUint(p.Data.Connected, 10, 64)
		if err != nil {
			return health{err: fmt.Errorf("peer count %q: %w", p.Data.Connected, err)}
		}

		return health{lag: lag, peers: peers}
	}

	return compare("slots", 2, probeAll(in.Updated, probe), probeAll(in.Remaining, probe)), nil
}

// network is what the reference beacon node says about the chain.
type network struct {
	slotsPerEpoch uint64
	epoch         uint64
	forks         map[string]uint64
}

func (h *Hooks) network(ctx context.Context) (*network, error) {
	if h.Beacon == "" {
		return nil, errors.New("ETHPANDAOPS_BEACON is not set")
	}

	var spec struct {
		Data map[string]any `json:"data"`
	}

	if err := h.beaconGet(ctx, h.Beacon+pathSpec, &spec); err != nil {
		return nil, err
	}

	n := &network{forks: map[string]uint64{}}

	for k, v := range spec.Data {
		s, _ := v.(string)
		if k == slotsPerEpoch || strings.HasSuffix(k, "_FORK_EPOCH") {
			u, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("spec %s %v: %w", k, v, err)
			}

			if k == slotsPerEpoch {
				n.slotsPerEpoch = u
			} else if u != farFuture {
				n.forks[k] = u
			}
		}
	}

	if n.slotsPerEpoch == 0 {
		return nil, errors.New("spec has no " + slotsPerEpoch)
	}

	var head struct {
		Data struct {
			Header struct {
				Message struct {
					Slot string `json:"slot"`
				} `json:"message"`
			} `json:"header"`
		} `json:"data"`
	}

	if err := h.beaconGet(ctx, h.Beacon+pathHead, &head); err != nil {
		return nil, err
	}

	slot, err := strconv.ParseUint(head.Data.Header.Message.Slot, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("head slot %q: %w", head.Data.Header.Message.Slot, err)
	}

	n.epoch = slot / n.slotsPerEpoch

	return n, nil
}

func distance(a, b uint64) uint64 {
	if a > b {
		return a - b
	}

	return b - a
}

// Environment passes while finality keeps up with the head and the head is
// not near a fork or a quiet epoch.
func Environment(ctx context.Context, h *Hooks, _ []byte) (Result, error) {
	n, err := h.network(ctx)
	if err != nil {
		return fail("reference beacon node: %v", err), nil
	}

	var fin struct {
		Data struct {
			Finalized struct {
				Epoch string `json:"epoch"`
			} `json:"finalized"`
		} `json:"data"`
	}

	if ferr := h.beaconGet(ctx, h.Beacon+pathFinality, &fin); ferr != nil {
		return fail("reference beacon node: %v", ferr), nil
	}

	finalized, err := strconv.ParseUint(fin.Data.Finalized.Epoch, 10, 64)
	if err != nil {
		return fail("finalized epoch %q: %v", fin.Data.Finalized.Epoch, err), nil
	}

	if lag := n.epoch - min(finalized, n.epoch); lag > h.FinalityLag {
		return fail("finality is %d epochs behind the head at epoch %d (limit %d)", lag, n.epoch, h.FinalityLag), nil
	}

	names := make([]string, 0, len(n.forks))
	for k := range n.forks {
		names = append(names, k)
	}

	sort.Strings(names)

	for _, k := range names {
		if d := distance(n.epoch, n.forks[k]); d <= h.ForkMargin {
			return fail("%s is epoch %d, %d epochs from the head at %d", k, n.forks[k], d, n.epoch), nil
		}
	}

	for _, e := range h.QuietEpochs {
		if d := distance(n.epoch, e); d <= h.ForkMargin {
			return fail("quiet epoch %d is %d epochs from the head at %d", e, d, n.epoch), nil
		}
	}

	return pass("head epoch %d, finalized %d", n.epoch, finalized), nil
}
