package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/hooks"
	"github.com/ethpandaops/rolloor/internal/targets"
)

const (
	tB1     = "b-1/el"
	b1Line  = `- {id: b-1/el, node: b-1, weight: 100, image: org/b:t, labels: {client: b, owner: b, role: el, wave: "1"}, hooks: {soak: soak-b}}`
	b1Moved = `- {id: b-1/el, node: b-1, weight: 100, address: "10.0.0.2", image: org/b:t, labels: {client: b, owner: b, role: el, wave: "1"}, hooks: {soak: soak-b}}`
)

// warmUpRunner is the fake world with one ready program that, like a real one
// excluding warm-up, says no until its target has run its digest a minute.
type warmUpRunner struct {
	*world
	clock  *fakeClock
	target string
	// since is every digestSince the program was given, guarded by world.mu.
	since []time.Time
}

func (w *warmUpRunner) Run(ctx context.Context, program, hook, targetID string, input any) (hooks.Result, error) {
	res, err := w.world.Run(ctx, program, hook, targetID, input)
	if err != nil || hook != config.HookReady || targetID != w.target {
		return res, err
	}

	in, _ := input.(HookInput)

	w.mu.Lock()
	w.since = append(w.since, in.DigestSince)
	w.mu.Unlock()

	if w.clock.Now().Sub(in.DigestSince) < time.Minute {
		res.OK, res.ExitCode, res.Reason = false, 1, "warming up"
	}

	return res, nil
}

func mustParse(t *testing.T, h *harness, doc string) *targets.Set {
	t.Helper()

	rules := h.current().Rules()
	set, err := parseTargets(doc, &rules)
	require.NoError(t, err)

	return set
}

func awaitGate(t *testing.T, entered <-chan struct{}) {
	t.Helper()

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("observation did not start")
	}
}

func TestQueuedProbeReadsDigestSinceWhenItStarts(t *testing.T) {
	h := newHarness(t, replaceLine(testConfig, "concurrency: 4", "concurrency: 1"), testTargets)
	runner := &warmUpRunner{world: h.world, clock: h.clock, target: tA2}
	c, err := New(h.ctx, &Options{
		Config: h.cfg, Targets: h.current, Resolver: h.world, Runner: runner,
		Store: h.store, Notifier: h.notes, Clock: h.clock, Log: logrus.New(), NewID: h.nextID,
	})
	require.NoError(t, err)

	h.c = c
	h.c.InspectAll(h.ctx)
	h.clock.Advance(time.Minute)
	h.c.ProbeAll(h.ctx)
	require.NoError(t, h.c.Tick(h.ctx))
	h.world.set(func(w *world) { w.updateStuck[tA2] = true })
	h.release(imgA, d2)
	id := h.active("a").ID
	require.Equal(t, PhaseUpdating, h.phases(h.rollout(id))[tA2])

	// With one probe at a time, tA2's probe waits behind tA1's while
	// inspection sees tA2's update land.
	entered, release := h.world.gate("ready:" + tA1)

	var wg sync.WaitGroup
	wg.Go(func() { h.c.ProbeAll(h.ctx) })
	awaitGate(t, entered)
	h.world.set(func(w *world) { w.running[tA2] = d2 })
	h.c.InspectAll(h.ctx)
	h.clock.Advance(time.Second)
	release()
	wg.Wait()
	require.NoError(t, h.c.Tick(h.ctx))

	h.c.mu.RLock()
	since := h.c.live[tA2].DigestSince
	h.c.mu.RUnlock()
	h.world.mu.Lock()
	given := runner.since[len(runner.since)-1]
	h.world.mu.Unlock()

	require.Equal(t, since, given, "the queued probe is told when the new digest was first seen")
	require.Equal(t, "warming up", h.view(tA2).Hooks[config.HookReady].Result.Reason)
	require.Equal(t, PhaseUpdating, h.phases(h.rollout(id))[tA2], "a probe of the warming build cannot certify it")
}

func TestResultsBegunBeforeATargetIsForgottenAreRefused(t *testing.T) {
	changes := []struct {
		name   string
		change func(t *testing.T, h *harness)
	}{
		{"changed", func(t *testing.T, h *harness) {
			t.Helper()
			h.swap(mustParse(t, h, replaceLine(testTargets, b1Line, b1Moved)))
			h.c.Forget(h.ctx, []string{tB1})
		}},
		{"removed and added back unchanged", func(t *testing.T, h *harness) {
			t.Helper()

			full := h.current()
			h.swap(mustParse(t, h, dropLines(testTargets, "id: b-1/el,")))
			h.c.Forget(h.ctx, []string{tB1})
			h.swap(full)
		}},
	}

	for _, hook := range []string{config.HookInspect, config.HookReady} {
		for _, tc := range changes {
			t.Run(hook+" "+tc.name, func(t *testing.T) {
				h := newHarness(t, testConfig, testTargets)
				h.prime()
				require.Zero(t, h.c.unavailableWeight(h.current()))
				entered, release := h.world.gate(hook + ":" + tB1)

				var wg sync.WaitGroup
				wg.Go(func() {
					if hook == config.HookInspect {
						h.c.InspectAll(h.ctx)
					} else {
						h.c.ProbeAll(h.ctx)
					}
				})
				awaitGate(t, entered)
				tc.change(t, h)
				release()
				wg.Wait()

				v := h.view(tB1)
				require.Empty(t, v.Live)
				require.Zero(t, v.Readiness)
				require.Empty(t, v.Hooks)
				require.InDelta(t, 100, h.c.unavailableWeight(h.current()), 0.001, "an unobserved target is unavailable")

				snap, err := h.store.Load(h.ctx)
				require.NoError(t, err)
				require.NotContains(t, snap.Live, tB1)
				require.NotContains(t, snap.HookRuns, tB1)
			})
		}
	}
}

func TestRestartForgetsObservationsOfChangedDefinitions(t *testing.T) {
	tests := []struct {
		name        string
		change      func(t *testing.T, h *harness)
		othersKept  bool
		unavailable float64
	}{
		{"target changed while down", func(t *testing.T, h *harness) {
			t.Helper()
			h.swap(mustParse(t, h, replaceLine(testTargets, b1Line, b1Moved)))
		}, true, 100},
		{"environment changed while down", func(t *testing.T, h *harness) {
			t.Helper()

			h.cfg.Environment = "staging"
		}, false, 500},
		{"non-JSON extra changed while down", func(t *testing.T, h *harness) {
			t.Helper()
			h.swap(mustParse(t, h, replaceLine(testTargets, b1Line,
				`- {id: b-1/el, node: b-1, weight: 100, image: org/b:t, labels: {client: b, owner: b, role: el, wave: "1"}, hooks: {soak: soak-b}, extra: {value: .nan}}`)))
		}, true, 100},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, testConfig, testTargets)
			h.prime()
			other := h.view(tA2)
			tc.change(t, h)
			h.c = h.newController()

			v := h.view(tB1)
			require.Empty(t, v.Live)
			require.Zero(t, v.Readiness)
			require.Empty(t, v.Hooks)

			if tc.othersKept {
				require.Equal(t, other.Readiness, h.view(tA2).Readiness)
				require.Equal(t, other.Live, h.view(tA2).Live)
			} else {
				require.Zero(t, h.view(tA2).Readiness)
			}

			require.InDelta(t, tc.unavailable, h.c.unavailableWeight(h.current()), 0.001)

			snap, err := h.store.Load(h.ctx)
			require.NoError(t, err)
			require.NotContains(t, snap.Live, tB1)
		})
	}
}
