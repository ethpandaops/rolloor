package reconcile

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/hooks"
)

const (
	guaranteeNodeOne    = "node-1"
	guaranteeNodeTwo    = "node-2"
	guaranteeNodeFive   = "node-5"
	guaranteeEchoTarget = "node-5/tool"
	updaterUnavailable  = "updater unavailable"
	resumeAction        = "resume"
	possiblyAccepted    = "possibly accepted update"
)

func TestGuaranteeDisruptionBudget(t *testing.T) {
	t.Run("shared across groups", guaranteeSharedBudget)
	t.Run("unrelated outage", guaranteeUnrelatedOutage)
	t.Run("unavailable updates are free", guaranteeUnavailableUpdatesAreFree)
	t.Run("zero budget", guaranteeZeroBudget)
	t.Run("one weighted node alone", func(t *testing.T) {
		h := newFleet(t, replaceLine(usageConfig, "maxUnavailable: 25%", "maxUnavailable: 10%"))
		h.world.set(func(w *world) { w.notReady["obs-2/tool"] = true })

		for range h.cfg.ReadinessProbe.FailureThreshold {
			h.c.ProbeAll(h.ctx)
		}

		used, allowed := h.unavailable()
		require.InDelta(t, 0, used, 0)
		require.Less(t, allowed, h.current().NodeWeight(guaranteeNodeFive))
		h.release(imgEcho, d2)
		r := h.active("echo")
		require.Equal(t, [][]string{{guaranteeNodeFive}}, batchNodes(&r))
		h.settle(30)
		require.Equal(t, Complete, h.rollout(r.ID).State)
		require.Equal(t, []string{guaranteeEchoTarget}, h.on(d2))
	})
}

func TestGuaranteeDurableUpdates(t *testing.T) {
	t.Run("landed during restart", guaranteeLandedDuringRestart)
	t.Run("unsaved admission cannot update", func(t *testing.T) {
		h := newFleet(t, usageConfig)
		h.store.FailRollouts = errFake
		h.release(imgAlpha, d2)
		require.Empty(t, h.world.callsFor("update"))
		h.store.FailRollouts = nil
		h.c = h.newController()
		h.settle(100)
		require.Equal(t, h.clientTargets(clientAlpha), h.on(d2))
	})

	const writes = 10000

	cfg := replaceLine(usageConfig, "duration: 5m", "duration: 0s")

	baseline := guaranteeFleetSimulation(t, cfg)
	baseline.proc.writes = writes
	baseline.h.release(imgAlpha, d2)
	baseline.h.settle(30)
	boundaries := writes - baseline.proc.writes

	for stop := range boundaries + 1 {
		t.Run(fmt.Sprintf("crash after %d writes", stop), func(t *testing.T) {
			s := guaranteeFleetSimulation(t, cfg)
			h := s.h
			s.proc.writes = stop

			h.release(imgAlpha, d2)

			for range 30 {
				s.proc.mu.Lock()
				dead := s.proc.dead
				s.proc.mu.Unlock()

				if dead || !h.anyActive() {
					break
				}

				h.step()
			}

			s.start()
			require.False(t, s.down)
			h.settle(30)
			require.NoError(t, s.check())
			require.Equal(t, h.clientTargets(clientAlpha), h.on(d2))

			for _, id := range h.clientTargets(clientAlpha) {
				require.Equal(t, 1, h.updates()[id], "%s update landed once", id)
			}
		})
	}
}

func TestGuaranteeHaltStopsOpenBatch(t *testing.T) {
	for _, resume := range []string{"manual retry", "newer build"} {
		t.Run(resume, func(t *testing.T) {
			cfg := replaceLine(usageConfig, "firstBatch: 1, batchSize: 2", "firstBatch: 2, batchSize: 2")
			h := newFleet(t, cfg)
			bad := []string{tNode4Server, tNode5Server}

			h.world.set(func(w *world) {
				w.running[tObs2Server] = d2

				for _, id := range bad {
					w.breakOnUpdate[id] = true
				}
			})
			h.release(imgBravoServer, d2)
			r := h.until(h.active(clientBravo).ID, Halted, 30)
			require.Equal(t, bad, r.Batches[0].Targets)

			// The broken targets are put back by hand and the fault is fixed;
			// that alone reopens nothing.
			before := h.updates()
			h.revert(bad...)
			h.c = h.newController()
			h.ticks(10, time.Minute)
			require.Equal(t, Halted, h.rollout(r.ID).State)
			require.Equal(t, before, h.updates(), "healing alone cannot reopen a halted batch")
			require.Equal(t, []string{tObs2Server}, h.on(d2))
			require.Len(t, h.rollout(r.ID).Batches, 1)

			want := d2

			if resume == "manual retry" {
				require.NoError(t, h.c.Retry(h.ctx, actor, r.ID, "readiness fixed"))
			} else {
				want = d3
				h.release(imgBravoServer, want)
				require.Equal(t, Superseded, h.rollout(r.ID).State)
			}

			h.settle(100)
			require.Equal(t, []string{tNode4Server, tNode5Server, "node-6/server", tObs2Server}, h.on(want))
		})
	}
}

// revert puts targets back on the first build by hand and fixes whatever
// broke them, as a person recovering a bad deployment would.
func (h *harness) revert(ids ...string) {
	h.world.set(func(w *world) {
		for _, id := range ids {
			w.running[id] = d1
			delete(w.notReady, id)
			delete(w.breakOnUpdate, id)
		}
	})
}

func TestGuaranteeTransientFailuresRecover(t *testing.T) {
	t.Run("failed update attempt", guaranteeUpdateRetries)
	t.Run("readiness unknown past the deadline", func(t *testing.T) {
		h := newFleet(t, usageConfig)
		h.world.set(func(w *world) { w.runErr[config.HookReady] = errors.New("checker unreachable") })
		h.release(imgAlpha, d2)
		r := h.active(clientAlpha)

		for range 3 * int(h.cfg.Strategy.ProgressDeadline/(30*time.Second)) {
			h.step()
		}

		r = h.rollout(r.ID)
		require.Equal(t, Running, r.State, r.Reason)
		require.Equal(t, PhaseUpdating, r.target(tObs1Server).Phase)
		require.Equal(t, []string{tObs1Server}, h.on(d2))
		require.Nil(t, h.view(tObs1Server).Quarantine)

		h.world.set(func(w *world) { clear(w.runErr) })
		h.settle(100)
		require.Equal(t, Complete, h.rollout(r.ID).State)
		require.Equal(t, h.clientTargets(clientAlpha), h.on(d2))
		require.NotContains(t, h.history(r.ID), "rollout.halted")
	})

	t.Run("inspection unknown past the deadline", func(t *testing.T) {
		h := newFleet(t, usageConfig)
		h.world.set(func(w *world) { w.breakOnUpdate[tObs1Server] = true })
		h.release(imgAlpha, d2)
		h.tick()
		r := h.active(clientAlpha)

		// Readiness says no, but what runs there can no longer be confirmed.
		h.world.set(func(w *world) { w.inspectFail[tObs1Server] = true })

		for range 3 * int(h.cfg.Strategy.ProgressDeadline/(30*time.Second)) {
			h.step()
		}

		r = h.rollout(r.ID)
		require.Equal(t, Running, r.State, r.Reason)
		require.Nil(t, h.view(tObs1Server).Quarantine)

		h.world.set(func(w *world) {
			clear(w.inspectFail)
			clear(w.notReady)
			clear(w.breakOnUpdate)
		})
		h.settle(100)
		require.Equal(t, Complete, h.rollout(r.ID).State)
		require.Equal(t, h.clientTargets(clientAlpha), h.on(d2))
		require.NotContains(t, h.history(r.ID), "rollout.halted")
	})
}

func TestGuaranteeAutonomousConvergence(t *testing.T) {
	t.Run("allowed intervention states", guaranteeAllowedHolds)
	t.Run("pending retry after weights change", func(t *testing.T) {
		cfg := replaceLine(usageConfig, "firstBatch: 1, batchSize: 2", "firstBatch: 2, batchSize: 2")
		s := guaranteeFleetSimulation(t, cfg)
		h := s.h
		h.world.set(func(w *world) {
			w.running[tObs1Server] = d2

			for _, id := range alphaRetried {
				w.breakOnUpdate[id] = true
			}
		})
		h.release(imgAlpha, d2)
		r := h.until(h.active(clientAlpha).ID, Halted, 30)
		require.Equal(t, [][]string{{guaranteeNodeOne, guaranteeNodeTwo}}, batchNodes(&r))
		h.swap(h.fleetWith(func(doc string) string {
			doc = strings.ReplaceAll(doc, "node: node-1, weight: 100", "node: node-1, weight: 200")

			return strings.ReplaceAll(doc, "node: node-2, weight: 100", "node: node-2, weight: 200")
		}))
		h.clock.Advance(h.cfg.Strategy.ProgressDeadline + time.Second)
		require.NoError(t, s.converge())

		resumed := h.rollout(r.ID)
		require.Equal(t, r.Batches[0], resumed.Batches[0])
		require.Equal(t, [][]string{{guaranteeNodeOne, guaranteeNodeTwo}, {guaranteeNodeOne}, {guaranteeNodeTwo}, {retryNodeThree}}, batchNodes(&resumed))
	})
}

// guaranteeAllowedHolds reaches every state a person may end, beside failures
// that must heal by themselves, and checks that converge needs nothing else.
func guaranteeAllowedHolds(t *testing.T) {
	t.Helper()

	s := guaranteeFleetSimulation(t, usageConfig)
	h := s.h
	s.declare(false, map[string]config.Group{clientCharlie: {Paused: true}})

	sp, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("node=node-2"), Reason: maintenanceReason, Expires: time.Hour})
	require.NoError(t, err)

	s.suspended = append(s.suspended, propSuspension{sel: sp.Selector, expires: sp.ExpiresAt})

	h.world.set(func(w *world) {
		w.updateFail["obs-1/store"] = updaterUnavailable
		w.inspectFail["node-3/server"] = true
		w.notReady["node-8/store"] = true
		w.breakOnUpdate[tObs1Server] = true
		w.runErr["soak-store"] = errors.New("check unavailable")

		for _, target := range h.current().Targets {
			w.registry[target.Image] = d2
		}
	})
	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()
	aborted := h.active("echo")
	require.NoError(t, h.c.Abort(h.ctx, actor, aborted.ID))
	require.NoError(t, h.pause(h.active(clientBravo).ID, 0))
	alpha := h.until(h.active(clientAlpha).ID, Halted, 30)
	bravo := h.until(h.active(clientBravo).ID, Paused, 40)
	charlie := h.active(clientCharlie)
	require.Equal(t, Running, charlie.State)
	require.Empty(t, charlie.Batches)
	require.Contains(t, charlie.Reason, "config")
	require.Equal(t, Aborted, h.rollout(aborted.ID).State)
	require.Equal(t, Suspended, h.view(tNode2Server).Health)
	require.Equal(t, Halted, h.rollout(alpha.ID).State)
	require.Equal(t, Paused, h.rollout(bravo.ID).State)

	require.NoError(t, s.converge())
	require.NoError(t, s.check())

	for _, v := range h.c.Targets(nil) {
		require.Equal(t, d2, v.Live, v.ID)
		require.Equal(t, Healthy, v.Health, v.ID)
		require.Equal(t, Synced, v.Sync, v.ID)
	}
}

func guaranteeFleetSimulation(t *testing.T, cfg string) *sim {
	t.Helper()

	h := newFleet(t, cfg)
	log := logrus.New()
	log.SetOutput(io.Discard)
	s := &sim{
		h: h, fleet: &propFleet{}, log: log, builds: map[string][]string{},
		terminal: map[string]RolloutState{}, strategy: map[string]string{},
	}

	for _, target := range h.current().Targets {
		s.fleet.targets = append(s.fleet.targets, propTarget{
			id: target.ID, node: target.Node, group: h.current().Group(&target), image: target.Image,
		})
	}

	s.start()
	require.False(t, s.down)

	return s
}
func guaranteeSharedBudget(t *testing.T) {
	t.Helper()

	s := guaranteeFleetSimulation(t, usageConfig)
	h := s.h

	// kilo and alpha both run on node-1 and node-2; their builds land in the
	// same poll. settle checks the shared budget after every step.
	h.world.set(func(w *world) { w.registry[imgKilo] = d2 })
	h.release(imgAlpha, d2)

	kilo, alpha := h.active(clientKilo), h.active(clientAlpha)

	h.settle(200)
	require.NoError(t, s.check())
	require.Equal(t, Complete, h.rollout(kilo.ID).State)
	require.Equal(t, Complete, h.rollout(alpha.ID).State)
	require.Len(t, h.on(d2), len(h.clientTargets(clientKilo))+len(h.clientTargets(clientAlpha)))
}
func TestGuaranteeOneRolloutPerGroup(t *testing.T) {
	h := newFleet(t, usageConfig)

	// A later image joins the group's desired build before weighted nodes move.
	h.release(imgBravoServer, d2)
	first := h.active(clientBravo)
	h.ticks(4, 30*time.Second)

	h.release(imgBravoAgent, d2)
	require.Equal(t, Superseded, h.rollout(first.ID).State)
	next := h.active(clientBravo)
	require.NotEqual(t, first.ID, next.ID)

	active := 0

	for _, r := range h.c.Rollouts() {
		if r.Group == clientBravo && r.State.Active() {
			active++

			require.Equal(t, next.ID, r.ID)
		}
	}

	require.Equal(t, 1, active)

	h.c = h.newController()
	require.Equal(t, next.ID, h.active(clientBravo).ID)

	h.settle(100)

	rollouts := h.rolloutsOf(clientBravo)
	require.Len(t, rollouts, 2)
	require.Equal(t, Superseded, rollouts[0].State)
	require.Equal(t, Complete, rollouts[1].State)
	require.Equal(t, [][]string{{"node-4"}, {guaranteeNodeFive, "node-6"}}, batchNodes(&rollouts[1]))

	for _, id := range h.clientTargets(clientBravo) {
		require.Equal(t, 1, h.updates()[id], "%s updated once", id)
	}

	require.Equal(t, h.clientTargets(clientBravo), h.on(d2))
}
func TestGuaranteeSuspendedTargetsNeverUpdate(t *testing.T) {
	h := newFleet(t, usageConfig)

	_, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("node=node-2"), Reason: "debugging a disk", Expires: time.Hour})
	require.NoError(t, err)

	// Two builds land while node-2 is held; both skip it.
	h.release(imgAlpha, d2)
	h.settle(100)
	h.release(imgAlpha, d3)
	h.settle(100)
	require.Equal(t, []string{tNode2Agent, tNode2Server}, intersect(h.on(d1), h.clientTargets(clientAlpha)))
	require.Zero(t, h.updates()[tNode2Server])
	require.Zero(t, h.updates()[tNode2Agent])

	// The suspension runs out: node-2 goes straight to the latest build in a
	// rollout of its own.
	h.clock.Advance(time.Hour)
	h.settle(100)
	require.Equal(t, h.clientTargets(clientAlpha), h.on(d3)[:len(h.clientTargets(clientAlpha))])

	last := h.rolloutsOf(clientAlpha)[2]
	require.Equal(t, Complete, last.State)
	require.Len(t, last.Targets, 2)
	require.Equal(t, 1, h.updates()[tNode2Server], "d2 was never deployed on node-2")
}
func guaranteeUnrelatedOutage(t *testing.T) {
	t.Helper()

	h := newFleet(t, replaceLine(usageConfig, "maxUnavailable: 25%", "maxUnavailable: 10%"))
	h.world.set(func(w *world) { w.notReady["node-8/store"] = true })

	for range h.cfg.ReadinessProbe.FailureThreshold {
		h.c.ProbeAll(h.ctx)
	}

	require.InDelta(t, 100, h.c.FleetStatus().UnavailableWeight, 0, "unrelated outages count before any rollout exists")

	h.release(imgEcho, d2)
	r := h.active("echo")
	require.Equal(t, WaitingForBudget, r.State)
	require.Empty(t, h.world.callsFor("update"))
	require.Equal(t, Degraded, h.view("node-8/store").Health)
	used, _ := h.unavailable()
	require.InDelta(t, 100, used, 0)

	h.world.set(func(w *world) { delete(w.notReady, "node-8/store") })
	h.settle(30)
	require.Equal(t, Complete, h.rollout(r.ID).State)
	require.Equal(t, []string{"node-5/tool"}, h.on(d2))
}
func guaranteeUnavailableUpdatesAreFree(t *testing.T) {
	t.Helper()

	h := newFleet(t, replaceLine(usageConfig, "duration: 5m", "duration: 0s"))
	h.world.set(func(w *world) {
		for _, target := range h.current().Targets {
			w.notReady[target.ID] = true
			w.recoverOnUpdate[target.ID] = true
			w.registry[target.Image] = d2
		}
	})

	for range h.cfg.ReadinessProbe.FailureThreshold {
		h.c.ProbeAll(h.ctx)
	}

	used, allowed := h.unavailable()
	require.Greater(t, used, allowed)

	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()

	for _, r := range h.c.Rollouts() {
		require.Equal(t, Running, r.State)
		require.NotEmpty(t, r.Batches)

		for _, rt := range r.Targets {
			if rt.Batch > 0 {
				require.True(t, rt.Free)
			}
		}
	}

	h.settle(100)

	for _, v := range h.c.Targets(nil) {
		require.Equal(t, d2, v.Live)
		require.Equal(t, Healthy, v.Health)
	}
}
func TestGuaranteeObservedHealth(t *testing.T) {
	for _, rollout := range []bool{false, true} {
		t.Run(fmt.Sprintf("rollout=%t", rollout), func(t *testing.T) {
			h := newFleet(t, usageConfig)
			id := "node-1/server"

			if rollout {
				h.release(imgAlpha, d2)
				h.until(h.active(clientAlpha).ID, Soaking, 10)

				id = tObs1Server
			}

			before := h.updates()
			h.world.set(func(w *world) { w.notReady[id] = true })

			for range h.cfg.ReadinessProbe.FailureThreshold {
				h.clock.Advance(h.cfg.ReadinessProbe.Period)
				h.tick()
			}

			failed := h.view(id)
			require.Equal(t, Degraded, failed.Health)
			require.Equal(t, Synced, failed.Sync)
			h.world.set(func(w *world) { delete(w.notReady, id) })
			h.clock.Advance(h.cfg.ReadinessProbe.Period)
			h.tick()
			recovered := h.view(id)
			require.Equal(t, Healthy, recovered.Health)
			require.True(t, recovered.Readiness.Since.After(failed.Readiness.Since))
			require.Equal(t, before, h.updates(), "observation changes health without an update")

			if !rollout {
				require.Equal(t, Healthy, h.c.Fleet().Health)
				require.Empty(t, h.c.Rollouts())
			}
		})
	}
}
func guaranteeUpdateRetries(t *testing.T) {
	t.Helper()

	h := newFleet(t, usageConfig)

	const id = tObs1Server

	h.world.set(func(w *world) { w.updateFail[id] = updaterUnavailable })
	h.release(imgAlpha, d2)
	r := h.active(clientAlpha)
	require.Equal(t, Running, r.State)
	require.Equal(t, 1, r.target(id).UpdateAttempts)
	require.False(t, r.target(id).RetryAt.IsZero())
	retryAt := r.target(id).RetryAt

	h.c = h.newController()
	h.clock.Advance(9 * time.Second)
	h.tick()
	require.Equal(t, retryAt, h.rolloutTarget(r.ID, id).RetryAt)
	require.Len(t, h.world.callsFor("update:"+id), 1)
	h.clock.Advance(time.Second)
	h.tick()
	require.Equal(t, 2, h.rolloutTarget(r.ID, id).UpdateAttempts)

	h.world.set(func(w *world) { delete(w.updateFail, id) })
	h.clock.Advance(20 * time.Second)
	h.tick()
	require.Equal(t, 3, h.rolloutTarget(r.ID, id).UpdateAttempts)
	require.Nil(t, h.view(id).Quarantine)
	h.settle(100)
	require.Equal(t, Complete, h.rollout(r.ID).State)
	require.Equal(t, h.clientTargets(clientAlpha), h.on(d2))
	require.Equal(t, 3, h.updates()[id])
}
func guaranteeZeroBudget(t *testing.T) {
	t.Helper()

	h := newFleet(t, replaceLine(usageConfig, "maxUnavailable: 25%", "maxUnavailable: 0"))
	h.release(imgAlpha, d2)
	r := h.active(clientAlpha)

	for range 20 {
		h.step()
	}

	r = h.rollout(r.ID)
	require.Equal(t, WaitingForBudget, r.State)
	require.Equal(t, []string{tObs1Server}, h.on(d2))
}
func guaranteeLandedDuringRestart(t *testing.T) {
	t.Helper()

	h := newFleet(t, usageConfig)
	entered, release := h.world.gate("update:" + tObs1Server)
	done := make(chan struct{})

	h.world.set(func(w *world) { w.registry[imgAlpha] = d2 })
	h.clock.Advance(h.cfg.Registry.Poll)

	go func() {
		defer close(done)

		h.tick()
	}()

	<-entered
	// The admitted update lands without its result reaching the old process.
	h.world.set(func(w *world) { w.running[tObs1Server] = d2 })
	h.c = h.newController()
	h.c.InspectAll(h.ctx)
	h.tick()
	require.Zero(t, h.updates()[tObs1Server])
	require.True(t, h.active(clientAlpha).Targets[0].UpdateDone)
	h.ticks(2, 0)
	require.Equal(t, PhaseReady, h.phases(h.active(clientAlpha))[tObs1Server])
	release()
	<-done
	h.settle(100)

	for _, id := range h.clientTargets(clientAlpha) {
		require.Equal(t, 1, h.updates()[id], "%s update landed once", id)
	}
}

func TestGuaranteeUpdaterExhaustionSkips(t *testing.T) {
	const skipped = "node-4/agent"

	agents := []string{skipped, "node-5/agent", "node-6/agent"}

	for _, tc := range []struct {
		name     string
		fail     func(w *world)
		attempts int
	}{
		{name: "retries run out", fail: func(w *world) { w.updateFail[skipped] = updaterUnavailable }, attempts: 5},
		{name: "the digest never arrives", fail: func(w *world) { w.updateStuck[skipped] = true }, attempts: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newFleet(t, usageConfig)
			h.world.set(tc.fail)
			h.release(imgBravoAgent, d2)
			first := h.until(h.active(clientBravo).ID, Complete, 60)

			// The target is skipped, its node stays reserved while the update
			// may still land, and the rest goes on without a person.
			rt := first.target(skipped)
			require.Equal(t, PhaseSkipped, rt.Phase, rt.Reason)
			require.Equal(t, tc.attempts, rt.UpdateAttempts)
			require.Equal(t, tc.attempts, h.updates()[skipped])
			require.Equal(t, [][]string{{"node-4"}, {guaranteeNodeFive}, {"node-6"}}, batchNodes(&first),
				"node-4's reservation leaves room for one more node at a time")
			require.NotContains(t, h.history(first.ID), "rollout.halted")
			require.Nil(t, h.view(skipped).Quarantine)
			require.Equal(t, agents[1:], intersect(h.on(d2), agents))

			// Once the updater works again a later rollout catches it up.
			h.world.set(func(w *world) {
				clear(w.updateFail)
				clear(w.updateStuck)
			})
			h.settle(100)
			require.Equal(t, agents, intersect(h.on(d2), agents))

			for _, r := range h.rolloutsOf(clientBravo) {
				require.NotContains(t, h.history(r.ID), "rollout.halted")
			}
		})
	}

	t.Run("a lost response that lands gates later batches", guaranteeLostResponseGatesLaterBatches)
	t.Run("no digest skips a deadline after the last dispatch", guaranteeWatchEndsAfterLastDispatch)
	t.Run("the first dispatch's deadline ends retries, not the watch", guaranteeFirstDeadlineEndsRetries)
	t.Run("a restart past the retry limit keeps watching", guaranteeRestartKeepsWatching)
	t.Run("a late build gets a full deadline to be ready", guaranteeLateBuildGetsFullDeadline)
}

// guaranteeLostResponseGatesLaterBatches: the updater accepted a weightless
// canary's only attempt but its response was lost. The build lands later and
// must be ready and soak before anything else moves.
func guaranteeLostResponseGatesLaterBatches(t *testing.T) {
	t.Helper()

	h := newHarness(t, replaceLine(testConfig, "strategy: {batchSize: 2,", "strategy: {firstBatch: 1, batchSize: 2,"), testTargets)
	h.prime()
	h.world.set(func(w *world) { w.updateFail[tA1] = updaterUnavailable })
	h.release(imgA, d2)
	id := h.active("a").ID

	r := h.rollout(id)
	require.Equal(t, []string{tA1}, r.Batches[0].Targets)
	require.Equal(t, PhaseUpdating, r.target(tA1).Phase, r.target(tA1).Reason)
	require.Contains(t, r.target(tA1).Reason, possiblyAccepted)

	h.clock.Advance(20 * time.Second)
	h.world.set(func(w *world) {
		delete(w.updateFail, tA1)
		w.running[tA1] = d2
		w.notReady[tA1] = true
	})
	h.ticks(3, 10*time.Second)

	r = h.rollout(id)
	require.Equal(t, Running, r.State, r.Reason)
	require.Len(t, r.Batches, 1, "nothing moves past the canary before its build is ready")
	require.True(t, r.target(tA1).Updated)
	require.Equal(t, PhaseUpdating, r.target(tA1).Phase)

	h.world.set(func(w *world) { delete(w.notReady, tA1) })
	h.tick()
	require.Equal(t, Soaking, h.rollout(id).State)
	h.tick()

	soak := h.lastSoakInput()
	require.Len(t, soak.Updated, 1)
	require.Equal(t, tA1, soak.Updated[0].ID, "the soak compares the late build")
	require.Len(t, h.rollout(id).Batches, 1)
	require.Empty(t, h.world.callsFor("update:"+tA2))

	r = h.drive(id, 80, 20*time.Second)
	require.Equal(t, Complete, r.State, r.Reason)
	require.Equal(t, PhasePassed, r.target(tA1).Phase)
	require.Len(t, h.world.callsFor("update:"+tA1), 1, "the accepted update is never sent again")
	require.NotContains(t, h.history(id), "rollout.halted")
}

func guaranteeWatchEndsAfterLastDispatch(t *testing.T) {
	t.Helper()

	h := newHarness(t, replaceLine(testConfig, "retry: {limit: 1}", "retry: {limit: 2}"), retryFleet)
	h.prime()
	h.world.set(func(w *world) { w.updateFail[tN1A] = updaterUnavailable })
	h.release(imgA, d2)
	id := h.active("a").ID
	h.clock.Advance(10 * time.Second)
	h.tick()
	require.Len(t, h.world.callsFor("update:"+tN1A), 2)

	// The second and last attempt may have been accepted.
	h.clock.Advance(h.cfg.Strategy.ProgressDeadline - time.Second)
	h.tick()
	rt := h.rolloutTarget(id, tN1A)
	require.Equal(t, PhaseUpdating, rt.Phase, rt.Reason)
	require.Contains(t, rt.Reason, "retry limit")
	require.Equal(t, Running, h.rollout(id).State)

	h.clock.Advance(time.Second)
	h.tick()
	rt = h.rolloutTarget(id, tN1A)
	require.Equal(t, PhaseSkipped, rt.Phase, rt.Reason)
	require.False(t, rt.HoldUntil.IsZero(), "the update could still land")
	require.Equal(t, Complete, h.drive(id, 5, 0).State)
	require.Len(t, h.world.callsFor("update:"+tN1A), 2)
	require.NotContains(t, h.history(id), "rollout.halted")
}

func guaranteeFirstDeadlineEndsRetries(t *testing.T) {
	t.Helper()

	cfg := replaceLine(testConfig, "retry: {limit: 1}", "retry: {limit: 5, backoff: {duration: 20s, factor: 1, maxDuration: 20s}}")
	h := newHarness(t, cfg, retryFleet)
	h.prime()
	h.world.set(func(w *world) { w.updateFail[tN1A] = updaterUnavailable })
	h.release(imgA, d2)
	id := h.active("a").ID

	// Attempts at 0s, 20s and 40s; the one due at 60s is past the deadline.
	for range 3 {
		h.clock.Advance(20 * time.Second)
		h.tick()
	}

	rt := h.rolloutTarget(id, tN1A)
	require.Len(t, h.world.callsFor("update:"+tN1A), 3)
	require.Equal(t, PhaseUpdating, rt.Phase, rt.Reason)
	require.Contains(t, rt.Reason, possiblyAccepted)

	// The third attempt was accepted after all and lands within its window.
	h.world.set(func(w *world) {
		delete(w.updateFail, tN1A)
		w.running[tN1A] = d2
	})
	h.clock.Advance(20 * time.Second)
	h.tick()
	require.Equal(t, PhaseReady, h.rolloutTarget(id, tN1A).Phase)
	require.Equal(t, Complete, h.drive(id, 20, 20*time.Second).State)
	require.Len(t, h.world.callsFor("update:"+tN1A), 3)
}

func guaranteeRestartKeepsWatching(t *testing.T) {
	t.Helper()

	h := newHarness(t, testConfig, retryFleet)
	h.prime()
	h.world.set(func(w *world) { w.updateStuck[tN1A] = true })
	h.release(imgA, d2)
	id := h.active("a").ID

	// The process stops while its only allowed update runs: the store holds
	// the dispatch but no result.
	h.store.mu.Lock()
	h.store.rollouts[id].Targets[0].UpdateDone = false
	h.store.mu.Unlock()
	h.clock.Advance(30 * time.Second)
	h.c = h.newController()

	for range 2 {
		h.tick()
		rt := h.rolloutTarget(id, tN1A)
		require.Equal(t, PhaseUpdating, rt.Phase, rt.Reason)
		require.Contains(t, rt.Reason, possiblyAccepted)
		h.clock.Advance(15 * time.Second)
	}

	// A deadline after the stored dispatch, not after the restart.
	h.tick()
	rt := h.rolloutTarget(id, tN1A)
	require.Equal(t, PhaseSkipped, rt.Phase, rt.Reason)
	require.Len(t, h.world.callsFor("update:"+tN1A), 1, "the interrupted update is never sent again")
}

func guaranteeLateBuildGetsFullDeadline(t *testing.T) {
	t.Helper()

	for _, tc := range []struct {
		name string
		fail func(w *world)
	}{
		{name: "accepted update", fail: func(w *world) { w.updateStuck[tN1A] = true }},
		{name: "lost response", fail: func(w *world) { w.updateFail[tN1A] = updaterUnavailable }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, testConfig, retryFleet)
			h.prime()
			h.world.set(tc.fail)
			h.release(imgA, d2)
			id := h.active("a").ID

			// The build lands 40s after its dispatch and never becomes ready.
			h.clock.Advance(40 * time.Second)
			h.world.set(func(w *world) {
				clear(w.updateFail)
				w.running[tN1A] = d2
				w.notReady[tN1A] = true
			})
			h.tick()

			seen := h.rolloutTarget(id, tN1A).DigestSeenAt
			require.False(t, seen.IsZero())

			for range 9 {
				h.clock.Advance(5 * time.Second)
				h.tick()
				require.Equal(t, Running, h.rollout(id).State, "conclusively not ready, but within a deadline of the build appearing")
			}

			h.clock.Advance(5 * time.Second)
			h.tick()

			r := h.rollout(id)
			require.Equal(t, Halted, r.State, r.Reason)
			require.False(t, r.Batches[0].EndedAt.Before(seen.Add(h.cfg.Strategy.ProgressDeadline)))
			require.Len(t, h.world.callsFor("update:"+tN1A), 1)
		})
	}
}

func TestGuaranteeSoakErrorsRecover(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail func(w *world)
	}{
		{name: "the check cannot start", fail: func(w *world) { w.runErr[soakServer] = errors.New("cannot start check") }},
		{name: "the check could not compare", fail: func(w *world) {
			w.runResult[soakServer] = hooks.Result{Program: soakServer, ExitCode: 3, Reason: "metrics unavailable"}
		}},
		{name: "the check timed out", fail: func(w *world) {
			w.runResult[soakServer] = hooks.Result{Program: soakServer, ExitCode: -1, TimedOut: true, Reason: "timed out"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newFleet(t, usageConfig)
			h.world.set(tc.fail)
			h.release(imgAlpha, d2)
			r := h.until(h.active(clientAlpha).ID, Soaking, 10)

			// Thirty intervals, six times the soak duration: neither a halt nor
			// a pass.
			for range 60 {
				h.step()
			}

			r = h.rollout(r.ID)
			require.Equal(t, Soaking, r.State, r.Reason)
			require.Len(t, r.Batches, 1)
			require.False(t, r.Batches[0].Passed)
			require.GreaterOrEqual(t, r.Soak.ConsecutiveErrors, 25)
			require.Len(t, r.Soak.Checks, r.Soak.ConsecutiveErrors)
			require.Zero(t, r.Soak.Failures)
			require.Zero(t, r.Soak.Streak)

			for _, check := range r.Soak.Checks {
				require.True(t, check.Error)
				require.False(t, check.OK)
			}

			require.Equal(t, []string{tObs1Server}, h.on(d2))

			h.world.set(func(w *world) {
				clear(w.runErr)
				clear(w.runResult)
			})
			h.settle(100)
			require.Equal(t, Complete, h.rollout(r.ID).State)
			require.Equal(t, h.clientTargets(clientAlpha), h.on(d2))
			require.NotContains(t, h.history(r.ID), "rollout.halted")
		})
	}
}

func TestGuaranteeConfigPause(t *testing.T) {
	t.Run("top level holds every group and keeps observing", func(t *testing.T) {
		h := newFleet(t, usageConfig)
		h.declare(true, nil)

		for _, g := range h.c.Fleet().Groups {
			require.True(t, g.Paused, g.Name)
		}

		resolves := h.world.resolves()
		h.world.set(func(w *world) { w.registry[imgBravoServer] = d2 })
		h.release(imgAlpha, d2)
		alpha := h.active(clientAlpha)
		_, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=alpha"), Force: true})
		require.NoError(t, err)

		for range 30 {
			h.step()
		}

		alpha = h.rollout(alpha.ID)
		require.Equal(t, Running, alpha.State)
		require.Empty(t, alpha.Batches)
		require.Contains(t, alpha.Reason, "config")
		require.Equal(t, Running, h.active(clientBravo).State)
		require.Empty(t, h.world.callsFor("update"))
		require.Greater(t, h.world.resolves(), resolves)
		require.Equal(t, OutOfSync, h.view(tObs1Server).Sync)

		// A newer build is still noticed.
		h.release(imgAlpha, d3)
		require.Equal(t, Superseded, h.rollout(alpha.ID).State)
		require.Equal(t, "3333333", h.active(clientAlpha).Digest)
		require.Empty(t, h.world.callsFor("update"))

		// Clearing the declaration resumes without any verb.
		h.declare(false, nil)
		h.settle(200)
		require.Equal(t, h.clientTargets(clientAlpha), h.on(d3))
		require.Equal(t, []string{tNode4Server, tNode5Server, "node-6/server", tObs2Server}, h.on(d2))
	})

	t.Run("a group pause lets the open batch finish and admits nothing more", func(t *testing.T) {
		h := newFleet(t, usageConfig)
		h.release(imgAlpha, d2)
		r := h.until(h.active(clientAlpha).ID, Soaking, 10)
		h.declare(false, map[string]config.Group{clientAlpha: {Paused: true}})
		h.world.set(func(w *world) { w.registry[imgKilo] = d2 })

		for range 30 {
			h.step()
		}

		r = h.rollout(r.ID)
		require.Equal(t, Running, r.State)
		require.Len(t, r.Batches, 1)
		require.True(t, r.Batches[0].Passed, "the open batch soaked to its end")
		require.Contains(t, r.Reason, "config")
		require.Equal(t, []string{tObs1Server}, intersect(h.on(d2), h.clientTargets(clientAlpha)))
		require.NotEmpty(t, intersect(h.on(d2), h.clientTargets(clientKilo)), "other groups carry on")

		alpha, _, _ := h.c.Group(clientAlpha)
		kilo, _, _ := h.c.Group(clientKilo)

		require.True(t, alpha.Paused)
		require.False(t, kilo.Paused)
		require.True(t, h.c.GroupPaused(clientAlpha))
		require.False(t, h.c.GroupPaused(clientKilo))

		h.declare(false, nil)
		h.settle(200)
		require.Equal(t, Complete, h.rollout(r.ID).State)
		require.Subset(t, h.on(d2), h.clientTargets(clientAlpha))
	})

	t.Run("an undispatched update waits without starting its clock", func(t *testing.T) {
		h, id, finish := queueUpdates(t, queuedFleet)
		h.declare(false, map[string]config.Group{"a": {Paused: true}})
		finish()

		// Forced or not, and for longer than the progress deadline: the
		// dispatched update is still observed, the queued one never starts.
		_, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=a"), Force: true})
		require.NoError(t, err)
		h.ticks(4, h.cfg.Strategy.ProgressDeadline)

		r := h.rollout(id)
		require.Equal(t, Running, r.State)
		require.Equal(t, PhaseReady, r.target(tN1A).Phase)

		queued := r.target(tN2A)
		require.Equal(t, PhasePending, queued.Phase)
		require.Zero(t, queued.UpdateAttempts)
		require.True(t, queued.UpdatedAt.IsZero())
		require.Empty(t, h.world.callsFor("update:"+tN2A))

		h.declare(false, nil)
		h.tick()
		require.Len(t, h.world.callsFor("update:"+tN2A), 1)
		require.Equal(t, Complete, h.drive(id, 10, 0).State)
	})

	t.Run("an interrupted update waits until the pause clears", func(t *testing.T) {
		h := newHarness(t, replaceLine(testConfig, "retry: {limit: 1}", "retry: {limit: 5}"), retryFleet)
		h.prime()
		h.world.set(func(w *world) { w.updateStuck[tN1A] = true })
		h.release(imgA, d2)
		id := h.active("a").ID

		// The process stops while the update runs and comes back paused.
		h.c.mu.Lock()
		h.c.rollouts[id].Targets[0].UpdateDone = false
		require.NoError(t, h.c.saveRollout(h.ctx, h.c.rollouts[id]))
		h.c.mu.Unlock()
		h.cfg.Paused = true
		h.c = h.newController()
		h.world.set(func(w *world) { delete(w.updateStuck, tN1A) })
		h.ticks(3, 0)
		require.Len(t, h.world.callsFor("update:"+tN1A), 1)
		require.Equal(t, 1, h.rolloutTarget(id, tN1A).UpdateAttempts)

		h.declare(false, nil)
		h.tick()
		require.Len(t, h.world.callsFor("update:"+tN1A), 2, "the interrupted update runs again")
		require.Equal(t, Complete, h.drive(id, 20, 20*time.Second).State)
	})

	t.Run("a failed update waits to retry until the pause clears", func(t *testing.T) {
		cfg := replaceLine(testConfig, "retry: {limit: 1}", "retry: {limit: 5}")
		cfg = replaceLine(cfg, "progressDeadline: 50s", "progressDeadline: 1h")
		h := newHarness(t, cfg, retryFleet)
		h.prime()
		h.world.set(func(w *world) { w.updateFail[tN1A] = boom })
		h.release(imgA, d2)
		id := h.active("a").ID
		h.declare(true, nil)
		h.world.set(func(w *world) { delete(w.updateFail, tN1A) })
		h.ticks(5, time.Minute)
		require.Len(t, h.world.callsFor("update:"+tN1A), 1)
		require.Equal(t, 1, h.rolloutTarget(id, tN1A).UpdateAttempts)
		require.InDelta(t, 100, h.c.FleetStatus().UnavailableWeight, 0, "the dispatched target keeps its reservation")

		h.declare(false, nil)
		require.Equal(t, Complete, h.drive(id, 20, 20*time.Second).State)
		require.Len(t, h.world.callsFor("update:"+tN1A), 2)
	})
}

func TestGuaranteeOperatorPauseExpires(t *testing.T) {
	// betweenBatches pauses group a's rollout once its first batch passed.
	betweenBatches := func(t *testing.T, expires time.Duration) (*harness, string) {
		t.Helper()

		h := newHarness(t, testConfig, testTargets)
		h.prime()
		h.release(imgA, d2)
		id := h.active("a").ID
		h.ticks(4, 0)
		h.ticks(4, 20*time.Second)
		r := h.rollout(id)
		require.Nil(t, r.CurrentBatch())
		require.NoError(t, h.pause(id, expires))
		require.Equal(t, Paused, h.rollout(id).State)

		return h, id
	}

	t.Run("expiry resumes it, across a restart", func(t *testing.T) {
		h, id := betweenBatches(t, time.Hour)
		require.Equal(t, h.clock.Now().Add(time.Hour), h.rollout(id).PauseExpiresAt)

		h.c = h.newController()
		h.ticks(5, 11*time.Minute)
		require.Equal(t, Paused, h.rollout(id).State)
		require.Len(t, h.rollout(id).Batches, 1)

		h.clock.Advance(5 * time.Minute)
		h.tick()

		r := h.rollout(id)
		require.Equal(t, Running, r.State)
		require.True(t, r.PauseExpiresAt.IsZero())
		require.Len(t, r.Batches, 2)
		require.Contains(t, h.notes.actions(), "pause.expired")
		require.Equal(t, Complete, h.drive(id, 60, 20*time.Second).State)
	})

	t.Run("a pending pause that expires before its batch ends never pauses", func(t *testing.T) {
		h := newHarness(t, testConfig, testTargets)
		h.prime()
		h.release(imgA, d2)
		id := h.active("a").ID
		require.NoError(t, h.pause(id, 30*time.Second))
		require.True(t, h.rollout(id).PausePending)

		h.ticks(4, 0)
		h.ticks(4, 20*time.Second)

		r := h.rollout(id)
		require.Equal(t, Running, r.State)
		require.False(t, r.PausePending)
		require.True(t, r.Batches[0].Passed)
		require.NotContains(t, h.history(id), "rollout.paused")
		require.Equal(t, Complete, h.drive(id, 60, 20*time.Second).State)
	})

	t.Run("promote resumes it before the expiry", func(t *testing.T) {
		h, id := betweenBatches(t, 0)
		require.Equal(t, h.clock.Now().Add(24*time.Hour), h.rollout(id).PauseExpiresAt)
		require.NoError(t, h.c.Promote(h.ctx, actor, id))

		r := h.rollout(id)
		require.Equal(t, Running, r.State)
		require.True(t, r.PauseExpiresAt.IsZero())
		require.Equal(t, Complete, h.drive(id, 60, 20*time.Second).State)
	})

	t.Run("expiry never lifts a halt", func(t *testing.T) {
		h := newHarness(t, testConfig, testTargets)
		h.prime()
		h.world.set(func(w *world) { w.soakFail[soakA] = true })
		h.release(imgA, d2)
		id := h.active("a").ID
		require.NoError(t, h.pause(id, 2*time.Minute))
		h.ticks(5, 0)
		h.clock.Advance(20 * time.Second)
		h.tick()
		require.Equal(t, Halted, h.rollout(id).State)

		h.clock.Advance(2 * time.Minute)
		h.tick()

		r := h.rollout(id)
		require.Equal(t, Halted, r.State)
		require.False(t, r.PausePending)
		require.NoError(t, h.c.Retry(h.ctx, actor, id, "probe fixed"))
	})

	t.Run("promote does not override a config pause", func(t *testing.T) {
		h, id := betweenBatches(t, time.Hour)
		h.declare(false, map[string]config.Group{"a": {Paused: true}})
		require.NoError(t, h.c.Promote(h.ctx, actor, id))
		h.ticks(3, 0)

		r := h.rollout(id)
		require.Equal(t, Running, r.State)
		require.Len(t, r.Batches, 1)
		require.Contains(t, r.Reason, "config")

		h.declare(false, nil)
		require.Equal(t, Complete, h.drive(id, 60, 20*time.Second).State)
	})
}

func TestGuaranteeAbortKeepsBuild(t *testing.T) {
	h := newFleet(t, usageConfig)
	d4 := "sha256:" + strings.Repeat("4", 64)

	h.release(imgAlpha, d2)
	r := h.active(clientAlpha)
	h.step()
	require.NoError(t, h.c.Abort(h.ctx, actor, r.ID))

	// The group stays behind; nothing reopens on its own, across a restart.
	for range 5 {
		h.step()
	}

	h.c = h.newController()
	h.step()
	require.False(t, h.anyActive())

	// A person syncs the same build: a new rollout finishes the job.
	_, err := h.c.Sync(h.ctx, SyncRequest{Actor: actor, Selector: mustSel("client=alpha")})
	require.NoError(t, err)
	h.settle(100)
	require.Equal(t, h.clientTargets(clientAlpha), intersect(h.on(d2), h.clientTargets(clientAlpha)))

	// Aborted again on the next build; a newer build rolls without a sync.
	h.release(imgAlpha, d3)
	require.NoError(t, h.c.Abort(h.ctx, actor, h.active(clientAlpha).ID))
	h.release(imgAlpha, d4)
	h.settle(100)
	require.Equal(t, h.clientTargets(clientAlpha), intersect(h.on(d4), h.clientTargets(clientAlpha)))
}

func TestGuaranteeSuspensionExpires(t *testing.T) {
	for _, lift := range []string{"expiry", resumeAction} {
		t.Run(lift, func(t *testing.T) {
			h := newFleet(t, usageConfig)
			sel := mustSel("node=node-2")
			_, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: sel, Reason: maintenanceReason, Expires: 2 * time.Hour})
			require.NoError(t, err)

			h.release(imgAlpha, d2)
			h.settle(100)
			require.Equal(t, []string{tNode2Agent, tNode2Server}, intersect(h.on(d1), h.clientTargets(clientAlpha)))
			require.Equal(t, Suspended, h.view(tNode2Server).Health)

			if lift == resumeAction {
				_, err = h.c.Resume(h.ctx, actor, sel)
				require.NoError(t, err)
			} else {
				h.clock.Advance(2 * time.Hour)
			}

			// The lifted node catches up in a rollout of its own.
			h.settle(100)
			require.Empty(t, h.c.Suspensions())
			require.Equal(t, h.clientTargets(clientAlpha), intersect(h.on(d2), h.clientTargets(clientAlpha)))

			last := h.rolloutsOf(clientAlpha)[1]
			require.Equal(t, Complete, last.State)
			require.Len(t, last.Targets, 2)
		})
	}
}
