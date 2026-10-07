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
)

const (
	guaranteeNodeOne    = "node-1"
	guaranteeNodeTwo    = "node-2"
	guaranteeNodeFive   = "node-5"
	guaranteeEchoTarget = "node-5/tool"
	updaterUnavailable  = "updater unavailable"
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
			cfg := replaceLine(usageConfig, "firstBatch: 1, batchSize: 2", "firstBatch: 2, batchSize: 2, retry: {limit: 1}")
			h := newFleet(t, cfg)
			h.world.set(func(w *world) {
				w.running["obs-2/server"] = d2
				w.updateFail["node-5/server"] = updaterUnavailable
				w.updateFail["node-4/server"] = updaterUnavailable
			})
			h.release(imgBravoServer, d2)
			r := h.until(h.active(clientBravo).ID, Halted, 10)
			require.Equal(t, []string{"node-4/server", "node-5/server"}, r.Batches[0].Targets)

			before := h.updates()
			h.world.set(func(w *world) { clear(w.updateFail) })
			h.c = h.newController()
			h.ticks(10, time.Minute)
			require.Equal(t, Halted, h.rollout(r.ID).State)
			require.Equal(t, before, h.updates(), "healing alone cannot reopen a halted batch")
			require.Equal(t, []string{"obs-2/server"}, h.on(d2))
			require.Len(t, h.rollout(r.ID).Batches, 1)

			want := d2

			if resume == "manual retry" {
				require.NoError(t, h.c.Retry(h.ctx, actor, r.ID, "updater recovered"))
			} else {
				want = d3
				h.release(imgBravoServer, want)
				require.Equal(t, Superseded, h.rollout(r.ID).State)
			}

			h.settle(100)
			require.Equal(t, []string{"node-4/server", "node-5/server", "node-6/server", "obs-2/server"}, h.on(want))
		})
	}
}

func TestGuaranteeTransientFailuresRecover(t *testing.T) {
	t.Run("failed update attempt", guaranteeUpdateRetries)
	t.Run("soak execution error", func(t *testing.T) {
		h := newFleet(t, usageConfig)
		h.world.set(func(w *world) { w.runErr[soakServer] = errors.New("cannot start check") })
		h.release(imgAlpha, d2)
		r := h.until(h.active(clientAlpha).ID, Soaking, 10)

		for range 10 {
			if h.rollout(r.ID).Soak.ConsecutiveErrors > 0 {
				break
			}

			h.step()
		}

		r = h.rollout(r.ID)
		require.Equal(t, Soaking, r.State)
		require.Equal(t, 1, r.Soak.ConsecutiveErrors)
		require.Zero(t, r.Soak.Failures)
		h.world.set(func(w *world) { clear(w.runErr) })
		h.settle(100)
		require.Equal(t, Complete, h.rollout(r.ID).State)
		require.Equal(t, h.clientTargets(clientAlpha), h.on(d2))
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
				w.updateFail[id] = updaterUnavailable
			}
		})
		h.release(imgAlpha, d2)
		r := h.until(h.active(clientAlpha).ID, Halted, 20)
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

func guaranteeAllowedHolds(t *testing.T) {
	t.Helper()

	s := guaranteeFleetSimulation(t, usageConfig)
	h := s.h
	require.NoError(t, h.c.SetPolicy(h.ctx, actor, clientCharlie, Policy{Mode: ModeManual, Strategy: careful}))
	require.NoError(t, h.c.SetPolicy(h.ctx, actor, clientBravo, Policy{Mode: ModeAutomated, Strategy: careful}))
	sp, err := h.c.Suspend(h.ctx, SuspendRequest{Actor: actor, Selector: mustSel("node=node-2"), Reason: maintenanceReason, Expires: time.Hour})
	require.NoError(t, err)

	s.suspended = append(s.suspended, propSuspension{sel: sp.Selector, expires: sp.ExpiresAt})
	s.modes[clientCharlie] = ModeManual

	h.world.set(func(w *world) {
		w.updateFail["obs-1/store"] = updaterUnavailable
		w.inspectFail["node-3/server"] = true
		w.notReady["node-8/store"] = true
		w.updateFail[tObs1Server] = updaterUnavailable
		w.runErr["soak-store"] = errors.New("check unavailable")

		for _, target := range h.current().Targets {
			w.registry[target.Image] = d2
		}
	})
	h.clock.Advance(h.cfg.Registry.Poll)
	h.tick()
	aborted := h.active("echo")
	require.NoError(t, h.c.Abort(h.ctx, actor, aborted.ID))
	alpha := h.until(h.active(clientAlpha).ID, Halted, 20)
	bravo := h.until(h.active(clientBravo).ID, Paused, 40)
	require.Equal(t, WaitingForSync, h.active(clientCharlie).State)
	require.Equal(t, Aborted, h.rollout(aborted.ID).State)
	require.Equal(t, Suspended, h.view("node-2/server").Health)
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
		h: h, fleet: &propFleet{}, log: log,
		builds: map[string][]string{}, modes: map[string]string{}, pins: map[string]map[string]string{},
		terminal: map[string]RolloutState{}, manual: map[string]bool{}, strategy: map[string]string{},
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
	require.Equal(t, [][]string{{"node-4"}, {"node-5", "node-6"}}, batchNodes(&rollouts[1]))

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
	require.Equal(t, []string{"node-2/agent", "node-2/server"}, intersect(h.on(d1), h.clientTargets(clientAlpha)))
	require.Zero(t, h.updates()["node-2/server"])
	require.Zero(t, h.updates()["node-2/agent"])

	// The suspension runs out: node-2 goes straight to the latest build in a
	// rollout of its own.
	h.clock.Advance(time.Hour)
	h.settle(100)
	require.Equal(t, h.clientTargets(clientAlpha), h.on(d3)[:len(h.clientTargets(clientAlpha))])

	last := h.rolloutsOf(clientAlpha)[2]
	require.Equal(t, Complete, last.State)
	require.Len(t, last.Targets, 2)
	require.Equal(t, 1, h.updates()["node-2/server"], "d2 was never deployed on node-2")
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
