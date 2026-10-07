package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/hooks"
	"github.com/ethpandaops/rolloor/internal/reconcile"
	"github.com/ethpandaops/rolloor/internal/registry"
	"github.com/ethpandaops/rolloor/internal/targets"
)

type reloadWorld struct{}

func (reloadWorld) Resolve(context.Context, string) (registry.Resolved, error) {
	return registry.Resolved{Digest: "sha256:reload"}, nil
}

func (reloadWorld) Run(context.Context, string, string, string, any) (hooks.Result, error) {
	return hooks.Result{OK: true, Reason: "sha256:reload"}, nil
}

func reloadController(t *testing.T, path string) (*reconcile.Controller, *reconcile.MemoryStore, *atomic.Pointer[targets.Set]) {
	t.Helper()

	cfg, err := config.Load(path)
	require.NoError(t, err)
	set, err := targets.Load(cfg.TargetsDir, rulesFor(cfg))
	require.NoError(t, err)

	published := &atomic.Pointer[targets.Set]{}
	published.Store(set)

	disk := reconcile.NewMemoryStore()
	c, err := reconcile.New(context.Background(), &reconcile.Options{Config: cfg, Targets: published.Load, Resolver: reloadWorld{}, Runner: reloadWorld{}, Store: disk, Log: logrus.New()})
	require.NoError(t, err)

	return c, disk, published
}

func TestWatchAutomationRetriesUnchangedFailedVersion(t *testing.T) {
	_, path := writeFixture(t)
	c, _, _ := reloadController(t, path)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)

	bad := append(raw, []byte("paused: true\ngroups: {a: {strategy: extra}}\nstrategies: {extra: {batchSize: 100%}}\n")...)
	require.NoError(t, os.WriteFile(path, bad, 0o644))
	info, err := os.Stat(path)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()

	log := logrus.New()
	attempted := make(chan struct{}, 1)
	log.AddHook(reloadLogHook{attempted: attempted})

	done := make(chan error, 1)
	go func() { done <- watchAutomation(ctx, path, c, log) }()

	select {
	case <-attempted:
	case <-ctx.Done():
		t.Fatal("reload did not attempt the changed file")
	}

	require.False(t, c.GroupPaused("a"))

	good := append(raw, []byte("paused: true\n")...)
	good = append(good, []byte("#"+strings.Repeat(" ", len(bad)-len(good)-2)+"\n")...)
	require.NoError(t, os.WriteFile(path, good, 0o644))
	require.NoError(t, os.Chtimes(path, info.ModTime(), info.ModTime()))
	require.Eventually(t, func() bool { return c.GroupPaused("a") }, 7*time.Second, 10*time.Millisecond)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

type reloadLogHook struct {
	attempted chan<- struct{}
}

func (reloadLogHook) Levels() []logrus.Level { return []logrus.Level{logrus.WarnLevel} }

func (h reloadLogHook) Fire(*logrus.Entry) error {
	select {
	case h.attempted <- struct{}{}:
	default:
	}

	return nil
}

func TestAutomationReloadKeepsEffectiveConfigOnFailures(t *testing.T) {
	_, path := writeFixture(t)
	c, _, _ := reloadController(t, path)
	r := automationReload{}
	require.NoError(t, r.apply(context.Background(), path, c))
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, []byte("paused: ["), 0o644))
	require.Error(t, r.apply(context.Background(), path, c))
	paused, failed, _ := c.AutomationStatus()
	require.False(t, paused)
	require.True(t, failed)
	require.Error(t, r.apply(context.Background(), path, c))
	require.NoError(t, os.WriteFile(path, append(raw, []byte("paused: true\n")...), 0o644))
	require.NoError(t, r.apply(context.Background(), path, c))
	paused, failed, groups := c.AutomationStatus()
	require.True(t, paused)
	require.False(t, failed)
	require.True(t, groups["a"])
	require.NoError(t, r.apply(context.Background(), path, c))
	require.Error(t, r.apply(context.Background(), filepath.Join(t.TempDir(), "missing"), c))
	_, failed, _ = c.AutomationStatus()
	require.True(t, failed)
	require.NoError(t, r.apply(context.Background(), path, c))
	_, failed, _ = c.AutomationStatus()
	require.False(t, failed)
}

func TestChangedTargetDefinitionForgetsStoredObservations(t *testing.T) {
	_, path := writeFixture(t)
	for _, field := range []string{"node", "address", "image", "weight", "labels", "hooks", "extra"} {
		t.Run(field, func(t *testing.T) {
			c, disk, published := reloadController(t, path)
			old := published.Load()

			c.InspectAll(context.Background())
			c.ProbeAll(context.Background())
			snap, err := disk.Load(context.Background())
			require.NoError(t, err)
			require.True(t, snap.Live[old.Targets[0].ID].Readiness.Ready)
			next := old.Targets[0]

			switch field {
			case "node":
				next.Node = "b"
			case "address":
				next.Address = "new-endpoint"
			case "image":
				next.Image = "org/new:t"
			case "weight":
				next.Weight = 2
			case "labels":
				next.Labels = map[string]string{"client": "a", "owner": "a", "zone": "new"}
			case "hooks":
				next.Hooks = map[string]string{config.HookReady: config.HookInspect}
			case "extra":
				next.Extra = map[string]any{"container": "new"}
			}

			current, err := targets.New([]targets.Target{next}, &targets.Rules{GroupLabel: "client", OwnerLabel: "owner", KnownHooks: config.TargetHooks})
			require.NoError(t, err)
			c.ReplaceTargets(context.Background(), func() (*targets.Set, *targets.Set) {
				return published.Swap(current), current
			})
			snap, err = disk.Load(context.Background())
			require.NoError(t, err)
			require.NotContains(t, snap.Live, next.ID)
			require.NotContains(t, snap.HookRuns, next.ID)
			require.Equal(t, current.TotalWeight(), c.FleetStatus().UnavailableWeight)
		})
	}
}
