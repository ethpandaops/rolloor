// Command rolloor runs the controller for one environment, validates its
// files, or runs a single hook for script authors.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"

	"github.com/ethpandaops/rolloor/internal/api"
	"github.com/ethpandaops/rolloor/internal/auth"
	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/hooks"
	"github.com/ethpandaops/rolloor/internal/metrics"
	"github.com/ethpandaops/rolloor/internal/observability"
	"github.com/ethpandaops/rolloor/internal/reconcile"
	"github.com/ethpandaops/rolloor/internal/registry"
	"github.com/ethpandaops/rolloor/internal/store"
	"github.com/ethpandaops/rolloor/internal/targets"
	"github.com/ethpandaops/rolloor/internal/ui"
)

// version is set at build time.
var version = "dev"

const (
	tickEvery    = 5 * time.Second
	watchEvery   = 5 * time.Second
	shutdownWait = 10 * time.Second
)

func main() {
	if err := newRoot().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRoot() *cobra.Command {
	var configPath string

	root := &cobra.Command{
		Use:           "rolloor",
		Short:         "Staged, health-gated container rollouts for one environment",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version,
	}

	root.PersistentFlags().StringVar(&configPath, "config", "/etc/rolloor/config.yaml", "path to config.yaml")

	root.AddCommand(
		&cobra.Command{
			Use:   "serve",
			Short: "Run the controller, API and metrics",
			RunE: func(cmd *cobra.Command, _ []string) error {
				return serve(cmd.Context(), configPath)
			},
		},
		&cobra.Command{
			Use:   "validate",
			Short: "Check the config, targets and teams files",
			RunE: func(cmd *cobra.Command, _ []string) error {
				return validate(cmd.OutOrStdout(), configPath)
			},
		},
		newHookCommand(&configPath),
	)

	return root
}

// load reads the config and the targets once.
func load(configPath string) (*config.Config, *targets.Set, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, nil, err
	}

	set, err := targets.Load(cfg.TargetsDir, rulesFor(cfg))
	if err != nil {
		return nil, nil, err
	}

	return cfg, set, nil
}

func rulesFor(cfg *config.Config) *targets.Rules {
	return &targets.Rules{
		GroupLabel: cfg.Labels.Group,
		OwnerLabel: cfg.Labels.Owner,
		WaveLabel:  cfg.Labels.Wave,
		HooksDir:   cfg.Hooks.Dir,
		KnownHooks: config.TargetHooks,
	}
}

func validate(out interface{ Write([]byte) (int, error) }, configPath string) error {
	cfg, set, err := load(configPath)
	if err != nil {
		return err
	}

	for hook, prog := range cfg.Hooks.Defaults {
		if prog == "" {
			continue
		}

		if _, err := os.Stat(cfg.Hooks.Dir + "/" + prog); err != nil {
			return fmt.Errorf("hooks.defaults.%s: %w", hook, err)
		}
	}

	if cfg.TeamsFile != "" {
		if _, err := loadTeams(cfg.TeamsFile); err != nil {
			return err
		}
	}

	fmt.Fprintf(out, "ok: %d targets on %d nodes in %d groups; total weight %g; maxUnavailable %s\n",
		set.Len(), len(set.Nodes()), len(set.Groups()), set.TotalWeight(), cfg.DisruptionBudget.MaxUnavailable.String())

	return nil
}

// loadTeams reads owner value → identities.
func loadTeams(path string) (map[string][]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("teams file: %w", err)
	}

	teams := map[string][]string{}
	if err := yaml.Unmarshal(raw, &teams); err != nil {
		return nil, fmt.Errorf("teams file: %w", err)
	}

	return teams, nil
}

func newHookCommand(configPath *string) *cobra.Command {
	var (
		targetID string
		program  string
		desired  string
	)

	cmd := &cobra.Command{
		Use:   "hook <inspect|update|ready|soak>",
		Short: "Run one hook against one target with the document the controller would send",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runHook(cmd.Context(), cmd.OutOrStdout(), *configPath, args[0], targetID, program, desired)
		},
	}

	cmd.Flags().StringVar(&targetID, "target", "", "target id (required for target hooks)")
	cmd.Flags().StringVar(&program, "program", "", "program name; defaults to the target's probe or the configured default")
	cmd.Flags().StringVar(&desired, "desired", "", "digest to send as desired; resolved from the registry when empty")

	return cmd
}

func runHook(ctx context.Context, out interface{ Write([]byte) (int, error) }, configPath, hook, targetID, program, desired string) error {
	cfg, set, err := load(configPath)
	if err != nil {
		return err
	}

	log, err := observability.NewLogger("debug", "text")
	if err != nil {
		return err
	}

	runner, err := hooks.NewRunner(cfg.Hooks.Dir, cfg.Hooks.Timeout, cfg.Environment, log)
	if err != nil {
		return err
	}

	if !slices.Contains(config.TargetHooks, hook) {
		return fmt.Errorf("unknown hook %q", hook)
	}

	t, ok := set.Get(targetID)
	if !ok {
		return fmt.Errorf("target %q is not in the targets files", targetID)
	}

	in := reconcile.HookInput{Target: t}

	if desired == "" && hook != config.HookSoak && hook != config.HookReady {
		resolver, rerr := registry.NewResolver(registryOptions(cfg), log)
		if rerr != nil {
			return rerr
		}

		res, rerr := resolver.Resolve(ctx, t.Image)
		if rerr != nil {
			return fmt.Errorf("resolve %s (pass --desired to skip): %w", t.Image, rerr)
		}

		desired = res.Digest
	}

	if desired != "" {
		in.Desired, in.DesiredRef = desired, reconcile.DesiredRef(t.Image, desired)
	}

	var input any = in

	if hook == config.HookSoak {
		updated := []struct {
			targets.Target
			UpdatedAt time.Time `json:"updatedAt"`
		}{{Target: t, UpdatedAt: time.Now().UTC()}}
		input = map[string]any{"updated": updated, "remaining": []targets.Target{}, "rollout": map[string]any{"id": "manual", "group": set.Group(&t), "batch": 1, "wave": set.Wave(&t)}}
	}

	if program == "" {
		program = t.Hooks[hook]
	}

	if program == "" {
		program = cfg.Hooks.Defaults[hook]
	}

	if program == "" {
		return fmt.Errorf("no program configured for hook %s", hook)
	}

	res, err := runner.Run(ctx, program, hook, targetID, input)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")

	if err := enc.Encode(map[string]any{"result": res, "stdout": res.Stdout}); err != nil {
		return err
	}

	if !res.OK {
		return errors.New("hook did not pass")
	}

	return nil
}

func serve(ctx context.Context, configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	log, err := observability.NewLogger(cfg.Log.Level, cfg.Log.Format)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	authorizer, teams, err := buildAuthorizer(ctx, cfg, log)
	if err != nil {
		return err
	}

	db, err := store.Open(ctx, cfg.DataDir)
	if err != nil {
		return err
	}
	defer db.Close()

	runner, err := hooks.NewRunner(cfg.Hooks.Dir, cfg.Hooks.Timeout, cfg.Environment, log)
	if err != nil {
		return err
	}

	runner.Hide(cfg.Auth.ClientSecretEnv, cfg.Auth.SessionSecretEnv)

	resolver, err := registry.NewResolver(registryOptions(cfg), log)
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	events := api.NewBroadcaster()

	var (
		controller *reconcile.Controller
		published  atomic.Pointer[targets.Set]
	)

	watcher, err := targets.NewWatcher(cfg.TargetsDir, rulesFor(cfg), watchEvery, log,
		func(_ *targets.Set, current *targets.Set) {
			if controller == nil {
				published.Store(current)

				return
			}

			controller.ReplaceTargets(ctx, func() (*targets.Set, *targets.Set) {
				return published.Swap(current), current
			})
			controller.ReportTargetsError(ctx, nil)
		},
		func(err error) {
			if controller != nil {
				controller.ReportTargetsError(ctx, err)
			}
		})
	if err != nil {
		return err
	}

	published.Store(watcher.Current())

	controller, err = reconcile.New(ctx, &reconcile.Options{
		Config: cfg, Targets: published.Load, Resolver: resolver, Runner: metrics.NewHookRunner(runner, reg),
		Store: db, Notifier: events, Log: log, Concurrency: cfg.Inspect.Concurrency,
	})
	if err != nil {
		return err
	}

	reg.MustRegister(metrics.NewCollector(controller))

	server := api.New(cfg, controller, published.Load, authorizer, events, version, log)

	var loginURL func(string) string
	if cfg.Auth.Mode != "none" {
		loginURL = auth.LoginURL
	}

	pages, err := ui.New(cfg, controller, published.Load, authorizer, loginURL, version, log)
	if err != nil {
		return err
	}

	// The API and the pages share one identity middleware, which also mounts
	// the login routes when OIDC is on. Metrics stay outside it.
	routes := server.Routes()
	app := http.NewServeMux()
	app.Handle("/healthz", routes)
	app.Handle("/api/", routes)
	app.Handle("/", pages.Handler())

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.Handle("/", authorizer.Middleware(app))

	httpServer := &http.Server{Addr: cfg.Listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	log.WithFields(logrus.Fields{"environment": cfg.Environment, "listen": cfg.Listen, "targets": watcher.Current().Len(), "version": version}).Info("rolloor starting")

	g, gctx := errgroup.WithContext(ctx)
	controller.InspectAll(gctx)
	controller.ProbeAll(gctx)

	g.Go(func() error { return controller.RunInspector(gctx) })
	g.Go(func() error { return controller.RunProber(gctx) })
	g.Go(func() error { return controller.Run(gctx, tickEvery) })
	g.Go(func() error { return watcher.Run(gctx) })
	g.Go(func() error { return watchAutomation(gctx, configPath, controller, log) })

	if teams != nil {
		g.Go(func() error { return teams.Run(gctx, watchEvery) })
	}

	g.Go(func() error {
		if serveErr := httpServer.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			return serveErr
		}

		return nil
	})
	g.Go(func() error {
		<-gctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownWait)
		defer cancel()

		return httpServer.Shutdown(shutdownCtx)
	})

	err = g.Wait()
	if errors.Is(err, context.Canceled) {
		log.Info("rolloor stopped")

		return nil
	}

	return err
}

func watchAutomation(ctx context.Context, path string, c *reconcile.Controller, log observability.ContextualLogger) error {
	ticker := time.NewTicker(watchEvery)
	defer ticker.Stop()

	var modified time.Time

	size := int64(-1)

	for {
		info, err := os.Stat(path)
		if err != nil {
			log.WithContext(ctx).WithError(err).Warn("config reload failed")
		} else if !info.ModTime().Equal(modified) || info.Size() != size {
			modified, size = info.ModTime(), info.Size()

			cfg, loadErr := config.Load(path)
			if loadErr == nil {
				loadErr = c.ConfigureGroups(ctx, cfg.Paused, cfg.Groups)
			}

			if loadErr != nil {
				log.WithContext(ctx).WithError(loadErr).Warn("config reload failed; keeping previous pause and group settings")
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// buildAuthorizer picks open access or OIDC from the config. The OIDC client
// secret and the session key come from the environment variables the config
// names.
func buildAuthorizer(ctx context.Context, cfg *config.Config, log observability.ContextualLogger) (api.Authorizer, *auth.Teams, error) {
	if cfg.Auth.Mode == "none" {
		return api.OpenAccess{}, nil, nil
	}

	var teams *auth.Teams

	if cfg.TeamsFile != "" {
		loaded, err := auth.LoadTeams(cfg.TeamsFile, log)
		if err != nil {
			return nil, nil, err
		}

		teams = loaded
	}

	// No secret env var configured means a public client.
	var secret string

	if cfg.Auth.ClientSecretEnv != "" {
		secret = os.Getenv(cfg.Auth.ClientSecretEnv)
		if secret == "" {
			return nil, nil, fmt.Errorf("auth: %s is not set", cfg.Auth.ClientSecretEnv)
		}
	}

	sessionKey := os.Getenv(cfg.Auth.SessionSecretEnv)
	if sessionKey == "" {
		return nil, nil, fmt.Errorf("auth: %s is not set", cfg.Auth.SessionSecretEnv)
	}

	oidcAuth, err := auth.NewOIDC(ctx, &cfg.Auth, secret, sessionKey, teams, log)
	if err != nil {
		return nil, nil, err
	}

	return oidcAuth, teams, nil
}

func registryOptions(cfg *config.Config) *registry.Options {
	return &registry.Options{AuthFile: cfg.Registry.AuthFile, PlainHTTP: cfg.Registry.PlainHTTP, CAFile: cfg.Registry.CAFile, Timeout: cfg.Registry.Timeout}
}
