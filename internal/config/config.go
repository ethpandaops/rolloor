// Package config loads and validates the controller configuration.
package config

import (
	"bytes"
	"fmt"
	"os"
	"slices"
	"sort"
	"time"

	"github.com/creasty/defaults"
	"gopkg.in/yaml.v3"
)

// Hook names the binary runs. Anything else named in a targets file is an error.
const (
	HookInspect     = "inspect"
	HookUpdate      = "update"
	HookReady       = "ready"
	HookSoak        = "soak"
	HookEnvironment = "environment"
)

// TargetHooks are the hooks a target may name a program for.
var TargetHooks = []string{HookInspect, HookUpdate, HookReady, HookSoak}

// Config is the whole file.
type Config struct {
	Environment string `yaml:"environment"`
	Listen      string `yaml:"listen" default:":8080"`
	DataDir     string `yaml:"dataDir" default:"/var/lib/rolloor"`
	TargetsDir  string `yaml:"targetsDir" default:"/etc/rolloor/targets.d"`
	TeamsFile   string `yaml:"teamsFile"`

	Labels           Labels           `yaml:"labels"`
	Registry         Registry         `yaml:"registry"`
	DisruptionBudget DisruptionBudget `yaml:"disruptionBudget"`
	Hooks            Hooks            `yaml:"hooks"`
	Inspect          Inspect          `yaml:"inspect"`
	// Strategy is how every group rolls out unless its policy names one of
	// Strategies. A named strategy starts from Strategy and overrides only
	// the fields it sets.
	Strategy   Strategy            `yaml:"strategy"`
	Strategies map[string]Strategy `yaml:"-"`
	// DefaultPolicy applies to every group without a stored policy.
	DefaultPolicy Policy `yaml:"defaultPolicy"`
	Auth          Auth   `yaml:"auth"`
	Log           Log    `yaml:"log"`

	// RawStrategies holds the strategies section until each is decoded on
	// top of Strategy.
	RawStrategies map[string]yaml.Node `yaml:"strategies"`
}

// DisruptionBudget limits how much of the environment may be changing at
// once, across every rollout.
type DisruptionBudget struct {
	// MaxUnavailable is a share of total weight ("10%") or an absolute weight.
	MaxUnavailable Fraction `yaml:"maxUnavailable"`
}

// Labels names the labels the controller gives meaning to.
type Labels struct {
	Group        string   `yaml:"group" default:"group"`
	Owner        string   `yaml:"owner" default:"owner"`
	Section      string   `yaml:"section"`
	Wave         string   `yaml:"wave" default:"wave"`
	HiddenGroups []string `yaml:"hiddenGroups"`
}

// Registry controls tag resolution.
type Registry struct {
	Poll     time.Duration `yaml:"poll" default:"60s"`
	AuthFile string        `yaml:"authFile"`
	// PlainHTTP lists registry hosts spoken to without TLS, for local registries.
	PlainHTTP []string `yaml:"plainHttp"`
	// CAFile is a PEM bundle trusted in addition to the system roots.
	CAFile string `yaml:"caFile"`
	// Timeout bounds one request to a registry.
	Timeout time.Duration `yaml:"timeout" default:"30s"`
}

// Hooks locates and bounds the programs.
type Hooks struct {
	Dir         string            `yaml:"dir" default:"/etc/rolloor/hooks"`
	Timeout     time.Duration     `yaml:"timeout" default:"60s"`
	Defaults    map[string]string `yaml:"defaults"`
	Environment EnvironmentHook   `yaml:"environment"`
}

// EnvironmentHook is the optional environment-wide check.
type EnvironmentHook struct {
	Program  string        `yaml:"program"`
	Interval time.Duration `yaml:"interval" default:"30s"`
}

// Inspect paces live-state observation.
type Inspect struct {
	Interval    time.Duration `yaml:"interval" default:"30s"`
	Concurrency int           `yaml:"concurrency" default:"16"`
	// FailureThreshold is how many inspections in a row may fail before a
	// target reads Unknown.
	FailureThreshold int `yaml:"failureThreshold" default:"3"`
}

// Strategy is how a rollout cuts batches and watches them.
type Strategy struct {
	// FirstBatch is how many nodes batch 1 takes; omitted means BatchSize.
	FirstBatch Fraction `yaml:"firstBatch"`
	// BatchSize is how many nodes every later batch takes, as a count or a
	// share of the rollout's nodes. A batch takes every target the group has
	// on each node it picks.
	BatchSize Fraction `yaml:"batchSize"`
	// PauseAfterFirstBatch waits for a promote once batch 1 has passed.
	PauseAfterFirstBatch bool `yaml:"pauseAfterFirstBatch"`
	// Waves nil means true: batches follow the wave label.
	Waves *bool `yaml:"waves"`
	Soak  Soak  `yaml:"soak"`
}

// BatchFor returns how many of total nodes batch n (from 1) takes.
func (s *Strategy) BatchFor(n, total int) int {
	if n == 1 && s.FirstBatch.set {
		return s.FirstBatch.Of(total)
	}

	return s.BatchSize.Of(total)
}

// UsesWaves reports whether batches follow the wave label.
func (s *Strategy) UsesWaves() bool {
	return s.Waves == nil || *s.Waves
}

// Soak is how long each batch is watched after it is ready, and how often
// the soak program runs. Duration zero skips it.
type Soak struct {
	Duration time.Duration `yaml:"duration" default:"10m"`
	Interval time.Duration `yaml:"interval" default:"1m"`
	// FailureLimit is how many checks may fail before the rollout halts.
	FailureLimit int `yaml:"failureLimit" default:"1"`
}

// Policy is a group's stored policy; here it is the default.
type Policy struct {
	Mode string `yaml:"mode" default:"automated"`
	// Strategy names one of the configured strategies; empty is the default.
	Strategy string `yaml:"strategy"`
}

// Auth configures sign-in.
type Auth struct {
	Mode     string `yaml:"mode" default:"none"`
	Issuer   string `yaml:"issuer"`
	ClientID string `yaml:"clientId"`
	// ClientSecretEnv names the env var holding the client secret. Empty
	// makes rolloor a public client; every login uses PKCE either way.
	ClientSecretEnv string `yaml:"clientSecretEnv" default:"ROLLOOR_OIDC_CLIENT_SECRET"`
	RedirectURL     string `yaml:"redirectUrl"`
	IdentityClaim   string `yaml:"identityClaim" default:"preferred_username"`
	AdminOwner      string `yaml:"adminOwner" default:"operators"`
	// SessionSecretEnv names the env var holding the cookie signing key.
	SessionSecretEnv string `yaml:"sessionSecretEnv" default:"ROLLOOR_SESSION_SECRET"`
	// PublicReads lets anyone read the pages and API without signing in;
	// acting always needs an identity.
	PublicReads bool `yaml:"publicReads"`
	// TrustedTokens are other issuers whose bearer tokens the API accepts,
	// such as a CLI's own client.
	TrustedTokens []TrustedToken `yaml:"trustedTokens"`
}

// TrustedToken is an issuer and audience whose ID or access tokens are
// accepted as bearer tokens, and the claim that names the person.
type TrustedToken struct {
	Issuer        string `yaml:"issuer"`
	Audience      string `yaml:"audience"`
	IdentityClaim string `yaml:"identityClaim"`
}

// Log configures the root logger.
type Log struct {
	Level  string `yaml:"level" default:"info"`
	Format string `yaml:"format" default:"json"`
}

// applyDefaults is defaults.Set, replaceable so its failure path is testable.
var applyDefaults = defaults.Set

// Load reads a YAML file, applies defaults, and validates.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	return Parse(raw)
}

// Parse decodes YAML bytes with unknown fields rejected.
func Parse(raw []byte) (*Config, error) {
	cfg := &Config{}
	if err := applyDefaults(cfg); err != nil {
		return nil, fmt.Errorf("apply defaults: %w", err)
	}

	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)

	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	if err := applyStrategyDefaults(cfg); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// applyStrategyDefaults completes the default strategy and builds every
// named one on top of it.
func applyStrategyDefaults(cfg *Config) error {
	if !cfg.Strategy.BatchSize.set {
		cfg.Strategy.BatchSize = Fraction{Percent: 10, IsPercent: true, set: true}
	}

	cfg.Strategies = map[string]Strategy{}

	for name := range cfg.RawStrategies {
		node := cfg.RawStrategies[name]

		// A node the decoder just produced always encodes again.
		raw, _ := yaml.Marshal(&node)

		st := cfg.Strategy
		if st.Waves != nil {
			w := *st.Waves
			st.Waves = &w
		}

		dec := yaml.NewDecoder(bytes.NewReader(raw))
		dec.KnownFields(true)

		if err := dec.Decode(&st); err != nil {
			return fmt.Errorf("config: strategies.%s: %w", name, err)
		}

		cfg.Strategies[name] = st
	}

	cfg.RawStrategies = nil

	if cfg.Hooks.Defaults == nil {
		cfg.Hooks.Defaults = map[string]string{}
	}

	for _, h := range []string{HookInspect, HookUpdate, HookReady} {
		if _, ok := cfg.Hooks.Defaults[h]; !ok {
			cfg.Hooks.Defaults[h] = h
		}
	}

	if !cfg.DisruptionBudget.MaxUnavailable.set {
		cfg.DisruptionBudget.MaxUnavailable = Fraction{Percent: 10, IsPercent: true, set: true}
	}

	return nil
}

// StrategyNamed returns a named strategy, or the default one for "".
func (c *Config) StrategyNamed(name string) (Strategy, bool) {
	if name == "" {
		return c.Strategy, true
	}

	st, ok := c.Strategies[name]

	return st, ok
}

// Validate checks constraints after defaults and overrides.
func (c *Config) Validate() error {
	if c.Environment == "" {
		return fmt.Errorf("config: environment is required")
	}

	if c.Labels.Group == "" || c.Labels.Owner == "" {
		return fmt.Errorf("config: labels.group and labels.owner are required")
	}

	if c.Registry.Poll <= 0 {
		return fmt.Errorf("config: registry.poll must be positive")
	}

	if c.Hooks.Timeout <= 0 {
		return fmt.Errorf("config: hooks.timeout must be positive")
	}

	if c.Inspect.Interval <= 0 || c.Inspect.Concurrency <= 0 || c.Inspect.FailureThreshold <= 0 {
		return fmt.Errorf("config: inspect.interval, concurrency and failureThreshold must be positive")
	}

	for h := range c.Hooks.Defaults {
		if !isTargetHook(h) {
			return fmt.Errorf("config: hooks.defaults.%s is not a hook", h)
		}
	}

	if err := c.Strategy.validate("strategy"); err != nil {
		return err
	}

	for name, st := range c.Strategies {
		if name == "" {
			return fmt.Errorf("config: strategies need a name")
		}

		if err := st.validate("strategies." + name); err != nil {
			return err
		}
	}

	if _, ok := c.StrategyNamed(c.DefaultPolicy.Strategy); !ok {
		return fmt.Errorf("config: defaultPolicy.strategy %q is not a strategy (have %v)", c.DefaultPolicy.Strategy, c.StrategyNames())
	}

	if c.DefaultPolicy.Mode != "automated" && c.DefaultPolicy.Mode != "manual" {
		return fmt.Errorf("config: defaultPolicy.mode must be automated or manual")
	}

	switch c.Auth.Mode {
	case "none":
	case "oidc":
		if c.Auth.Issuer == "" || c.Auth.ClientID == "" || c.Auth.RedirectURL == "" {
			return fmt.Errorf("config: auth.issuer, clientId and redirectUrl are required for oidc")
		}

		for i, t := range c.Auth.TrustedTokens {
			if t.Issuer == "" || t.Audience == "" {
				return fmt.Errorf("config: auth.trustedTokens[%d] needs an issuer and an audience", i)
			}
		}
	default:
		return fmt.Errorf("config: auth.mode must be none or oidc")
	}

	return nil
}

// StrategyNames lists the named strategies sorted, for messages.
func (c *Config) StrategyNames() []string {
	names := make([]string, 0, len(c.Strategies))
	for n := range c.Strategies {
		names = append(names, n)
	}

	sort.Strings(names)

	return names
}

func (s *Strategy) validate(field string) error {
	if s.BatchSize.Of(100) == 0 {
		return fmt.Errorf("config: %s.batchSize selects nothing", field)
	}

	if s.FirstBatch.set && s.FirstBatch.Of(100) == 0 {
		return fmt.Errorf("config: %s.firstBatch selects nothing", field)
	}

	if s.Soak.Duration > 0 && (s.Soak.Interval <= 0 || s.Soak.Interval > s.Soak.Duration) {
		return fmt.Errorf("config: %s.soak.interval must be positive and within duration", field)
	}

	if s.Soak.FailureLimit < 0 {
		return fmt.Errorf("config: %s.soak.failureLimit must not be negative", field)
	}

	return nil
}

func isTargetHook(h string) bool {
	return slices.Contains(TargetHooks, h)
}
