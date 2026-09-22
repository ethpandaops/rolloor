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

	Labels   Labels            `yaml:"labels"`
	Registry Registry          `yaml:"registry"`
	Budget   Fraction          `yaml:"budget"`
	Hooks    Hooks             `yaml:"hooks"`
	Inspect  Inspect           `yaml:"inspect"`
	Presets  map[string]Preset `yaml:"presets"`
	// DefaultPolicy applies to every group without a stored policy.
	DefaultPolicy Policy `yaml:"defaultPolicy"`
	Auth          Auth   `yaml:"auth"`
	Log           Log    `yaml:"log"`
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
	Interval     time.Duration `yaml:"interval" default:"30s"`
	Concurrency  int           `yaml:"concurrency" default:"16"`
	UnknownAfter int           `yaml:"unknownAfter" default:"3"`
}

// Preset is a named speed.
type Preset struct {
	Batch           []Fraction `yaml:"batch"`
	Soak            Soak       `yaml:"soak"`
	PauseAfterFirst bool       `yaml:"pauseAfterFirst"`
	// Waves nil means true.
	Waves *bool `yaml:"waves"`
}

// UsesWaves reports whether the preset honours the wave label.
func (p Preset) UsesWaves() bool {
	return p.Waves == nil || *p.Waves
}

// Soak is the after-batch comparison schedule. Duration zero disables it.
// Passes and Grace are pointers so that an explicit zero is kept and only an
// omitted value takes the default.
type Soak struct {
	Duration time.Duration `yaml:"duration"`
	Interval time.Duration `yaml:"interval"`
	Passes   *int          `yaml:"passes"`
	Grace    *int          `yaml:"grace"`
}

// PassesN is the number of consecutive passes required.
func (s Soak) PassesN() int {
	if s.Passes == nil {
		return 2
	}

	return *s.Passes
}

// GraceN is how many failures are tolerated before a halt.
func (s Soak) GraceN() int {
	if s.Grace == nil {
		return 1
	}

	return *s.Grace
}

// Policy is a group's stored policy; here it is the default.
type Policy struct {
	Mode  string `yaml:"mode" default:"automated"`
	Speed string `yaml:"speed" default:"normal"`
}

// Auth configures sign-in.
type Auth struct {
	Mode            string `yaml:"mode" default:"none"`
	Issuer          string `yaml:"issuer"`
	ClientID        string `yaml:"clientId"`
	ClientSecretEnv string `yaml:"clientSecretEnv" default:"ROLLOOR_OIDC_CLIENT_SECRET"`
	RedirectURL     string `yaml:"redirectUrl"`
	IdentityClaim   string `yaml:"identityClaim" default:"preferred_username"`
	AdminOwner      string `yaml:"adminOwner" default:"operators"`
	// SessionSecretEnv names the env var holding the cookie signing key.
	SessionSecretEnv string `yaml:"sessionSecretEnv" default:"ROLLOOR_SESSION_SECRET"`
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

	applyPresetDefaults(cfg)

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// applyPresetDefaults fills the built-in presets when the file names none, and
// completes soak schedules for the ones it does name.
func applyPresetDefaults(cfg *Config) {
	if cfg.Presets == nil {
		cfg.Presets = BuiltinPresets()
	}

	for name, p := range cfg.Presets {
		if p.Soak.Interval == 0 {
			p.Soak.Interval = 60 * time.Second
		}

		cfg.Presets[name] = p
	}

	if cfg.Hooks.Defaults == nil {
		cfg.Hooks.Defaults = map[string]string{}
	}

	for _, h := range []string{HookInspect, HookUpdate, HookReady} {
		if _, ok := cfg.Hooks.Defaults[h]; !ok {
			cfg.Hooks.Defaults[h] = h
		}
	}

	if !cfg.Budget.set {
		cfg.Budget = Fraction{Percent: 10, IsPercent: true, set: true}
	}
}

// BuiltinPresets are the four speeds every install has.
func BuiltinPresets() map[string]Preset {
	no := false

	return map[string]Preset{
		"careful": {
			Batch:           []Fraction{{Count: 1}, {Percent: 10, IsPercent: true}},
			Soak:            Soak{Duration: 12 * time.Minute, Interval: 60 * time.Second},
			PauseAfterFirst: true,
		},
		"normal": {
			Batch: []Fraction{{Percent: 10, IsPercent: true}},
			Soak:  Soak{Duration: 6 * time.Minute, Interval: 60 * time.Second},
		},
		"fast": {
			Batch: []Fraction{{Percent: 50, IsPercent: true}},
			Soak:  Soak{Interval: 60 * time.Second},
		},
		"all": {
			Batch: []Fraction{{Percent: 100, IsPercent: true}},
			Soak:  Soak{Interval: 60 * time.Second},
			Waves: &no,
		},
	}
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

	if c.Inspect.Interval <= 0 || c.Inspect.Concurrency <= 0 || c.Inspect.UnknownAfter <= 0 {
		return fmt.Errorf("config: inspect.interval, concurrency and unknown_after must be positive")
	}

	for h := range c.Hooks.Defaults {
		if !isTargetHook(h) {
			return fmt.Errorf("config: hooks.defaults.%s is not a hook", h)
		}
	}

	if len(c.Presets) == 0 {
		return fmt.Errorf("config: at least one preset is required")
	}

	for name, p := range c.Presets {
		if len(p.Batch) == 0 {
			return fmt.Errorf("config: presets.%s.batch is required", name)
		}

		for i, b := range p.Batch {
			if b.Of(100) == 0 {
				return fmt.Errorf("config: presets.%s.batch[%d] selects nothing", name, i)
			}
		}

		if p.Soak.Duration > 0 {
			if p.Soak.Interval <= 0 || p.Soak.Interval > p.Soak.Duration {
				return fmt.Errorf("config: presets.%s.soak.interval must be positive and within duration", name)
			}

			if p.Soak.PassesN() <= 0 {
				return fmt.Errorf("config: presets.%s.soak.passes must be positive", name)
			}

			if p.Soak.GraceN() < 0 {
				return fmt.Errorf("config: presets.%s.soak.grace must not be negative", name)
			}
		}
	}

	if _, ok := c.Presets[c.DefaultPolicy.Speed]; !ok {
		return fmt.Errorf("config: default_policy.speed %q is not a preset (have %v)", c.DefaultPolicy.Speed, c.PresetNames())
	}

	if c.DefaultPolicy.Mode != "automated" && c.DefaultPolicy.Mode != "manual" {
		return fmt.Errorf("config: default_policy.mode must be automated or manual")
	}

	switch c.Auth.Mode {
	case "none":
	case "oidc":
		if c.Auth.Issuer == "" || c.Auth.ClientID == "" || c.Auth.RedirectURL == "" {
			return fmt.Errorf("config: auth.issuer, client_id and redirect_url are required for oidc")
		}
	default:
		return fmt.Errorf("config: auth.mode must be none or oidc")
	}

	return nil
}

// PresetNames lists presets sorted, for messages.
func (c *Config) PresetNames() []string {
	names := make([]string, 0, len(c.Presets))
	for n := range c.Presets {
		names = append(names, n)
	}

	sort.Strings(names)

	return names
}

func isTargetHook(h string) bool {
	return slices.Contains(TargetHooks, h)
}
