// Package ethpandaops holds the hook programs for ethpandaops devnets: the
// node updater's HTTP API for inspect and update, the beacon and execution
// APIs for readiness, soak and the environment check, and the coredevs
// registry for the teams file.
package ethpandaops

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Result is what a program reports: pass or fail, the reason people see and
// optionally the soak numbers line.
type Result struct {
	OK      bool
	Reason  string
	Numbers string
}

func pass(format string, args ...any) Result {
	return Result{OK: true, Reason: fmt.Sprintf(format, args...)}
}

func fail(format string, args ...any) Result {
	return Result{Reason: fmt.Sprintf(format, args...)}
}

// Program is one hook. stdin is the document rolloor sends.
type Program func(ctx context.Context, h *Hooks, stdin []byte) (Result, error)

// Programs maps hook program names to their implementations.
var Programs = map[string]Program{
	"inspect":         Inspect,
	"update":          Update,
	"ready-running":   ReadyRunning,
	"ready-beacon":    ReadyBeacon,
	"ready-execution": ReadyExecution,
	"soak-beacon":     SoakBeacon,
	"soak-execution":  SoakExecution,
	"soak-validator":  SoakValidator,
	"environment":     Environment,
}

// Names lists the program names, sorted.
func Names() []string {
	out := make([]string, 0, len(Programs))
	for n := range Programs {
		out = append(out, n)
	}

	sort.Strings(out)

	return out
}

// Settings come from the environment rolloor runs in.
type Settings struct {
	// UpdaterToken is the node updater's API token.
	UpdaterToken string
	// NodeAuth is "user:password" for the nodes' beacon and RPC endpoints.
	NodeAuth string
	// Beacon is a beacon API that speaks for the network, used by the
	// environment check and the validator soak.
	Beacon string
	// FinalityLag is how many epochs finality may trail the head.
	FinalityLag uint64
	// ForkMargin is how many epochs either side of a fork are kept quiet.
	ForkMargin uint64
	// QuietEpochs are further epochs to keep quiet, such as gas limit steps.
	QuietEpochs []uint64
	// Tolerance is how much worse, as a fraction, updated validators may do
	// than the remaining ones.
	Tolerance float64
	// Floor is the timely-target rate updated validators need when nothing
	// remains to compare against.
	Floor float64
	// Sample is how many validators per target the validator soak asks about.
	Sample int
	// UpdateWait bounds how long update waits for the updater to take it.
	UpdateWait time.Duration
}

// SettingsFromEnv reads ETHPANDAOPS_* variables, with defaults.
func SettingsFromEnv(getenv func(string) string) (Settings, error) {
	s := Settings{
		UpdaterToken: getenv("ETHPANDAOPS_UPDATER_TOKEN"),
		NodeAuth:     getenv("ETHPANDAOPS_NODE_AUTH"),
		Beacon:       strings.TrimRight(getenv("ETHPANDAOPS_BEACON"), "/"),
		FinalityLag:  4,
		ForkMargin:   4,
		Tolerance:    0.05,
		Floor:        0.8,
		Sample:       256,
		UpdateWait:   45 * time.Second,
	}

	var err error

	if v := getenv("ETHPANDAOPS_FINALITY_LAG"); v != "" {
		if s.FinalityLag, err = strconv.ParseUint(v, 10, 64); err != nil {
			return s, fmt.Errorf("ETHPANDAOPS_FINALITY_LAG: %w", err)
		}
	}

	if v := getenv("ETHPANDAOPS_FORK_MARGIN"); v != "" {
		if s.ForkMargin, err = strconv.ParseUint(v, 10, 64); err != nil {
			return s, fmt.Errorf("ETHPANDAOPS_FORK_MARGIN: %w", err)
		}
	}

	for f := range strings.SplitSeq(getenv("ETHPANDAOPS_QUIET_EPOCHS"), ",") {
		if f = strings.TrimSpace(f); f == "" {
			continue
		}

		e, perr := strconv.ParseUint(f, 10, 64)
		if perr != nil {
			return s, fmt.Errorf("ETHPANDAOPS_QUIET_EPOCHS: %w", perr)
		}

		s.QuietEpochs = append(s.QuietEpochs, e)
	}

	if v := getenv("ETHPANDAOPS_TOLERANCE"); v != "" {
		if s.Tolerance, err = strconv.ParseFloat(v, 64); err != nil {
			return s, fmt.Errorf("ETHPANDAOPS_TOLERANCE: %w", err)
		}
	}

	if v := getenv("ETHPANDAOPS_FLOOR"); v != "" {
		if s.Floor, err = strconv.ParseFloat(v, 64); err != nil {
			return s, fmt.Errorf("ETHPANDAOPS_FLOOR: %w", err)
		}
	}

	if v := getenv("ETHPANDAOPS_SAMPLE"); v != "" {
		if s.Sample, err = strconv.Atoi(v); err != nil || s.Sample < 1 {
			return s, fmt.Errorf("ETHPANDAOPS_SAMPLE must be a positive number, got %q", v)
		}
	}

	if v := getenv("ETHPANDAOPS_UPDATE_WAIT"); v != "" {
		if s.UpdateWait, err = time.ParseDuration(v); err != nil {
			return s, fmt.Errorf("ETHPANDAOPS_UPDATE_WAIT: %w", err)
		}
	}

	return s, nil
}

// Hooks carries the settings and the HTTP client every program uses.
type Hooks struct {
	Settings

	Client *http.Client
	// Sleep waits between update attempts; replaceable in tests.
	Sleep func(ctx context.Context, d time.Duration) error
}

// New returns hooks with a bounded HTTP client.
func New(s *Settings) *Hooks {
	return &Hooks{Settings: *s, Client: &http.Client{Timeout: 15 * time.Second}, Sleep: sleep}
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Run executes the named program and writes its reason (and numbers line)
// to out. It returns the exit code.
func Run(ctx context.Context, h *Hooks, name string, stdin io.Reader, out io.Writer) int {
	prog, ok := Programs[name]
	if !ok {
		fmt.Fprintf(out, "unknown program %q; known: %s\n", name, strings.Join(Names(), ", "))

		return 2
	}

	raw, err := io.ReadAll(stdin)
	if err != nil {
		fmt.Fprintf(out, "read stdin: %v\n", err)

		return 1
	}

	res, err := prog(ctx, h, raw)
	if err != nil {
		fmt.Fprintln(out, err.Error())

		return 1
	}

	fmt.Fprintln(out, res.Reason)

	if res.Numbers != "" {
		fmt.Fprintln(out, res.Numbers)
	}

	if !res.OK {
		return 1
	}

	return 0
}

// Target is the part of rolloor's target document these programs read.
type Target struct {
	ID      string            `json:"id"`
	Node    string            `json:"node"`
	Image   string            `json:"image"`
	Labels  map[string]string `json:"labels"`
	Extra   Extra             `json:"extra"`
	Desired string            `json:"desired"`
}

// Extra is what the targets template puts under extra for each target.
type Extra struct {
	// Container is the container name on the node.
	Container string `json:"container"`
	// Updater is the base URL of the node's updater API.
	Updater string `json:"updater"`
	// Beacon and RPC are the node's beacon and execution API base URLs.
	Beacon string `json:"beacon"`
	RPC    string `json:"rpc"`
	// Validators is the node's validator index range, "start-end" with end
	// exclusive.
	Validators string `json:"validators"`
}

// SoakInput is the soak document.
type SoakInput struct {
	Updated   []Target `json:"updated"`
	Remaining []Target `json:"remaining"`
}

func decodeTarget(stdin []byte) (*Target, error) {
	var t Target
	if err := json.Unmarshal(stdin, &t); err != nil {
		return nil, fmt.Errorf("target document: %w", err)
	}

	if t.ID == "" {
		return nil, fmt.Errorf("target document has no id")
	}

	return &t, nil
}

func decodeSoak(stdin []byte) (*SoakInput, error) {
	var s SoakInput
	if err := json.Unmarshal(stdin, &s); err != nil {
		return nil, fmt.Errorf("soak document: %w", err)
	}

	if len(s.Updated) == 0 {
		return nil, fmt.Errorf("soak document has no updated targets")
	}

	return &s, nil
}

func short(digest string) string {
	d := strings.TrimPrefix(digest, "sha256:")
	if len(d) > 12 {
		return d[:12]
	}

	return d
}
