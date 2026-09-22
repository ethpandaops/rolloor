// Package hooks runs the operator-supplied programs with the fixed contract:
// JSON on stdin, exit 0 means yes, the first line of stdout is the reason.
package hooks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/ethpandaops/rolloor/internal/observability"
)

// Result is what one run produced.
type Result struct {
	Program  string        `json:"program"`
	ExitCode int           `json:"exitCode"`
	OK       bool          `json:"ok"`
	Reason   string        `json:"reason"`
	Stdout   string        `json:"-"`
	Duration time.Duration `json:"duration"`
	TimedOut bool          `json:"timedOut"`
	RanAt    time.Time     `json:"ranAt"`
}

// maxCapture bounds how much of a hook's stdout and stderr is kept. The
// contract only needs the first lines; the rest is drained and dropped.
const maxCapture = 64 << 10

// capped keeps the first limit bytes written and counts the rest.
type capped struct {
	buf     bytes.Buffer
	limit   int
	dropped int
}

func (c *capped) Write(p []byte) (int, error) {
	n := len(p)

	room := c.limit - c.buf.Len()
	if room <= 0 {
		c.dropped += n

		return n, nil
	}

	if n > room {
		c.dropped += n - room
		p = p[:room]
	}

	c.buf.Write(p)

	return n, nil
}

func (c *capped) String() string { return c.buf.String() }

func (c *capped) Len() int { return c.buf.Len() }

// Runner executes programs from one directory with one timeout.
type Runner struct {
	dir         string
	timeout     time.Duration
	environment string
	log         observability.ContextualLogger
}

// NewRunner returns a runner. dir must exist.
func NewRunner(dir string, timeout time.Duration, environment string, log observability.ContextualLogger) (*Runner, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("hooks: %w", err)
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("hooks: %s is not a directory", dir)
	}

	return &Runner{dir: dir, timeout: timeout, environment: environment, log: log.WithField("component", "hooks")}, nil
}

// Exists reports whether a program is present and executable.
func (r *Runner) Exists(program string) bool {
	info, err := os.Stat(filepath.Join(r.dir, program))

	return err == nil && !info.IsDir() && info.Mode()&0o111 != 0
}

// Run executes program with input encoded as JSON on stdin. hook and targetID
// are exposed to the program as environment variables. A non-zero exit is not
// an error; an error means the program could not be run at all.
func (r *Runner) Run(ctx context.Context, program, hook, targetID string, input any) (Result, error) {
	if program == "" || strings.ContainsAny(program, `/\`) {
		return Result{}, fmt.Errorf("hooks: %q is not a bare program name", program)
	}

	stdin, err := json.Marshal(input)
	if err != nil {
		return Result{}, fmt.Errorf("hooks: encode input for %s: %w", program, err)
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	//nolint:gosec // program is a bare name resolved under a configured directory; that is the contract.
	cmd := exec.CommandContext(ctx, filepath.Join(r.dir, program))
	cmd.Stdin = bytes.NewReader(stdin)
	isolate(cmd)

	cmd.Env = append(os.Environ(),
		"ROLLOOR_HOOK="+hook,
		"ROLLOOR_ENVIRONMENT="+r.environment,
		"ROLLOOR_TARGET_ID="+targetID,
	)

	stdout := &capped{limit: maxCapture}
	stderr := &capped{limit: maxCapture}

	cmd.Stdout = stdout
	cmd.Stderr = stderr

	start := time.Now()
	runErr := cmd.Run()
	res := Result{Program: program, Duration: time.Since(start), RanAt: start, Stdout: stdout.String()}
	res.Reason = firstLine(res.Stdout)

	log := r.log.WithContext(ctx).WithFields(map[string]any{"hook": hook, "program": program, "target": targetID, "duration": res.Duration})

	if stderr.Len() > 0 {
		log.WithField("stderr", strings.TrimSpace(stderr.String())).Debug("hook stderr")
	}

	switch {
	case runErr == nil:
		res.OK = true
		res.ExitCode = 0
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		res.TimedOut = true
		res.ExitCode = -1
		res.Reason = fmt.Sprintf("timed out after %s", r.timeout)
	default:
		var exit *exec.ExitError
		if !errors.As(runErr, &exit) {
			return res, fmt.Errorf("hooks: run %s: %w", program, runErr)
		}

		res.ExitCode = exit.ExitCode()
		if res.Reason == "" {
			res.Reason = firstLine(stderr.String())
		}

		if res.Reason == "" {
			res.Reason = fmt.Sprintf("exit %d", res.ExitCode)
		}
	}

	log.WithFields(map[string]any{"ok": res.OK, "exit": res.ExitCode}).Debug("hook ran")

	return res, nil
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}

	return strings.TrimSpace(s)
}
