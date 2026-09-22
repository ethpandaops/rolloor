// Package auth signs people in with any OIDC issuer and decides which owner
// values they may act on, from a teams file or from the claim itself.
package auth

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ethpandaops/rolloor/internal/observability"
)

// Teams maps owner values to the identities allowed to act on them. It is a
// YAML file, reloaded when its modification time changes.
type Teams struct {
	path string
	log  observability.ContextualLogger

	mu      sync.RWMutex
	byIdent map[string][]string
	mtime   time.Time
}

// readFile is os.ReadFile, replaceable so the race between stat and read is
// testable.
var readFile = os.ReadFile

// LoadTeams reads the file once. It fails if the file is unreadable or is
// not a map of owner to a list of identities.
func LoadTeams(path string, log observability.ContextualLogger) (*Teams, error) {
	t := &Teams{path: path, log: log.WithField("component", "teams")}

	if err := t.Reload(); err != nil {
		return nil, err
	}

	return t, nil
}

// Reload re-reads the file if it changed. A broken file keeps the previous
// mapping and returns the error.
func (t *Teams) Reload() error {
	info, err := os.Stat(t.path)
	if err != nil {
		return fmt.Errorf("teams: %w", err)
	}

	t.mu.RLock()
	same := !t.mtime.IsZero() && info.ModTime().Equal(t.mtime)
	t.mu.RUnlock()

	if same {
		return nil
	}

	raw, err := readFile(t.path)
	if err != nil {
		return fmt.Errorf("teams: %w", err)
	}

	parsed := map[string][]string{}
	if err := yaml.Unmarshal(raw, &parsed); err != nil {
		return fmt.Errorf("teams: %s: %w", t.path, err)
	}

	byIdent := map[string][]string{}

	for owner, idents := range parsed {
		for _, id := range idents {
			byIdent[id] = append(byIdent[id], owner)
		}
	}

	for id := range byIdent {
		sort.Strings(byIdent[id])
	}

	t.mu.Lock()
	t.byIdent = byIdent
	t.mtime = info.ModTime()
	t.mu.Unlock()

	t.log.WithField("identities", len(byIdent)).WithField("owners", len(parsed)).Info("teams loaded")

	return nil
}

// Owners returns the owner values an identity may act on.
func (t *Teams) Owners(identity string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return append([]string(nil), t.byIdent[identity]...)
}

// Run reloads on the interval until ctx ends. It blocks.
func (t *Teams) Run(ctx context.Context, every time.Duration) error {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := t.Reload(); err != nil {
				t.log.WithError(err).Warn("teams reload failed; keeping the previous mapping")
			}
		}
	}
}
