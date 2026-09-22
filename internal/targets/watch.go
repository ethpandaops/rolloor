package targets

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ethpandaops/rolloor/internal/observability"
)

// Watcher reloads the targets directory when any file in it changes. A load
// that fails keeps the previous set and reports the error.
type Watcher struct {
	dir      string
	rules    Rules
	interval time.Duration
	log      observability.ContextualLogger
	onChange func(old, current *Set)
	onError  func(error)

	mu      sync.RWMutex
	current *Set
	stamp   string
}

// NewWatcher loads once, synchronously, so Current is never nil after a
// successful construction.
func NewWatcher(dir string, rules *Rules, interval time.Duration, log observability.ContextualLogger, onChange func(old, current *Set), onError func(error)) (*Watcher, error) {
	w := &Watcher{
		dir:      dir,
		rules:    *rules,
		interval: interval,
		log:      log.WithField("component", "targets"),
		onChange: onChange,
		onError:  onError,
	}

	set, err := Load(dir, &w.rules)
	if err != nil {
		return nil, err
	}

	stamp, err := fingerprint(dir)
	if err != nil {
		return nil, err
	}

	w.current = set
	w.stamp = stamp

	return w, nil
}

// Current is the latest valid set.
func (w *Watcher) Current() *Set {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.current
}

// Run polls until ctx ends. It blocks.
func (w *Watcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			w.poll()
		}
	}
}

// Reload forces a load now and returns its error.
func (w *Watcher) Reload() error {
	stamp, err := fingerprint(w.dir)
	if err != nil {
		return err
	}

	return w.load(stamp)
}

func (w *Watcher) poll() {
	stamp, err := fingerprint(w.dir)
	if err != nil {
		w.report(err)

		return
	}

	w.mu.RLock()
	same := stamp == w.stamp
	w.mu.RUnlock()

	if same {
		return
	}

	if err := w.load(stamp); err != nil {
		w.report(err)
	}
}

func (w *Watcher) load(stamp string) error {
	set, err := Load(w.dir, &w.rules)
	if err != nil {
		// Remember the stamp so a broken file is reported once, not every tick.
		w.mu.Lock()
		w.stamp = stamp
		w.mu.Unlock()

		return err
	}

	w.mu.Lock()
	old := w.current
	w.current = set
	w.stamp = stamp
	w.mu.Unlock()

	w.log.WithField("targets", set.Len()).Info("targets reloaded")

	if w.onChange != nil {
		w.onChange(old, set)
	}

	return nil
}

func (w *Watcher) report(err error) {
	w.log.WithError(err).Warn("targets reload failed; keeping the previous set")

	if w.onError != nil {
		w.onError(err)
	}
}

// statFile is os.Stat, replaceable so the race between listing and stating a
// file that is being removed can be exercised in tests.
var statFile = os.Stat

// fingerprint summarises the directory's yaml files by name, size and mtime.
func fingerprint(dir string) (string, error) {
	paths, err := listFiles(dir)
	if err != nil {
		return "", err
	}

	var b strings.Builder

	for _, p := range paths {
		info, err := statFile(p)
		if err != nil {
			return "", fmt.Errorf("targets: stat %s: %w", p, err)
		}

		fmt.Fprintf(&b, "%s|%d|%d\n", p, info.Size(), info.ModTime().UnixNano())
	}

	return b.String(), nil
}
