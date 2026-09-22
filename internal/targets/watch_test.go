package targets

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

const one = "- {id: a, node: n, weight: 1, image: x:y, labels: {client: a, owner: a}}\n"

func TestWatcherReloadsOnChangeAndKeepsLastGoodOnError(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "t.yaml")
	require.NoError(t, os.WriteFile(file, []byte(one), 0o644))

	changes := make(chan *Set, 4)
	errs := make(chan error, 4)

	w, err := NewWatcher(dir, &testRules, 10*time.Millisecond, logrus.New(),
		func(s *Set) { changes <- s }, func(err error) { errs <- err })
	require.NoError(t, err)
	require.Equal(t, 1, w.Current().Len())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- w.Run(ctx) }()

	// A second target appears.
	require.NoError(t, os.WriteFile(file, []byte(one+"- {id: b, node: m, weight: 2, image: x:y, labels: {client: b, owner: b}}\n"), 0o644))

	select {
	case s := <-changes:
		require.Equal(t, 2, s.Len())
	case <-time.After(3 * time.Second):
		t.Fatal("no reload")
	}

	// A broken file is reported once and the previous set stays.
	require.NoError(t, os.WriteFile(file, []byte("- {id: broken, node: n, weight: -1}\n"), 0o644))

	select {
	case err := <-errs:
		require.Error(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("no error report")
	}

	require.Equal(t, 2, w.Current().Len())

	// Reload while still broken returns the error; fixing it succeeds.
	require.Error(t, w.Reload())
	require.NoError(t, os.WriteFile(file, []byte(one), 0o644))
	require.NoError(t, w.Reload())
	require.Equal(t, 1, w.Current().Len())

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestWatcherReportsDirectoryErrors(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "t.yaml"), []byte(one), 0o644))

	errs := make(chan error, 4)

	w, err := NewWatcher(dir, &testRules, time.Hour, logrus.New(), nil, func(err error) { errs <- err })
	require.NoError(t, err)

	require.NoError(t, os.RemoveAll(dir))

	w.poll()
	require.Error(t, <-errs)
	require.Error(t, w.Reload())

	// A poll that sees no change does nothing.
	fresh := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(fresh, "t.yaml"), []byte(one), 0o644))

	quiet, err := NewWatcher(fresh, &testRules, time.Hour, logrus.New(), nil, nil)
	require.NoError(t, err)
	quiet.poll()
	quiet.report(errors.New("x"))
	require.Equal(t, 1, quiet.Current().Len())
}

func TestNewWatcherErrors(t *testing.T) {
	_, err := NewWatcher(filepath.Join(t.TempDir(), "missing"), &testRules, time.Second, logrus.New(), nil, nil)
	require.Error(t, err)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "t.yaml"), []byte(one), 0o644))

	orig := statFile
	statFile = func(string) (os.FileInfo, error) { return nil, errors.New("vanished") }

	t.Cleanup(func() { statFile = orig })

	_, err = NewWatcher(dir, &testRules, time.Second, logrus.New(), nil, nil)
	require.ErrorContains(t, err, "vanished")
}
