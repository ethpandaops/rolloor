package hooks

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestNewRunnerErrors(t *testing.T) {
	_, err := NewRunner(filepath.Join(t.TempDir(), "missing"), time.Second, "e", logrus.New())
	require.Error(t, err)

	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o644))

	_, err = NewRunner(file, time.Second, "e", logrus.New())
	require.ErrorContains(t, err, "not a directory")
}

func TestRunEdgeCases(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "silent", `exit 4`)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "noexec"), []byte("#!/bin/sh\n"), 0o644))

	r, err := NewRunner(dir, 2*time.Second, "e", logrus.New())
	require.NoError(t, err)

	res, err := r.Run(context.Background(), "silent", "ready", "t", nil)
	require.NoError(t, err)
	require.Equal(t, "exit 4", res.Reason)

	// A program that cannot start at all is an error, not a failed run.
	_, err = r.Run(context.Background(), "noexec", "ready", "t", nil)
	require.Error(t, err)

	// Input that cannot be encoded is an error.
	_, err = r.Run(context.Background(), "silent", "ready", "t", make(chan int))
	require.Error(t, err)

	_, err = r.Run(context.Background(), "", "ready", "t", nil)
	require.Error(t, err)

	require.False(t, r.Exists("noexec"))
	require.Equal(t, "", firstLine("  \n"))
}
