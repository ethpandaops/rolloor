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
	require.NoError(t, os.WriteFile(filepath.Join(dir, "noexec"), []byte("#!/bin/sh\n"), 0o644))

	r, err := NewRunner(dir, 10*time.Second, "e", logrus.New())
	require.NoError(t, err)

	require.False(t, r.Exists("noexec"))
	require.Equal(t, "", firstLine("  \n"))
}

func TestOutputIsBounded(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "loud", `echo first; head -c 200000 /dev/zero | tr '\0' 'x'; echo; head -c 100000 /dev/zero | tr '\0' 'y' >&2`)

	r, err := NewRunner(dir, 5*time.Second, "e", logrus.New())
	require.NoError(t, err)

	res, err := r.Run(context.Background(), "loud", "ready", "t", nil)
	require.NoError(t, err)
	require.True(t, res.OK)
	require.Equal(t, "first", res.Reason)
	require.Len(t, res.Stdout, maxCapture)

	c := &capped{limit: 4}
	n, err := c.Write([]byte("abcdef"))
	require.NoError(t, err)
	require.Equal(t, 6, n)
	require.Equal(t, "abcd", c.String())
	require.Equal(t, 4, c.Len())
	require.Equal(t, 2, c.dropped)
}
