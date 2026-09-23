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

func writeScript(t *testing.T, dir, name, body string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body), 0o755))
}

func TestRun(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "ok", `read in; echo "got $in $ROLLOOR_HOOK $ROLLOOR_TARGET_ID"; echo second`)
	writeScript(t, dir, "fail", `echo "nope: $ROLLOOR_ENVIRONMENT" >&2; exit 3`)
	writeScript(t, dir, "slow", `sleep 10`)

	r, err := NewRunner(dir, 2*time.Second, "env-1", logrus.New())
	require.NoError(t, err)
	require.True(t, r.Exists("ok"))
	require.False(t, r.Exists("missing"))

	res, err := r.Run(context.Background(), "ok", "ready", "t-1", map[string]string{"id": "t-1"})
	require.NoError(t, err)
	require.True(t, res.OK)
	require.Equal(t, `got {"id":"t-1"} ready t-1`, res.Reason)

	res, err = r.Run(context.Background(), "fail", "ready", "t-1", nil)
	require.NoError(t, err)
	require.False(t, res.OK)
	require.Equal(t, 3, res.ExitCode)
	require.Equal(t, "nope: env-1", res.Reason)

	res, err = r.Run(context.Background(), "slow", "ready", "t-1", nil)
	require.NoError(t, err)
	require.False(t, res.OK)
	require.True(t, res.TimedOut)

	_, err = r.Run(context.Background(), "missing", "ready", "t-1", nil)
	require.Error(t, err)

	_, err = r.Run(context.Background(), "../x", "ready", "t-1", nil)
	require.Error(t, err)
}

func TestHiddenVariablesNeverReachPrograms(t *testing.T) {
	t.Setenv("ROLLOOR_TEST_SECRET", "hunter2")
	t.Setenv("ROLLOOR_TEST_PLAIN", "visible")

	dir := t.TempDir()
	writeScript(t, dir, "env", `echo "[$ROLLOOR_TEST_SECRET][$ROLLOOR_TEST_PLAIN]"`)

	r, err := NewRunner(dir, 2*time.Second, "env-1", logrus.New())
	require.NoError(t, err)
	r.Hide("ROLLOOR_TEST_SECRET", "")

	res, err := r.Run(context.Background(), "env", "ready", "t-1", nil)
	require.NoError(t, err)
	require.Equal(t, "[][visible]", res.Reason)
}
