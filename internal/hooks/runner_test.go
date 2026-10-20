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

const programOK = "ok"

func writeScript(t *testing.T, dir, name, body string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body), 0o755))
}

func TestRun(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, programOK, `read in; echo "got $in $ROLLOOR_HOOK $ROLLOOR_TARGET_ID"; echo second`)

	r, err := NewRunner(dir, 2*time.Second, "env-1", logrus.New())
	require.NoError(t, err)
	require.True(t, r.Exists(programOK))
	require.False(t, r.Exists("missing"))

	res, err := r.Run(context.Background(), programOK, "ready", "t-1", map[string]string{"id": "t-1"})
	require.NoError(t, err)
	require.True(t, res.OK)
	require.Equal(t, `got {"id":"t-1"} ready t-1`, res.Reason)
}

func TestRunClassifiesExits(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, "yes", `echo fine`)
	writeScript(t, dir, "no", `echo "outside tolerance"; exit 1`)
	writeScript(t, dir, "unknown", `echo "api down: $ROLLOOR_ENVIRONMENT" >&2; exit 3`)
	writeScript(t, dir, "odd", `exit 4`)
	writeScript(t, dir, "killed", `kill -9 $$`)
	writeScript(t, dir, "slow", `sleep 10`)

	r, err := NewRunner(dir, 2*time.Second, "env-1", logrus.New())
	require.NoError(t, err)

	cases := []struct {
		program       string
		ok            bool
		exit          int
		couldNotCheck bool
		timedOut      bool
		reason        string
	}{
		{program: "yes", ok: true, exit: 0, reason: "fine"},
		{program: "no", exit: 1, reason: "outside tolerance"},
		{program: "unknown", exit: 3, couldNotCheck: true, reason: "api down: env-1"},
		{program: "odd", exit: 4, reason: "exit 4"},
		{program: "killed", exit: -1, couldNotCheck: true, reason: "signal: killed"},
		{program: "slow", exit: -1, couldNotCheck: true, timedOut: true, reason: "timed out after 2s"},
	}

	for _, tc := range cases {
		t.Run(tc.program, func(t *testing.T) {
			res, err := r.Run(context.Background(), tc.program, "ready", "t-1", nil)
			require.NoError(t, err)
			require.Equal(t, tc.ok, res.OK)
			require.Equal(t, tc.exit, res.ExitCode)
			require.Equal(t, tc.couldNotCheck, res.CouldNotCheck())
			require.Equal(t, tc.timedOut, res.TimedOut)
			require.Equal(t, tc.reason, res.Reason)
		})
	}
}

func TestRunErrorsCouldNotCheck(t *testing.T) {
	dir := t.TempDir()
	writeScript(t, dir, programOK, `exit 0`)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "noexec"), []byte("#!/bin/sh\n"), 0o644))

	r, err := NewRunner(dir, 2*time.Second, "env-1", logrus.New())
	require.NoError(t, err)

	cases := []struct {
		name     string
		program  string
		input    any
		canceled bool
	}{
		{name: "missing", program: "missing"},
		{name: "not executable", program: "noexec"},
		{name: "not a bare name", program: "../x"},
		{name: "no name", program: ""},
		{name: "input not encodable", program: programOK, input: make(chan int)},
		{name: "canceled", program: programOK, canceled: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if tc.canceled {
				cancel()
			}

			res, err := r.Run(ctx, tc.program, "ready", "t-1", tc.input)
			require.Error(t, err)
			require.False(t, res.OK)
			require.Equal(t, -1, res.ExitCode)
			require.True(t, res.CouldNotCheck())
		})
	}
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
