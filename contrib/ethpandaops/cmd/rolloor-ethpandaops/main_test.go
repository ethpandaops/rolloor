package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	cmd      = "rolloor-ethpandaops"
	flagTeam = "--team"
)

func noEnv(string) string { return "" }

func TestDispatch(t *testing.T) {
	ctx := context.Background()

	var out strings.Builder

	// Called by its own name with nothing else: usage.
	require.Equal(t, 2, run(ctx, []string{"/usr/local/bin/rolloor-ethpandaops"}, strings.NewReader(""), &out, noEnv))
	require.Contains(t, out.String(), "usage: rolloor-ethpandaops <program>|teams")

	// Linked under a program name, the name picks the program.
	out.Reset()
	require.Equal(t, 1, run(ctx, []string{"/etc/rolloor/hooks/environment"}, strings.NewReader("{}"), &out, noEnv))
	require.Equal(t, "reference beacon node: ETHPANDAOPS_BEACON is not set\n", out.String())

	// Or the first argument does.
	out.Reset()
	require.Equal(t, 1, run(ctx, []string{cmd, "environment"}, strings.NewReader("{}"), &out, noEnv))
	require.Contains(t, out.String(), "ETHPANDAOPS_BEACON")

	out.Reset()
	require.Equal(t, 2, run(ctx, []string{cmd, "nope"}, strings.NewReader(""), &out, noEnv))
	require.Contains(t, out.String(), `unknown program "nope"`)

	// Bad settings stop before any program runs.
	out.Reset()

	bad := func(k string) string {
		if k == "ETHPANDAOPS_SAMPLE" {
			return "0"
		}

		return ""
	}
	require.Equal(t, 2, run(ctx, []string{"inspect"}, strings.NewReader("{}"), &out, bad))
	require.Contains(t, out.String(), "ETHPANDAOPS_SAMPLE")
}

func TestTeams(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/users/lighthouse" {
			w.WriteHeader(http.StatusNotFound)

			return
		}

		_, _ = w.Write([]byte("michaelsproul\n"))
	}))
	t.Cleanup(srv.Close)

	file := filepath.Join(t.TempDir(), "teams.yaml")
	ctx := context.Background()

	var out strings.Builder

	code := run(ctx, []string{cmd, teamsCommand, "--api", srv.URL, "--out", file, flagTeam, "lighthouse=lighthouse", "--member", "operators=samcm"}, nil, &out, noEnv)
	require.Equal(t, 0, code, out.String())
	require.Equal(t, "teams: wrote 2 owners to "+file+"\n", out.String())

	raw, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Contains(t, string(raw), "michaelsproul")
	require.Contains(t, string(raw), "samcm")

	out.Reset()
	code = run(ctx, []string{cmd, teamsCommand, "--api", srv.URL, "--out", file, flagTeam, "x=missing"}, nil, &out, noEnv)
	require.Equal(t, 1, code)
	require.Contains(t, out.String(), "left as it was")

	out.Reset()
	require.Equal(t, 2, run(ctx, []string{cmd, teamsCommand, flagTeam, "novalue"}, nil, &out, noEnv))
	require.Contains(t, out.String(), `want owner=value, got "novalue"`)

	p := pairs{"a": {"b"}}
	require.Equal(t, "map[a:[b]]", p.String())
}
