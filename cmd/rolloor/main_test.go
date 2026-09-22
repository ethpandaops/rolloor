package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func writeFixture(t *testing.T) (string, string) {
	t.Helper()

	dir := t.TempDir()
	hooksDir := filepath.Join(dir, "hooks")
	targetsDir := filepath.Join(dir, "targets.d")

	require.NoError(t, os.MkdirAll(hooksDir, 0o755))
	require.NoError(t, os.MkdirAll(targetsDir, 0o755))

	for _, name := range []string{"inspect", "update", "ready", "env"} {
		body := "#!/bin/sh\ncat >/dev/null; echo sha256:abc\n"
		if name == "ready" {
			body = "#!/bin/sh\nexit 1\n"
		}

		require.NoError(t, os.WriteFile(filepath.Join(hooksDir, name), []byte(body), 0o755))
	}

	require.NoError(t, os.WriteFile(filepath.Join(targetsDir, "t.yaml"), []byte(`
- {id: a/x, node: a, weight: 1, image: 127.0.0.1:1/org/a:t, labels: {client: a, owner: a}}
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "teams.yaml"), []byte("a: [sam]\n"), 0o644))

	cfg := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(cfg, []byte(`
environment: t
dataDir: `+filepath.Join(dir, "data")+`
targetsDir: `+targetsDir+`
teamsFile: `+filepath.Join(dir, "teams.yaml")+`
labels: {group: client, owner: owner}
registry: {plainHttp: ["127.0.0.1:1"], poll: 1s}
hooks: {dir: `+hooksDir+`, environment: {program: env}}
`), 0o644))

	return dir, cfg
}

func TestValidateAndHook(t *testing.T) {
	dir, cfg := writeFixture(t)

	var out bytes.Buffer
	require.NoError(t, validate(&out, cfg))
	require.Contains(t, out.String(), "ok: 1 targets on 1 nodes in 1 groups")

	require.Error(t, validate(&out, filepath.Join(dir, "missing.yaml")))

	// A default program that does not exist fails validation.
	bad := filepath.Join(dir, "bad.yaml")
	require.NoError(t, os.WriteFile(bad, []byte("environment: t\ntargetsDir: "+filepath.Join(dir, "targets.d")+"\nlabels: {group: client, owner: owner}\nhooks: {dir: "+filepath.Join(dir, "hooks")+", defaults: {soak: nope}}\n"), 0o644))
	require.ErrorContains(t, validate(&out, bad), "hooks.defaults.soak")

	// A bad teams file fails validation.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "teams.yaml"), []byte("- not a map\n"), 0o644))
	require.ErrorContains(t, validate(&out, cfg), "teams file")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "teams.yaml"), []byte("a: [sam]\n"), 0o644))

	_, err := loadTeams(filepath.Join(dir, "nope.yaml"))
	require.Error(t, err)

	ctx := context.Background()

	out.Reset()
	require.NoError(t, runHook(ctx, &out, cfg, "inspect", "a/x", ""))
	require.Contains(t, out.String(), "sha256:abc")

	require.NoError(t, runHook(ctx, &out, cfg, "environment", "", ""))
	require.ErrorContains(t, runHook(ctx, &out, cfg, "ready", "a/x", ""), "did not pass")
	require.ErrorContains(t, runHook(ctx, &out, cfg, "soak", "a/x", ""), "no program configured")
	require.ErrorContains(t, runHook(ctx, &out, cfg, "inspect", "nope", ""), "not in the targets files")
	require.Error(t, runHook(ctx, &out, cfg, "inspect", "a/x", "missing-program"))
	require.Error(t, runHook(ctx, &out, filepath.Join(dir, "missing.yaml"), "inspect", "a/x", ""))
}

func TestRootCommand(t *testing.T) {
	_, cfg := writeFixture(t)

	root := newRoot()
	root.SetArgs([]string{"validate", "--config", cfg})

	var out bytes.Buffer
	root.SetOut(&out)
	require.NoError(t, root.Execute())
	require.Contains(t, out.String(), "ok:")

	root = newRoot()
	root.SetArgs([]string{"hook", "inspect", "--target", "a/x", "--config", cfg})
	root.SetOut(&out)
	require.NoError(t, root.Execute())
}

func TestServeRefusesOIDCUntilBuilt(t *testing.T) {
	dir, _ := writeFixture(t)
	cfg := filepath.Join(dir, "oidc.yaml")
	require.NoError(t, os.WriteFile(cfg, []byte("environment: t\nauth: {mode: oidc, issuer: i, clientId: c, redirectUrl: r}\n"), 0o644))
	require.ErrorContains(t, serve(context.Background(), cfg), "auth.mode")
	require.Error(t, serve(context.Background(), filepath.Join(dir, "missing.yaml")))
}

func TestServeRunsAndStops(t *testing.T) {
	_, cfg := writeFixture(t)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := l.Addr().String()
	require.NoError(t, l.Close())

	raw, err := os.ReadFile(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(cfg, append(raw, []byte("listen: \""+addr+"\"\n")...), 0o644))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- serve(ctx, cfg) }()

	client := &http.Client{Timeout: time.Second}

	require.Eventually(t, func() bool {
		resp, err := client.Get("http://" + addr + "/healthz")
		if err != nil {
			return false
		}

		resp.Body.Close()

		return resp.StatusCode == http.StatusOK
	}, 10*time.Second, 20*time.Millisecond)

	for _, path := range []string{"/metrics", "/api/v1/fleet", "/api/v1/targets"} {
		resp, err := client.Get("http://" + addr + path)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, path)
	}

	cancel()
	require.NoError(t, <-done)
}
