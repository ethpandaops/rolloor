package ethpandaops

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"
)

// jsonServer answers fixed bodies by path.
func jsonServer(t *testing.T, bodies map[string]string) string {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := bodies[r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)

			return
		}

		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	return srv.URL
}

func TestSettingsFromEnv(t *testing.T) {
	env := map[string]string{
		"ETHPANDAOPS_UPDATER_TOKEN": "t", "ETHPANDAOPS_NODE_AUTH": "u:p", "ETHPANDAOPS_BEACON": "https://bn/",
		"ETHPANDAOPS_FINALITY_LAG": "3", "ETHPANDAOPS_FORK_MARGIN": "2", "ETHPANDAOPS_QUIET_EPOCHS": "10, 20,",
		"ETHPANDAOPS_TOLERANCE": "0.1", "ETHPANDAOPS_FLOOR": "0.9", "ETHPANDAOPS_SAMPLE": "5", "ETHPANDAOPS_UPDATE_WAIT": "5s",
	}

	s, err := SettingsFromEnv(func(k string) string { return env[k] })
	require.NoError(t, err)
	require.Equal(t, Settings{
		UpdaterToken: "t", NodeAuth: "u:p", Beacon: "https://bn", FinalityLag: 3, ForkMargin: 2,
		QuietEpochs: []uint64{10, 20}, Tolerance: 0.1, Floor: 0.9, Sample: 5, UpdateWait: 5 * time.Second,
	}, s)

	for _, k := range []string{"ETHPANDAOPS_FINALITY_LAG", "ETHPANDAOPS_FORK_MARGIN", "ETHPANDAOPS_QUIET_EPOCHS",
		"ETHPANDAOPS_TOLERANCE", "ETHPANDAOPS_FLOOR", "ETHPANDAOPS_SAMPLE", "ETHPANDAOPS_UPDATE_WAIT"} {
		_, err := SettingsFromEnv(func(name string) string {
			if name == k {
				return "x"
			}

			return ""
		})
		require.ErrorContains(t, err, k, k)
	}
}

func TestRunDispatch(t *testing.T) {
	h := testHooks()

	var out strings.Builder
	require.Equal(t, 2, Run(context.Background(), h, "nope", strings.NewReader(""), &out))
	require.Contains(t, out.String(), "known: environment, inspect")

	out.Reset()
	require.Equal(t, 1, Run(context.Background(), h, "inspect", iotest.ErrReader(errors.New("gone")), &out))
	require.Equal(t, "read stdin: gone\n", out.String())

	require.Equal(t, "123456789012", short("sha256:1234567890123456"))
	require.Equal(t, "abc", short("abc"))
	require.Equal(t, strings.Repeat("x", 200), firstLine(strings.Repeat("x", 300)))

	// sleep honours the context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, sleep(ctx, time.Hour), context.Canceled)
	require.NoError(t, sleep(context.Background(), time.Millisecond))
}

func TestCallEdges(t *testing.T) {
	h := testHooks()

	_, _, err := h.call(context.Background(), http.MethodPost, "http://x", make(chan int), h.nodeAuth, nil)
	require.Error(t, err)

	_, _, err = h.call(context.Background(), "BAD METHOD", "http://x", nil, h.nodeAuth, nil)
	require.Error(t, err)

	_, _, err = h.call(context.Background(), http.MethodGet, "http://127.0.0.1:1", nil, h.nodeAuth, nil)
	require.Error(t, err)

	srv := jsonServer(t, map[string]string{"/bad": "{", "/empty": ""})

	var v map[string]any

	_, _, err = h.call(context.Background(), http.MethodGet, srv+"/bad?secret=1", nil, h.nodeAuth, &v)
	require.ErrorContains(t, err, "decode")
	require.NotContains(t, err.Error(), "secret")

	_, _, err = h.call(context.Background(), http.MethodGet, srv+"/empty", nil, h.nodeAuth, &v)
	require.NoError(t, err)

	_, _, err = h.call(context.Background(), http.MethodGet, srv+"/missing", nil, h.nodeAuth, &v)
	require.ErrorIs(t, err, errNotFound)

	// A body cut short mid-read.
	cut := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		_, _ = w.Write([]byte("{"))
	}))
	t.Cleanup(cut.Close)

	_, _, err = h.call(context.Background(), http.MethodGet, cut.URL, nil, h.nodeAuth, &v)
	require.Error(t, err)

	// No token and no node auth send no Authorization header.
	h.UpdaterToken, h.NodeAuth = "", ""
	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	h.updaterAuth(req)
	h.nodeAuth(req)
	require.Empty(t, req.Header.Get("Authorization"))
}

func TestRefreshTeams(t *testing.T) {
	srv := jsonServer(t, map[string]string{
		"/api/v1/users/lighthouse":  "michaelsproul\n# comment\n\npaulhauner\n",
		"/api/v1/users/ethpandaops": "samcm\npk910\n",
	})

	dir := t.TempDir()
	out := filepath.Join(dir, "teams.yaml")

	n, err := RefreshTeams(context.Background(), http.DefaultClient, &TeamsRequest{
		API: srv + "/", Out: out,
		Teams:   map[string][]string{"lighthouse": {"lighthouse"}, "operators": {"ethpandaops"}},
		Members: map[string][]string{"operators": {"bharath-123", "samcm"}},
	})
	require.NoError(t, err)
	require.Equal(t, 2, n)

	raw, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Equal(t, `# Written by rolloor-ethpandaops teams from the coredevs registry; edits are overwritten.
lighthouse:
    - michaelsproul
    - paulhauner
operators:
    - bharath-123
    - pk910
    - samcm
`, string(raw))

	// A team that cannot be read leaves the file as it was.
	_, err = RefreshTeams(context.Background(), http.DefaultClient, &TeamsRequest{API: srv, Out: out, Teams: map[string][]string{"x": {"missing"}}})
	require.ErrorContains(t, err, "team missing")

	after, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Equal(t, raw, after)

	_, err = RefreshTeams(context.Background(), http.DefaultClient, &TeamsRequest{API: "http://127.0.0.1:1", Out: out, Teams: map[string][]string{"x": {"y"}}})
	require.Error(t, err)

	_, err = RefreshTeams(context.Background(), http.DefaultClient, &TeamsRequest{API: "::bad", Out: out, Teams: map[string][]string{"x": {"y"}}})
	require.Error(t, err)

	_, err = RefreshTeams(context.Background(), http.DefaultClient, &TeamsRequest{Out: filepath.Join(dir, "missing", "teams.yaml")})
	require.Error(t, err)

	// A registry answer cut short mid-read.
	cut := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		_, _ = w.Write([]byte("a"))
	}))
	t.Cleanup(cut.Close)

	_, err = RefreshTeams(context.Background(), http.DefaultClient, &TeamsRequest{API: cut.URL, Out: out, Teams: map[string][]string{"x": {"y"}}})
	require.Error(t, err)

	// The destination is a directory, so the rename fails.
	require.NoError(t, os.Mkdir(filepath.Join(dir, "adir"), 0o755))
	_, err = RefreshTeams(context.Background(), http.DefaultClient, &TeamsRequest{Out: filepath.Join(dir, "adir")})
	require.Error(t, err)
}
