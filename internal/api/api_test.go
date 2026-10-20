package api

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/hooks"
	"github.com/ethpandaops/rolloor/internal/reconcile"
	"github.com/ethpandaops/rolloor/internal/registry"
	"github.com/ethpandaops/rolloor/internal/targets"
)

const (
	d1 = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	d2 = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
)

const (
	foreignOrigin = "https://evil.example"
	mediaText     = "text/plain"
	keyPaused     = "paused"
)

var errFake = errors.New("fake")

const admin = "sam"

// fakeWorld is a registry and a fleet whose hooks always succeed.
type fakeWorld struct {
	mu      sync.Mutex
	digest  string
	running map[string]string
}

func (f *fakeWorld) Resolve(context.Context, string) (registry.Resolved, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return registry.Resolved{Digest: f.digest, Revision: "rev"}, nil
}

func (f *fakeWorld) Run(_ context.Context, program, hook, id string, input any) (hooks.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	res := hooks.Result{Program: program, OK: true}

	switch hook {
	case config.HookInspect:
		res.Reason = f.running[id]
	case config.HookUpdate:
		if in, ok := input.(reconcile.HookInput); ok {
			f.running[id] = in.Desired
		}
	}

	return res, nil
}

type fakeAuth struct {
	id  Identity
	err error
}

func (a *fakeAuth) Identity(*http.Request) (Identity, error) { return a.id, a.err }
func (a *fakeAuth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Middleware", "yes")
		next.ServeHTTP(w, r)
	})
}

const fleetYAML = `
- {id: a-1/cl, node: a-1, weight: 0,   image: org/a:t, labels: {client: a, owner: a, wave: "0"}}
- {id: a-2/cl, node: a-2, weight: 100, image: org/a:t, labels: {client: a, owner: a, wave: "1"}}
- {id: b-1/el, node: a-2, weight: 100, image: org/b:t, labels: {client: b, owner: b, wave: "1"}}
- {id: c-1/x,  node: c-1, weight: 0,   image: org/c:t, labels: {client: c, owner: a, wave: "0"}}
- {id: c-2/x,  node: c-2, weight: 0,   image: org/c:t, labels: {client: c, owner: b, wave: "0"}}
`

type fixture struct {
	t     *testing.T
	ctx   context.Context //nolint:containedctx // test fixture
	cfg   *config.Config
	c     *reconcile.Controller
	store *reconcile.MemoryStore
	world *fakeWorld
	auth  *fakeAuth
	bc    *Broadcaster
	srv   *httptest.Server
	set   *targets.Set
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	cfg, err := config.Parse([]byte("environment: test\ndisruptionBudget: {maxUnavailable: 100%}\nlabels: {group: client, owner: owner}\nhooks: {dir: /tmp}\nstrategy: {batchSize: 100%, soak: {duration: 0s}}\nstrategies:\n  fast: {batchSize: 100%}\n"))
	require.NoError(t, err)

	set, err := targets.Parse([]byte(fleetYAML), &targets.Rules{GroupLabel: "client", OwnerLabel: "owner", WaveLabel: "wave", KnownHooks: config.TargetHooks})
	require.NoError(t, err)

	f := &fixture{t: t, ctx: context.Background(), cfg: cfg, store: reconcile.NewMemoryStore(), set: set,
		world: &fakeWorld{digest: d1, running: map[string]string{"a-1/cl": d1, "a-2/cl": d1, "b-1/el": d1, "c-1/x": d1, "c-2/x": d1}},
		auth:  &fakeAuth{id: Identity{Name: admin, Admin: true}}, bc: NewBroadcaster()}

	ids := 0
	f.c, err = reconcile.New(f.ctx, &reconcile.Options{Config: cfg, Targets: func() *targets.Set { return f.set }, Resolver: f.world, Runner: f.world,
		Store: f.store, Notifier: f.bc, Log: logrus.New(), NewID: func() string {
			ids++

			return "r-" + strconv.Itoa(ids)
		}})
	require.NoError(t, err)

	f.c.InspectAll(f.ctx)
	f.c.ProbeAll(f.ctx)
	require.NoError(t, f.c.Tick(f.ctx))

	s := New(cfg, f.c, func() *targets.Set { return f.set }, f.auth, f.bc, "test", logrus.New())
	f.srv = httptest.NewServer(s.Handler())
	t.Cleanup(f.srv.Close)

	return f
}

// release moves the tag and opens a rollout for both groups.
func (f *fixture) release() {
	f.t.Helper()
	f.world.mu.Lock()
	f.world.digest = d2
	f.world.mu.Unlock()
	f.c.Refresh(f.ctx, "test")
	require.NoError(f.t, f.c.Tick(f.ctx))
}

func (f *fixture) do(method, path, body string) (int, map[string]any, string) {
	f.t.Helper()

	req, err := http.NewRequestWithContext(f.ctx, method, f.srv.URL+path, strings.NewReader(body))
	require.NoError(f.t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(f.t, err)

	defer resp.Body.Close()

	var buf strings.Builder

	_, _ = bufio.NewReader(resp.Body).WriteTo(&buf)

	raw := buf.String()
	out := map[string]any{}
	_ = json.Unmarshal([]byte(raw), &out)

	return resp.StatusCode, out, raw
}

func TestReadRoutes(t *testing.T) {
	f := newFixture(t)

	code, body, _ := f.do(http.MethodGet, "/healthz", "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "test", body["version"])

	code, body, _ = f.do(http.MethodGet, "/api/v1/me", "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, admin, body["name"])

	code, body, _ = f.do(http.MethodGet, "/api/v1/fleet", "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "test", body["environment"])

	code, _, _ = f.do(http.MethodGet, "/api/v1/groups/owner/a", "")
	require.Equal(t, http.StatusNotFound, code)
	code, _, _ = f.do(http.MethodGet, "/api/v1/groups/client/zzz", "")
	require.Equal(t, http.StatusNotFound, code)
	code, body, _ = f.do(http.MethodGet, "/api/v1/groups/client/a", "")
	require.Equal(t, http.StatusOK, code)
	require.Len(t, body["targets"], 2)
	require.Equal(t, map[string]any{keyPaused: false}, pick(body["group"], keyPaused, "strategy"), "the default strategy has no name")

	code, _, _ = f.do(http.MethodGet, "/api/v1/nodes/zzz", "")
	require.Equal(t, http.StatusNotFound, code)
	code, body, _ = f.do(http.MethodGet, "/api/v1/nodes/a-2", "")
	require.Equal(t, http.StatusOK, code)
	require.Len(t, body["targets"], 2)

	code, _, _ = f.do(http.MethodGet, "/api/v1/targets?selector=nope", "")
	require.Equal(t, http.StatusBadRequest, code)
	code, _, raw := f.do(http.MethodGet, "/api/v1/targets?selector=client=zzz", "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "[]\n", raw)
	code, _, raw = f.do(http.MethodGet, "/api/v1/targets?selector=client=a", "")
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, raw, "a-1/cl")

	code, _, raw = f.do(http.MethodGet, "/api/v1/rollouts", "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "[]\n", raw)

	code, _, _ = f.do(http.MethodGet, "/api/v1/rollouts/nope", "")
	require.Equal(t, http.StatusNotFound, code)

	code, _, raw = f.do(http.MethodGet, "/api/v1/history?limit=5", "")
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, raw, "digest.changed")
	code, _, raw = f.do(http.MethodGet, "/api/v1/history?node=a-2", "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "[]\n", raw)

	code, _, _ = f.do(http.MethodGet, "/api/v1/history?selector=nope", "")
	require.Equal(t, http.StatusBadRequest, code)

	code, _, raw = f.do(http.MethodGet, "/api/v1/history?selector=client=a", "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "[]\n", raw, "digest events name images, not targets")

	code, _, raw = f.do(http.MethodGet, "/api/v1/suspensions", "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "[]\n", raw)

	f.store.Fail = errFake
	code, _, _ = f.do(http.MethodGet, "/api/v1/history", "")
	require.Equal(t, http.StatusInternalServerError, code)

	f.store.Fail = nil
}

func TestRolloutRoutesAndVerbs(t *testing.T) {
	f := newFixture(t)
	f.release()

	code, body, _ := f.do(http.MethodGet, "/api/v1/rollouts/r-1", "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "a", body["group"])

	// Bad bodies.
	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/pause", "{")
	require.Equal(t, http.StatusBadRequest, code)
	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/pause", `{"bogus": 1}`)
	require.Equal(t, http.StatusBadRequest, code)
	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/pause", `{"rollout": "nope"}`)
	require.Equal(t, http.StatusNotFound, code)

	// promote before pause is a state conflict.
	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/promote", `{"rollout": "r-1"}`)
	require.Equal(t, http.StatusConflict, code)

	code, body, _ = f.do(http.MethodPost, "/api/v1/actions/pause", `{"rollout": "r-1"}`)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, true, body["pausePending"])

	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/promote", `{"rollout": "r-1"}`)
	require.Equal(t, http.StatusOK, code)

	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/retry", `{"rollout": "r-1", "reason": "x"}`)
	require.Equal(t, http.StatusConflict, code)

	code, body, _ = f.do(http.MethodPost, "/api/v1/actions/abort", `{"rollout": "r-1"}`)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "Aborted", body["state"])

	// Sync recreates it.
	code, body, _ = f.do(http.MethodPost, "/api/v1/actions/sync", `{"selector": "client=a", "force": true, "strategy": "fast"}`)
	require.Equal(t, http.StatusOK, code)
	require.Len(t, body["rollouts"], 1)

	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/sync", `{"selector": "client=a", "strategy": "warp"}`)
	require.Equal(t, http.StatusBadRequest, code)
	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/sync", `{"selector": "nope"}`)
	require.Equal(t, http.StatusBadRequest, code)
	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/sync", `{"selector": "client=zzz"}`)
	require.Equal(t, http.StatusNotFound, code)

	// A selector across two owners needs confirmation.
	code, body, _ = f.do(http.MethodPost, "/api/v1/actions/sync", `{"selector": "node=a-2"}`)
	require.Equal(t, http.StatusConflict, code)
	require.Equal(t, "confirm_required", body["code"])

	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/sync", `{"selector": "node=a-2", "confirm": true}`)
	require.Equal(t, http.StatusOK, code)

	code, body, _ = f.do(http.MethodPost, "/api/v1/actions/refresh", "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "refreshing", body["status"])
}

// pick returns the named fields present in a decoded JSON object.
func pick(v any, keys ...string) map[string]any {
	obj, _ := v.(map[string]any)
	out := map[string]any{}

	for _, k := range keys {
		if x, ok := obj[k]; ok {
			out[k] = x
		}
	}

	return out
}

func TestGroupsShowDeclaredPauseAndStrategy(t *testing.T) {
	f := newFixture(t)
	require.NoError(t, f.c.ConfigureGroups(f.ctx, false, map[string]config.Group{"a": {Paused: true, Strategy: "fast"}}))

	_, body, _ := f.do(http.MethodGet, "/api/v1/groups/client/a", "")
	require.Equal(t, map[string]any{keyPaused: true, "strategy": "fast"}, pick(body["group"], keyPaused, "strategy"))
	_, body, _ = f.do(http.MethodGet, "/api/v1/groups/client/b", "")
	require.Equal(t, map[string]any{keyPaused: false}, pick(body["group"], keyPaused, "strategy"))

	// A pause at the top holds every group, whatever its own settings say.
	require.NoError(t, f.c.ConfigureGroups(f.ctx, true, nil))

	_, body, _ = f.do(http.MethodGet, "/api/v1/fleet", "")
	groups, isList := body["groups"].([]any)
	require.True(t, isList)
	require.NotEmpty(t, groups)

	for _, g := range groups {
		require.Equal(t, map[string]any{keyPaused: true}, pick(g, keyPaused, "strategy"))
	}
}

func TestPolicyRoutesAreGone(t *testing.T) {
	f := newFixture(t)

	for _, method := range []string{http.MethodGet, http.MethodPut} {
		code, _, _ := f.do(method, "/api/v1/policies/a", `{"mode": "manual", "strategy": "fast"}`)
		require.Equal(t, http.StatusNotFound, code, method)
	}

	g, _, ok := f.c.Group("a")
	require.True(t, ok)
	require.Empty(t, g.Strategy)
}

func TestPauseTakesAnOptionalExpiry(t *testing.T) {
	f := newFixture(t)
	f.release()

	code, body, _ := f.do(http.MethodPost, "/api/v1/actions/pause", `{"rollout": "r-1", "expiresIn": "soon"}`)
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body["error"], "expiresIn")

	r1, ok := f.c.Rollout("r-1")
	require.True(t, ok)
	require.False(t, r1.PausePending, "a malformed expiry pauses nothing")
	require.Zero(t, r1.PauseExpiresAt)

	for _, tc := range []struct {
		name string
		body string
		want time.Duration
	}{
		{name: "absent", body: `{"rollout": "r-1"}`, want: 24 * time.Hour},
		{name: "a duration", body: `{"rollout": "r-1", "expiresIn": "2h"}`, want: 2 * time.Hour},
		{name: "nonpositive", body: `{"rollout": "r-1", "expiresIn": "-1h"}`, want: 24 * time.Hour},
	} {
		code, body, _ = f.do(http.MethodPost, "/api/v1/actions/pause", tc.body)
		require.Equal(t, http.StatusOK, code, tc.name)
		require.Equal(t, true, body["pausePending"], tc.name)

		raw, isString := body["pauseExpiresAt"].(string)
		require.True(t, isString, tc.name)

		at, err := time.Parse(time.RFC3339Nano, raw)
		require.NoError(t, err, tc.name)
		require.WithinDuration(t, time.Now().Add(tc.want), at, time.Minute, tc.name)

		// Promote lifts the pause before it lapses and forgets the expiry.
		code, body, _ = f.do(http.MethodPost, "/api/v1/actions/promote", `{"rollout": "r-1"}`)
		require.Equal(t, http.StatusOK, code, tc.name)
		require.NotContains(t, body, "pausePending", tc.name)
		require.NotContains(t, body, "pauseExpiresAt", tc.name)
	}
}

func TestSuspendAndResume(t *testing.T) {
	f := newFixture(t)

	code, _, _ := f.do(http.MethodPost, "/api/v1/actions/suspend", `{"selector": "node=a-1", "reason": "x", "expiresIn": "soon"}`)
	require.Equal(t, http.StatusBadRequest, code)
	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/suspend", `{"selector": "node=a-1"}`)
	require.Equal(t, http.StatusBadRequest, code, "reason required")
	code, body, _ := f.do(http.MethodPost, "/api/v1/actions/suspend", `{"selector": "node=a-1", "reason": "debugging", "expiresIn": "1h"}`)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "debugging", body["reason"])

	code, _, raw := f.do(http.MethodGet, "/api/v1/suspensions", "")
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, raw, "debugging")

	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/resume", `{"selector": "node=zzz"}`)
	require.Equal(t, http.StatusNotFound, code)
	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/resume", `{"selector": "id=a-2/cl"}`)
	require.Equal(t, http.StatusNotFound, code, "no suspension with that selector")
	code, body, _ = f.do(http.MethodPost, "/api/v1/actions/resume", `{"selector": "node=a-1"}`)
	require.Equal(t, http.StatusOK, code)
	require.InDelta(t, 1, body["lifted"], 0)
}

func TestAuthorization(t *testing.T) {
	f := newFixture(t)
	f.release()

	// An identity that owns only b may read everything but act only on b.
	f.auth.id = Identity{Name: "dev", Owners: []string{"b"}}

	code, _, _ := f.do(http.MethodGet, "/api/v1/fleet", "")
	require.Equal(t, http.StatusOK, code)

	code, body, _ := f.do(http.MethodPost, "/api/v1/actions/sync", `{"selector": "client=a"}`)
	require.Equal(t, http.StatusForbidden, code)
	require.Equal(t, "you're not listed under a", body["error"])
	require.Equal(t, []any{"b"}, body["ownersHeld"])

	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/pause", `{"rollout": "r-1"}`)
	require.Equal(t, http.StatusForbidden, code)

	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/sync", `{"selector": "client=b"}`)
	require.Equal(t, http.StatusOK, code)

	// A sync acts on whole groups, so owning the selected target is not enough
	// when its group has targets from other owners.
	code, body, _ = f.do(http.MethodPost, "/api/v1/actions/sync", `{"selector": "id=c-2/x"}`)
	require.Equal(t, http.StatusForbidden, code)
	require.Equal(t, "you're not listed under a", body["error"])

	// Suspend acts only on the selection, so that same target is fine.
	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/suspend", `{"selector": "id=c-2/x", "reason": "x"}`)
	require.Equal(t, http.StatusOK, code)

	// Lifting a suspension that spans two owners needs the confirmation too.
	f.auth.id = Identity{Name: admin, Admin: true}
	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/suspend", `{"selector": "client=c", "reason": "x", "confirm": true}`)
	require.Equal(t, http.StatusOK, code)

	code, body, _ = f.do(http.MethodPost, "/api/v1/actions/resume", `{"selector": "client=c"}`)
	require.Equal(t, http.StatusConflict, code)
	require.Equal(t, "confirm_required", body["code"])

	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/resume", `{"selector": "client=c", "confirm": true}`)
	require.Equal(t, http.StatusOK, code)

	// No identity at all is a 401 on every route that needs one.
	f.auth.err = errFake
	code, _, _ = f.do(http.MethodGet, "/api/v1/me", "")
	require.Equal(t, http.StatusUnauthorized, code)
	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/refresh", "")
	require.Equal(t, http.StatusUnauthorized, code)
	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/sync", `{"selector": "client=a"}`)
	require.Equal(t, http.StatusUnauthorized, code)
	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/pause", `{"rollout": "r-1"}`)
	require.Equal(t, http.StatusUnauthorized, code)

	// The authorizer's middleware wraps every route.
	resp, err := http.Get(f.srv.URL + "/healthz")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, "yes", resp.Header.Get("X-Middleware"))
}

// act posts a verb with extra headers and returns the status and body.
func (f *fixture) act(verb, body string, header map[string]string) (int, map[string]any) {
	f.t.Helper()

	req, err := http.NewRequestWithContext(f.ctx, http.MethodPost, f.srv.URL+"/api/v1/actions/"+verb, strings.NewReader(body))
	require.NoError(f.t, err)

	for k, v := range header {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(f.t, err)

	defer resp.Body.Close()

	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)

	return resp.StatusCode, out
}

func TestSessionActionsMustComeFromThisSiteAsJSON(t *testing.T) {
	f := newFixture(t)
	f.release()

	f.auth.id = Identity{Name: admin, Admin: true, Session: true}

	// Every verb refuses a cookie session from elsewhere, or one that does not
	// declare JSON, before reading the body, and nothing happens.
	body := `{"selector": "client=a", "rollout": "r-1", "reason": "x"}`

	for _, verb := range []string{"sync", "refresh", "suspend", "resume", "pause", "promote", "abort", "retry"} {
		code, _ := f.act(verb, body, map[string]string{headerOrigin: foreignOrigin, headerContentType: mediaJSON})
		require.Equal(t, http.StatusForbidden, code, verb)

		code, _ = f.act(verb, body, map[string]string{headerOrigin: f.srv.URL, headerContentType: mediaText})
		require.Equal(t, http.StatusUnsupportedMediaType, code, verb)
	}

	r1, ok := f.c.Rollout("r-1")
	require.True(t, ok)
	require.False(t, r1.PausePending)
	require.Empty(t, f.c.Suspensions())

	for name, h := range map[string]map[string]string{
		"no origin at all":            {headerContentType: mediaJSON},
		"a script without an origin":  {headerContentType: mediaJSON, "Authorization": "Basic c2FtOng="},
		"another site's referer":      {headerReferer: foreignOrigin + "/page", headerContentType: mediaJSON},
		"cross-site despite origin":   {headerFetchSite: fetchCrossSite, headerOrigin: f.srv.URL, headerContentType: mediaJSON},
		"a sibling subdomain":         {headerFetchSite: "same-site", headerContentType: mediaJSON},
		"this site, another origin":   {headerFetchSite: fetchSameOrigin, headerOrigin: foreignOrigin, headerContentType: mediaJSON},
		"an origin that is not a URL": {headerOrigin: "http://[::1", headerContentType: mediaJSON},
	} {
		code, _ := f.act("refresh", "", h)
		require.Equal(t, http.StatusForbidden, code, name)
	}

	// From this site, the body must be declared JSON: a form or text post is
	// what a page elsewhere could send without asking first.
	for name, h := range map[string]map[string]string{
		"no content type":    {headerOrigin: f.srv.URL},
		"a form":             {headerFetchSite: fetchSameOrigin, headerContentType: "application/x-www-form-urlencoded"},
		"a multipart form":   {headerFetchSite: fetchSameOrigin, headerContentType: "multipart/form-data; boundary=x"},
		"json-ish but not":   {headerFetchSite: fetchSameOrigin, headerContentType: "application/json-seq"},
		"a broken parameter": {headerFetchSite: fetchSameOrigin, headerContentType: "application/json; ="},
	} {
		code, _ := f.act("refresh", "{}", h)
		require.Equal(t, http.StatusUnsupportedMediaType, code, name)
	}

	// Same-origin JSON, with parameters, from any of the signals, acts.
	for name, h := range map[string]map[string]string{
		"fetch metadata":       {headerFetchSite: fetchSameOrigin, headerContentType: mediaJSON},
		"typed into the bar":   {headerFetchSite: fetchUserInitiated, headerContentType: "Application/JSON"},
		"origin with charset":  {headerOrigin: f.srv.URL, headerContentType: "application/json; charset=utf-8"},
		"referer on this host": {headerReferer: f.srv.URL + "/rollouts/r-1", headerContentType: mediaJSON},
	} {
		code, out := f.act("refresh", "", h)
		require.Equal(t, http.StatusOK, code, name)
		require.Equal(t, "refreshing", out["status"], name)
	}

	code, out := f.act("pause", `{"rollout": "r-1"}`, map[string]string{headerFetchSite: fetchSameOrigin, headerContentType: mediaJSON})
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, true, out["pausePending"])

	// A caller the authorizer turned away is a 401 whatever it sent.
	f.auth.err = errFake
	code, _ = f.act("refresh", "", map[string]string{headerOrigin: foreignOrigin})
	require.Equal(t, http.StatusUnauthorized, code)

	// A verified token is not a browser session: it acts from anywhere, with
	// any declared type.
	f.auth.err = nil
	f.auth.id = Identity{Name: admin, Admin: true}
	code, _ = f.act("suspend", `{"selector": "client=b", "reason": "x"}`, map[string]string{headerOrigin: foreignOrigin, headerContentType: mediaText})
	require.Equal(t, http.StatusOK, code)
	require.Len(t, f.c.Suspensions(), 1)

	code, _ = f.act("refresh", "", nil)
	require.Equal(t, http.StatusOK, code)
}

func TestMayActAndOpenAccess(t *testing.T) {
	ok, why := MayAct(&Identity{Admin: true}, []string{"a", "b"})
	require.True(t, ok)
	require.Empty(t, why)

	ok, why = MayAct(&Identity{Owners: []string{"a"}}, []string{"a", "b", "c"})
	require.False(t, ok)
	require.Equal(t, "you're not listed under b, c", why)

	ok, _ = MayAct(&Identity{Owners: []string{"a"}}, []string{"a"})
	require.True(t, ok)

	var oa OpenAccess

	id, err := oa.Identity(&http.Request{RemoteAddr: "10.0.0.1:4242"})
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1", id.Name)
	require.True(t, id.Admin)

	id, _ = oa.Identity(&http.Request{RemoteAddr: "weird"})
	require.Equal(t, "weird", id.Name)
	id, _ = oa.Identity(&http.Request{})
	require.Equal(t, "anonymous", id.Name)

	called := false

	oa.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })).ServeHTTP(httptest.NewRecorder(), &http.Request{})
	require.True(t, called)
}

func TestEventStreamAndBroadcaster(t *testing.T) {
	f := newFixture(t)

	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.srv.URL+"/api/v1/events", http.NoBody)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, ": connected\n", line)

	require.Eventually(t, func() bool { return f.bc.Subscribers() == 1 }, time.Second, 5*time.Millisecond)

	f.release()

	var got []string

	for i := 0; i < 40 && !slices.Contains(got, "rollout.created"); i++ {
		line, err = reader.ReadString('\n')
		require.NoError(t, err)

		if strings.HasPrefix(line, "event: ") {
			got = append(got, strings.TrimSpace(strings.TrimPrefix(line, "event: ")))
		}
	}

	require.Contains(t, got, "digest.changed")
	require.Contains(t, got, "rollout.created")

	cancel()
	require.Eventually(t, func() bool { return f.bc.Subscribers() == 0 }, time.Second, 5*time.Millisecond)

	// A subscriber that falls behind is closed instead of blocking the
	// publisher, so the stream can tell the client to replay.
	bc := NewBroadcaster()
	ch, stop := bc.Subscribe()

	for i := 0; i < 100; i++ {
		bc.Publish(&reconcile.Event{ID: int64(i)})
	}

	require.Len(t, ch, 64)
	require.Equal(t, 0, bc.Subscribers())

	for range 64 {
		<-ch
	}

	_, open := <-ch
	require.False(t, open)
	stop()
}

func TestEventStreamReplaysAndSignalsGaps(t *testing.T) {
	f := newFixture(t)
	f.release()

	// Reconnecting with the id of the first event gets everything after it
	// from the store before live events.
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.srv.URL+"/api/v1/events", http.NoBody)
	require.NoError(t, err)
	req.Header.Set("Last-Event-ID", "1")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)

	var ids []string

	for len(ids) < 2 {
		line, err := reader.ReadString('\n')
		require.NoError(t, err)

		if strings.HasPrefix(line, "id: ") {
			ids = append(ids, strings.TrimSpace(strings.TrimPrefix(line, "id: ")))
		}
	}

	require.Equal(t, "2", ids[0], "replay starts right after the id the client had")
	require.Equal(t, "3", ids[1])

	// A subscriber that falls behind gets a gap event and the stream ends.
	require.Eventually(t, func() bool { return f.bc.Subscribers() == 1 }, time.Second, 5*time.Millisecond)

	for i := 0; i < 200; i++ {
		f.bc.Publish(&reconcile.Event{ID: int64(1000 + i), Action: "x"})
	}

	var sawGap bool

	for i := 0; i < 400 && !sawGap; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		sawGap = line == "event: gap\n"
	}

	require.True(t, sawGap)

	// A bad Last-Event-ID is ignored; a failing store is reported.
	f.store.Fail = errFake

	code, _, _ := f.doWithHeader(http.MethodGet, "/api/v1/events", "Last-Event-ID", "5")
	require.Equal(t, http.StatusInternalServerError, code)

	f.store.Fail = nil
}

func (f *fixture) doWithHeader(method, path, key, value string) (int, map[string]any, string) {
	f.t.Helper()

	req, err := http.NewRequestWithContext(f.ctx, method, f.srv.URL+path, http.NoBody)
	require.NoError(f.t, err)
	req.Header.Set(key, value)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(f.t, err)

	defer resp.Body.Close()

	var buf strings.Builder

	_, _ = bufio.NewReader(resp.Body).WriteTo(&buf)

	return resp.StatusCode, nil, buf.String()
}

func TestAuthorizeSharedRule(t *testing.T) {
	f := newFixture(t)
	root := &Identity{Name: admin, Admin: true}
	dev := &Identity{Name: "dev", Owners: []string{"b"}}

	_, err := Authorize(f.set, root, mustSel("client=zzz"), false, false)
	require.ErrorContains(t, err, "matches nothing")

	var ae *ActionError

	require.ErrorAs(t, err, &ae)
	require.Equal(t, http.StatusNotFound, ae.Status)

	_, err = Authorize(f.set, dev, mustSel("id=c-2/x"), false, true)
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "forbidden", ae.Code)
	require.Equal(t, []string{"a", "b"}, ae.Owners)

	matched, err := Authorize(f.set, dev, mustSel("id=c-2/x"), false, false)
	require.NoError(t, err)
	require.Len(t, matched, 1)

	_, err = Authorize(f.set, root, mustSel("client=c"), false, false)
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "confirm_required", ae.Code)
	require.Equal(t, http.StatusConflict, ae.Status)

	rec := httptest.NewRecorder()
	writeActionRefusal(rec, errFake, nil)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestSuspendAcceptsAnAbsoluteExpiry(t *testing.T) {
	f := newFixture(t)

	at := time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	code, body, _ := f.do(http.MethodPost, "/api/v1/actions/suspend", `{"selector": "id=a-1/cl", "reason": "x", "expiresAt": "`+at+`"}`)
	require.Equal(t, http.StatusOK, code)

	raw, isString := body["expiresAt"].(string)
	require.True(t, isString)

	got, err := time.Parse(time.RFC3339Nano, raw)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now().Add(2*time.Hour), got, time.Minute)

	code, body, _ = f.do(http.MethodPost, "/api/v1/actions/suspend", `{"selector": "id=a-2/cl", "reason": "x", "expiresAt": "yesterday"}`)
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body["error"], "expiresAt")

	code, body, _ = f.do(http.MethodPost, "/api/v1/actions/suspend", `{"selector": "id=a-2/cl", "reason": "x", "expiresAt": "2001-01-01T00:00:00Z"}`)
	require.Equal(t, http.StatusBadRequest, code)
	require.Contains(t, body["error"], "already past")
}

func TestHistoryAboutANodeIncludesItsGroupsAndSuspensions(t *testing.T) {
	f := newFixture(t)
	f.release()

	code, _, _ := f.do(http.MethodPost, "/api/v1/actions/suspend", `{"selector": "node=a-2", "reason": "x", "confirm": true}`)
	require.Equal(t, http.StatusOK, code)

	code, _, raw := f.do(http.MethodGet, "/api/v1/history?node=a-2", "")
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, raw, `"action":"suspend"`, "a selector event that covers the node")
	require.Contains(t, raw, `"action":"rollout.created"`, "a group event for a group on the node")
	require.NotContains(t, raw, `"target":"org/c:t"`, "group c is not on this node")
}

func TestStreamNeedsFlusher(t *testing.T) {
	f := newFixture(t)
	s := New(nil, f.c, func() *targets.Set { return f.set }, f.auth, nil, "v", logrus.New())

	rec := &noFlush{header: http.Header{}}
	s.stream(rec, httptest.NewRequest(http.MethodGet, "/api/v1/events", http.NoBody))
	require.Equal(t, http.StatusInternalServerError, rec.status)
}

// noFlush is a ResponseWriter without Flush.
type noFlush struct {
	header http.Header
	status int
	body   strings.Builder
}

func (n *noFlush) Header() http.Header         { return n.header }
func (n *noFlush) Write(b []byte) (int, error) { return n.body.Write(b) }
func (n *noFlush) WriteHeader(code int)        { n.status = code }

func mustSel(s string) targets.Selector {
	sel, err := targets.ParseSelector(s)
	if err != nil {
		panic(err)
	}

	return sel
}

func TestReadsNeedAnIdentityUnlessPublic(t *testing.T) {
	f := newFixture(t)
	f.auth.err = errors.New("not signed in")

	code, _, _ := f.do(http.MethodGet, "/api/v1/fleet", "")
	require.Equal(t, http.StatusUnauthorized, code)

	code, _, _ = f.do(http.MethodGet, "/healthz", "")
	require.Equal(t, http.StatusOK, code, "health is always open")

	f.cfg.Auth.PublicReads = true

	for _, path := range []string{"/api/v1/fleet", "/api/v1/rollouts", "/api/v1/history", "/api/v1/targets"} {
		code, _, _ = f.do(http.MethodGet, path, "")
		require.Equal(t, http.StatusOK, code, path)
	}

	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/refresh", "{}")
	require.Equal(t, http.StatusUnauthorized, code, "acting still needs an identity")
}

func TestHistoryAfterAnID(t *testing.T) {
	f := newFixture(t)
	f.release()

	_, _, raw := f.do(http.MethodGet, "/api/v1/history?limit=500", "")

	var all []reconcile.Event
	require.NoError(t, json.Unmarshal([]byte(raw), &all))
	require.Greater(t, len(all), 2)

	// History is newest first; ask for everything after an id in the middle.
	cut := all[len(all)/2].ID

	want := 0

	for _, e := range all {
		if e.ID > cut {
			want++
		}
	}

	_, _, raw = f.do(http.MethodGet, "/api/v1/history?after="+strconv.FormatInt(cut, 10), "")

	var later []reconcile.Event
	require.NoError(t, json.Unmarshal([]byte(raw), &later))
	require.Len(t, later, want)
	require.Equal(t, cut+1, later[0].ID, "oldest first, starting right after the id")
	require.Greater(t, later[len(later)-1].ID, later[0].ID)

	// after=0 reads a history from its first event, as a new follower must.
	_, _, raw = f.do(http.MethodGet, "/api/v1/history?after=0&limit=2", "")

	var first []reconcile.Event
	require.NoError(t, json.Unmarshal([]byte(raw), &first))
	require.Len(t, first, 2)
	require.Equal(t, int64(1), first[0].ID)
	require.Equal(t, int64(2), first[1].ID)
}
