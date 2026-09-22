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

var errFake = errors.New("fake")

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

	cfg, err := config.Parse([]byte("environment: test\nbudget: 100%\nlabels: {group: client, owner: owner}\nhooks: {dir: /tmp}\npresets:\n  fast: {batch: [100%], soak: {duration: 0s}}\ndefaultPolicy: {speed: fast}\n"))
	require.NoError(t, err)

	set, err := targets.Parse([]byte(fleetYAML), &targets.Rules{GroupLabel: "client", OwnerLabel: "owner", WaveLabel: "wave", KnownHooks: config.TargetHooks})
	require.NoError(t, err)

	f := &fixture{t: t, ctx: context.Background(), store: reconcile.NewMemoryStore(), set: set,
		world: &fakeWorld{digest: d1, running: map[string]string{"a-1/cl": d1, "a-2/cl": d1, "b-1/el": d1, "c-1/x": d1, "c-2/x": d1}},
		auth:  &fakeAuth{id: Identity{Name: "sam", Admin: true}}, bc: NewBroadcaster()}

	ids := 0
	f.c, err = reconcile.New(f.ctx, &reconcile.Options{Config: cfg, Targets: func() *targets.Set { return f.set }, Resolver: f.world, Runner: f.world,
		Store: f.store, Notifier: f.bc, Log: logrus.New(), NewID: func() string {
			ids++

			return "r-" + strconv.Itoa(ids)
		}})
	require.NoError(t, err)

	f.c.InspectAll(f.ctx)
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
	require.Equal(t, "sam", body["name"])

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

	code, _, _ = f.do(http.MethodGet, "/api/v1/policies/zzz", "")
	require.Equal(t, http.StatusNotFound, code)
	code, body, _ = f.do(http.MethodGet, "/api/v1/policies/a", "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "fast", body["speed"])

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
	code, body, _ = f.do(http.MethodPost, "/api/v1/actions/sync", `{"selector": "client=a", "force": true, "speed": "fast"}`)
	require.Equal(t, http.StatusOK, code)
	require.Len(t, body["rollouts"], 1)

	code, _, _ = f.do(http.MethodPost, "/api/v1/actions/sync", `{"selector": "client=a", "speed": "warp"}`)
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

func TestSuspendResumeAndPolicy(t *testing.T) {
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

	code, _, _ = f.do(http.MethodPut, "/api/v1/policies/zzz", `{"mode": "manual", "speed": "fast"}`)
	require.Equal(t, http.StatusNotFound, code)
	code, _, _ = f.do(http.MethodPut, "/api/v1/policies/a", `{"mode": "manual"`)
	require.Equal(t, http.StatusBadRequest, code)
	code, _, _ = f.do(http.MethodPut, "/api/v1/policies/a", `{"mode": "manual", "speed": "warp"}`)
	require.Equal(t, http.StatusBadRequest, code)
	code, body, _ = f.do(http.MethodPut, "/api/v1/policies/a", `{"mode": "manual", "speed": "fast"}`)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "manual", body["mode"])
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
	code, _, _ = f.do(http.MethodPut, "/api/v1/policies/a", `{"mode": "manual", "speed": "fast"}`)
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
	f.auth.id = Identity{Name: "sam", Admin: true}
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

	// A full subscriber drops events instead of blocking the publisher.
	bc := NewBroadcaster()
	ch, stop := bc.Subscribe()

	for i := 0; i < 100; i++ {
		bc.Publish(&reconcile.Event{ID: int64(i)})
	}

	require.Len(t, ch, 64)
	stop()
	require.Equal(t, 0, bc.Subscribers())
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
