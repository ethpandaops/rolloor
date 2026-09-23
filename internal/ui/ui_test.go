package ui

import (
	"context"
	"errors"
	"html/template"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/api"
	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/hooks"
	"github.com/ethpandaops/rolloor/internal/reconcile"
	"github.com/ethpandaops/rolloor/internal/registry"
	"github.com/ethpandaops/rolloor/internal/targets"
)

const (
	d1 = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	d2 = "sha256:2222222222222222222222222222222222222222222222222222222222222222"

	tA1 = "a-1/cl"
	tB1 = "b-1/el"

	formType  = "application/x-www-form-urlencoded"
	on        = "on"
	selA      = "client=a"
	selC      = "client=c"
	nope      = "nope"
	keyGroup  = "group"
	keyRoll   = "rollout"
	keyNext   = "next"
	keyReason = "reason"
	keySel    = "selector"
	keyMode   = "mode"
	keySpeed  = "strategy"
	slow      = "slow"
	keyConf   = "confirm"
	dev       = "dev"
	hdrOrigin = "Origin"
	hdrSite   = "Sec-Fetch-Site"
	hdrRef    = "Referer"
	manual    = "manual"
)

var errFake = errors.New("fake")

const testConfig = `
environment: test
disruptionBudget: {maxUnavailable: 100%}
labels: {group: client, owner: owner, section: role, hiddenGroups: [side]}
hooks: {dir: /tmp, defaults: {soak: ""}}
strategy: {batchSize: 100%, soak: {duration: 0s}}
strategies:
  slow: {batchSize: 1, soak: {duration: 1h, interval: 1s, failureLimit: 1}}
`

const testTargets = `
- {id: a-1/cl, node: a-1, weight: 0,   image: org/a:t, labels: {client: a, owner: a, role: cl, wave: "0"}, hooks: {soak: soak-a}}
- {id: a-2/cl, node: a-2, weight: 100, image: org/a:t, labels: {client: a, owner: a, role: cl, wave: "1"}, hooks: {soak: soak-a}}
- {id: b-1/el, node: a-2, weight: 100, image: org/b:t, labels: {client: b, owner: b, role: el, wave: "1"}}
- {id: c-1/x,  node: c-1, weight: 0,   image: org/c:t, labels: {client: c, owner: a, role: x, wave: "0"}}
- {id: c-2/x,  node: c-2, weight: 0,   image: org/c:t, labels: {client: c, owner: b, role: x, wave: "0"}}
- {id: s-1/side, node: a-1, weight: 0, image: org/s:t, labels: {client: side, owner: ops, role: sidecar}}
`

// fakeWorld is a registry and a fleet whose hooks succeed unless told not to.
type fakeWorld struct {
	mu      sync.Mutex
	digest  string
	running map[string]string
	fail    map[string]string
}

func (f *fakeWorld) Resolve(context.Context, string) (registry.Resolved, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return registry.Resolved{Digest: f.digest, Revision: "rev"}, nil
}

func (f *fakeWorld) Run(_ context.Context, program, hook, id string, input any) (hooks.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	res := hooks.Result{Program: program, OK: true, Reason: "fine"}

	switch hook {
	case config.HookInspect:
		res.Reason = f.running[id]
	case config.HookUpdate:
		if why, ok := f.fail[id]; ok {
			return hooks.Result{Program: program, Reason: why, ExitCode: 1}, nil
		}

		if in, ok := input.(reconcile.HookInput); ok {
			f.running[id] = in.Desired
		}
	case config.HookSoak:
		res.Stdout = "fine\nupdated=1 remaining=2 unit=rps\n"
	}

	return res, nil
}

type fakeAuth struct {
	mu  sync.Mutex
	id  api.Identity
	err error
}

func (a *fakeAuth) Identity(*http.Request) (api.Identity, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.id, a.err
}

func (a *fakeAuth) Middleware(next http.Handler) http.Handler { return next }

func (a *fakeAuth) set(id api.Identity, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.id, a.err = id, err
}

type fixture struct {
	t     *testing.T
	ctx   context.Context //nolint:containedctx // test fixture
	cfg   *config.Config
	c     *reconcile.Controller
	store *reconcile.MemoryStore
	world *fakeWorld
	auth  *fakeAuth
	set   *targets.Set
	srv   *httptest.Server
}

func newFixture(t *testing.T, loginURL func(string) string) *fixture {
	t.Helper()

	cfg, err := config.Parse([]byte(testConfig))
	require.NoError(t, err)

	set, err := targets.Parse([]byte(testTargets), &targets.Rules{GroupLabel: "client", OwnerLabel: "owner", WaveLabel: "wave", KnownHooks: config.TargetHooks})
	require.NoError(t, err)

	f := &fixture{t: t, ctx: context.Background(), cfg: cfg, store: reconcile.NewMemoryStore(), set: set,
		world: &fakeWorld{digest: d1, running: map[string]string{}, fail: map[string]string{}},
		auth:  &fakeAuth{id: api.Identity{Name: "sam", Admin: true}}}

	for i := range set.Targets {
		f.world.running[set.Targets[i].ID] = d1
	}

	ids := 0
	f.c, err = reconcile.New(f.ctx, &reconcile.Options{Config: cfg, Targets: func() *targets.Set { return f.set }, Resolver: f.world, Runner: f.world,
		Store: f.store, Log: logrus.New(), NewID: func() string {
			ids++

			return "r-" + strconv.Itoa(ids)
		}})
	require.NoError(t, err)

	f.c.InspectAll(f.ctx)
	require.NoError(t, f.c.Tick(f.ctx))

	s, err := New(cfg, f.c, func() *targets.Set { return f.set }, f.auth, loginURL, "test", logrus.New())
	require.NoError(t, err)

	f.srv = httptest.NewServer(s.Handler())
	t.Cleanup(f.srv.Close)

	return f
}

// release moves the tag, opens rollouts for every group and ticks them along.
// Group a soaks under the slow preset; group b halts because b-1 refuses.
func (f *fixture) release() {
	f.t.Helper()
	require.NoError(f.t, f.c.SetPolicy(f.ctx, "sam", "a", reconcile.Policy{Mode: reconcile.ModeAutomated, Strategy: slow}))

	f.world.mu.Lock()
	f.world.digest = d2
	f.world.fail[tB1] = "refused"
	f.world.mu.Unlock()

	f.c.Refresh(f.ctx, "test")

	for range 6 {
		require.NoError(f.t, f.c.Tick(f.ctx))
	}
}

func (f *fixture) get(path string, hx bool) (int, string) {
	f.t.Helper()

	req, err := http.NewRequestWithContext(f.ctx, http.MethodGet, f.srv.URL+path, http.NoBody)
	require.NoError(f.t, err)

	if hx {
		req.Header.Set("HX-Request", "true")
	}

	return f.send(req)
}

func (f *fixture) post(verb string, form url.Values) (int, string) {
	f.t.Helper()

	req, err := http.NewRequestWithContext(f.ctx, http.MethodPost, f.srv.URL+"/actions/"+verb, strings.NewReader(form.Encode()))
	require.NoError(f.t, err)
	req.Header.Set("Content-Type", formType)
	req.Header.Set(hdrOrigin, f.srv.URL)

	return f.send(req)
}

// send returns the status and, for a redirect, the Location; otherwise the body.
func (f *fixture) send(req *http.Request) (int, string) {
	f.t.Helper()

	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	resp, err := client.Do(req)
	require.NoError(f.t, err)

	defer resp.Body.Close()

	if loc := resp.Header.Get("Location"); loc != "" {
		return resp.StatusCode, loc
	}

	body, err := io.ReadAll(resp.Body)
	require.NoError(f.t, err)

	return resp.StatusCode, string(body)
}

func (f *fixture) rolloutFor(group string) reconcile.RolloutView {
	f.t.Helper()

	for _, r := range f.c.Rollouts() {
		if r.Group == group {
			return r
		}
	}

	f.t.Fatalf("no rollout for %s", group)

	return reconcile.RolloutView{}
}

func TestPagesRender(t *testing.T) {
	f := newFixture(t, nil)
	f.release()

	code, body := f.get("/", false)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "<html")
	require.Contains(t, body, "hides 1 groups: side")
	require.NotContains(t, body, `href="/groups/client/side"`)

	code, body = f.get("/?hidden=1", true)
	require.Equal(t, http.StatusOK, code)
	require.NotContains(t, body, "<html", "an htmx poll gets the fragment only")
	require.Contains(t, body, `/groups/client/side`)
	require.Contains(t, body, "Hide the operator groups again")

	code, body = f.get("/groups/client/a", false)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, tA1)
	require.NotContains(t, body, "/actions/policy", "policy is not edited from the pages")

	code, _ = f.get("/groups/team/a", false)
	require.Equal(t, http.StatusNotFound, code)
	code, _ = f.get("/groups/client/zzz", false)
	require.Equal(t, http.StatusNotFound, code)

	soaking := f.rolloutFor("a")
	require.Equal(t, reconcile.Soaking, soaking.State)
	code, body = f.get("/rollouts/"+soaking.ID, false)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "1rps", "the last soak check shows its numbers")
	require.Contains(t, body, "not yet updated")
	require.Contains(t, body, "Within tolerance on check 1")

	halted := f.rolloutFor("b")
	require.Equal(t, reconcile.Halted, halted.State)
	code, body = f.get("/rollouts/"+halted.ID, false)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "1 quarantined targets")
	require.Contains(t, body, "refused")

	code, body = f.get("/rollouts/"+f.rolloutFor("c").ID, false)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "Complete")

	code, _ = f.get("/rollouts/nope", false)
	require.Equal(t, http.StatusNotFound, code)

	code, body = f.get("/rollouts", false)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, soaking.ID)
	require.Contains(t, body, halted.ID)

	code, body = f.get("/nodes/a-2", false)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, tB1)
	require.Contains(t, body, "targets from 2 owners")

	code, _ = f.get("/nodes/zz", false)
	require.Equal(t, http.StatusNotFound, code)

	code, body = f.get("/history", false)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "digest.changed")
	require.Contains(t, body, "refused")

	code, body = f.get("/history?node=a-2&group=b&rollout="+halted.ID, false)
	require.Equal(t, http.StatusOK, code)
	require.NotContains(t, body, "digest.changed", "events without a target on that node are dropped")

	f.store.Fail = errFake

	code, _ = f.get("/history", false)
	require.Equal(t, http.StatusInternalServerError, code)

	f.store.Fail = nil

	code, body = f.get("/static/style.css", false)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, ":root")
}

func TestViewerNeedsAnIdentity(t *testing.T) {
	f := newFixture(t, nil)
	f.auth.set(api.Identity{}, errFake)

	code, body := f.get("/", false)
	require.Equal(t, http.StatusUnauthorized, code)
	require.Contains(t, body, "fake")

	withLogin := newFixture(t, func(next string) string { return "/auth/login?next=" + url.QueryEscape(next) })
	code, body = withLogin.get("/", false)
	require.Equal(t, http.StatusOK, code)
	require.NotContains(t, body, "/auth/login", "a signed-in viewer sees no login link")

	// Open access without an identity: the page renders and offers to sign in.
	withLogin.auth.set(api.Identity{}, nil)
	code, body = withLogin.get("/", false)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "/auth/login?next=%2F", "the layout offers the login link")

	withLogin.auth.set(api.Identity{}, errFake)
	code, loc := withLogin.get("/rollouts?x=1", false)
	require.Equal(t, http.StatusFound, code)
	require.Equal(t, "/auth/login?next=%2Frollouts%3Fx%3D1", loc)
}

func TestControlsAreDisabledWithAReason(t *testing.T) {
	f := newFixture(t, nil)
	f.release()
	f.auth.set(api.Identity{Name: dev, Owners: []string{"b"}}, nil)

	code, body := f.get("/groups/client/a", false)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, `disabled title="you&#39;re not listed under a"`)

	code, body = f.get("/nodes/a-2", false)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "so these are view-only for you")

	code, body = f.get("/rollouts/"+f.rolloutFor("a").ID, false)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, `disabled title="you&#39;re not listed under a">Abort`)
	require.Contains(t, body, "so these are view-only for you")

	// Their own halted rollout offers the retry dialog.
	code, body = f.get("/rollouts/"+f.rolloutFor("b").ID, false)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "Retry the soak")
	require.NotContains(t, body, `class="disabled"`)
}

func TestActionsRedirectWithFlashOrError(t *testing.T) {
	f := newFixture(t, nil)

	// A sync of an already-synced group has nothing to do.
	code, loc := f.post("sync", url.Values{keySel: {selA}, keyNext: {"/groups/client/a"}})
	require.Equal(t, http.StatusSeeOther, code)
	require.Equal(t, "/groups/client/a?flash=Nothing+to+sync%3A+client%3Da+is+already+on+the+desired+build.", loc)

	// Bad selectors and empty matches come back as errors on the same page.
	code, loc = f.post("sync", url.Values{keySel: {nope}, keyNext: {"/x"}})
	require.Equal(t, http.StatusSeeOther, code)
	require.Contains(t, loc, "/x?error=")

	_, loc = f.post("sync", url.Values{keySel: {"client=zzz"}, keyNext: {"/x"}})
	require.Contains(t, loc, "error=selector+client%3Dzzz+matches+nothing")

	// An off-site next is replaced by the root.
	_, loc = f.post("sync", url.Values{keySel: {nope}, keyNext: {"//evil.example/"}})
	require.True(t, strings.HasPrefix(loc, "/?error="), loc)

	// Two owners need the confirm box, for lifting a suspension too.
	_, loc = f.post("suspend", url.Values{keySel: {selC}, keyReason: {"x"}, keyNext: {"/"}})
	require.Contains(t, loc, "spans+2+owners")

	_, loc = f.post("suspend", url.Values{keySel: {selC}, keyReason: {"x"}, keyConf: {on}, "expiresIn": {"2h"}, keyNext: {"/"}})
	require.Contains(t, loc, "flash=Suspended+client%3Dc+until")

	_, loc = f.post("suspend", url.Values{keySel: {"id=" + tA1}, keyReason: {"x"}, "expiresIn": {"soon"}, keyNext: {"/"}})
	require.Contains(t, loc, "error=expiry")

	_, loc = f.post("resume", url.Values{keySel: {selC}, keyNext: {"/"}})
	require.Contains(t, loc, "spans+2+owners")

	_, loc = f.post("resume", url.Values{keySel: {selC}, keyConf: {on}, keyNext: {"/"}})
	require.Contains(t, loc, "flash=Resumed+client%3Dc+%281+lifted%29.")

	_, loc = f.post("resume", url.Values{keySel: {selC}, keyConf: {on}, keyNext: {"/"}})
	require.Contains(t, loc, "error=", "nothing left to lift")

	// Policy is not a page action.
	_, loc = f.post("policy", url.Values{keyGroup: {"a"}, keyMode: {manual}, keySpeed: {slow}, keyNext: {"/"}})
	require.Contains(t, loc, "error=unknown+action+%22policy%22")

	// A malformed return path falls back to the root before anything runs.
	_, loc = f.post("sync", url.Values{keySel: {nope}, keyNext: {"/%zz"}})
	require.True(t, strings.HasPrefix(loc, "/?error="), loc)
	_, loc = f.post("sync", url.Values{keySel: {nope}, keyNext: {"/a?b=c"}})
	require.True(t, strings.HasPrefix(loc, "/a?b=c&error="), loc)

	// A sync that opens rollouts says how many.
	f.world.mu.Lock()
	f.world.digest = d2
	f.world.mu.Unlock()

	_, loc = f.post("sync", url.Values{keySel: {selA}, "force": {on}, keySpeed: {slow}, keyNext: {"/"}})
	require.Contains(t, loc, "flash=Syncing+client%3Da+%281+rollouts%29.")

	_, loc = f.post("sync", url.Values{keySel: {selA}, keySpeed: {"warp"}, keyNext: {"/"}})
	require.Contains(t, loc, "error=strategy")

	// Rollout verbs.
	r := f.rolloutFor("a")
	_, loc = f.post("pause", url.Values{keyRoll: {r.ID}, keyNext: {"/rollouts/" + r.ID}})
	require.Equal(t, "/rollouts/"+r.ID+"?flash=Pause+sent.", loc)

	_, loc = f.post("promote", url.Values{keyRoll: {r.ID}, keyNext: {"/"}})
	require.Contains(t, loc, "flash=Promote+sent.")

	_, loc = f.post("retry", url.Values{keyRoll: {r.ID}, keyReason: {"x"}, keyNext: {"/"}})
	require.Contains(t, loc, "error=rollout+is+", "not halted")

	_, loc = f.post("abort", url.Values{keyRoll: {r.ID}, keyNext: {"/"}})
	require.Contains(t, loc, "flash=Abort+sent.")

	_, loc = f.post("abort", url.Values{keyRoll: {nope}, keyNext: {"/"}})
	require.Contains(t, loc, "error=rollout+nope%3A+not+found")

	_, loc = f.post("dance", url.Values{keyNext: {"/"}})
	require.Contains(t, loc, "error=unknown+action+%22dance%22")

	// A body that is not a form is a plain 400.
	req, err := http.NewRequestWithContext(f.ctx, http.MethodPost, f.srv.URL+"/actions/sync", strings.NewReader("a=%zz"))
	require.NoError(t, err)
	req.Header.Set("Content-Type", formType)
	req.Header.Set(hdrOrigin, f.srv.URL)
	code, _ = f.send(req)
	require.Equal(t, http.StatusBadRequest, code)
}

func TestActionsRespectOwnership(t *testing.T) {
	f := newFixture(t, nil)
	f.release()
	f.auth.set(api.Identity{Name: dev, Owners: []string{"b"}}, nil)

	_, loc := f.post("sync", url.Values{keySel: {selA}, keyNext: {"/"}})
	require.Contains(t, loc, "error=you%27re+not+listed+under+a")

	// Owning the selected target is not enough for a sync when its group
	// has other owners' targets too.
	_, loc = f.post("sync", url.Values{keySel: {"id=c-2/x"}, keyNext: {"/"}})
	require.Contains(t, loc, "error=you%27re+not+listed+under+a")

	_, loc = f.post("abort", url.Values{keyRoll: {f.rolloutFor("a").ID}, keyNext: {"/"}})
	require.Contains(t, loc, "error=not+allowed")

	// Their own group is fine.
	_, loc = f.post("retry", url.Values{keyRoll: {f.rolloutFor("b").ID}, keyReason: {"probe was wrong"}, keyNext: {"/"}})
	require.Contains(t, loc, "flash=Retry+sent.")

	f.auth.set(api.Identity{}, errFake)
	code, _ := f.post("sync", url.Values{keySel: {"client=b"}, keyNext: {"/"}})
	require.Equal(t, http.StatusUnauthorized, code)
}

func TestTemplateHelpers(t *testing.T) {
	s := &Server{now: func() time.Time { return time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC) }}
	fn := s.funcs()

	short := helper[func(string) string](t, fn, "short")
	require.Equal(t, "1111111", short(d1))
	require.Equal(t, "abc", short("abc"))

	ago := helper[func(time.Time) string](t, fn, "ago")
	require.Equal(t, "", ago(time.Time{}))
	require.Equal(t, "5 min ago", ago(time.Date(2026, 9, 22, 11, 55, 0, 0, time.UTC)))

	require.Equal(t, "Tue 12:00:00", helper[func(time.Time) string](t, fn, "clock")(time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)))

	pct := helper[func(float64, float64) string](t, fn, "pct")
	require.Equal(t, "0%", pct(1, 0))
	require.Equal(t, "50.0%", pct(1, 2))

	first := helper[func(map[string]string) string](t, fn, "first")
	require.Equal(t, "", first(nil))
	require.Equal(t, "x", first(map[string]string{"b": "y", "a": "x"}))

	rev := helper[func(reconcile.RolloutView) string](t, fn, "rev")
	require.Equal(t, "r", rev(reconcile.RolloutView{Revision: "r", Digest: "d"}))
	require.Equal(t, "d", rev(reconcile.RolloutView{Digest: "d"}))

	require.Equal(t, "0s", humanDuration(-time.Second))
	require.Equal(t, "42s", humanDuration(42*time.Second))
	require.Equal(t, "3 min", humanDuration(3*time.Minute))
	require.Equal(t, "5 h", humanDuration(5*time.Hour))
	require.Equal(t, "3 days", humanDuration(72*time.Hour))
}

func TestNewRejectsBadTemplates(t *testing.T) {
	orig := templateFS

	t.Cleanup(func() { templateFS = orig })

	templateFS = fstest.MapFS{"templates/broken.html": {Data: []byte("{{define \"x\"}}{{.Nope")}}

	_, err := New(&config.Config{}, nil, nil, nil, nil, "", logrus.New())
	require.ErrorContains(t, err, "parse templates")
}

// helper fetches one template function with its expected signature.
func helper[T any](t *testing.T, fn template.FuncMap, name string) T {
	t.Helper()

	v, ok := fn[name].(T)
	require.True(t, ok, name)

	return v
}

func TestFormPostsMustComeFromThisSite(t *testing.T) {
	f := newFixture(t, nil)

	post := func(headers map[string]string) int {
		req, err := http.NewRequestWithContext(f.ctx, http.MethodPost, f.srv.URL+"/actions/sync", strings.NewReader(url.Values{keySel: {selA}, keyNext: {"/"}}.Encode()))
		require.NoError(t, err)
		req.Header.Set("Content-Type", formType)

		for k, v := range headers {
			req.Header.Set(k, v)
		}

		code, _ := f.send(req)

		return code
	}

	require.Equal(t, http.StatusForbidden, post(nil), "no origin at all")
	require.Equal(t, http.StatusForbidden, post(map[string]string{hdrOrigin: "https://evil.example"}))
	require.Equal(t, http.StatusForbidden, post(map[string]string{hdrSite: "cross-site", hdrOrigin: f.srv.URL}))
	require.Equal(t, http.StatusForbidden, post(map[string]string{hdrSite: "same-site"}))
	require.Equal(t, http.StatusSeeOther, post(map[string]string{hdrSite: "same-origin"}))
	require.Equal(t, http.StatusSeeOther, post(map[string]string{hdrSite: "none"}))
	require.Equal(t, http.StatusSeeOther, post(map[string]string{hdrRef: f.srv.URL + "/groups/client/a"}))
	require.Equal(t, http.StatusForbidden, post(map[string]string{hdrRef: "://bad"}))

	require.Equal(t, "/", returnPath("http://x/").Path)
	require.Equal(t, "/", returnPath("").Path)
}

func TestGroupAndNodeFormsCarryTheConfirmBox(t *testing.T) {
	f := newFixture(t, nil)

	code, body := f.get("/groups/client/c", false)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, "2 owners (a, b); sync them all")

	code, body = f.get("/groups/client/a", false)
	require.Equal(t, http.StatusOK, code)
	require.NotContains(t, body, "sync them all")

	// A suspension whose selector spans owners: the row's Resume is judged on
	// everything it would lift, not on the row alone.
	_, loc := f.post("suspend", url.Values{keySel: {"node=a-2"}, keyReason: {"x"}, keyConf: {on}, keyNext: {"/"}})
	require.Contains(t, loc, "flash=Suspended")

	f.auth.set(api.Identity{Name: dev, Owners: []string{"b"}}, nil)
	code, body = f.get("/nodes/a-2", false)
	require.Equal(t, http.StatusOK, code)
	require.Contains(t, body, `disabled title="you&#39;re not listed under a" title="Lifts node=a-2"`)
}

func TestPublicReadsShowPagesButNotControls(t *testing.T) {
	login := func(next string) string { return "/auth/login?next=" + url.QueryEscape(next) }
	f := newFixture(t, login)
	f.release()
	f.cfg.Auth.PublicReads = true
	f.auth.set(api.Identity{}, errFake)

	for _, path := range []string{"/", "/groups/client/a", "/rollouts", "/nodes/a-1", "/history"} {
		code, body := f.get(path, false)
		require.Equal(t, http.StatusOK, code, path)
		require.Contains(t, body, "Sign in", path)
	}

	_, body := f.get("/groups/client/a", false)
	require.Contains(t, body, "Sign in to act.")

	// Acting still sends the browser to sign in.
	code, loc := f.post("refresh", url.Values{})
	require.Equal(t, http.StatusFound, code)
	require.Contains(t, loc, "/auth/login")
}
