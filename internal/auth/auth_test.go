package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/rolloor/internal/api"
	"github.com/ethpandaops/rolloor/internal/config"
)

const (
	clientID      = "rolloor"
	sessionKey    = "0123456789abcdef0123456789abcdef"
	sam           = "sam"
	operators     = "operators"
	claimSub      = "sub"
	modeOIDC      = "oidc"
	claimUsername = "preferred_username"
	callbackURL   = "http://app.example/auth/callback"
	alpha         = "alpha"
)

// issuer is a fake OpenID provider: discovery, JWKS, and a token endpoint
// that mints ID tokens for whatever name the test asks for.
type issuer struct {
	srv    *httptest.Server
	key    *rsa.PrivateKey
	name   string
	fail   bool
	codes  []string
	claim  string
	nonce  string
	expiry time.Duration
	aud    string
	forms  []url.Values
	basic  []string
}

func newIssuer(t *testing.T) *issuer {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	is := &issuer{key: key, name: sam, claim: claimUsername, expiry: time.Hour}
	mux := http.NewServeMux()

	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer": is.srv.URL, "authorization_endpoint": is.srv.URL + "/authorize", "token_endpoint": is.srv.URL + "/token",
			"jwks_uri": is.srv.URL + "/keys", "id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "k1", Algorithm: "RS256", Use: "sig"}}})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		is.codes = append(is.codes, r.Form.Get("code"))
		is.forms = append(is.forms, r.PostForm)
		user, _, _ := r.BasicAuth()
		is.basic = append(is.basic, user)

		if is.fail {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "at", "token_type": "Bearer", "id_token": is.token(t, is.name)})
	})

	is.srv = httptest.NewServer(mux)
	t.Cleanup(is.srv.Close)

	return is
}

// token mints an ID token for name with the issuer's key.
func (is *issuer) token(t *testing.T, name string) string {
	t.Helper()

	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: is.key}, (&jose.SignerOptions{}).WithHeader("kid", "k1"))
	require.NoError(t, err)

	claims := map[string]any{
		"iss": is.srv.URL, "aud": is.audience(), claimSub: name, "exp": time.Now().Add(is.expiry).Unix(), "iat": time.Now().Unix(),
	}
	if name != "" {
		claims[is.claim] = name
	}

	if is.nonce != "" {
		claims["nonce"] = is.nonce
	}

	raw, err := jwt.Signed(signer).Claims(claims).Serialize()
	require.NoError(t, err)

	return raw
}

func (is *issuer) audience() string {
	if is.aud != "" {
		return is.aud
	}

	return clientID
}

func newOIDC(t *testing.T, is *issuer, teams *Teams) *OIDC {
	t.Helper()

	cfg := &config.Auth{Mode: modeOIDC, Issuer: is.srv.URL, ClientID: clientID, RedirectURL: callbackURL, IdentityClaim: claimUsername, AdminOwner: operators}

	o, err := NewOIDC(context.Background(), cfg, "secret", sessionKey, teams, logrus.New())
	require.NoError(t, err)

	return o
}

func writeTeams(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "teams.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))

	return path
}

func TestNewOIDCErrors(t *testing.T) {
	is := newIssuer(t)
	cfg := &config.Auth{Issuer: is.srv.URL, ClientID: clientID}

	_, err := NewOIDC(context.Background(), cfg, "s", "short", nil, logrus.New())
	require.ErrorContains(t, err, "session key")

	_, err = NewOIDC(context.Background(), &config.Auth{Issuer: "http://127.0.0.1:1", ClientID: clientID}, "s", sessionKey, nil, logrus.New())
	require.ErrorContains(t, err, "discover")
}

func TestBearerTokens(t *testing.T) {
	is := newIssuer(t)
	teams, err := LoadTeams(writeTeams(t, "alpha: [sam, paul]\noperators: [sam]\n"), logrus.New())
	require.NoError(t, err)

	o := newOIDC(t, is, teams)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/me", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+is.token(t, sam))

	id, err := o.Identity(req)
	require.NoError(t, err)
	require.Equal(t, sam, id.Name)
	require.Equal(t, []string{alpha, operators}, id.Owners)
	require.True(t, id.Admin)
	require.False(t, id.Session, "a token is not a browser session")

	req.Header.Set("Authorization", "Bearer "+is.token(t, "paul"))
	id, err = o.Identity(req)
	require.NoError(t, err)
	require.Equal(t, []string{alpha}, id.Owners)
	require.False(t, id.Admin)

	// Someone the teams file does not know owns nothing.
	req.Header.Set("Authorization", "Bearer "+is.token(t, "stranger"))
	id, err = o.Identity(req)
	require.NoError(t, err)
	require.Empty(t, id.Owners)

	// A token without the claim, a garbage token, and an expired token are all rejected.
	req.Header.Set("Authorization", "Bearer "+is.token(t, ""))
	_, err = o.Identity(req)
	require.ErrorIs(t, err, ErrNotSignedIn)
	require.ErrorContains(t, err, claimUsername)

	req.Header.Set("Authorization", "Bearer not.a.token")
	_, err = o.Identity(req)
	require.ErrorIs(t, err, ErrNotSignedIn)

	is.expiry = -time.Hour
	req.Header.Set("Authorization", "Bearer "+is.token(t, sam))
	_, err = o.Identity(req)
	require.ErrorIs(t, err, ErrNotSignedIn)

	// No cookie and no token is not signed in.
	_, err = o.Identity(httptest.NewRequest(http.MethodGet, "/", http.NoBody))
	require.ErrorIs(t, err, ErrNotSignedIn)
}

func TestWithoutTeamsFileTheClaimIsTheOwner(t *testing.T) {
	is := newIssuer(t)
	o := newOIDC(t, is, nil)

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+is.token(t, operators))

	id, err := o.Identity(req)
	require.NoError(t, err)
	require.Equal(t, []string{operators}, id.Owners)
	require.True(t, id.Admin)
}

func TestLoginFlow(t *testing.T) {
	is := newIssuer(t)
	o := newOIDC(t, is, nil)

	protected := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, err := o.Identity(r)
		if err != nil {
			http.Redirect(w, r, LoginURL(r.URL.RequestURI()), http.StatusFound)

			return
		}

		_, _ = w.Write([]byte("hello " + id.Name))
	})

	app := httptest.NewServer(o.Middleware(protected))
	t.Cleanup(app.Close)

	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	// Unauthenticated: sent to login with next.
	resp, err := client.Get(app.URL + "/groups/client/a")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusFound, resp.StatusCode)
	require.Equal(t, LoginPath+"?next=%2Fgroups%2Fclient%2Fa", resp.Header.Get("Location"))

	// Login sends the issuer a fresh random state and nothing else of the
	// login: the return path and the PKCE verifier stay in an HttpOnly cookie.
	loc, stateCk := startLogin(t, client, app.URL, "/groups/client/a")
	require.Equal(t, is.srv.URL+"/authorize", loc.Scheme+"://"+loc.Host+loc.Path)
	require.Equal(t, clientID, loc.Query().Get("client_id"))
	require.True(t, stateCk.HttpOnly)
	require.False(t, stateCk.Secure, "plain http in tests")

	state, nonce := loc.Query().Get("state"), loc.Query().Get("nonce")
	require.NotEmpty(t, state)
	require.NotEmpty(t, nonce)
	require.NotContains(t, loc.RawQuery, "groups", "the return path stays in the cookie")
	require.NotContains(t, loc.RawQuery, stateCk.Value)

	other, otherCk := startLogin(t, client, app.URL, "/")
	require.NotEqual(t, state, other.Query().Get("state"))

	// A state that is not this browser's login is refused before any code is
	// exchanged: a wrong one, none, the cookie itself, another login's, and
	// this state against another login's cookie or posing as a cookie itself.
	for _, tc := range []struct {
		query string
		ck    *http.Cookie
	}{
		{codeQuery("wrong"), stateCk},
		{"code=c", stateCk},
		{codeQuery(url.QueryEscape(stateCk.Value)), stateCk},
		{codeQuery(other.Query().Get("state")), stateCk},
		{codeQuery(state), otherCk},
		{codeQuery(state), &http.Cookie{Name: stateCookie, Value: state}},
	} {
		code, _, _ := callback(t, client, app.URL, tc.query, tc.ck)
		require.Equal(t, http.StatusBadRequest, code, tc.query)
	}

	require.Empty(t, is.codes, "no refused callback reached the issuer")

	// A token minted for another login's nonce is refused.
	is.nonce = "someone-else"
	code, _, _ := callback(t, client, app.URL, "state="+state+"&code=replayed", stateCk)
	require.Equal(t, http.StatusBadGateway, code)

	is.nonce = nonce
	is.codes = nil

	// The right state exchanges the code and returns to the kept path, even on
	// another instance sharing the key: the cookie alone carries the login.
	peer := httptest.NewServer(newOIDC(t, is, nil).Middleware(protected))
	t.Cleanup(peer.Close)

	code, location, sessCk := callback(t, client, peer.URL, "state="+state+"&code=the-code", stateCk)
	require.Equal(t, http.StatusFound, code)
	require.Equal(t, "/groups/client/a", location)
	require.Equal(t, []string{"the-code"}, is.codes)
	require.NotNil(t, sessCk)

	// Each cookie is signed for its own name: a login state is no session, and
	// a session is no login state.
	posing := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	posing.AddCookie(&http.Cookie{Name: sessionCookie, Value: stateCk.Value})
	_, err = o.Identity(posing)
	require.ErrorIs(t, err, ErrNotSignedIn)

	code, _, _ = callback(t, client, app.URL, "code=c", &http.Cookie{Name: stateCookie, Value: sessCk.Value})
	require.Equal(t, http.StatusBadRequest, code)

	// A cookie session is marked as one whatever else the request carries;
	// only a bearer token that verifies stands in for the cookie.
	apiReq := httptest.NewRequest(http.MethodPost, "/api/v1/actions/refresh", http.NoBody)
	apiReq.AddCookie(sessCk)
	apiReq.Header.Set("Authorization", "Basic c2FtOng=")

	id, err := o.Identity(apiReq)
	require.NoError(t, err)
	require.Equal(t, sam, id.Name)
	require.True(t, id.Session)

	apiReq.Header.Set("Authorization", "Bearer not.a.token")
	_, err = o.Identity(apiReq)
	require.ErrorIs(t, err, ErrNotSignedIn)

	apiReq.Header.Set("Authorization", "Bearer "+is.token(t, sam))
	id, err = o.Identity(apiReq)
	require.NoError(t, err)
	require.False(t, id.Session)

	// The session works on the protected route.
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, app.URL+"/groups/client/a", http.NoBody)
	req.AddCookie(sessCk)
	resp, err = client.Do(req)
	require.NoError(t, err)

	body := make([]byte, 64)
	n, _ := resp.Body.Read(body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "hello sam", string(body[:n]))

	// A tampered cookie is not.
	req, _ = http.NewRequestWithContext(context.Background(), http.MethodGet, app.URL+"/x", http.NoBody)
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: sessCk.Value + "x"})
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusFound, resp.StatusCode)

	// Logout clears it.
	req, _ = http.NewRequestWithContext(context.Background(), http.MethodPost, app.URL+LogoutPath, http.NoBody)
	req.AddCookie(sessCk)
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusFound, resp.StatusCode)

	for _, c := range resp.Cookies() {
		if c.Name == sessionCookie {
			require.Equal(t, -1, c.MaxAge)
		}
	}
}

// startLogin begins a login that returns to next, and gives back where the
// issuer was sent and the state cookie.
func startLogin(t *testing.T, client *http.Client, app, next string) (*url.URL, *http.Cookie) {
	t.Helper()

	resp, err := client.Get(app + LoginURL(next))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusFound, resp.StatusCode)

	loc, err := url.Parse(resp.Header.Get("Location"))
	require.NoError(t, err)

	var ck *http.Cookie

	for _, c := range resp.Cookies() {
		if c.Name == stateCookie {
			ck = c
		}
	}

	require.NotNil(t, ck)

	return loc, ck
}

// codeQuery is the callback query an issuer sends back with a code.
func codeQuery(state string) string {
	return "state=" + state + "&code=c"
}

// callback calls the callback route with a query and a state cookie, if any,
// and returns the status, the redirect, and the session cookie it set.
func callback(t *testing.T, client *http.Client, app, query string, ck *http.Cookie) (code int, location string, sess *http.Cookie) {
	t.Helper()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, app+CallbackPath+"?"+query, http.NoBody)
	require.NoError(t, err)

	if ck != nil {
		req.AddCookie(ck)
	}

	resp, err := client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	for _, c := range resp.Cookies() {
		if c.Name == sessionCookie && c.Value != "" {
			sess = c
		}
	}

	return resp.StatusCode, resp.Header.Get("Location"), sess
}

func TestCallbackFailures(t *testing.T) {
	is := newIssuer(t)
	o := newOIDC(t, is, nil)
	app := httptest.NewServer(o.Middleware(http.NotFoundHandler()))
	t.Cleanup(app.Close)

	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	// The login returns only to a path on this site; anything a browser would
	// read as another host, or as no path, returns to the root.
	for next, want := range map[string]string{
		"/groups/client/a?x=1": "/groups/client/a?x=1",
		"":                     "/",
		"groups":               "/",
		"//evil.example/x":     "/",
		"https://evil.example": "/",
		"/\\evil.example":      "/%5Cevil.example",
		"/\t/evil.example":     "/",
	} {
		loc, ck := startLogin(t, client, app.URL, next)
		is.nonce = loc.Query().Get("nonce")

		code, location, _ := callback(t, client, app.URL, codeQuery(loc.Query().Get("state")), ck)
		require.Equal(t, http.StatusFound, code, next)
		require.Equal(t, want, location, next)
	}

	loc, st := startLogin(t, client, app.URL, "/")
	query := codeQuery(loc.Query().Get("state"))

	// The issuer refusing the code is a 502.
	is.fail = true
	code, _, _ := callback(t, client, app.URL, query, st)
	require.Equal(t, http.StatusBadGateway, code)

	// An ID token without the claim is a 502 too.
	is.fail = false
	is.name = ""
	code, _, _ = callback(t, client, app.URL, query, st)
	require.Equal(t, http.StatusBadGateway, code)

	// A tampered, missing, stateless or expired login state is refused before
	// any code reaches the issuer.
	is.codes = nil

	stateless, err := o.sessions.encode(stateCookie, loginState{Nonce: is.nonce, Verifier: "v", Next: "/", Expires: time.Now().Add(time.Hour)})
	require.NoError(t, err)

	code, _, _ = callback(t, client, app.URL, query, &http.Cookie{Name: stateCookie, Value: st.Value + "x"})
	require.Equal(t, http.StatusBadRequest, code)

	code, _, _ = callback(t, client, app.URL, query, nil)
	require.Equal(t, http.StatusBadRequest, code)

	code, _, _ = callback(t, client, app.URL, "code=c", &http.Cookie{Name: stateCookie, Value: stateless})
	require.Equal(t, http.StatusBadRequest, code)

	o.now = func() time.Time { return time.Now().Add(time.Hour) }
	code, _, _ = callback(t, client, app.URL, query, st)
	require.Equal(t, http.StatusBadRequest, code)
	require.Empty(t, is.codes)

	// An expired session is not signed in.
	o.now = time.Now

	value, err := o.sessions.encode(sessionCookie, session{Name: sam, Expires: time.Now().Add(-time.Minute)})
	require.NoError(t, err)

	r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: value})
	_, err = o.Identity(r)
	require.ErrorIs(t, err, ErrNotSignedIn)

	// Secure cookies behind a TLS-terminating proxy.
	rec := httptest.NewRecorder()
	fwd := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	fwd.Header.Set("X-Forwarded-Proto", "https")
	setCookie(rec, fwd, "c", "v", 10)
	require.Contains(t, rec.Header().Get("Set-Cookie"), "Secure")
}

func TestInjectedFailures(t *testing.T) {
	is := newIssuer(t)
	o := newOIDC(t, is, nil)
	app := httptest.NewServer(o.Middleware(http.NotFoundHandler()))
	t.Cleanup(app.Close)

	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	// Random source failing at login, for the state and for the nonce.
	o.random = func() (string, error) { return "", errFake }
	resp, err := client.Get(app.URL + LoginPath)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	calls := 0
	o.random = func() (string, error) {
		calls++
		if calls == 2 {
			return "", errFake
		}

		return randomToken()
	}

	resp, err = client.Get(app.URL + LoginPath)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.Equal(t, 2, calls)

	o.random = randomToken

	// Encoding failing at login, and for the session after a good exchange.
	o.encode = func(string, any) (string, error) { return "", errFake }
	resp, err = client.Get(app.URL + LoginPath)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	is.nonce = "x"
	value, err := o.sessions.encode(stateCookie, loginState{State: "s", Nonce: "x", Next: "/", Expires: time.Now().Add(time.Hour)})
	require.NoError(t, err)

	code, _, _ := callback(t, client, app.URL, codeQuery("s"), &http.Cookie{Name: stateCookie, Value: value})
	require.Equal(t, http.StatusInternalServerError, code)

	// The random source itself.
	orig := randRead
	randRead = func([]byte) (int, error) { return 0, errFake }

	t.Cleanup(func() { randRead = orig })

	_, err = randomToken()
	require.ErrorIs(t, err, errFake)

	randRead = orig

	// A teams file that vanishes between stat and read.
	path := writeTeams(t, "a: [x]\n")
	teams, err := LoadTeams(path, logrus.New())
	require.NoError(t, err)

	origRead := readFile
	readFile = func(string) ([]byte, error) { return nil, errFake }

	t.Cleanup(func() { readFile = origRead })

	require.NoError(t, os.Chtimes(path, time.Now(), time.Now().Add(time.Second)))
	require.ErrorIs(t, teams.Reload(), errFake)
	require.Equal(t, []string{"a"}, teams.Owners("x"))
}

var errFake = errors.New("fake")

func TestCodec(t *testing.T) {
	c := codec{key: []byte(sessionKey)}

	_, err := c.encode(sessionCookie, make(chan int))
	require.Error(t, err)

	var out map[string]any

	require.ErrorIs(t, c.decode(sessionCookie, "no-dot", &out), errBadSession)
	require.ErrorIs(t, c.decode(sessionCookie, "!!!."+c.sign(sessionCookie, "!!!"), &out), errBadSession)
	require.ErrorIs(t, c.decode(sessionCookie, "bm90IGpzb24."+c.sign(sessionCookie, "bm90IGpzb24"), &out), errBadSession)

	// A value decodes only under the name it was signed for.
	value, err := c.encode(sessionCookie, session{Name: sam})
	require.NoError(t, err)
	require.NoError(t, c.decode(sessionCookie, value, &out))
	require.ErrorIs(t, c.decode(stateCookie, value, &out), errBadSession)

	tok, err := randomToken()
	require.NoError(t, err)
	require.Len(t, tok, 22)
}

func TestTeams(t *testing.T) {
	path := writeTeams(t, "alpha: [sam, paul]\noperators: [sam]\n")

	teams, err := LoadTeams(path, logrus.New())
	require.NoError(t, err)
	require.Equal(t, []string{alpha, operators}, teams.Owners(sam))
	require.Equal(t, []string{alpha, operators}, teams.Owners(strings.ToUpper(sam)), "identities compare without case")
	require.Empty(t, teams.Owners("nobody"))

	// Unchanged file: no reload.
	require.NoError(t, teams.Reload())

	// A broken rewrite keeps the old mapping.
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, os.WriteFile(path, []byte("- not a map\n"), 0o644))
	require.NoError(t, os.Chtimes(path, time.Now(), time.Now().Add(time.Second)))
	require.Error(t, teams.Reload())
	require.Equal(t, []string{alpha, operators}, teams.Owners(sam))

	// A good rewrite replaces it.
	require.NoError(t, os.WriteFile(path, []byte("beta: [paul]\n"), 0o644))
	require.NoError(t, os.Chtimes(path, time.Now(), time.Now().Add(2*time.Second)))
	require.NoError(t, teams.Reload())
	require.Empty(t, teams.Owners(sam))
	require.Equal(t, []string{"beta"}, teams.Owners("paul"))

	// Missing file.
	_, err = LoadTeams(filepath.Join(t.TempDir(), "missing.yaml"), logrus.New())
	require.Error(t, err)

	require.NoError(t, os.Remove(path))
	require.Error(t, teams.Reload())

	// The run loop reloads on its ticker and stops with the context.
	path2 := writeTeams(t, "a: [x]\n")
	teams2, err := LoadTeams(path2, logrus.New())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- teams2.Run(ctx, 5*time.Millisecond) }()

	require.NoError(t, os.WriteFile(path2, []byte("b: [x]\n"), 0o644))
	require.NoError(t, os.Chtimes(path2, time.Now(), time.Now().Add(3*time.Second)))
	require.Eventually(t, func() bool { return strings.Join(teams2.Owners("x"), ",") == "b" }, 2*time.Second, 5*time.Millisecond)

	require.NoError(t, os.Remove(path2))
	time.Sleep(20 * time.Millisecond)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestTrustedTokensFromAnotherIssuer(t *testing.T) {
	is := newIssuer(t)
	cli := newIssuer(t)
	cli.aud, cli.claim = "panda-proxy", claimSub

	cfg := &config.Auth{Mode: modeOIDC, Issuer: is.srv.URL, ClientID: clientID, RedirectURL: callbackURL,
		IdentityClaim: claimUsername, AdminOwner: operators,
		TrustedTokens: []config.TrustedToken{{Issuer: cli.srv.URL, Audience: "panda-proxy", IdentityClaim: claimSub}}}

	o, err := NewOIDC(context.Background(), cfg, "secret", sessionKey, nil, logrus.New())
	require.NoError(t, err)

	bearer := func(tok string) (api.Identity, error) {
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.Header.Set("Authorization", "Bearer "+tok)

		return o.Identity(req)
	}

	// The CLI's token names the person in sub.
	id, err := bearer(cli.token(t, sam))
	require.NoError(t, err)
	require.Equal(t, sam, id.Name)

	// rolloor's own tokens still work, and a token for another audience does not.
	id, err = bearer(is.token(t, sam))
	require.NoError(t, err)
	require.Equal(t, sam, id.Name)

	cli.aud = "someone-else"
	_, err = bearer(cli.token(t, sam))
	require.ErrorIs(t, err, ErrNotSignedIn)

	// The trusted entry falls back to the main identity claim.
	cfg.TrustedTokens[0].IdentityClaim = ""
	fallback, err := NewOIDC(context.Background(), cfg, "secret", sessionKey, nil, logrus.New())
	require.NoError(t, err)
	require.Equal(t, claimUsername, fallback.trusted[0].claim)

	cfg.TrustedTokens[0].Issuer = "http://127.0.0.1:1"
	_, err = NewOIDC(context.Background(), cfg, "secret", sessionKey, nil, logrus.New())
	require.ErrorContains(t, err, "trusted issuer")
}

func TestLoginUsesPKCEAndPublicClientsSendNoSecret(t *testing.T) {
	is := newIssuer(t)
	cfg := &config.Auth{Mode: modeOIDC, Issuer: is.srv.URL, ClientID: clientID, RedirectURL: callbackURL, IdentityClaim: claimUsername, AdminOwner: operators}

	o, err := NewOIDC(context.Background(), cfg, "", sessionKey, nil, logrus.New())
	require.NoError(t, err)

	app := httptest.NewServer(o.Middleware(http.NotFoundHandler()))
	t.Cleanup(app.Close)

	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	loc, stateCk := startLogin(t, client, app.URL, "/")
	require.Equal(t, "S256", loc.Query().Get("code_challenge_method"))

	is.nonce = loc.Query().Get("nonce")
	code, _, _ := callback(t, client, app.URL, codeQuery(loc.Query().Get("state")), stateCk)
	require.Equal(t, http.StatusFound, code)

	// The token request proves the challenge with a verifier the login URL
	// never carried.
	require.Len(t, is.forms, 1)

	verifier := is.forms[0].Get("code_verifier")
	sum := sha256.Sum256([]byte(verifier))
	require.Equal(t, base64.RawURLEncoding.EncodeToString(sum[:]), loc.Query().Get("code_challenge"))
	require.NotContains(t, loc.String(), verifier)
	require.Equal(t, clientID, is.forms[0].Get("client_id"), "a public client names itself in the form")
	require.Empty(t, is.forms[0].Get("client_secret"))
	require.Empty(t, is.basic[0])
}
