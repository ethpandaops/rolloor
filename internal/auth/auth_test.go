package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
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

	"github.com/ethpandaops/rolloor/internal/config"
)

const (
	clientID   = "rolloor"
	sessionKey = "0123456789abcdef0123456789abcdef"
	sam        = "sam"
	operators  = "operators"
	alpha      = "alpha"
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
}

func newIssuer(t *testing.T) *issuer {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	is := &issuer{key: key, name: sam, claim: "preferred_username", expiry: time.Hour}
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
		"iss": is.srv.URL, "aud": clientID, "sub": name, "exp": time.Now().Add(is.expiry).Unix(), "iat": time.Now().Unix(),
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

func newOIDC(t *testing.T, is *issuer, teams *Teams) *OIDC {
	t.Helper()

	cfg := &config.Auth{Mode: "oidc", Issuer: is.srv.URL, ClientID: clientID, RedirectURL: "http://app.example/auth/callback", IdentityClaim: "preferred_username", AdminOwner: operators}

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
	require.ErrorContains(t, err, "preferred_username")

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

	// Login: state cookie set, redirected to the issuer with that state.
	resp, err = client.Get(app.URL + resp.Header.Get("Location"))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusFound, resp.StatusCode)

	loc, err := url.Parse(resp.Header.Get("Location"))
	require.NoError(t, err)
	require.Equal(t, is.srv.URL+"/authorize", loc.Scheme+"://"+loc.Host+loc.Path)
	require.Equal(t, clientID, loc.Query().Get("client_id"))

	state := loc.Query().Get("state")
	require.NotEmpty(t, state)
	require.NotEmpty(t, loc.Query().Get("nonce"))

	var stateCk *http.Cookie

	for _, c := range resp.Cookies() {
		if c.Name == stateCookie {
			stateCk = c
		}
	}

	require.NotNil(t, stateCk)
	require.Equal(t, state, stateCk.Value)
	require.True(t, stateCk.HttpOnly)
	require.False(t, stateCk.Secure, "plain http in tests")

	// Callback with a mismatched state is refused.
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, app.URL+CallbackPath+"?state=wrong&code=c", http.NoBody)
	req.AddCookie(stateCk)
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// A token minted for another login's nonce is refused.
	is.nonce = "someone-else"
	req, _ = http.NewRequestWithContext(context.Background(), http.MethodGet, app.URL+CallbackPath+"?state="+url.QueryEscape(state)+"&code=replayed", http.NoBody)
	req.AddCookie(stateCk)
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusBadGateway, resp.StatusCode)

	is.nonce = loc.Query().Get("nonce")
	is.codes = nil

	// Callback with the right state exchanges the code and sets the session.
	req, _ = http.NewRequestWithContext(context.Background(), http.MethodGet, app.URL+CallbackPath+"?state="+url.QueryEscape(state)+"&code=the-code", http.NoBody)
	req.AddCookie(stateCk)
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusFound, resp.StatusCode)
	require.Equal(t, "/groups/client/a", resp.Header.Get("Location"))
	require.Equal(t, []string{"the-code"}, is.codes)

	var sessCk *http.Cookie

	for _, c := range resp.Cookies() {
		if c.Name == sessionCookie && c.Value != "" {
			sessCk = c
		}
	}

	require.NotNil(t, sessCk)

	// The session works on the protected route.
	req, _ = http.NewRequestWithContext(context.Background(), http.MethodGet, app.URL+"/groups/client/a", http.NoBody)
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

func TestCallbackFailures(t *testing.T) {
	is := newIssuer(t)
	o := newOIDC(t, is, nil)
	app := httptest.NewServer(o.Middleware(http.NotFoundHandler()))
	t.Cleanup(app.Close)

	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	// A "next" that points off-site is replaced with /.
	resp, err := client.Get(app.URL + LoginPath + "?next=//evil.example/x")
	require.NoError(t, err)
	resp.Body.Close()

	var st *http.Cookie

	for _, c := range resp.Cookies() {
		if c.Name == stateCookie {
			st = c
		}
	}

	require.NotNil(t, st)

	var decoded struct {
		Next string `json:"next"`
	}

	require.NoError(t, o.sessions.decode(st.Value, &decoded))
	require.Equal(t, "/", decoded.Next)

	// The issuer refusing the code is a 502.
	is.fail = true
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, app.URL+CallbackPath+"?state="+url.QueryEscape(st.Value)+"&code=c", http.NoBody)
	req.AddCookie(st)
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusBadGateway, resp.StatusCode)

	// An ID token without the claim is a 502 too.
	is.fail = false
	is.name = ""
	req, _ = http.NewRequestWithContext(context.Background(), http.MethodGet, app.URL+CallbackPath+"?state="+url.QueryEscape(st.Value)+"&code=c", http.NoBody)
	req.AddCookie(st)
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusBadGateway, resp.StatusCode)

	// An expired state is refused.
	o.now = func() time.Time { return time.Now().Add(time.Hour) }
	req, _ = http.NewRequestWithContext(context.Background(), http.MethodGet, app.URL+CallbackPath+"?state="+url.QueryEscape(st.Value)+"&code=c", http.NoBody)
	req.AddCookie(st)
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// An expired session is not signed in.
	o.now = time.Now

	value, err := o.sessions.encode(session{Name: sam, Expires: time.Now().Add(-time.Minute)})
	require.NoError(t, err)

	r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: value})
	_, err = o.Identity(r)
	require.ErrorIs(t, err, ErrNotSignedIn)

	// Missing state cookie.
	resp, err = client.Get(app.URL + CallbackPath + "?state=x")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

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

	// Random source failing at login.
	o.random = func() (string, error) { return "", errFake }
	resp, err := client.Get(app.URL + LoginPath)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	o.random = randomToken

	// Encoding failing at login.
	o.encode = func(any) (string, error) { return "", errFake }
	resp, err = client.Get(app.URL + LoginPath)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	// Encoding failing only for the session, after a good exchange.
	is.nonce = "x"
	state, err := o.sessions.encode(map[string]any{"n": "x", "next": "/", "e": time.Now().Add(time.Hour)})
	require.NoError(t, err)

	o.encode = func(v any) (string, error) {
		if _, ok := v.(session); ok {
			return "", errFake
		}

		return o.sessions.encode(v)
	}

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, app.URL+CallbackPath+"?state="+url.QueryEscape(state)+"&code=c", http.NoBody)
	req.AddCookie(&http.Cookie{Name: stateCookie, Value: state})
	resp, err = client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

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

	_, err := c.encode(make(chan int))
	require.Error(t, err)

	var out map[string]any

	require.ErrorIs(t, c.decode("no-dot", &out), errBadSession)
	require.ErrorIs(t, c.decode("!!!."+c.sign("!!!"), &out), errBadSession)
	require.ErrorIs(t, c.decode("bm90IGpzb24."+c.sign("bm90IGpzb24"), &out), errBadSession)

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
