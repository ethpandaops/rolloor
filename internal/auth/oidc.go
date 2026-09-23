package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"

	"github.com/ethpandaops/rolloor/internal/api"
	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/observability"
)

// ErrNotSignedIn is returned when a request carries no usable identity.
var ErrNotSignedIn = errors.New("not signed in")

// Paths the middleware owns.
const (
	LoginPath    = "/auth/login"
	CallbackPath = "/auth/callback"
	LogoutPath   = "/auth/logout"
)

const (
	stateTTL   = 10 * time.Minute
	sessionTTL = 12 * time.Hour
)

// OIDC signs people in with an OpenID Connect issuer and turns the configured
// claim into an identity with owner values.
type OIDC struct {
	verifier   *oidc.IDTokenVerifier
	oauth      oauth2.Config
	claim      string
	adminOwner string
	teams      *Teams
	sessions   codec
	log        observability.ContextualLogger
	now        func() time.Time
	// encode and random are the codec's and crypto/rand's, replaceable in tests.
	encode func(any) (string, error)
	random func() (string, error)
}

var _ api.Authorizer = (*OIDC)(nil)

// NewOIDC discovers the issuer. teams may be nil, in which case a claim value
// equal to an owner value grants that owner. secret is the client secret and
// sessionKey signs cookies.
func NewOIDC(ctx context.Context, cfg *config.Auth, secret, sessionKey string, teams *Teams, log observability.ContextualLogger) (*OIDC, error) {
	if len(sessionKey) < 16 {
		return nil, errors.New("auth: the session key must be at least 16 bytes")
	}

	provider, err := oidc.NewProvider(ctx, cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("auth: discover %s: %w", cfg.Issuer, err)
	}

	sessions := codec{key: []byte(sessionKey)}

	return &OIDC{
		verifier: provider.Verifier(&oidc.Config{ClientID: cfg.ClientID}),
		oauth: oauth2.Config{
			ClientID: cfg.ClientID, ClientSecret: secret, RedirectURL: cfg.RedirectURL,
			Endpoint: provider.Endpoint(), Scopes: []string{oidc.ScopeOpenID, "profile", "email"},
		},
		claim:      cfg.IdentityClaim,
		adminOwner: cfg.AdminOwner,
		teams:      teams,
		sessions:   sessions,
		log:        log.WithField("component", "auth"),
		now:        time.Now,
		encode:     sessions.encode,
		random:     randomToken,
	}, nil
}

// Identity resolves a bearer token or a session cookie.
func (o *OIDC) Identity(r *http.Request) (api.Identity, error) {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		name, err := o.nameFromToken(r.Context(), strings.TrimPrefix(h, "Bearer "))
		if err != nil {
			return api.Identity{}, err
		}

		return o.identityFor(name), nil
	}

	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return api.Identity{}, ErrNotSignedIn
	}

	var s session
	if err := o.sessions.decode(c.Value, &s); err != nil || o.now().After(s.Expires) || s.Name == "" {
		return api.Identity{}, ErrNotSignedIn
	}

	return o.identityFor(s.Name), nil
}

// identityFor builds the identity: owners from the teams file, or the name
// itself as an owner when there is no file.
func (o *OIDC) identityFor(name string) api.Identity {
	owners := []string{name}
	if o.teams != nil {
		owners = o.teams.Owners(name)
	}

	return api.Identity{Name: name, Owners: owners, Admin: slices.Contains(owners, o.adminOwner)}
}

// nameFromToken verifies an ID token against the issuer and reads the claim.
func (o *OIDC) nameFromToken(ctx context.Context, raw string) (string, error) {
	name, _, err := o.verify(ctx, raw)

	return name, err
}

// verify checks an ID token and returns the claim and the token's nonce.
func (o *OIDC) verify(ctx context.Context, raw string) (name, nonce string, err error) {
	tok, err := o.verifier.Verify(ctx, raw)
	if err != nil {
		return "", "", fmt.Errorf("%w: %w", ErrNotSignedIn, err)
	}

	// A verified token's payload is always a JSON object, so this cannot fail;
	// if it somehow did, the claim would be missing and rejected below.
	claims := map[string]any{}
	_ = tok.Claims(&claims)

	name, _ = claims[o.claim].(string)
	if name == "" {
		return "", "", fmt.Errorf("%w: token has no %q claim", ErrNotSignedIn, o.claim)
	}

	return name, tok.Nonce, nil
}

// Middleware serves the login, callback and logout routes and passes
// everything else through.
func (o *OIDC) Middleware(next http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+LoginPath, o.login)
	mux.HandleFunc("GET "+CallbackPath, o.callback)
	mux.HandleFunc("GET "+LogoutPath, o.logout)
	mux.HandleFunc("POST "+LogoutPath, o.logout)
	mux.Handle("/", next)

	return mux
}

func (o *OIDC) login(w http.ResponseWriter, r *http.Request) {
	nonce, err := o.random()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	next := r.URL.Query().Get("next")
	if !strings.HasPrefix(next, "/") || strings.HasPrefix(next, "//") {
		next = "/"
	}

	state, err := o.encode(map[string]any{"n": nonce, "next": next, "e": o.now().Add(stateTTL)})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	setCookie(w, r, stateCookie, state, int(stateTTL.Seconds()))
	http.Redirect(w, r, o.oauth.AuthCodeURL(state, oidc.Nonce(nonce)), http.StatusFound)
}

func (o *OIDC) callback(w http.ResponseWriter, r *http.Request) {
	c, err := r.Cookie(stateCookie)
	if err != nil || c.Value != r.URL.Query().Get("state") {
		http.Error(w, "login state mismatch; start again", http.StatusBadRequest)

		return
	}

	var st struct {
		Nonce   string    `json:"n"`
		Next    string    `json:"next"`
		Expires time.Time `json:"e"`
	}

	if decodeErr := o.sessions.decode(c.Value, &st); decodeErr != nil || o.now().After(st.Expires) {
		http.Error(w, "login state expired; start again", http.StatusBadRequest)

		return
	}

	setCookie(w, r, stateCookie, "", -1)

	tok, err := o.oauth.Exchange(r.Context(), r.URL.Query().Get("code"))
	if err != nil {
		o.log.WithContext(r.Context()).WithError(err).Warn("code exchange failed")
		http.Error(w, "the issuer rejected the login", http.StatusBadGateway)

		return
	}

	rawID, _ := tok.Extra("id_token").(string)

	name, nonce, err := o.verify(r.Context(), rawID)
	if err == nil && nonce != st.Nonce {
		err = errors.New("nonce does not match the login")
	}

	if err != nil {
		o.log.WithContext(r.Context()).WithError(err).Warn("id token rejected")
		http.Error(w, "the issuer's token could not be verified", http.StatusBadGateway)

		return
	}

	value, err := o.encode(session{Name: name, Expires: o.now().Add(sessionTTL)})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	setCookie(w, r, sessionCookie, value, int(sessionTTL.Seconds()))
	http.Redirect(w, r, st.Next, http.StatusFound)
}

func (o *OIDC) logout(w http.ResponseWriter, r *http.Request) {
	setCookie(w, r, sessionCookie, "", -1)
	http.Redirect(w, r, "/", http.StatusFound)
}

// LoginURL is where an unauthenticated browser should be sent.
func LoginURL(next string) string {
	return LoginPath + "?next=" + url.QueryEscape(next)
}
