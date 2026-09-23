package registry

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ethpandaops/rolloor/internal/observability"
)

// RevisionLabel is the OCI annotation carrying the source commit.
const RevisionLabel = "org.opencontainers.image.revision"

const acceptManifests = "application/vnd.oci.image.index.v1+json, " +
	"application/vnd.docker.distribution.manifest.list.v2+json, " +
	"application/vnd.oci.image.manifest.v1+json, " +
	"application/vnd.docker.distribution.manifest.v2+json"

// Resolved is what a tag points at right now.
type Resolved struct {
	Digest   string    `json:"digest"`
	Revision string    `json:"revision,omitempty"`
	At       time.Time `json:"at"`
}

// Options configure a Resolver.
type Options struct {
	// AuthFile is a docker config.json with an "auths" map; optional.
	AuthFile string
	// PlainHTTP hosts are spoken to over http.
	PlainHTTP []string
	// CAFile is a PEM bundle trusted in addition to the system roots, for
	// registries behind a private certificate authority.
	CAFile  string
	Timeout time.Duration
	// Platform selects the manifest whose config carries the revision label
	// when a tag is a multi-platform index. "os/arch".
	Platform string
}

// Resolver talks to OCI distribution registries.
type Resolver struct {
	client    *http.Client
	auths     map[string]string // host → "user:pass"
	plainHTTP []string
	platform  string
	log       observability.ContextualLogger

	mu        sync.Mutex
	tokens    map[string]string // host/path → bearer token
	revisions map[string]string // digest → revision
}

// NewResolver builds a resolver. It reads the auth file once.
func NewResolver(opts *Options, log observability.ContextualLogger) (*Resolver, error) {
	r := &Resolver{
		client:    &http.Client{Timeout: opts.Timeout},
		auths:     map[string]string{},
		plainHTTP: opts.PlainHTTP,
		platform:  opts.Platform,
		log:       log.WithField("component", "registry"),
		tokens:    map[string]string{},
		revisions: map[string]string{},
	}

	if r.platform == "" {
		r.platform = "linux/amd64"
	}

	if opts.Timeout == 0 {
		r.client.Timeout = 30 * time.Second
	}

	if opts.AuthFile != "" {
		if err := r.loadAuth(opts.AuthFile); err != nil {
			return nil, err
		}
	}

	if opts.CAFile != "" {
		if err := r.trust(opts.CAFile); err != nil {
			return nil, err
		}
	}

	return r, nil
}

// systemRoots is x509.SystemCertPool, replaceable so its failure is testable.
var systemRoots = x509.SystemCertPool

// trust adds the certificates in a PEM file to the roots the client accepts.
func (r *Resolver) trust(path string) error {
	pem, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("registry: read CA file: %w", err)
	}

	pool, err := systemRoots()
	if err != nil {
		pool = x509.NewCertPool()
	}

	if !pool.AppendCertsFromPEM(pem) {
		return fmt.Errorf("registry: %s holds no PEM certificates", path)
	}

	transport, _ := http.DefaultTransport.(*http.Transport)
	transport = transport.Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	r.client.Transport = transport

	return nil
}

func (r *Resolver) loadAuth(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("registry: read auth file: %w", err)
	}

	var cfg struct {
		Auths map[string]struct {
			Auth     string `json:"auth"`
			Username string `json:"username"`
			Password string `json:"password"`
		} `json:"auths"`
	}

	if err := json.Unmarshal(raw, &cfg); err != nil {
		return fmt.Errorf("registry: parse auth file: %w", err)
	}

	for host, a := range cfg.Auths {
		host = strings.TrimPrefix(strings.TrimPrefix(host, "https://"), "http://")
		host = strings.TrimSuffix(host, "/v1/")
		host = strings.TrimSuffix(host, "/")

		// Docker writes Hub credentials under index.docker.io; images name docker.io.
		if host == "index.docker.io" || host == dockerHubAPIHost {
			host = dockerHubHost
		}

		switch {
		case a.Auth != "":
			dec, err := base64.StdEncoding.DecodeString(a.Auth)
			if err != nil {
				return fmt.Errorf("registry: auth for %s is not base64", host)
			}

			r.auths[host] = string(dec)
		case a.Username != "":
			r.auths[host] = a.Username + ":" + a.Password
		}
	}

	return nil
}

// Resolve returns the digest a tag points at and the revision label of the
// image it names.
func (r *Resolver) Resolve(ctx context.Context, ref string) (Resolved, error) {
	parsed, err := ParseReference(ref)
	if err != nil {
		return Resolved{}, err
	}

	// A HEAD gives the digest cheaply; an unchanged tag whose revision is
	// already cached costs nothing more than that.
	digest, err := r.head(ctx, parsed)
	if err != nil {
		return Resolved{}, err
	}

	res := Resolved{Digest: digest, At: time.Now()}

	r.mu.Lock()
	rev, cached := r.revisions[digest]
	r.mu.Unlock()

	if digest != "" && cached {
		res.Revision = rev

		return res, nil
	}

	digest, body, err := r.manifest(ctx, parsed, parsed.Tag)
	if err != nil {
		return Resolved{}, err
	}

	res.Digest = digest

	rev, err = r.revision(ctx, parsed, digest, body)
	if err != nil {
		r.log.WithContext(ctx).WithError(err).WithField("image", ref).Debug("revision label unavailable")
	} else {
		res.Revision = rev
	}

	return res, nil
}

// head returns the tag's digest from a HEAD request, or "" when the registry
// sends none, in which case the manifest is fetched.
func (r *Resolver) head(ctx context.Context, ref Reference) (string, error) {
	resp, err := r.do(ctx, ref, http.MethodHead, r.baseURL(ref)+"/manifests/"+ref.Tag, acceptManifests)
	if err != nil {
		return "", err
	}

	resp.Body.Close()

	return resp.Header.Get("Docker-Content-Digest"), nil
}

// manifest fetches a manifest by tag or digest and returns its digest and body.
func (r *Resolver) manifest(ctx context.Context, ref Reference, tagOrDigest string) (digest string, body []byte, err error) {
	u := r.baseURL(ref) + "/manifests/" + tagOrDigest

	resp, err := r.do(ctx, ref, http.MethodGet, u, acceptManifests)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	body, err = io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return "", nil, fmt.Errorf("registry: read manifest %s: %w", ref, err)
	}

	digest = resp.Header.Get("Docker-Content-Digest")
	if digest == "" {
		sum := sha256.Sum256(body)
		digest = "sha256:" + hex.EncodeToString(sum[:])
	}

	return digest, body, nil
}

// revision reads org.opencontainers.image.revision from the image config,
// descending through an index to the configured platform when needed.
func (r *Resolver) revision(ctx context.Context, ref Reference, digest string, body []byte) (string, error) {
	r.mu.Lock()
	rev, ok := r.revisions[digest]
	r.mu.Unlock()

	if ok {
		return rev, nil
	}

	var m struct {
		MediaType string `json:"mediaType"`
		Config    struct {
			Digest string `json:"digest"`
		} `json:"config"`
		Manifests []struct {
			Digest   string `json:"digest"`
			Platform struct {
				OS   string `json:"os"`
				Arch string `json:"architecture"`
			} `json:"platform"`
		} `json:"manifests"`
	}

	if err := json.Unmarshal(body, &m); err != nil {
		return "", fmt.Errorf("registry: parse manifest: %w", err)
	}

	if len(m.Manifests) > 0 {
		pick := m.Manifests[0].Digest

		for _, c := range m.Manifests {
			if c.Platform.OS+"/"+c.Platform.Arch == r.platform {
				pick = c.Digest

				break
			}
		}

		_, child, err := r.manifest(ctx, ref, pick)
		if err != nil {
			return "", err
		}

		if err := json.Unmarshal(child, &m); err != nil {
			return "", fmt.Errorf("registry: parse platform manifest: %w", err)
		}
	}

	if m.Config.Digest == "" {
		return "", fmt.Errorf("registry: manifest has no config")
	}

	resp, err := r.do(ctx, ref, http.MethodGet, r.baseURL(ref)+"/blobs/"+m.Config.Digest, "")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var cfg struct {
		Config struct {
			Labels map[string]string `json:"Labels"` //nolint:tagliatelle // the image config wire format capitalises it
		} `json:"config"`
	}

	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&cfg); err != nil {
		return "", fmt.Errorf("registry: parse image config: %w", err)
	}

	rev = cfg.Config.Labels[RevisionLabel]

	r.mu.Lock()
	r.revisions[digest] = rev
	r.mu.Unlock()

	return rev, nil
}

func (r *Resolver) baseURL(ref Reference) string {
	scheme := "https"
	if slices.Contains(r.plainHTTP, ref.APIHost()) {
		scheme = "http"
	}

	return scheme + "://" + ref.APIHost() + "/v2/" + ref.Path
}

// do performs a request, obtaining a bearer token on 401 as the distribution
// spec describes, and retrying once with it.
func (r *Resolver) do(ctx context.Context, ref Reference, method, u, accept string) (*http.Response, error) {
	key := ref.APIHost() + "/" + ref.Path

	r.mu.Lock()
	token := r.tokens[key]
	r.mu.Unlock()

	resp, err := r.send(ctx, method, u, accept, token, ref)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusUnauthorized {
		challenge := resp.Header.Get("WWW-Authenticate")
		resp.Body.Close()

		token, err = r.fetchToken(ctx, ref, challenge)
		if err != nil {
			return nil, err
		}

		r.mu.Lock()
		r.tokens[key] = token
		r.mu.Unlock()

		resp, err = r.send(ctx, method, u, accept, token, ref)
		if err != nil {
			return nil, err
		}
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()

		return nil, fmt.Errorf("registry: %s %s: %s", method, u, resp.Status)
	}

	return resp, nil
}

func (r *Resolver) send(ctx context.Context, method, u, accept, token string, ref Reference) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("registry: build request: %w", err)
	}

	if accept != "" {
		req.Header.Set("Accept", accept)
	}

	switch creds := r.creds(ref); {
	case token != "":
		req.Header.Set("Authorization", "Bearer "+token)
	case creds != "":
		req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(creds)))
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("registry: %s %s: %w", method, u, err)
	}

	return resp, nil
}

// creds returns "user:pass" for the registry, by the host as written or by
// the API host it maps to.
func (r *Resolver) creds(ref Reference) string {
	if c := r.auths[ref.Host]; c != "" {
		return c
	}

	return r.auths[ref.APIHost()]
}

// fetchToken follows a Bearer challenge: realm, service and scope.
func (r *Resolver) fetchToken(ctx context.Context, ref Reference, challenge string) (string, error) {
	params := parseChallenge(challenge)

	realm := params["realm"]
	if realm == "" {
		return "", fmt.Errorf("registry: %s wants auth but sent no bearer realm", ref.APIHost())
	}

	q := url.Values{}
	if s := params["service"]; s != "" {
		q.Set("service", s)
	}

	scope := params["scope"]
	if scope == "" {
		scope = "repository:" + ref.Path + ":pull"
	}

	q.Set("scope", scope)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, realm+"?"+q.Encode(), http.NoBody)
	if err != nil {
		return "", fmt.Errorf("registry: build token request: %w", err)
	}

	if creds := r.creds(ref); creds != "" {
		req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(creds)))
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("registry: token request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("registry: token request for %s: %s", ref, resp.Status)
	}

	var tok struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"` //nolint:tagliatelle // OAuth wire format
	}

	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&tok); err != nil {
		return "", fmt.Errorf("registry: parse token: %w", err)
	}

	if tok.Token == "" {
		tok.Token = tok.AccessToken
	}

	if tok.Token == "" {
		return "", fmt.Errorf("registry: token response for %s had no token", ref)
	}

	return tok.Token, nil
}

// parseChallenge reads `Bearer realm="…",service="…",scope="…"`.
func parseChallenge(h string) map[string]string {
	out := map[string]string{}

	h = strings.TrimSpace(h)
	if !strings.HasPrefix(strings.ToLower(h), "bearer ") {
		return out
	}

	for part := range strings.SplitSeq(h[len("bearer "):], ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}

		out[strings.ToLower(k)] = strings.Trim(v, `"`)
	}

	return out
}
