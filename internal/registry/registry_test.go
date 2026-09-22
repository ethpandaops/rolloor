package registry

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestParseReference(t *testing.T) {
	cases := []struct {
		in   string
		want Reference
		api  string
	}{
		{"nginx", Reference{dockerHubHost, "library/nginx", defaultTag}, dockerHubAPIHost},
		{"nginx:1.27", Reference{dockerHubHost, "library/nginx", "1.27"}, dockerHubAPIHost},
		{"acme/app:unstable", Reference{dockerHubHost, "acme/app", "unstable"}, dockerHubAPIHost},
		{"ghcr.io/org/app:v1", Reference{"ghcr.io", "org/app", "v1"}, "ghcr.io"},
		{"localhost:5000/app:latest", Reference{"localhost:5000", "app", defaultTag}, "localhost:5000"},
		{"localhost/app:x", Reference{localhost, "app", "x"}, localhost},
		{"my.registry:443/a/b/c:t", Reference{"my.registry:443", "a/b/c", "t"}, "my.registry:443"},
	}

	for _, c := range cases {
		got, err := ParseReference(c.in)
		require.NoError(t, err, c.in)
		require.Equal(t, c.want, got, c.in)
		require.Equal(t, c.api, got.APIHost(), c.in)
		require.Equal(t, c.want.Host+"/"+c.want.Path+":"+c.want.Tag, got.String())
	}

	for _, bad := range []string{"", "org/app@sha256:abc", "org/app:", "localhost:5000/"} {
		_, err := ParseReference(bad)
		require.Error(t, err, bad)
	}
}

func TestParseChallenge(t *testing.T) {
	p := parseChallenge(`Bearer realm="https://auth.example/token",service="registry.example",scope="repository:a/b:pull"`)
	require.Equal(t, "https://auth.example/token", p["realm"])
	require.Equal(t, "registry.example", p["service"])
	require.Equal(t, "repository:a/b:pull", p["scope"])
	require.Empty(t, parseChallenge("Basic realm=x"))
	require.Empty(t, parseChallenge(""))
}

// fakeRegistry serves one repository with a multi-platform index, a platform
// manifest and a config blob, behind bearer token auth.
type fakeRegistry struct {
	t         *testing.T
	srv       *httptest.Server
	requireTk bool
	tokenHits int
	basicSeen string
	digestHdr bool
}

const (
	keyDigest      = "digest"
	keyConfig      = "config"
	indexDigest    = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	manifestDigest = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	configDigest   = "sha256:3333333333333333333333333333333333333333333333333333333333333333"
)

func newFakeRegistry(t *testing.T, requireToken, digestHeader bool) *fakeRegistry {
	t.Helper()

	f := &fakeRegistry{t: t, requireTk: requireToken, digestHdr: digestHeader}
	mux := http.NewServeMux()

	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		f.tokenHits++
		f.basicSeen = r.Header.Get("Authorization")
		require.Equal(t, "repository:org/app:pull", r.URL.Query().Get("scope"))

		_ = json.NewEncoder(w).Encode(map[string]string{"token": "tok-123"})
	})

	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		if f.requireTk && r.Header.Get("Authorization") != "Bearer tok-123" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+f.srv.URL+`/token",service="fake"`)
			w.WriteHeader(http.StatusUnauthorized)

			return
		}

		switch r.URL.Path {
		case "/v2/org/app/manifests/unstable":
			require.Contains(t, r.Header.Get("Accept"), "image.index")

			if f.digestHdr {
				w.Header().Set("Docker-Content-Digest", indexDigest)
			}

			_ = json.NewEncoder(w).Encode(map[string]any{
				"mediaType": "application/vnd.oci.image.index.v1+json",
				"manifests": []map[string]any{
					{keyDigest: "sha256:arm", "platform": map[string]string{"os": "linux", "architecture": "arm64"}},
					{keyDigest: manifestDigest, "platform": map[string]string{"os": "linux", "architecture": "amd64"}},
				},
			})
		case "/v2/org/app/manifests/" + manifestDigest:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"mediaType": "application/vnd.oci.image.manifest.v1+json",
				keyConfig:   map[string]string{keyDigest: configDigest},
			})
		case "/v2/org/app/manifests/single":
			w.Header().Set("Docker-Content-Digest", manifestDigest)
			_ = json.NewEncoder(w).Encode(map[string]any{keyConfig: map[string]string{keyDigest: configDigest}})
		case "/v2/org/app/manifests/noconfig":
			w.Header().Set("Docker-Content-Digest", "sha256:nocfg")
			_ = json.NewEncoder(w).Encode(map[string]any{"layers": []string{}})
		case "/v2/org/app/blobs/" + configDigest:
			_ = json.NewEncoder(w).Encode(map[string]any{
				keyConfig: map[string]any{"Labels": map[string]string{RevisionLabel: "7b2d0e4"}},
			})
		default:
			http.NotFound(w, r)
		}
	})

	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)

	return f
}

func (f *fakeRegistry) host() string { return strings.TrimPrefix(f.srv.URL, "http://") }

func newResolver(t *testing.T, f *fakeRegistry, authFile string) *Resolver {
	t.Helper()

	r, err := NewResolver(Options{PlainHTTP: []string{f.host()}, AuthFile: authFile, Timeout: 5 * time.Second}, logrus.New())
	require.NoError(t, err)

	return r
}

func TestResolveWithTokenAndIndex(t *testing.T) {
	f := newFakeRegistry(t, true, true)
	r := newResolver(t, f, "")

	res, err := r.Resolve(context.Background(), f.host()+"/org/app:unstable")
	require.NoError(t, err)
	require.Equal(t, indexDigest, res.Digest)
	require.Equal(t, "7b2d0e4", res.Revision)
	require.False(t, res.At.IsZero())
	require.Equal(t, 1, f.tokenHits)

	// Second resolve reuses the token and the cached revision.
	res, err = r.Resolve(context.Background(), f.host()+"/org/app:unstable")
	require.NoError(t, err)
	require.Equal(t, "7b2d0e4", res.Revision)
	require.Equal(t, 1, f.tokenHits)
}

func TestResolveComputesDigestWhenHeaderMissing(t *testing.T) {
	f := newFakeRegistry(t, false, false)
	r := newResolver(t, f, "")

	res, err := r.Resolve(context.Background(), f.host()+"/org/app:unstable")
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(res.Digest, "sha256:"))
	require.Len(t, res.Digest, len("sha256:")+64)
}

func TestResolveSingleManifestAndMissingConfig(t *testing.T) {
	f := newFakeRegistry(t, false, true)
	r := newResolver(t, f, "")

	res, err := r.Resolve(context.Background(), f.host()+"/org/app:single")
	require.NoError(t, err)
	require.Equal(t, manifestDigest, res.Digest)
	require.Equal(t, "7b2d0e4", res.Revision)

	res, err = r.Resolve(context.Background(), f.host()+"/org/app:noconfig")
	require.NoError(t, err)
	require.Equal(t, "sha256:nocfg", res.Digest)
	require.Empty(t, res.Revision)
}

func TestResolveErrors(t *testing.T) {
	f := newFakeRegistry(t, false, true)
	r := newResolver(t, f, "")

	_, err := r.Resolve(context.Background(), f.host()+"/org/app:missing")
	require.Error(t, err)

	_, err = r.Resolve(context.Background(), "bad@sha256:x")
	require.Error(t, err)

	_, err = r.Resolve(context.Background(), "127.0.0.1:1/org/app:x")
	require.Error(t, err)
}

func TestAuthFileBasicCredentials(t *testing.T) {
	f := newFakeRegistry(t, true, true)
	dir := t.TempDir()
	authPath := filepath.Join(dir, "config.json")
	creds := base64.StdEncoding.EncodeToString([]byte("user:pass"))
	require.NoError(t, os.WriteFile(authPath, []byte(`{"auths":{"https://`+f.host()+`/":{"auth":"`+creds+`"},"other.example":{"username":"u","password":"p"}}}`), 0o600))

	r := newResolver(t, f, authPath)
	require.Equal(t, "user:pass", r.auths[f.host()])
	require.Equal(t, "u:p", r.auths["other.example"])

	_, err := r.Resolve(context.Background(), f.host()+"/org/app:unstable")
	require.NoError(t, err)
	require.Equal(t, "Basic "+creds, f.basicSeen)
}

func TestAuthFileErrors(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.json")
	require.NoError(t, os.WriteFile(bad, []byte(`{"auths":{"h":{"auth":"%%%"}}}`), 0o600))

	_, err := NewResolver(Options{AuthFile: bad}, logrus.New())
	require.Error(t, err)

	require.NoError(t, os.WriteFile(bad, []byte(`not json`), 0o600))
	_, err = NewResolver(Options{AuthFile: bad}, logrus.New())
	require.Error(t, err)

	_, err = NewResolver(Options{AuthFile: filepath.Join(dir, "missing.json")}, logrus.New())
	require.Error(t, err)
}

func TestChallengeWithoutRealm(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("WWW-Authenticate", "Bearer service=x")
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(srv.Close)

	host := strings.TrimPrefix(srv.URL, "http://")
	r, err := NewResolver(Options{PlainHTTP: []string{host}}, logrus.New())
	require.NoError(t, err)

	_, err = r.Resolve(context.Background(), host+"/org/app:x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no bearer realm")
}

func TestTokenEndpointFailures(t *testing.T) {
	var mode string

	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, _ *http.Request) {
		switch mode {
		case "500":
			w.WriteHeader(http.StatusInternalServerError)
		case "empty":
			_, _ = w.Write([]byte(`{}`))
		case "junk":
			_, _ = w.Write([]byte(`junk`))
		case "access":
			_, _ = w.Write([]byte(`{"access_token":"tok-123"}`))
		}
	})

	var srv *httptest.Server

	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok-123" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+srv.URL+`/token"`)
			w.WriteHeader(http.StatusUnauthorized)

			return
		}

		w.Header().Set("Docker-Content-Digest", "sha256:ok")
		_, _ = w.Write([]byte(`{}`))
	})

	srv = httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	host := strings.TrimPrefix(srv.URL, "http://")

	for _, m := range []string{"500", "empty", "junk"} {
		mode = m

		r, err := NewResolver(Options{PlainHTTP: []string{host}}, logrus.New())
		require.NoError(t, err)

		_, err = r.Resolve(context.Background(), host+"/org/app:x")
		require.Error(t, err, m)
	}

	mode = "access"

	r, err := NewResolver(Options{PlainHTTP: []string{host}}, logrus.New())
	require.NoError(t, err)

	res, err := r.Resolve(context.Background(), host+"/org/app:x")
	require.NoError(t, err)
	require.Equal(t, "sha256:ok", res.Digest)
	require.Empty(t, res.Revision)
}
