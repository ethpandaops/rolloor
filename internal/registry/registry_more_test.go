package registry

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// awkwardRegistry serves the corner cases: junk manifests, indexes pointing at
// missing children, truncated bodies, and a connection dropped once a token is
// presented.
func awkwardRegistry(t *testing.T) (*httptest.Server, string) {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/org/app/manifests/", func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/v2/org/app/manifests/")

		switch name {
		case tagJunk:
			w.Header().Set("Docker-Content-Digest", "sha256:junk")
			_, _ = w.Write([]byte("not json"))
		case "badindex":
			w.Header().Set("Docker-Content-Digest", "sha256:badindex")
			_ = json.NewEncoder(w).Encode(map[string]any{keyManifests: []map[string]any{{keyDigest: "sha256:missing"}}})
		case "junkchild":
			w.Header().Set("Docker-Content-Digest", "sha256:junkchild")
			_ = json.NewEncoder(w).Encode(map[string]any{keyManifests: []map[string]any{{keyDigest: tagJunk}}})
		case "badblob":
			w.Header().Set("Docker-Content-Digest", "sha256:badblob")
			_ = json.NewEncoder(w).Encode(map[string]any{keyConfig: map[string]string{keyDigest: "sha256:missingblob"}})
		case "junkblob":
			w.Header().Set("Docker-Content-Digest", "sha256:junkblob")
			_ = json.NewEncoder(w).Encode(map[string]any{keyConfig: map[string]string{keyDigest: "sha256:junkblob"}})
		case "truncated":
			w.Header().Set("Content-Length", "100")
			_, _ = w.Write([]byte("short"))
		default:
			http.NotFound(w, r)
		}
	})
	mux.HandleFunc("/v2/org/app/blobs/sha256:junkblob", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json"))
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return srv, strings.TrimPrefix(srv.URL, "http://")
}

func TestRevisionFailuresLeaveDigestUsable(t *testing.T) {
	_, host := awkwardRegistry(t)

	r, err := NewResolver(Options{PlainHTTP: []string{host}}, logrus.New())
	require.NoError(t, err)

	for _, tag := range []string{tagJunk, "badindex", "junkchild", "badblob", "junkblob"} {
		var res Resolved

		res, err = r.Resolve(context.Background(), host+"/org/app:"+tag)
		require.NoError(t, err, tag)
		require.Equal(t, "sha256:"+tag, res.Digest, tag)
		require.Empty(t, res.Revision, tag)
	}

	_, err = r.Resolve(context.Background(), host+"/org/app:truncated")
	require.Error(t, err)
}

func TestRequestBuildErrors(t *testing.T) {
	r, err := NewResolver(Options{}, logrus.New())
	require.NoError(t, err)

	// A host with a space cannot become a URL.
	_, err = r.Resolve(context.Background(), "bad host.example/org/app:x")
	require.Error(t, err)

	// A realm that is not a URL fails at request construction.
	_, err = r.fetchToken(context.Background(), Reference{Host: "h", Path: "p"}, `Bearer realm="http://bad host/token"`)
	require.Error(t, err)

	// A realm nobody listens on fails at the request.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := l.Addr().String()
	require.NoError(t, l.Close())

	_, err = r.fetchToken(context.Background(), Reference{Host: "h", Path: "p"}, `Bearer realm="http://`+addr+`/token",scope="s"`)
	require.Error(t, err)
}

func TestCredsByAPIHost(t *testing.T) {
	r, err := NewResolver(Options{}, logrus.New())
	require.NoError(t, err)

	r.auths[dockerHubAPIHost] = "u:p"
	require.Equal(t, "u:p", r.creds(Reference{Host: dockerHubHost}))
	r.auths[dockerHubHost] = "a:b"
	require.Equal(t, "a:b", r.creds(Reference{Host: dockerHubHost}))
	require.Empty(t, r.creds(Reference{Host: "other"}))

	require.Equal(t, map[string]string{"realm": "x"}, parseChallenge(`Bearer realm="x",junk`))
}

func TestAuthFileHubAliases(t *testing.T) {
	for _, alias := range []string{"https://index.docker.io/v1/", dockerHubAPIHost} {
		path := filepath.Join(t.TempDir(), "config.json")
		require.NoError(t, os.WriteFile(path, []byte(`{"auths":{"`+alias+`":{"username":"hub","password":"pw"}}}`), 0o600))

		r, err := NewResolver(Options{AuthFile: path}, logrus.New())
		require.NoError(t, err)
		require.Equal(t, "hub:pw", r.auths[dockerHubHost], alias)
		require.Equal(t, "hub:pw", r.creds(Reference{Host: dockerHubHost}), alias)
	}
}

func TestAuthFileSkipsEmptyEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"auths":{"h":{}}}`), 0o600))

	r, err := NewResolver(Options{AuthFile: path}, logrus.New())
	require.NoError(t, err)
	require.Empty(t, r.auths)
}

func TestConnectionDroppedAfterToken(t *testing.T) {
	var srv *httptest.Server

	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"token":"tok-123"}`))
	})
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != bearerTok {
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+srv.URL+`/token"`)
			w.WriteHeader(http.StatusUnauthorized)

			return
		}

		hj, ok := w.(http.Hijacker)
		require.True(t, ok)

		conn, _, err := hj.Hijack()
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	})

	srv = httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	host := strings.TrimPrefix(srv.URL, "http://")

	r, err := NewResolver(Options{PlainHTTP: []string{host}}, logrus.New())
	require.NoError(t, err)

	_, err = r.Resolve(context.Background(), host+"/org/app:x")
	require.Error(t, err)
}
