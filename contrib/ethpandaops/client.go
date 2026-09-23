package ethpandaops

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// errNotFound is a 404 from an API.
var errNotFound = errors.New("not found")

// call sends one request and decodes a JSON answer into out when out is not
// nil. It returns the status code.
func (h *Hooks) call(ctx context.Context, method, url string, body any, auth func(*http.Request), out any) (int, http.Header, error) {
	var rd io.Reader = http.NoBody

	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}

		rd = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return 0, nil, err
	}

	req.Header.Set("Accept", "application/json")

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	auth(req)

	resp, err := h.Client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return resp.StatusCode, resp.Header, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return resp.StatusCode, resp.Header, fmt.Errorf("%s %s: %w", method, redact(url), errNotFound)
	}

	if resp.StatusCode >= 300 {
		return resp.StatusCode, resp.Header, fmt.Errorf("%s %s: %s: %s", method, redact(url), resp.Status, firstLine(string(raw)))
	}

	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, resp.Header, fmt.Errorf("%s %s: decode: %w", method, redact(url), err)
		}
	}

	return resp.StatusCode, resp.Header, nil
}

// updaterAuth sends the updater token as a bearer token.
func (h *Hooks) updaterAuth(req *http.Request) {
	if h.UpdaterToken != "" {
		req.Header.Set("Authorization", "Bearer "+h.UpdaterToken)
	}
}

// nodeAuth sends the shared basic auth the node endpoints sit behind.
func (h *Hooks) nodeAuth(req *http.Request) {
	if user, pass, ok := strings.Cut(h.NodeAuth, ":"); ok {
		req.SetBasicAuth(user, pass)
	}
}

// redact drops any query string, which may carry filters but never needs
// to be shown.
func redact(url string) string {
	base, _, _ := strings.Cut(url, "?")

	return base
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}

	if len(s) > 200 {
		s = s[:200]
	}

	return s
}
