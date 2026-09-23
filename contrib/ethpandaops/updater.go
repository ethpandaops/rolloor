package ethpandaops

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// container is one entry of the updater's container listings.
type container struct {
	Name         string `json:"name"`
	Image        string `json:"image"`
	Digest       string `json:"digest"`
	Running      bool   `json:"running"`
	LatestDigest string `json:"latest_digest"`
	Error        string `json:"error"`
}

const checkPath = "/v1/check"

type containerList struct {
	Containers []container `json:"containers"`
}

func updaterBase(t *Target) (string, error) {
	if t.Extra.Updater == "" || t.Extra.Container == "" {
		return "", fmt.Errorf("%s: extra.updater and extra.container are required", t.ID)
	}

	return strings.TrimRight(t.Extra.Updater, "/"), nil
}

// lookup asks one of the updater's listings for exactly the target's
// container. found is false when the updater does not watch it.
func (h *Hooks) lookup(ctx context.Context, t *Target, method, path string) (c container, found bool, err error) {
	base, err := updaterBase(t)
	if err != nil {
		return container{}, false, err
	}

	var list containerList

	// The listings filter by exact name; check reads a regular expression.
	q := url.Values{}
	if path == checkPath {
		q.Set("container", regexp.QuoteMeta(t.Extra.Container))
	} else {
		q.Set("name", t.Extra.Container)
	}

	if _, _, err := h.call(ctx, method, base+path+"?"+q.Encode(), nil, h.updaterAuth, &list); err != nil {
		return container{}, false, fmt.Errorf("updater: %w", err)
	}

	for _, c := range list.Containers {
		if strings.TrimPrefix(c.Name, "/") == t.Extra.Container {
			return c, true, nil
		}
	}

	return container{}, false, nil
}

// Inspect prints the registry digest the target's container runs, or none
// when the node has no such container.
func Inspect(ctx context.Context, h *Hooks, stdin []byte) (Result, error) {
	t, err := decodeTarget(stdin)
	if err != nil {
		return Result{}, err
	}

	c, found, err := h.lookup(ctx, t, http.MethodGet, "/v1/containers")
	if err != nil {
		return Result{}, err
	}

	if !found {
		return pass("none"), nil
	}

	if c.Digest == "" {
		return fail("%s runs %s, which has no registry digest", t.Extra.Container, c.Image), nil
	}

	return pass("%s", c.Digest), nil
}

// Update asks the node's updater to move the container to the tag's head,
// but only when the head is the digest the rollout wants: the updater cannot
// deploy anything else. When the head has moved on, it does nothing and says
// so; rolloor sees the new build on its next registry poll and replaces the
// rollout.
func Update(ctx context.Context, h *Hooks, stdin []byte) (Result, error) {
	t, err := decodeTarget(stdin)
	if err != nil {
		return Result{}, err
	}

	if t.Desired == "" {
		return fail("no desired digest in the target document"), nil
	}

	cur, found, err := h.lookup(ctx, t, http.MethodGet, "/v1/containers")
	if err != nil {
		return Result{}, err
	}

	if !found {
		return fail("the updater on %s does not watch a container named %s", t.Node, t.Extra.Container), nil
	}

	if cur.Digest == t.Desired {
		return pass("already running %s", short(t.Desired)), nil
	}

	head, _, err := h.lookup(ctx, t, http.MethodPost, checkPath)
	if err != nil {
		return Result{}, err
	}

	if head.Error != "" {
		return fail("updater could not read the registry: %s", head.Error), nil
	}

	if head.LatestDigest != t.Desired {
		return pass("tag head is %s, not %s; left as is (a pinned digest cannot be deployed by the updater)",
			short(head.LatestDigest), short(t.Desired)), nil
	}

	return h.startUpdate(ctx, t)
}

// startUpdate posts an asynchronous, targeted update, waiting out a busy
// updater until UpdateWait has passed.
func (h *Hooks) startUpdate(ctx context.Context, t *Target) (Result, error) {
	base, err := updaterBase(t)
	if err != nil {
		return Result{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, h.UpdateWait)
	defer cancel()

	q := url.Values{}
	q.Set("container", regexp.QuoteMeta(t.Extra.Container))
	q.Set("async", "true")

	for {
		status, header, err := h.call(ctx, http.MethodPost, base+"/v1/update?"+q.Encode(), nil, h.updaterAuth, nil)
		if status != http.StatusTooManyRequests {
			if err != nil {
				return Result{}, fmt.Errorf("updater: %w", err)
			}

			return pass("update to %s started", short(t.Desired)), nil
		}

		wait := 2 * time.Second
		if secs, perr := strconv.Atoi(header.Get("Retry-After")); perr == nil && secs > 0 && secs < 10 {
			wait = time.Duration(secs) * time.Second
		}

		if serr := h.Sleep(ctx, wait); serr != nil {
			if errors.Is(serr, context.DeadlineExceeded) {
				return fail("the updater stayed busy for %s", h.UpdateWait), nil
			}

			return Result{}, serr
		}
	}
}

// ReadyRunning passes when the container is running the desired digest. It
// suits containers with no API of their own, such as validator clients and
// sidecars.
func ReadyRunning(ctx context.Context, h *Hooks, stdin []byte) (Result, error) {
	t, err := decodeTarget(stdin)
	if err != nil {
		return Result{}, err
	}

	c, found, err := h.lookup(ctx, t, http.MethodGet, "/v1/containers/details")
	if err != nil {
		return Result{}, err
	}

	switch {
	case !found:
		return fail("%s is not on %s", t.Extra.Container, t.Node), nil
	case !c.Running:
		return fail("%s is not running", t.Extra.Container), nil
	case t.Desired != "" && c.Digest != t.Desired:
		return fail("%s runs %s, not %s", t.Extra.Container, short(c.Digest), short(t.Desired)), nil
	}

	return pass("%s running %s", t.Extra.Container, short(c.Digest)), nil
}
