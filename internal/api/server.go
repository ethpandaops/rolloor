// Package api serves the controller over HTTP: JSON reads, the verbs, and a
// live event stream.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/observability"
	"github.com/ethpandaops/rolloor/internal/reconcile"
	"github.com/ethpandaops/rolloor/internal/targets"
)

// errKey is the JSON field every error response carries.
const errKey = "error"

const (
	headerContentType    = "Content-Type"
	mediaJSON            = "application/json"
	headerOrigin         = "Origin"
	headerReferer        = "Referer"
	headerFetchSite      = "Sec-Fetch-Site"
	headerForwardedProto = "X-Forwarded-Proto"
	fetchSameOrigin      = "same-origin"
	fetchCrossSite       = "cross-site"
	fetchUserInitiated   = "none"
	originOpaque         = "null"
	schemeHTTPS          = "https"
)

const (
	sseWriteTimeout = 10 * time.Second
	sseHeartbeat    = 15 * time.Second
)

// Server holds the handlers' dependencies.
type Server struct {
	cfg     *config.Config
	c       *reconcile.Controller
	targets func() *targets.Set
	auth    Authorizer
	events  *Broadcaster
	log     observability.ContextualLogger
	// version is reported on /healthz.
	version string
}

// New wires a server. events may be nil when no live stream is wanted.
func New(cfg *config.Config, c *reconcile.Controller, ts func() *targets.Set, auth Authorizer, events *Broadcaster, version string, log observability.ContextualLogger) *Server {
	if events == nil {
		events = NewBroadcaster()
	}

	return &Server{cfg: cfg, c: c, targets: ts, auth: auth, events: events, log: log.WithField("component", "api"), version: version}
}

// Handler returns the routes wrapped in the authorizer's middleware.
func (s *Server) Handler() http.Handler {
	return s.auth.Middleware(s.Routes())
}

// Routes returns the API routes under /api/v1 plus /healthz, without the
// middleware, so a caller can mount them beside other handlers under one.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /api/v1/me", s.me)

	for path, h := range map[string]http.HandlerFunc{
		"/fleet": s.fleet, "/groups/{label}/{value}": s.group, "/nodes/{node}": s.node, "/targets": s.targetsList,
		"/rollouts": s.rollouts, "/rollouts/{id}": s.rollout, "/history": s.history,
		"/suspensions": s.suspensions, "/events": s.stream,
	} {
		mux.HandleFunc("GET /api/v1"+path, s.readable(h))
	}

	for verb, h := range map[string]actionHandler{
		"sync": s.actionSync, "refresh": s.actionRefresh, "suspend": s.actionSuspend, "resume": s.actionResume,
		"pause": s.actionPause, "promote": s.actionPromote, "abort": s.actionAbort, "retry": s.actionRetry,
	} {
		mux.HandleFunc("POST /api/v1/actions/"+verb, s.acting(h))
	}

	return mux
}

// readable lets a read through when reads are public or the caller has an
// identity.
func (s *Server) readable(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.cfg.Auth.PublicReads {
			if _, ok := s.identity(w, r); !ok {
				return
			}
		}

		h(w, r)
	}
}

// actionHandler serves a verb for a caller already resolved and admitted.
type actionHandler func(w http.ResponseWriter, r *http.Request, id *Identity)

// acting resolves the caller once. A session cookie rides along on requests
// any page can make, so a session must also act from this site and send JSON,
// which a page elsewhere cannot do without passing a CORS preflight.
func (s *Server) acting(h actionHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := s.identity(w, r)
		if !ok {
			return
		}

		if id.Session && !SameOrigin(r) {
			writeError(w, http.StatusForbidden, "cross-site request refused")

			return
		}

		if id.Session && !sendsJSON(r) {
			writeError(w, http.StatusUnsupportedMediaType, headerContentType+" must be "+mediaJSON)

			return
		}

		h(w, r, &id)
	}
}

// sendsJSON reports whether the request declares a JSON body, with any
// parameters such as a charset.
func sendsJSON(r *http.Request) bool {
	media, _, err := mime.ParseMediaType(r.Header.Get(headerContentType))

	return err == nil && media == mediaJSON
}

// SameOrigin requires browser provenance to match this server's origin.
// Every supplied provenance header must agree and at least one must vouch.
func SameOrigin(r *http.Request) bool {
	vouched := false

	switch r.Header.Get(headerFetchSite) {
	case fetchSameOrigin, fetchUserInitiated:
		vouched = true
	case "":
	default:
		return false
	}

	for _, h := range []string{headerOrigin, headerReferer} {
		v := r.Header.Get(h)
		if v == "" {
			continue
		}

		if !ownOrigin(r, v) {
			return false
		}

		vouched = true
	}

	return vouched
}

// ownOrigin compares the browser's URL against the request's external origin.
// TLS-terminating proxies must preserve Host and set X-Forwarded-Proto.
func ownOrigin(r *http.Request, raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || !strings.EqualFold(u.Host, r.Host) {
		return false
	}

	switch strings.ToLower(u.Scheme) {
	case schemeHTTPS:
		return Secure(r)
	case "http":
		return !Secure(r)
	default:
		return false
	}
}

// Secure reports whether the request arrived over TLS, directly or through a
// proxy that says so.
func Secure(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get(headerForwardedProto), schemeHTTPS)
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "version": s.version, "environment": s.cfg.Environment,
	})
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	id, ok := s.identity(w, r)
	if !ok {
		return
	}

	writeJSON(w, http.StatusOK, id)
}

func (s *Server) fleet(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.c.Fleet())
}

func (s *Server) group(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("label") != s.cfg.Labels.Group {
		writeError(w, http.StatusNotFound, fmt.Sprintf("groups are by %q here", s.cfg.Labels.Group))

		return
	}

	g, ts, ok := s.c.Group(r.PathValue("value"))
	if !ok {
		writeError(w, http.StatusNotFound, "no such group")

		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"group": g, "targets": ts})
}

func (s *Server) node(w http.ResponseWriter, r *http.Request) {
	n, ok := s.c.Node(r.PathValue("node"))
	if !ok {
		writeError(w, http.StatusNotFound, "no such node")

		return
	}

	writeJSON(w, http.StatusOK, n)
}

func (s *Server) targetsList(w http.ResponseWriter, r *http.Request) {
	var sel targets.Selector

	if raw := r.URL.Query().Get("selector"); raw != "" {
		parsed, err := targets.ParseSelector(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())

			return
		}

		sel = parsed
	}

	out := s.c.Targets(sel)
	if out == nil {
		out = []reconcile.TargetView{}
	}

	writeJSON(w, http.StatusOK, out)
}

func (s *Server) rollouts(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.c.Rollouts())
}

func (s *Server) rollout(w http.ResponseWriter, r *http.Request) {
	v, ok := s.c.Rollout(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "no such rollout")

		return
	}

	writeJSON(w, http.StatusOK, v)
}

func (s *Server) history(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))

	var about []targets.Target

	if node := q.Get("node"); node != "" {
		about = s.targets().Node(node)
	}

	if raw := q.Get("selector"); raw != "" {
		sel, err := targets.ParseSelector(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())

			return
		}

		about = s.targets().Select(sel)
	}

	// Any after, zero included, asks for history oldest first from that id;
	// without it the newest events come first.
	after, _ := strconv.ParseInt(q.Get("after"), 10, 64)
	_, oldest := q["after"]
	query := reconcile.EventQuery{Group: q.Get("group"), Rollout: q.Get("rollout"), Target: q.Get("target"), Limit: limit, After: max(after, 0), Oldest: oldest}

	var (
		events []reconcile.Event
		err    error
	)

	if about != nil {
		events, err = s.c.EventsAbout(r.Context(), about, &query)
	} else {
		events, err = s.c.Events(r.Context(), &query)
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	if events == nil {
		events = []reconcile.Event{}
	}

	w.Header().Set("Rolloor-History", s.c.HistoryID())

	writeJSON(w, http.StatusOK, events)
}

func (s *Server) suspensions(w http.ResponseWriter, _ *http.Request) {
	out := s.c.Suspensions()
	if out == nil {
		out = []reconcile.Suspension{}
	}

	writeJSON(w, http.StatusOK, out)
}

// stream sends every event as server-sent events until the client leaves.
func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	if _, ok := w.(http.Flusher); !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")

		return
	}

	ch, stop := s.events.Subscribe()
	defer stop()

	// A client that reconnects says where it was; everything since is
	// replayed from the store before live events continue.
	var last int64

	if raw := r.Header.Get("Last-Event-ID"); raw != "" {
		last, _ = strconv.ParseInt(raw, 10, 64)
	}

	var replay []reconcile.Event

	if last > 0 {
		missed, err := s.c.Events(r.Context(), &reconcile.EventQuery{After: last})
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())

			return
		}

		replay = missed
	}

	// A client that stops reading gets a write deadline, not a parked handler.
	rc := http.NewResponseController(w)
	write := func(format string, args ...any) bool {
		_ = rc.SetWriteDeadline(time.Now().Add(sseWriteTimeout))

		if _, err := fmt.Fprintf(w, format, args...); err != nil {
			return false
		}

		return rc.Flush() == nil
	}

	w.Header().Set(headerContentType, "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	if !write(": connected\n\n") {
		return
	}

	send := func(e *reconcile.Event) bool {
		if e.ID <= last {
			return true
		}

		raw, err := json.Marshal(e)
		if err != nil {
			return true
		}

		last = e.ID

		return write("id: %d\nevent: %s\ndata: %s\n\n", e.ID, e.Action, raw)
	}

	for i := range replay {
		if !send(&replay[i]) {
			return
		}
	}

	heartbeat := time.NewTicker(sseHeartbeat)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			if !write(": ping\n\n") {
				return
			}
		case e, ok := <-ch:
			if !ok {
				// The client fell too far behind. It reconnects with the last id
				// it saw and gets the rest replayed rather than silently missing it.
				write("event: gap\ndata: {\"lastId\": %d}\n\n", last)

				return
			}

			if !send(&e) {
				return
			}
		}
	}
}

// identity resolves the caller or writes a 401.
func (s *Server) identity(w http.ResponseWriter, r *http.Request) (Identity, bool) {
	id, err := s.auth.Identity(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())

		return Identity{}, false
	}

	return id, true
}

// authorizeSelector checks the caller may act on everything the selector
// matches and confirmed a selection spanning several owners. With wholeGroups
// it covers every target in the touched groups, because a sync moves those.
func (s *Server) authorizeSelector(w http.ResponseWriter, id *Identity, sel targets.Selector, confirm, wholeGroups bool) bool {
	if _, err := Authorize(s.targets(), id, sel, confirm, wholeGroups); err != nil {
		writeActionRefusal(w, err, id.Owners)

		return false
	}

	return true
}

// writeActionRefusal answers with the status and body an ActionError carries.
func writeActionRefusal(w http.ResponseWriter, err error, held []string) {
	var ae *ActionError
	if !errors.As(err, &ae) {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	switch ae.Code {
	case "forbidden":
		writeForbidden(w, ae.Message, ae.Owners, held)
	case "confirm_required":
		writeJSON(w, ae.Status, map[string]any{errKey: ae.Message, "code": ae.Code, "owners": ae.Owners})
	default:
		writeError(w, ae.Status, ae.Message)
	}
}

// authorizeGroup checks the caller may act on a group.
func (s *Server) authorizeGroup(w http.ResponseWriter, id *Identity, group string) bool {
	members := s.targets().InGroup(group)
	if len(members) == 0 {
		writeError(w, http.StatusNotFound, "no such group")

		return false
	}

	owners := OwnersOf(s.targets(), members)
	if allowed, why := MayAct(id, owners); !allowed {
		writeForbidden(w, why, owners, id.Owners)

		return false
	}

	return true
}

// authorizeRollout checks the caller may act on a rollout's group.
func (s *Server) authorizeRollout(w http.ResponseWriter, id *Identity, rolloutID string) bool {
	v, ok := s.c.Rollout(rolloutID)
	if !ok {
		writeError(w, http.StatusNotFound, "no such rollout")

		return false
	}

	return s.authorizeGroup(w, id, v.Group)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set(headerContentType, mediaJSON)
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{errKey: msg})
}

func writeForbidden(w http.ResponseWriter, why string, required, held []string) {
	if held == nil {
		held = []string{}
	}

	writeJSON(w, http.StatusForbidden, map[string]any{errKey: why, "ownersRequired": required, "ownersHeld": held})
}

// writeActionError maps controller errors to statuses.
func writeActionError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, reconcile.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, reconcile.ErrState):
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusBadRequest, err.Error())
	}
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()

	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "bad request body: "+err.Error())

		return false
	}

	return true
}
