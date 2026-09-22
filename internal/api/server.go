// Package api serves the controller over HTTP: JSON reads, the verbs, and a
// live event stream.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/observability"
	"github.com/ethpandaops/rolloor/internal/reconcile"
	"github.com/ethpandaops/rolloor/internal/targets"
)

// errKey is the JSON field every error response carries.
const errKey = "error"

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
	mux.HandleFunc("GET /api/v1/fleet", s.fleet)
	mux.HandleFunc("GET /api/v1/groups/{label}/{value}", s.group)
	mux.HandleFunc("GET /api/v1/nodes/{node}", s.node)
	mux.HandleFunc("GET /api/v1/targets", s.targetsList)
	mux.HandleFunc("GET /api/v1/rollouts", s.rollouts)
	mux.HandleFunc("GET /api/v1/rollouts/{id}", s.rollout)
	mux.HandleFunc("GET /api/v1/history", s.history)
	mux.HandleFunc("GET /api/v1/policies/{group}", s.policyGet)
	mux.HandleFunc("PUT /api/v1/policies/{group}", s.policyPut)
	mux.HandleFunc("GET /api/v1/suspensions", s.suspensions)
	mux.HandleFunc("GET /api/v1/events", s.stream)

	for verb, h := range map[string]http.HandlerFunc{
		"sync": s.actionSync, "refresh": s.actionRefresh, "suspend": s.actionSuspend, "resume": s.actionResume,
		"pause": s.actionPause, "promote": s.actionPromote, "abort": s.actionAbort, "retry": s.actionRetry,
	} {
		mux.HandleFunc("POST /api/v1/actions/"+verb, h)
	}

	return mux
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	ok, reason, at := s.c.EnvironmentStatus()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true, "version": s.version, "environment": s.cfg.Environment,
		"environmentCheck": map[string]any{"ok": ok, "reason": reason, "at": at},
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

	query := reconcile.EventQuery{Group: q.Get("group"), Rollout: q.Get("rollout"), Target: q.Get("target"), Limit: limit}

	var (
		events []reconcile.Event
		err    error
	)

	if about != nil {
		events, err = s.c.EventsAbout(r.Context(), about, query)
	} else {
		events, err = s.c.Events(r.Context(), query)
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	if events == nil {
		events = []reconcile.Event{}
	}

	writeJSON(w, http.StatusOK, events)
}

func (s *Server) policyGet(w http.ResponseWriter, r *http.Request) {
	g, _, ok := s.c.Group(r.PathValue("group"))
	if !ok {
		writeError(w, http.StatusNotFound, "no such group")

		return
	}

	writeJSON(w, http.StatusOK, g.Policy)
}

func (s *Server) policyPut(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")

	id, ok := s.authorizeGroup(w, r, group)
	if !ok {
		return
	}

	var p reconcile.Policy
	if !readJSON(w, r, &p) {
		return
	}

	if err := s.c.SetPolicy(r.Context(), id.Name, group, p); err != nil {
		writeActionError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, p)
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
		missed, err := s.c.Events(r.Context(), reconcile.EventQuery{After: last})
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

	w.Header().Set("Content-Type", "text/event-stream")
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

// ownersOf lists the distinct owner values of some targets, sorted.
// authorizeSelector checks the caller may act on everything the selector
// matches, and that a selector spanning several owners was confirmed. With
// wholeGroups, the check covers every target in the groups the selector
// touches, because that is what a sync acts on.
func (s *Server) authorizeSelector(w http.ResponseWriter, r *http.Request, sel targets.Selector, confirm, wholeGroups bool) (Identity, []targets.Target, bool) {
	id, ok := s.identity(w, r)
	if !ok {
		return Identity{}, nil, false
	}

	matched, err := Authorize(s.targets(), &id, sel, confirm, wholeGroups)
	if err != nil {
		writeActionRefusal(w, err, id.Owners)

		return Identity{}, nil, false
	}

	return id, matched, true
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
func (s *Server) authorizeGroup(w http.ResponseWriter, r *http.Request, group string) (Identity, bool) {
	id, ok := s.identity(w, r)
	if !ok {
		return Identity{}, false
	}

	members := s.targets().InGroup(group)
	if len(members) == 0 {
		writeError(w, http.StatusNotFound, "no such group")

		return Identity{}, false
	}

	owners := OwnersOf(s.targets(), members)
	if allowed, why := MayAct(&id, owners); !allowed {
		writeForbidden(w, why, owners, id.Owners)

		return Identity{}, false
	}

	return id, true
}

// authorizeRollout checks the caller may act on a rollout's group.
func (s *Server) authorizeRollout(w http.ResponseWriter, r *http.Request, rolloutID string) (Identity, bool) {
	v, ok := s.c.Rollout(rolloutID)
	if !ok {
		writeError(w, http.StatusNotFound, "no such rollout")

		return Identity{}, false
	}

	return s.authorizeGroup(w, r, v.Group)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
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
