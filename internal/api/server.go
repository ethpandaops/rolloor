// Package api serves the controller over HTTP: JSON reads, the verbs, and a
// live event stream.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/observability"
	"github.com/ethpandaops/rolloor/internal/reconcile"
	"github.com/ethpandaops/rolloor/internal/targets"
)

// errKey is the JSON field every error response carries.
const errKey = "error"

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

// Handler returns the API routes under /api/v1 plus /healthz.
func (s *Server) Handler() http.Handler {
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

	return s.auth.Middleware(mux)
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

	events, err := s.c.Events(r.Context(), reconcile.EventQuery{Group: q.Get("group"), Rollout: q.Get("rollout"), Target: q.Get("target"), Limit: limit})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())

		return
	}

	if node := q.Get("node"); node != "" {
		onNode := map[string]struct{}{}
		for _, t := range s.targets().Node(node) {
			onNode[t.ID] = struct{}{}
		}

		filtered := make([]reconcile.Event, 0, len(events))

		for i := range events {
			if _, ok := onNode[events[i].Target]; ok {
				filtered = append(filtered, events[i])
			}
		}

		events = filtered
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
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")

		return
	}

	ch, stop := s.events.Subscribe()
	defer stop()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case e := <-ch:
			raw, err := json.Marshal(e)
			if err != nil {
				continue
			}

			fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", e.ID, e.Action, raw)
			flusher.Flush()
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
func (s *Server) ownersOf(ts []targets.Target) []string {
	set := s.targets()
	seen := map[string]struct{}{}

	for i := range ts {
		seen[set.Owner(&ts[i])] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for o := range seen {
		out = append(out, o)
	}

	sort.Strings(out)

	return out
}

// authorizeSelector checks the caller may act on everything the selector
// matches, and that a selector spanning several owners was confirmed.
func (s *Server) authorizeSelector(w http.ResponseWriter, r *http.Request, sel targets.Selector, confirm bool) (Identity, []targets.Target, bool) {
	id, ok := s.identity(w, r)
	if !ok {
		return Identity{}, nil, false
	}

	matched := s.targets().Select(sel)
	if len(matched) == 0 {
		writeError(w, http.StatusNotFound, "selector "+sel.String()+" matches nothing")

		return Identity{}, nil, false
	}

	owners := s.ownersOf(matched)

	if allowed, why := MayAct(&id, owners); !allowed {
		writeForbidden(w, why, owners, id.Owners)

		return Identity{}, nil, false
	}

	if len(owners) > 1 && !confirm {
		writeJSON(w, http.StatusConflict, map[string]any{
			errKey: fmt.Sprintf("selector %s spans %d owners (%v); send confirm: true to proceed", sel, len(owners), owners),
			"code": "confirm_required", "owners": owners,
		})

		return Identity{}, nil, false
	}

	return id, matched, true
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

	owners := s.ownersOf(members)
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
