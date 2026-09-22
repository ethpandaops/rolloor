// Package ui serves the web interface: server-rendered pages that poll their
// own fragments, and the handful of controls the API offers.
package ui

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ethpandaops/rolloor/internal/api"
	"github.com/ethpandaops/rolloor/internal/config"
	"github.com/ethpandaops/rolloor/internal/observability"
	"github.com/ethpandaops/rolloor/internal/reconcile"
	"github.com/ethpandaops/rolloor/internal/targets"
)

//go:embed templates/*.html
var embeddedTemplates embed.FS

// templateFS is what New parses; tests swap it for a broken set.
var templateFS fs.FS = embeddedTemplates

//go:embed static/*
var staticFS embed.FS

// Server renders the pages.
type Server struct {
	cfg      *config.Config
	c        *reconcile.Controller
	targets  func() *targets.Set
	auth     api.Authorizer
	loginURL func(next string) string
	version  string
	tmpl     *template.Template
	log      observability.ContextualLogger
	now      func() time.Time
}

// New parses the templates. loginURL may be nil, in which case a request
// without an identity gets a 401 instead of a redirect.
func New(cfg *config.Config, c *reconcile.Controller, ts func() *targets.Set, auth api.Authorizer, loginURL func(string) string, version string, log observability.ContextualLogger) (*Server, error) {
	s := &Server{cfg: cfg, c: c, targets: ts, auth: auth, loginURL: loginURL, version: version, log: log.WithField("component", "ui"), now: time.Now}

	tmpl, err := template.New("").Funcs(s.funcs()).ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("ui: parse templates: %w", err)
	}

	s.tmpl = tmpl

	return s, nil
}

// Handler returns the page routes, the fragments they poll, the static files
// and the form actions.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.Handle("GET /static/", http.FileServer(http.FS(staticFS)))
	mux.HandleFunc("GET /{$}", s.fleet)
	mux.HandleFunc("GET /groups/{label}/{value}", s.group)
	mux.HandleFunc("GET /rollouts", s.rollouts)
	mux.HandleFunc("GET /rollouts/{id}", s.rollout)
	mux.HandleFunc("GET /nodes/{node}", s.node)
	mux.HandleFunc("GET /history", s.history)
	mux.HandleFunc("POST /actions/{verb}", s.action)

	return mux
}

// page is what the layout renders.
type page struct {
	Title    string
	Env      string
	Version  string
	Path     string
	Viewer   api.Identity
	SignedIn bool
	LoginURL string
	Flash    string
	Error    string
	Body     template.HTML
}

// viewer resolves the identity, or sends the browser to log in. ok false
// means the response has been written.
func (s *Server) viewer(w http.ResponseWriter, r *http.Request) (api.Identity, bool) {
	id, err := s.auth.Identity(r)
	if err == nil {
		return id, true
	}

	if s.loginURL == nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)

		return api.Identity{}, false
	}

	http.Redirect(w, r, s.loginURL(r.URL.RequestURI()), http.StatusFound)

	return api.Identity{}, false
}

// render writes a fragment for htmx polls and a full page otherwise.
func (s *Server) render(w http.ResponseWriter, r *http.Request, id api.Identity, title, body string, data any) {
	var buf bytes.Buffer

	if err := s.tmpl.ExecuteTemplate(&buf, body, data); err != nil {
		s.log.WithContext(r.Context()).WithError(err).Error("render body")
		http.Error(w, "render failed", http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	if r.Header.Get("HX-Request") == "true" {
		_, _ = w.Write(buf.Bytes())

		return
	}

	p := page{
		Title: title, Env: s.cfg.Environment, Version: s.version, Path: r.URL.Path,
		Viewer: id, SignedIn: id.Name != "", Body: template.HTML(buf.String()), //nolint:gosec // rendered by our own templates
		Flash: r.URL.Query().Get("flash"), Error: r.URL.Query().Get("error"),
	}

	if s.loginURL != nil {
		p.LoginURL = s.loginURL(r.URL.RequestURI())
	}

	if err := s.tmpl.ExecuteTemplate(w, "layout", p); err != nil {
		s.log.WithContext(r.Context()).WithError(err).Error("render layout")
	}
}

// can reports whether the viewer may act on these owners, for disabled
// controls with a reason.
type can struct {
	OK  bool
	Why string
}

func (s *Server) canAct(id *api.Identity, owners []string) can {
	ok, why := api.MayAct(id, owners)

	return can{OK: ok, Why: why}
}

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

// section groups tiles under a heading from the section label.
type section struct {
	Name   string
	Groups []reconcile.GroupView
}

type fleetData struct {
	Fleet      reconcile.FleetView
	Sections   []section
	Hidden     []reconcile.GroupView
	ShowHidden bool
	GroupLabel string
}

func (s *Server) fleet(w http.ResponseWriter, r *http.Request) {
	id, ok := s.viewer(w, r)
	if !ok {
		return
	}

	f := s.c.Fleet()
	data := fleetData{Fleet: f, ShowHidden: r.URL.Query().Get("hidden") == "1", GroupLabel: s.cfg.Labels.Group}
	bySection := map[string][]reconcile.GroupView{}

	var order []string

	for i := range f.Groups {
		g := &f.Groups[i]
		if g.Hidden && !data.ShowHidden {
			data.Hidden = append(data.Hidden, *g)

			continue
		}

		if _, seen := bySection[g.Section]; !seen {
			order = append(order, g.Section)
		}

		bySection[g.Section] = append(bySection[g.Section], *g)
	}

	sort.Strings(order)

	for _, name := range order {
		data.Sections = append(data.Sections, section{Name: name, Groups: bySection[name]})
	}

	s.render(w, r, id, "Fleet", "fleet", data)
}

type groupData struct {
	Group      reconcile.GroupView
	Targets    []reconcile.TargetView
	GroupLabel string
	Can        can
	Presets    []string
}

func (s *Server) group(w http.ResponseWriter, r *http.Request) {
	id, ok := s.viewer(w, r)
	if !ok {
		return
	}

	if r.PathValue("label") != s.cfg.Labels.Group {
		http.NotFound(w, r)

		return
	}

	g, ts, found := s.c.Group(r.PathValue("value"))
	if !found {
		http.NotFound(w, r)

		return
	}

	data := groupData{Group: g, Targets: ts, GroupLabel: s.cfg.Labels.Group, Can: s.canAct(&id, s.ownersOf(s.targets().InGroup(g.Name))), Presets: s.cfg.PresetNames()}
	s.render(w, r, id, g.Name, "group", data)
}

type rolloutData struct {
	Rollout    reconcile.RolloutView
	Waves      []wave
	Quarantine []reconcile.RolloutTarget
	LastCheck  *reconcile.SoakCheck
	GroupLabel string
	Can        can
	Preset     config.Preset
}

// wave is a rollout's targets in one wave, batched and not yet.
type wave struct {
	Number  int
	Batches []batchView
	Pending []reconcile.RolloutTarget
	Done    bool
}

type batchView struct {
	reconcile.Batch

	Targets []reconcile.RolloutTarget
	Open    bool
}

func (s *Server) rollout(w http.ResponseWriter, r *http.Request) {
	id, ok := s.viewer(w, r)
	if !ok {
		return
	}

	v, found := s.c.Rollout(r.PathValue("id"))
	if !found {
		http.NotFound(w, r)

		return
	}

	data := rolloutData{Rollout: v, GroupLabel: s.cfg.Labels.Group, Can: s.canAct(&id, s.ownersOf(s.targets().InGroup(v.Group))), Preset: s.cfg.Presets[v.Speed]}
	byID := map[string]reconcile.RolloutTarget{}
	waves := map[int]*wave{}

	var order []int

	for _, rt := range v.Targets {
		byID[rt.ID] = rt

		if _, seen := waves[rt.Wave]; !seen {
			waves[rt.Wave] = &wave{Number: rt.Wave}
			order = append(order, rt.Wave)
		}

		if rt.Batch == 0 && rt.Phase == reconcile.PhasePending {
			waves[rt.Wave].Pending = append(waves[rt.Wave].Pending, rt)
		}

		if rt.Phase == reconcile.PhaseFailed {
			data.Quarantine = append(data.Quarantine, rt)
		}
	}

	sort.Ints(order)

	for _, b := range v.Batches {
		bv := batchView{Batch: b, Open: b.EndedAt.IsZero()}
		for _, tid := range b.Targets {
			bv.Targets = append(bv.Targets, byID[tid])
		}

		if wv, ok := waves[b.Wave]; ok {
			wv.Batches = append(wv.Batches, bv)
		}
	}

	for _, n := range order {
		wv := waves[n]
		wv.Done = len(wv.Pending) == 0 && !hasOpen(wv.Batches)
		data.Waves = append(data.Waves, *wv)
	}

	if n := len(v.Soak.Checks); n > 0 {
		data.LastCheck = &v.Soak.Checks[n-1]
	}

	s.render(w, r, id, "Rollout "+v.Digest, "rollout", data)
}

func hasOpen(bs []batchView) bool {
	for i := range bs {
		if bs[i].Open {
			return true
		}
	}

	return false
}

func (s *Server) rollouts(w http.ResponseWriter, r *http.Request) {
	id, ok := s.viewer(w, r)
	if !ok {
		return
	}

	s.render(w, r, id, "Rollouts", "rollouts", map[string]any{"Rollouts": s.c.Rollouts(), "GroupLabel": s.cfg.Labels.Group})
}

type nodeData struct {
	Node       reconcile.NodeView
	GroupLabel string
	Can        map[string]can
	NodeCan    can
}

func (s *Server) node(w http.ResponseWriter, r *http.Request) {
	id, ok := s.viewer(w, r)
	if !ok {
		return
	}

	n, found := s.c.Node(r.PathValue("node"))
	if !found {
		http.NotFound(w, r)

		return
	}

	data := nodeData{Node: n, GroupLabel: s.cfg.Labels.Group, Can: map[string]can{}}

	for i := range n.Targets {
		data.Can[n.Targets[i].ID] = s.canAct(&id, []string{n.Targets[i].Owner})
	}

	data.NodeCan = s.canAct(&id, s.ownersOf(s.targets().Node(n.Name)))
	s.render(w, r, id, n.Name, "node", data)
}

type historyData struct {
	Events     []reconcile.Event
	Group      string
	Node       string
	Rollout    string
	GroupLabel string
}

func (s *Server) history(w http.ResponseWriter, r *http.Request) {
	id, ok := s.viewer(w, r)
	if !ok {
		return
	}

	q := r.URL.Query()
	data := historyData{Group: q.Get("group"), Node: q.Get("node"), Rollout: q.Get("rollout"), GroupLabel: s.cfg.Labels.Group}

	events, err := s.c.Events(r.Context(), reconcile.EventQuery{Group: data.Group, Rollout: data.Rollout, Limit: 200})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	if data.Node != "" {
		onNode := map[string]struct{}{}
		for _, t := range s.targets().Node(data.Node) {
			onNode[t.ID] = struct{}{}
		}

		kept := events[:0]

		for i := range events {
			if _, ok := onNode[events[i].Target]; ok {
				kept = append(kept, events[i])
			}
		}

		events = kept
	}

	data.Events = events
	s.render(w, r, id, "History", "history", data)
}

// action handles the forms: it authorizes like the API does, calls the
// controller, and sends the browser back with a flash or an error.
func (s *Server) action(w http.ResponseWriter, r *http.Request) {
	id, ok := s.viewer(w, r)
	if !ok {
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	next := r.Form.Get("next")
	if !strings.HasPrefix(next, "/") || strings.HasPrefix(next, "//") {
		next = "/"
	}

	flash, err := s.perform(r, &id)

	target, _ := url.Parse(next)
	q := target.Query()
	q.Del("flash")
	q.Del("error")

	if err != nil {
		q.Set("error", err.Error())
	} else {
		q.Set("flash", flash)
	}

	target.RawQuery = q.Encode()
	//nolint:gosec // next is a same-origin path: it must start with one slash and is re-encoded above.
	http.Redirect(w, r, target.String(), http.StatusSeeOther)
}

var errForbidden = errors.New("not allowed")

// checked is what a browser sends for a ticked checkbox.
const checked = "on"

func (s *Server) perform(r *http.Request, id *api.Identity) (string, error) {
	ctx := r.Context()
	f := r.Form
	verb := r.PathValue("verb")

	switch verb {
	case "sync", "suspend", "resume":
		sel, err := targets.ParseSelector(f.Get("selector"))
		if err != nil {
			return "", err
		}

		matched := s.targets().Select(sel)
		if len(matched) == 0 {
			return "", fmt.Errorf("selector %s matches nothing", sel)
		}

		owners := s.ownersOf(matched)
		if ok, why := api.MayAct(id, owners); !ok {
			return "", fmt.Errorf("%w: %s", errForbidden, why)
		}

		if len(owners) > 1 && f.Get("confirm") != checked && verb != "resume" {
			return "", fmt.Errorf("%s spans %d owners (%s); tick confirm to proceed", sel, len(owners), strings.Join(owners, ", "))
		}

		switch verb {
		case "sync":
			ids, err := s.c.Sync(ctx, reconcile.SyncRequest{Actor: id.Name, Selector: sel, Force: f.Get("force") == checked, Speed: f.Get("speed")})
			if err != nil {
				return "", err
			}

			if len(ids) == 0 {
				return "Nothing to sync: " + sel.String() + " is already on the desired build.", nil
			}

			return fmt.Sprintf("Syncing %s (%d rollouts).", sel, len(ids)), nil
		case "suspend":
			var expires time.Duration

			if raw := f.Get("expiresIn"); raw != "" {
				d, err := time.ParseDuration(raw)
				if err != nil {
					return "", fmt.Errorf("expiry: %w", err)
				}

				expires = d
			}

			sp, err := s.c.Suspend(ctx, reconcile.SuspendRequest{Actor: id.Name, Selector: sel, Reason: f.Get("reason"), Expires: expires})
			if err != nil {
				return "", err
			}

			return fmt.Sprintf("Suspended %s until %s.", sel, sp.ExpiresAt.UTC().Format("Mon 15:04 UTC")), nil
		default:
			n, err := s.c.Resume(ctx, id.Name, sel)
			if err != nil {
				return "", err
			}

			return fmt.Sprintf("Resumed %s (%d lifted).", sel, n), nil
		}
	case "pause", "promote", "abort", "retry":
		rid := f.Get("rollout")

		v, ok := s.c.Rollout(rid)
		if !ok {
			return "", fmt.Errorf("rollout %s: %w", rid, reconcile.ErrNotFound)
		}

		if ok, why := api.MayAct(id, s.ownersOf(s.targets().InGroup(v.Group))); !ok {
			return "", fmt.Errorf("%w: %s", errForbidden, why)
		}

		var err error

		switch verb {
		case "pause":
			err = s.c.Pause(ctx, id.Name, rid)
		case "promote":
			err = s.c.Promote(ctx, id.Name, rid)
		case "abort":
			err = s.c.Abort(ctx, id.Name, rid)
		default:
			err = s.c.Retry(ctx, id.Name, rid, f.Get("reason"))
		}

		if err != nil {
			return "", err
		}

		return strings.ToUpper(verb[:1]) + verb[1:] + " sent.", nil
	case "policy":
		group := f.Get("group")
		if ok, why := api.MayAct(id, s.ownersOf(s.targets().InGroup(group))); !ok {
			return "", fmt.Errorf("%w: %s", errForbidden, why)
		}

		p := reconcile.Policy{Mode: f.Get("mode"), Speed: f.Get("speed")}
		if err := s.c.SetPolicy(ctx, id.Name, group, p); err != nil {
			return "", err
		}

		return fmt.Sprintf("Policy for %s: %s, %s.", group, p.Mode, p.Speed), nil
	}

	return "", fmt.Errorf("unknown action %q", verb)
}

// funcs are the template helpers.
func (s *Server) funcs() template.FuncMap {
	return template.FuncMap{
		"short": func(d string) string {
			d = strings.TrimPrefix(d, "sha256:")
			if len(d) > 7 {
				return d[:7]
			}

			return d
		},
		"ago": func(t time.Time) string {
			if t.IsZero() {
				return ""
			}

			return humanDuration(s.now().Sub(t)) + " ago"
		},
		"dur":   humanDuration,
		"clock": func(t time.Time) string { return t.UTC().Format("Mon 15:04:05") },
		"lower": strings.ToLower,
		"itoa":  strconv.Itoa,
		"join":  strings.Join,
		"pct": func(part, whole float64) string {
			if whole <= 0 {
				return "0%"
			}

			return fmt.Sprintf("%.1f%%", 100*part/whole)
		},
		"escape": url.QueryEscape,
		"first": func(m map[string]string) string {
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}

			sort.Strings(keys)

			if len(keys) == 0 {
				return ""
			}

			return m[keys[0]]
		},
		"rev": func(v reconcile.RolloutView) string {
			if v.Revision != "" {
				return v.Revision
			}

			return v.Digest
		},
	}
}

// humanDuration renders a duration the way a person would say it.
func humanDuration(d time.Duration) string {
	switch {
	case d < 0:
		return "0s"
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%d min", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%d h", int(d.Hours()))
	}

	return fmt.Sprintf("%d days", int(d.Hours()/24))
}
