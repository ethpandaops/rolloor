package api

import (
	"net/http"
	"time"

	"github.com/ethpandaops/rolloor/internal/reconcile"
	"github.com/ethpandaops/rolloor/internal/targets"
)

type selectorBody struct {
	Selector string `json:"selector"`
	Confirm  bool   `json:"confirm"`
	// Sync only.
	Force bool   `json:"force"`
	Speed string `json:"speed"`
	// Suspend only.
	Reason    string `json:"reason"`
	ExpiresIn string `json:"expiresIn"`
}

type rolloutBody struct {
	Rollout string `json:"rollout"`
	Reason  string `json:"reason"`
}

func parseSelectorBody(w http.ResponseWriter, r *http.Request) (selectorBody, targets.Selector, bool) {
	var body selectorBody
	if !readJSON(w, r, &body) {
		return body, nil, false
	}

	sel, err := targets.ParseSelector(body.Selector)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())

		return body, nil, false
	}

	return body, sel, true
}

func (s *Server) actionSync(w http.ResponseWriter, r *http.Request) {
	body, sel, ok := parseSelectorBody(w, r)
	if !ok {
		return
	}

	id, _, ok := s.authorizeSelector(w, r, sel, body.Confirm, true)
	if !ok {
		return
	}

	started, err := s.c.Sync(r.Context(), reconcile.SyncRequest{Actor: id.Name, Selector: sel, Force: body.Force, Speed: body.Speed})
	if err != nil {
		writeActionError(w, err)

		return
	}

	if started == nil {
		started = []string{}
	}

	writeJSON(w, http.StatusOK, map[string]any{"rollouts": started})
}

func (s *Server) actionRefresh(w http.ResponseWriter, r *http.Request) {
	id, ok := s.identity(w, r)
	if !ok {
		return
	}

	s.c.Refresh(r.Context(), id.Name)
	writeJSON(w, http.StatusOK, map[string]string{"status": "refreshing"})
}

func (s *Server) actionSuspend(w http.ResponseWriter, r *http.Request) {
	body, sel, ok := parseSelectorBody(w, r)
	if !ok {
		return
	}

	var expires time.Duration

	if body.ExpiresIn != "" {
		d, err := time.ParseDuration(body.ExpiresIn)
		if err != nil {
			writeError(w, http.StatusBadRequest, "expiresIn: "+err.Error())

			return
		}

		expires = d
	}

	id, _, ok := s.authorizeSelector(w, r, sel, body.Confirm, false)
	if !ok {
		return
	}

	sp, err := s.c.Suspend(r.Context(), reconcile.SuspendRequest{Actor: id.Name, Selector: sel, Reason: body.Reason, Expires: expires})
	if err != nil {
		writeActionError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, sp)
}

func (s *Server) actionResume(w http.ResponseWriter, r *http.Request) {
	body, sel, ok := parseSelectorBody(w, r)
	if !ok {
		return
	}

	id, _, ok := s.authorizeSelector(w, r, sel, body.Confirm, false)
	if !ok {
		return
	}

	n, err := s.c.Resume(r.Context(), id.Name, sel)
	if err != nil {
		writeActionError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, map[string]int{"lifted": n})
}

func (s *Server) rolloutAction(w http.ResponseWriter, r *http.Request, act func(actor, rollout, reason string) error) {
	var body rolloutBody
	if !readJSON(w, r, &body) {
		return
	}

	id, ok := s.authorizeRollout(w, r, body.Rollout)
	if !ok {
		return
	}

	if err := act(id.Name, body.Rollout, body.Reason); err != nil {
		writeActionError(w, err)

		return
	}

	v, _ := s.c.Rollout(body.Rollout)
	writeJSON(w, http.StatusOK, v)
}

func (s *Server) actionPause(w http.ResponseWriter, r *http.Request) {
	s.rolloutAction(w, r, func(actor, rollout, _ string) error { return s.c.Pause(r.Context(), actor, rollout) })
}

func (s *Server) actionPromote(w http.ResponseWriter, r *http.Request) {
	s.rolloutAction(w, r, func(actor, rollout, _ string) error { return s.c.Promote(r.Context(), actor, rollout) })
}

func (s *Server) actionAbort(w http.ResponseWriter, r *http.Request) {
	s.rolloutAction(w, r, func(actor, rollout, _ string) error { return s.c.Abort(r.Context(), actor, rollout) })
}

func (s *Server) actionRetry(w http.ResponseWriter, r *http.Request) {
	s.rolloutAction(w, r, func(actor, rollout, reason string) error { return s.c.Retry(r.Context(), actor, rollout, reason) })
}
