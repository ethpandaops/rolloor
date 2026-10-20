package api

import (
	"fmt"
	"net/http"
	"time"

	"github.com/ethpandaops/rolloor/internal/reconcile"
	"github.com/ethpandaops/rolloor/internal/targets"
)

type selectorBody struct {
	Selector string `json:"selector"`
	Confirm  bool   `json:"confirm"`
	// Sync only.
	Force    bool   `json:"force"`
	Strategy string `json:"strategy"`
	// Suspend only. ExpiresIn is a duration; ExpiresAt an RFC 3339 time.
	Reason    string `json:"reason"`
	ExpiresIn string `json:"expiresIn"`
	ExpiresAt string `json:"expiresAt"`
}

type rolloutBody struct {
	Rollout string `json:"rollout"`
	Reason  string `json:"reason"`
	// Pause only. A duration; empty or nonpositive takes the controller's default.
	ExpiresIn string `json:"expiresIn"`
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

func (s *Server) actionSync(w http.ResponseWriter, r *http.Request, id *Identity) {
	body, sel, ok := parseSelectorBody(w, r)
	if !ok || !s.authorizeSelector(w, id, sel, body.Confirm, true) {
		return
	}

	started, err := s.c.Sync(r.Context(), reconcile.SyncRequest{Actor: id.Name, Selector: sel, Force: body.Force, Strategy: body.Strategy})
	if err != nil {
		writeActionError(w, err)

		return
	}

	if started == nil {
		started = []string{}
	}

	writeJSON(w, http.StatusOK, map[string]any{"rollouts": started})
}

func (s *Server) actionRefresh(w http.ResponseWriter, r *http.Request, id *Identity) {
	s.c.Refresh(r.Context(), id.Name)
	writeJSON(w, http.StatusOK, map[string]string{"status": "refreshing"})
}

func (s *Server) actionSuspend(w http.ResponseWriter, r *http.Request, id *Identity) {
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

	if body.ExpiresAt != "" {
		at, err := time.Parse(time.RFC3339, body.ExpiresAt)
		if err != nil {
			writeError(w, http.StatusBadRequest, "expiresAt: "+err.Error())

			return
		}

		expires = time.Until(at)
		if expires <= 0 {
			writeError(w, http.StatusBadRequest, "expiresAt: already past")

			return
		}
	}

	if !s.authorizeSelector(w, id, sel, body.Confirm, false) {
		return
	}

	sp, err := s.c.Suspend(r.Context(), reconcile.SuspendRequest{Actor: id.Name, Selector: sel, Reason: body.Reason, Expires: expires})
	if err != nil {
		writeActionError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, sp)
}

func (s *Server) actionResume(w http.ResponseWriter, r *http.Request, id *Identity) {
	body, sel, ok := parseSelectorBody(w, r)
	if !ok || !s.authorizeSelector(w, id, sel, body.Confirm, false) {
		return
	}

	n, err := s.c.Resume(r.Context(), id.Name, sel)
	if err != nil {
		writeActionError(w, err)

		return
	}

	writeJSON(w, http.StatusOK, map[string]int{"lifted": n})
}

func (s *Server) rolloutAction(w http.ResponseWriter, r *http.Request, id *Identity, act func(actor string, body *rolloutBody) error) {
	var body rolloutBody
	if !readJSON(w, r, &body) || !s.authorizeRollout(w, id, body.Rollout) {
		return
	}

	if err := act(id.Name, &body); err != nil {
		writeActionError(w, err)

		return
	}

	v, _ := s.c.Rollout(body.Rollout)
	writeJSON(w, http.StatusOK, v)
}

func (s *Server) actionPause(w http.ResponseWriter, r *http.Request, id *Identity) {
	s.rolloutAction(w, r, id, func(actor string, body *rolloutBody) error {
		var expires time.Duration

		if body.ExpiresIn != "" {
			d, err := time.ParseDuration(body.ExpiresIn)
			if err != nil {
				return fmt.Errorf("expiresIn: %w", err)
			}

			expires = d
		}

		return s.c.Pause(r.Context(), reconcile.PauseRequest{Actor: actor, Rollout: body.Rollout, Expires: expires})
	})
}

func (s *Server) actionPromote(w http.ResponseWriter, r *http.Request, id *Identity) {
	s.rolloutAction(w, r, id, func(actor string, body *rolloutBody) error { return s.c.Promote(r.Context(), actor, body.Rollout) })
}

func (s *Server) actionAbort(w http.ResponseWriter, r *http.Request, id *Identity) {
	s.rolloutAction(w, r, id, func(actor string, body *rolloutBody) error { return s.c.Abort(r.Context(), actor, body.Rollout) })
}

func (s *Server) actionRetry(w http.ResponseWriter, r *http.Request, id *Identity) {
	s.rolloutAction(w, r, id, func(actor string, body *rolloutBody) error {
		return s.c.Retry(r.Context(), actor, body.Rollout, body.Reason)
	})
}
