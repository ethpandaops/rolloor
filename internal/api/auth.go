package api

import (
	"net"
	"net/http"
	"slices"
	"strings"
)

// Identity is who is making a request and what they may act on.
type Identity struct {
	Name string `json:"name"`
	// Owners are the owner-label values this identity may act on.
	Owners []string `json:"owners"`
	// Admin identities may act on anything.
	Admin bool `json:"admin"`
}

// Authorizer resolves a request to an identity and decides what it may do.
// The OIDC implementation arrives with the UI; OpenAccess serves until then
// and whenever auth is turned off.
type Authorizer interface {
	// Identity returns who is calling, or an error that means 401.
	Identity(r *http.Request) (Identity, error)
	// Middleware may wrap the whole handler, for login routes and sessions.
	Middleware(next http.Handler) http.Handler
}

// MayAct reports whether an identity may act on every one of the owners, and
// why not when it may not.
func MayAct(id *Identity, owners []string) (allowed bool, why string) {
	if id.Admin {
		return true, ""
	}

	var missing []string

	for _, o := range owners {
		if !slices.Contains(id.Owners, o) {
			missing = append(missing, o)
		}
	}

	if len(missing) == 0 {
		return true, ""
	}

	return false, "you're not listed under " + strings.Join(missing, ", ")
}

// OpenAccess is the authorizer for auth.mode none: everyone is an admin and
// history records where the request came from.
type OpenAccess struct{}

// Identity names the caller by remote address.
func (OpenAccess) Identity(r *http.Request) (Identity, error) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	if host == "" {
		host = "anonymous"
	}

	return Identity{Name: host, Admin: true}, nil
}

// Middleware passes through.
func (OpenAccess) Middleware(next http.Handler) http.Handler { return next }
