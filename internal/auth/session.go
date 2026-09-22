package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	sessionCookie = "rolloor_session"
	stateCookie   = "rolloor_oauth_state"
)

var errBadSession = errors.New("session cookie is not valid")

// session is what the cookie carries.
type session struct {
	Name    string    `json:"n"`
	Expires time.Time `json:"e"`
}

// codec signs and verifies cookie payloads with HMAC-SHA256.
type codec struct {
	key []byte
}

// encode returns base64(payload).base64(mac).
func (c codec) encode(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}

	payload := base64.RawURLEncoding.EncodeToString(raw)

	return payload + "." + c.sign(payload), nil
}

// decode verifies the signature and unmarshals the payload.
func (c codec) decode(s string, v any) error {
	payload, mac, ok := strings.Cut(s, ".")
	if !ok || !hmac.Equal([]byte(mac), []byte(c.sign(payload))) {
		return errBadSession
	}

	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return errBadSession
	}

	if err := json.Unmarshal(raw, v); err != nil {
		return errBadSession
	}

	return nil
}

func (c codec) sign(payload string) string {
	h := hmac.New(sha256.New, c.key)
	h.Write([]byte(payload))

	return base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

// randRead is crypto/rand.Read, replaceable so its failure is testable.
var randRead = rand.Read

func randomToken() (string, error) {
	var b [16]byte
	if _, err := randRead(b[:]); err != nil {
		return "", fmt.Errorf("random: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// secure reports whether the request arrived over TLS, directly or through
// a proxy that says so.
func secure(r *http.Request) bool {
	return r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

func setCookie(w http.ResponseWriter, r *http.Request, name, value string, maxAge int) {
	//nolint:gosec // Secure follows the request scheme so local plain-http runs still work; production is TLS.
	http.SetCookie(w, &http.Cookie{
		Name: name, Value: value, Path: "/", MaxAge: maxAge,
		HttpOnly: true, Secure: secure(r), SameSite: http.SameSiteLaxMode,
	})
}
