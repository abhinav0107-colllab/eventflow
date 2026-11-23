// Package middleware contains HTTP middleware: request IDs, structured
// access logs, panic recovery, metrics, body limits and authentication.
package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/abhinav0107-colllab/eventflow/internal/auth"
	"github.com/abhinav0107-colllab/eventflow/internal/observability"
)

type ctxKey int

const (
	requestIDKey ctxKey = iota
	principalKey
)

// RequestIDHeader is echoed on every response.
const RequestIDHeader = "X-Request-ID"

var validRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// RequestID returns the request ID stored in ctx.
func RequestID(ctx context.Context) string {
	s, _ := ctx.Value(requestIDKey).(string)
	return s
}

// PrincipalFrom returns the authenticated caller.
func PrincipalFrom(ctx context.Context) (auth.Principal, bool) {
	p, ok := ctx.Value(principalKey).(auth.Principal)
	return p, ok
}

// WithPrincipal stores p in ctx (exported for tests).
func WithPrincipal(ctx context.Context, p auth.Principal) context.Context {
	return context.WithValue(ctx, principalKey, p)
}

func newRequestID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// ErrorBody is the consistent error envelope returned by the API.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail describes an error.
type ErrorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
	Details   any    `json:"details,omitempty"`
}

// WriteJSON writes v as JSON with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError writes the standard error envelope. Messages must be safe for
// clients: never include internal error strings.
func WriteError(w http.ResponseWriter, r *http.Request, status int, code, msg string, details any) {
	WriteJSON(w, status, ErrorBody{Error: ErrorDetail{Code: code, Message: msg, RequestID: RequestID(r.Context()), Details: details}})
}

type recorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *recorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// Observe assigns a request ID, recovers panics, records metrics and emits
// one structured access log line per request. It must wrap the router so
// that r.Pattern (the matched route) is available after dispatch.
func Observe(log *slog.Logger, m *observability.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			id := r.Header.Get(RequestIDHeader)
			if !validRequestID.MatchString(id) {
				id = newRequestID()
			}
			w.Header().Set(RequestIDHeader, id)
			r = r.WithContext(context.WithValue(r.Context(), requestIDKey, id))
			rec := &recorder{ResponseWriter: w}

			defer func() {
				if p := recover(); p != nil {
					if p == http.ErrAbortHandler {
						panic(p)
					}
					log.Error("panic serving request", "request_id", id, "panic", slog.AnyValue(p).String())
					if rec.status == 0 {
						WriteError(rec, r, http.StatusInternalServerError, "internal_error", "internal server error", nil)
					}
				}
				route := r.Pattern
				if route == "" {
					route = "unmatched"
				} else if _, after, ok := strings.Cut(route, " "); ok {
					route = after // pattern is "METHOD /path"; method is a separate label
				}
				status := rec.status
				if status == 0 {
					status = http.StatusOK
				}
				dur := time.Since(start)
				m.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(status)).Inc()
				m.HTTPDuration.WithLabelValues(r.Method, route).Observe(dur.Seconds())
				level := slog.LevelInfo
				if status >= 500 {
					level = slog.LevelError
				} else if route == "/metrics" || route == "/health" || route == "/ready" {
					level = slog.LevelDebug
				}
				log.Log(r.Context(), level, "http request",
					"request_id", id, "method", r.Method, "route", route, "path", r.URL.Path,
					"status", status, "bytes", rec.bytes, "duration_ms", float64(dur.Microseconds())/1000,
					"remote_addr", r.RemoteAddr)
			}()
			next.ServeHTTP(rec, r)
		})
	}
}

// LimitBody caps request bodies at n bytes.
func LimitBody(n int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ContentLength > n {
				WriteError(w, r, http.StatusRequestEntityTooLarge, "payload_too_large",
					"request body exceeds "+strconv.FormatInt(n, 10)+" bytes", nil)
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, n)
			next.ServeHTTP(w, r)
		})
	}
}

// Authenticator resolves API keys.
type Authenticator interface {
	Authenticate(ctx context.Context, raw string) (auth.Principal, error)
}

// APIKeyHeader is the authentication header. "Authorization: Bearer <key>"
// is accepted as well.
const APIKeyHeader = "X-API-Key"

// RequireAPIKey rejects requests without a valid API key.
func RequireAPIKey(a Authenticator, log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get(APIKeyHeader)
			if key == "" {
				if v, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
					key = strings.TrimSpace(v)
				}
			}
			if key == "" {
				w.Header().Set("WWW-Authenticate", `Bearer realm="eventflow"`)
				WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing API key", nil)
				return
			}
			p, err := a.Authenticate(r.Context(), key)
			if errors.Is(err, auth.ErrInvalidKey) {
				w.Header().Set("WWW-Authenticate", `Bearer realm="eventflow"`)
				WriteError(w, r, http.StatusUnauthorized, "unauthorized", "invalid API key", nil)
				return
			}
			if err != nil {
				log.Error("authentication backend error", "request_id", RequestID(r.Context()), "error", err.Error())
				WriteError(w, r, http.StatusServiceUnavailable, "unavailable", "authentication temporarily unavailable", nil)
				return
			}
			next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), p)))
		})
	}
}

// SecurityHeaders sets conservative response headers for a JSON API.
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Cache-Control", "no-store")
		h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		h.Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}
