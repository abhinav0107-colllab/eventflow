package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/abhinav0107-colllab/eventflow/internal/auth"
	"github.com/abhinav0107-colllab/eventflow/internal/observability"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type fakeAuth map[string]auth.Principal

func (f fakeAuth) Authenticate(_ context.Context, raw string) (auth.Principal, error) {
	if raw == "backend-down" {
		return auth.Principal{}, errors.New("db down")
	}
	p, ok := f[raw]
	if !ok {
		return auth.Principal{}, auth.ErrInvalidKey
	}
	return p, nil
}

func decodeErr(t *testing.T, body io.Reader) ErrorDetail {
	t.Helper()
	var e ErrorBody
	if err := json.NewDecoder(body).Decode(&e); err != nil {
		t.Fatal(err)
	}
	return e.Error
}

func TestRequireAPIKey(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, _ := PrincipalFrom(r.Context())
		_, _ = io.WriteString(w, p.Name)
	})
	h := RequireAPIKey(fakeAuth{"good-key": {KeyID: 1, Name: "svc"}}, discard)(ok)
	cases := []struct {
		name, header, value string
		status              int
	}{
		{"missing", "", "", 401},
		{"invalid", APIKeyHeader, "nope", 401},
		{"valid header", APIKeyHeader, "good-key", 200},
		{"valid bearer", "Authorization", "Bearer good-key", 200},
		{"wrong scheme", "Authorization", "Basic good-key", 401},
		{"backend error", APIKeyHeader, "backend-down", 503},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/x", nil)
			if c.header != "" {
				r.Header.Set(c.header, c.value)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != c.status {
				t.Fatalf("status %d, want %d", w.Code, c.status)
			}
			if c.status == 200 && w.Body.String() != "svc" {
				t.Fatalf("principal not propagated: %q", w.Body.String())
			}
			if c.status == 503 && strings.Contains(w.Body.String(), "db down") {
				t.Fatal("internal error leaked to client")
			}
		})
	}
}

func TestObserveRequestIDMetricsAndRecovery(t *testing.T) {
	m := observability.NewMetrics()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /items/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") == "boom" {
			panic("handler bug")
		}
		_, _ = io.WriteString(w, RequestID(r.Context()))
	})
	h := Observe(discard, m)(mux)

	// Client-supplied valid request IDs are echoed.
	r := httptest.NewRequest("GET", "/items/1", nil)
	r.Header.Set(RequestIDHeader, "client-req-1")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Header().Get(RequestIDHeader) != "client-req-1" || w.Body.String() != "client-req-1" {
		t.Fatalf("request id not propagated: %q / %q", w.Header().Get(RequestIDHeader), w.Body.String())
	}

	// Invalid IDs are replaced.
	r = httptest.NewRequest("GET", "/items/2", nil)
	r.Header.Set(RequestIDHeader, "bad id with spaces")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if got := w.Header().Get(RequestIDHeader); got == "" || strings.Contains(got, " ") {
		t.Fatalf("invalid request id not replaced: %q", got)
	}

	// Panics become 500 with the error envelope.
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/items/boom", nil))
	if w.Code != 500 {
		t.Fatalf("panic: status %d", w.Code)
	}
	if e := decodeErr(t, w.Body); e.Code != "internal_error" || e.RequestID == "" || strings.Contains(e.Message, "handler bug") {
		t.Fatalf("unexpected panic response: %+v", e)
	}

	// Metrics use the route pattern, never the raw path.
	if v := testutil.ToFloat64(m.HTTPRequests.WithLabelValues("GET", "/items/{id}", "200")); v != 2 {
		t.Fatalf("expected 2 requests on route pattern, got %v", v)
	}
	if v := testutil.ToFloat64(m.HTTPRequests.WithLabelValues("GET", "/items/{id}", "500")); v != 1 {
		t.Fatalf("expected 1 failed request, got %v", v)
	}
}

func TestLimitBody(t *testing.T) {
	h := LimitBody(10)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(204)
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/", strings.NewReader("0123456789ABC")))
	if w.Code != 413 {
		t.Fatalf("declared oversize body: %d", w.Code)
	}
	r := httptest.NewRequest("POST", "/", io.NopCloser(strings.NewReader("0123456789ABC")))
	r.ContentLength = -1 // chunked: limit enforced while reading
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 413 {
		t.Fatalf("streamed oversize body: %d", w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/", strings.NewReader("ok")))
	if w.Code != 204 {
		t.Fatalf("small body: %d", w.Code)
	}
}

func TestSecurityHeaders(t *testing.T) {
	w := httptest.NewRecorder()
	SecurityHeaders(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})).ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	for _, h := range []string{"X-Content-Type-Options", "X-Frame-Options", "Cache-Control", "Content-Security-Policy"} {
		if w.Header().Get(h) == "" {
			t.Errorf("missing %s", h)
		}
	}
}
