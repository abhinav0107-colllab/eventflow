package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/abhinav0107-colllab/eventflow/internal/auth"
	"github.com/abhinav0107-colllab/eventflow/internal/jobs/handlers"
	mw "github.com/abhinav0107-colllab/eventflow/internal/middleware"
	"github.com/abhinav0107-colllab/eventflow/internal/observability"
)

// These tests cover request handling that is resolved before any storage
// access (authentication, decoding, validation, routing). Persistence paths
// are covered by the integration suite against real PostgreSQL and Redis.

type staticAuth struct{}

func (staticAuth) Authenticate(_ context.Context, raw string) (auth.Principal, error) {
	if raw == "test-key" {
		return auth.Principal{KeyID: 1, Name: "test"}, nil
	}
	return auth.Principal{}, auth.ErrInvalidKey
}

func newTestServer() http.Handler {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := NewServer(Config{MaxBodyBytes: 4096, MaxPayloadBytes: 1024, DefaultMaxAttempts: 3,
		DefaultTimeout: 30 * time.Second, WorkerActiveWindow: time.Minute},
		nil, nil, handlers.NewRegistry(), nil, nil, staticAuth{}, log, observability.NewMetrics())
	return s.Handler()
}

func do(h http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("X-API-Key", "test-key")
	for k, v := range headers {
		if v == "" {
			r.Header.Del(k)
		} else {
			r.Header.Set(k, v)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func errorOf(t *testing.T, w *httptest.ResponseRecorder) mw.ErrorDetail {
	t.Helper()
	var e mw.ErrorBody
	if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
		t.Fatalf("response is not an error envelope: %s", w.Body.String())
	}
	if e.Error.RequestID == "" || e.Error.RequestID != w.Header().Get("X-Request-ID") {
		t.Fatalf("error must carry the request id: %+v", e)
	}
	return e.Error
}

func TestCreateJobRejections(t *testing.T) {
	h := newTestServer()
	valid := `{"type":"EMAIL_SIMULATION","payload":{"to":"a@example.com","subject":"s"}}`
	cases := []struct {
		name    string
		body    string
		headers map[string]string
		status  int
		code    string
	}{
		{"no api key", valid, map[string]string{"X-API-Key": ""}, 401, "unauthorized"},
		{"bad api key", valid, map[string]string{"X-API-Key": "wrong"}, 401, "unauthorized"},
		{"empty body", "", nil, 400, "invalid_json"},
		{"malformed json", `{"type":`, nil, 400, "invalid_json"},
		{"unknown field", `{"type":"EMAIL_SIMULATION","payload":{},"colour":"red"}`, nil, 400, "invalid_json"},
		{"wrong field type", `{"type":"EMAIL_SIMULATION","payload":{},"priority":"high"}`, nil, 400, "invalid_json"},
		{"trailing data", valid + valid, nil, 400, "invalid_json"},
		{"body too large", `{"type":"x","payload":{"a":"` + strings.Repeat("x", 5000) + `"}}`, nil, 413, "payload_too_large"},
		{"payload too large", `{"type":"EMAIL_SIMULATION","payload":{"to":"a@example.com","subject":"s","body":"` +
			strings.Repeat("x", 2000) + `"}}`, nil, 422, "validation_failed"},
		{"unknown type", `{"type":"NOPE","payload":{}}`, nil, 422, "validation_failed"},
		{"bad priority", `{"type":"EMAIL_SIMULATION","payload":{"to":"a@example.com","subject":"s"},"priority":42}`, nil, 422, "validation_failed"},
		{"handler validation", `{"type":"EMAIL_SIMULATION","payload":{"to":"nope","subject":"s"}}`, nil, 422, "validation_failed"},
		{"idempotency mismatch", `{"type":"EMAIL_SIMULATION","payload":{"to":"a@example.com","subject":"s"},"idempotency_key":"a"}`,
			map[string]string{"Idempotency-Key": "b"}, 400, "invalid_request"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := do(h, "POST", "/api/v1/jobs", c.body, c.headers)
			if w.Code != c.status {
				t.Fatalf("status %d, want %d: %s", w.Code, c.status, w.Body.String())
			}
			if e := errorOf(t, w); e.Code != c.code {
				t.Fatalf("code %q, want %q", e.Code, c.code)
			}
		})
	}
}

func TestValidationErrorListsFields(t *testing.T) {
	w := do(newTestServer(), "POST", "/api/v1/jobs",
		`{"type":"EMAIL_SIMULATION","payload":{"to":"a@example.com","subject":"s"},"priority":0,"max_attempts":0}`, nil)
	var body struct {
		Error struct {
			Details []struct{ Field string } `json:"details"`
		} `json:"error"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if len(body.Error.Details) != 2 {
		t.Fatalf("expected two field errors: %s", w.Body.String())
	}
}

func TestQueryAndPathValidation(t *testing.T) {
	h := newTestServer()
	cases := []struct {
		method, path string
		status       int
		code         string
	}{
		{"GET", "/api/v1/jobs?status=bogus", 400, "invalid_query"},
		{"GET", "/api/v1/jobs?limit=0", 400, "invalid_query"},
		{"GET", "/api/v1/jobs?limit=1000", 400, "invalid_query"},
		{"GET", "/api/v1/jobs?queue=urgent", 400, "invalid_query"},
		{"GET", "/api/v1/jobs?created_after=yesterday", 400, "invalid_query"},
		{"GET", "/api/v1/jobs?cursor=!!!", 400, "invalid_query"},
		{"GET", "/api/v1/jobs?dead_lettered=maybe", 400, "invalid_query"},
		{"GET", "/api/v1/jobs/not-a-uuid", 400, "invalid_id"},
		{"POST", "/api/v1/jobs/not-a-uuid/cancel", 400, "invalid_id"},
		{"GET", "/api/v1/nothing-here", 404, "not_found"},
	}
	for _, c := range cases {
		w := do(h, c.method, c.path, "", nil)
		if w.Code != c.status {
			t.Errorf("%s %s: status %d, want %d", c.method, c.path, w.Code, c.status)
			continue
		}
		if e := errorOf(t, w); e.Code != c.code {
			t.Errorf("%s %s: code %q, want %q", c.method, c.path, e.Code, c.code)
		}
	}
}

func TestMethodNotAllowed(t *testing.T) {
	w := do(newTestServer(), "DELETE", "/api/v1/jobs", "", nil)
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") == "" {
		t.Fatalf("status %d allow=%q", w.Code, w.Header().Get("Allow"))
	}
	if e := errorOf(t, w); e.Code != "method_not_allowed" {
		t.Fatalf("code %q", e.Code)
	}
}

func TestPublicEndpoints(t *testing.T) {
	h := newTestServer()
	w := do(h, "GET", "/health", "", map[string]string{"X-API-Key": ""})
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"ok"`) {
		t.Fatalf("health: %d %s", w.Code, w.Body.String())
	}
	w = do(h, "GET", "/openapi.yaml", "", map[string]string{"X-API-Key": ""})
	if w.Code != 200 || !strings.HasPrefix(w.Body.String(), "openapi: 3.0") {
		t.Fatalf("openapi: %d", w.Code)
	}
	w = do(h, "GET", "/metrics", "", map[string]string{"X-API-Key": ""})
	if w.Code != 200 || !strings.Contains(w.Body.String(), "go_goroutines") {
		t.Fatalf("metrics: %d", w.Code)
	}
	w = do(h, "GET", "/api/v1/job-types", "", nil)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "DELAYED_TASK") {
		t.Fatalf("job types: %d %s", w.Code, w.Body.String())
	}
}
