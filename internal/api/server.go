// Package api implements the EventFlow REST API.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	apispec "github.com/abhinav0107-colllab/eventflow/api"
	"github.com/abhinav0107-colllab/eventflow/internal/broker"
	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
	mw "github.com/abhinav0107-colllab/eventflow/internal/middleware"
	"github.com/abhinav0107-colllab/eventflow/internal/observability"
	"github.com/abhinav0107-colllab/eventflow/internal/worker"
)

// Config holds API behaviour settings.
type Config struct {
	MaxBodyBytes       int64
	MaxPayloadBytes    int
	DefaultMaxAttempts int
	DefaultTimeout     time.Duration
	WorkerActiveWindow time.Duration
}

// Server wires HTTP handlers to the domain services.
type Server struct {
	cfg      Config
	pool     *pgxpool.Pool
	store    *jobs.Store
	handlers *jobs.Registry
	broker   *broker.Broker
	workers  *worker.Registry
	auth     mw.Authenticator
	log      *slog.Logger
	metrics  *observability.Metrics
	draining atomic.Bool
	now      func() time.Time
}

// NewServer constructs a Server.
func NewServer(cfg Config, pool *pgxpool.Pool, store *jobs.Store, handlers *jobs.Registry, b *broker.Broker,
	workers *worker.Registry, a mw.Authenticator, log *slog.Logger, m *observability.Metrics) *Server {
	return &Server{cfg: cfg, pool: pool, store: store, handlers: handlers, broker: b, workers: workers,
		auth: a, log: log.With("component", "api"), metrics: m, now: time.Now}
}

// SetDraining makes /ready fail so load balancers stop routing new traffic
// before the HTTP server shuts down.
func (s *Server) SetDraining() { s.draining.Store(true) }

// Handler returns the fully wrapped HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	protected := mw.RequireAPIKey(s.auth, s.log)
	p := func(h http.HandlerFunc) http.Handler { return protected(h) }

	mux.Handle("POST /api/v1/jobs", p(s.createJob))
	mux.Handle("GET /api/v1/jobs", p(s.listJobs))
	mux.Handle("GET /api/v1/jobs/{id}", p(s.getJob))
	mux.Handle("POST /api/v1/jobs/{id}/retry", p(s.retryJob))
	mux.Handle("POST /api/v1/jobs/{id}/cancel", p(s.cancelJob))
	mux.Handle("GET /api/v1/jobs/{id}/attempts", p(s.listAttempts))
	mux.Handle("GET /api/v1/queues", p(s.listQueues))
	mux.Handle("GET /api/v1/workers", p(s.listWorkers))
	mux.Handle("GET /api/v1/job-types", p(s.listJobTypes))

	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /ready", s.ready)
	mux.Handle("GET /metrics", s.metrics.Handler())
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(apispec.OpenAPI)
	})

	h := jsonRoutingErrors(mux)
	h = mw.LimitBody(s.cfg.MaxBodyBytes)(h)
	h = mw.SecurityHeaders(h)
	h = mw.Observe(s.log, s.metrics)(h)
	return h
}

// jsonRoutingErrors converts the ServeMux's plain-text 404 and 405 replies
// into the standard JSON error envelope (the Allow header is preserved).
func jsonRoutingErrors(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, pattern := mux.Handler(r); pattern != "" {
			mux.ServeHTTP(w, r)
			return
		}
		mux.ServeHTTP(&routingErrorWriter{ResponseWriter: w, r: r}, r)
	})
}

type routingErrorWriter struct {
	http.ResponseWriter
	r         *http.Request
	rewritten bool
}

func (w *routingErrorWriter) WriteHeader(code int) {
	switch code {
	case http.StatusNotFound:
		w.rewritten = true
		mw.WriteError(w.ResponseWriter, w.r, code, "not_found", "resource not found", nil)
	case http.StatusMethodNotAllowed:
		w.rewritten = true
		mw.WriteError(w.ResponseWriter, w.r, code, "method_not_allowed", "method not allowed for this resource", nil)
	default:
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *routingErrorWriter) Write(b []byte) (int, error) {
	if w.rewritten {
		return len(b), nil
	}
	return w.ResponseWriter.Write(b)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	mw.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	checks := map[string]string{"database": "ok", "redis": "ok"}
	ok := true
	if err := s.pool.Ping(ctx); err != nil {
		checks["database"], ok = "unavailable", false
	}
	if err := s.broker.Ping(ctx); err != nil {
		checks["redis"], ok = "unavailable", false
	}
	status := "ready"
	code := http.StatusOK
	if s.draining.Load() {
		status, code = "draining", http.StatusServiceUnavailable
	} else if !ok {
		status, code = "not_ready", http.StatusServiceUnavailable
	}
	mw.WriteJSON(w, code, map[string]any{"status": status, "checks": checks})
}
