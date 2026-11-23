//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/abhinav0107-colllab/eventflow/internal/api"
	"github.com/abhinav0107-colllab/eventflow/internal/auth"
	"github.com/abhinav0107-colllab/eventflow/internal/broker"
	"github.com/abhinav0107-colllab/eventflow/internal/database"
	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
	"github.com/abhinav0107-colllab/eventflow/internal/jobs/handlers"
	"github.com/abhinav0107-colllab/eventflow/internal/observability"
	"github.com/abhinav0107-colllab/eventflow/internal/outbox"
	"github.com/abhinav0107-colllab/eventflow/internal/worker"
)

const (
	testAPIKey  = "integration-test-key-0001"
	otherAPIKey = "integration-test-key-0002"
	group       = "eventflow-workers"
)

var prefixSeq atomic.Int64

func testLogger() *slog.Logger {
	if os.Getenv("EVENTFLOW_TEST_LOG") != "" {
		return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// harness wires real components together for one test. Tests run
// sequentially; each starts from empty tables and an empty Redis.
type harness struct {
	t       *testing.T
	ctx     context.Context
	pool    *pgxpool.Pool
	rdb     *redis.Client
	broker  *broker.Broker
	store   *jobs.Store
	workers *worker.Registry
	metrics *observability.Metrics
	log     *slog.Logger
	prefix  string
	api     *httptest.Server
	apiSrv  *api.Server
	backoff jobs.Backoff
}

type harnessOpts struct {
	noRelay   bool
	apiRedis  string // override Redis address used by the API server
	relayPoll time.Duration
}

func newHarness(t *testing.T, opts harnessOpts) *harness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	h := &harness{t: t, ctx: ctx, log: testLogger(), metrics: observability.NewMetrics(),
		prefix:  fmt.Sprintf("it%d", prefixSeq.Add(1)),
		backoff: jobs.Backoff{Base: 100 * time.Millisecond, Max: time.Second}}

	pool, err := database.Connect(ctx, database.PoolConfig{URL: pgURL, MaxConns: 30})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	h.pool = pool
	if _, err := pool.Exec(ctx, `TRUNCATE jobs, job_attempts, outbox_events, workers, api_keys RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}
	h.rdb = broker.NewClient(broker.ClientConfig{Addr: redisAddr, PoolSize: 50})
	t.Cleanup(func() { _ = h.rdb.Close() })
	if err := h.rdb.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	h.broker = broker.New(h.rdb, h.prefix, group)
	if err := h.broker.EnsureGroups(ctx, []string{"high", "normal", "low"}); err != nil {
		t.Fatal(err)
	}
	h.store = jobs.NewStore(pool)
	h.workers = worker.NewRegistry(pool)

	authStore := auth.NewStore(pool, time.Minute)
	if err := authStore.Bootstrap(ctx, []string{testAPIKey, otherAPIKey}); err != nil {
		t.Fatal(err)
	}
	apiBroker := h.broker
	if opts.apiRedis != "" {
		c := broker.NewClient(broker.ClientConfig{Addr: opts.apiRedis})
		t.Cleanup(func() { _ = c.Close() })
		apiBroker = broker.New(c, h.prefix, group)
	}
	h.apiSrv = api.NewServer(api.Config{MaxBodyBytes: 1 << 20, MaxPayloadBytes: 64 << 10, DefaultMaxAttempts: 3,
		DefaultTimeout: 30 * time.Second, WorkerActiveWindow: 30 * time.Second},
		pool, h.store, handlers.NewRegistry(), apiBroker, h.workers, authStore, h.log, h.metrics)
	h.api = httptest.NewServer(h.apiSrv.Handler())
	t.Cleanup(h.api.Close)

	if !opts.noRelay {
		poll := opts.relayPoll
		if poll == 0 {
			poll = 25 * time.Millisecond
		}
		h.startRelay(h.broker, poll)
	}
	return h
}

func (h *harness) newRelay(b *broker.Broker, poll time.Duration) *outbox.Relay {
	return outbox.NewRelay(h.pool, b, h.log, h.metrics, outbox.RelayConfig{
		BatchSize: 100, PollInterval: poll, RetryBaseDelay: 50 * time.Millisecond, RetryMaxDelay: 200 * time.Millisecond,
		PublishTimeout: time.Second,
	})
}

func (h *harness) startRelay(b *broker.Broker, poll time.Duration) {
	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan struct{})
	go func() { defer close(done); h.newRelay(b, poll).Run(ctx) }()
	h.t.Cleanup(func() { cancel(); <-done })
}

func (h *harness) workerConfig(id string) worker.Config {
	return worker.Config{
		ID: id, Version: "test", Concurrency: 4,
		LeaseDuration: 2 * time.Second, HeartbeatInterval: 200 * time.Millisecond,
		BlockTimeout: 100 * time.Millisecond, ClaimInterval: 300 * time.Millisecond, ClaimMinIdle: time.Second,
		ShutdownTimeout: 5 * time.Second, Backoff: h.backoff,
	}
}

// runningWorker is an in-process worker.
type runningWorker struct {
	w      *worker.Worker
	cancel context.CancelFunc
	done   chan error
}

// Stop triggers graceful shutdown and waits for it.
func (r *runningWorker) Stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		if err != nil {
			t.Errorf("worker returned error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("worker did not stop")
	}
}

func (h *harness) startWorker(cfg worker.Config, b *broker.Broker, store *jobs.Store) *runningWorker {
	if b == nil {
		b = h.broker
	}
	if store == nil {
		store = h.store
	}
	w := worker.New(cfg, store, b, handlers.NewRegistry(), worker.NewRegistry(h.pool), h.log, h.metrics)
	ctx, cancel := context.WithCancel(h.ctx)
	rw := &runningWorker{w: w, cancel: cancel, done: make(chan error, 1)}
	go func() { rw.done <- w.Run(ctx) }()
	var once sync.Once
	h.t.Cleanup(func() {
		once.Do(func() {
			cancel()
			select {
			case <-rw.done:
			case <-time.After(30 * time.Second):
			}
		})
	})
	return rw
}

func (h *harness) recovery(stuckAfter time.Duration) *worker.Recovery {
	return worker.NewRecovery(worker.RecoveryConfig{Interval: 200 * time.Millisecond, StuckQueuedAfter: stuckAfter,
		WorkerDeadAfter: 5 * time.Second, Backoff: h.backoff}, h.store, h.broker, h.workers, h.log, h.metrics)
}

func (h *harness) startRecovery(stuckAfter time.Duration) {
	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan struct{})
	go func() { defer close(done); h.recovery(stuckAfter).Run(ctx) }()
	h.t.Cleanup(func() { cancel(); <-done })
}

// ---- HTTP helpers ----

type response struct {
	Status int
	Header http.Header
	Body   []byte
}

func (r response) decode(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.Body, v); err != nil {
		t.Fatalf("decode %s: %v", r.Body, err)
	}
}

func (h *harness) request(method, path, body, key string, headers ...string) response {
	h.t.Helper()
	var rd io.Reader
	if body != "" {
		rd = bytes.NewBufferString(body)
	}
	req, err := http.NewRequestWithContext(h.ctx, method, h.api.URL+path, rd)
	if err != nil {
		h.t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("X-API-Key", key)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return response{Status: res.StatusCode, Header: res.Header, Body: b}
}

func (h *harness) submit(body string, headers ...string) jobs.Job {
	h.t.Helper()
	r := h.request("POST", "/api/v1/jobs", body, testAPIKey, headers...)
	if r.Status != http.StatusCreated {
		h.t.Fatalf("submit: status %d: %s", r.Status, r.Body)
	}
	var j jobs.Job
	r.decode(h.t, &j)
	return j
}

func (h *harness) get(id uuid.UUID) jobs.Job {
	h.t.Helper()
	j, err := h.store.Get(h.ctx, id)
	if err != nil {
		h.t.Fatal(err)
	}
	return *j
}

func (h *harness) attempts(id uuid.UUID) []jobs.Attempt {
	h.t.Helper()
	a, err := h.store.ListAttempts(h.ctx, id)
	if err != nil {
		h.t.Fatal(err)
	}
	return a
}

// waitFor polls cond until it returns true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

func (h *harness) waitStatus(id uuid.UUID, timeout time.Duration, want ...jobs.Status) jobs.Job {
	h.t.Helper()
	var last jobs.Job
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		last = h.get(id)
		for _, s := range want {
			if last.Status == s {
				return last
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	h.t.Fatalf("job %s: timed out waiting for %v, last status %s (attempts %d, error %v)",
		id, want, last.Status, last.Attempts, deref(last.LastError))
	return last
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func (h *harness) queryInt(sql string, args ...any) int64 {
	h.t.Helper()
	var n int64
	if err := h.pool.QueryRow(h.ctx, sql, args...).Scan(&n); err != nil {
		h.t.Fatal(err)
	}
	return n
}

// dsnVia rewrites the PostgreSQL DSN to connect through addr.
func dsnVia(addr string) string {
	u, err := url.Parse(pgURL)
	if err != nil {
		panic(err)
	}
	u.Host = addr
	return u.String()
}

func pgHostPort() string {
	u, _ := url.Parse(pgURL)
	return u.Host
}
