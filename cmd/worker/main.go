// Command worker runs the EventFlow job execution engine and recovery manager.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/abhinav0107-colllab/eventflow/internal/broker"
	"github.com/abhinav0107-colllab/eventflow/internal/config"
	"github.com/abhinav0107-colllab/eventflow/internal/database"
	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
	"github.com/abhinav0107-colllab/eventflow/internal/jobs/handlers"
	mw "github.com/abhinav0107-colllab/eventflow/internal/middleware"
	"github.com/abhinav0107-colllab/eventflow/internal/observability"
	"github.com/abhinav0107-colllab/eventflow/internal/outbox"
	"github.com/abhinav0107-colllab/eventflow/internal/worker"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		os.Stderr.WriteString("eventflow-worker: " + err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadWorker()
	if err != nil {
		return err
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = worker.NewID()
	}
	log := observability.NewLogger("worker", cfg.LogLevel)
	metrics := observability.NewMetrics()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := database.ConnectWithRetry(ctx, database.PoolConfig{
		URL: cfg.DatabaseURL, MaxConns: cfg.DBMaxConns, MinConns: cfg.DBMinConns,
		MaxConnLifetime: time.Hour, MaxConnIdleTime: 10 * time.Minute,
	}, 30, 2*time.Second)
	if err != nil {
		return err
	}
	defer pool.Close()

	rdb := broker.NewClient(broker.ClientConfig{Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB,
		PoolSize: cfg.Concurrency + 10})
	defer rdb.Close()
	if err := broker.WaitReady(ctx, rdb, 30, 2*time.Second); err != nil {
		return errors.New("redis unavailable: " + err.Error())
	}
	b := broker.New(rdb, cfg.StreamPrefix, cfg.ConsumerGroup)
	store := jobs.NewStore(pool)
	registry := worker.NewRegistry(pool)
	backoff := jobs.Backoff{Base: cfg.Retry.BaseDelay, Max: cfg.Retry.MaxDelay}

	w := worker.New(worker.Config{
		ID: cfg.WorkerID, Version: version, Concurrency: cfg.Concurrency,
		LeaseDuration: cfg.LeaseDuration, HeartbeatInterval: cfg.HeartbeatInterval,
		BlockTimeout: cfg.BlockTimeout, ClaimInterval: cfg.ClaimInterval, ClaimMinIdle: cfg.ClaimMinIdle,
		ShutdownTimeout: cfg.ShutdownTimeout, Backoff: backoff,
	}, store, b, handlers.NewRegistry(), registry, log, metrics)

	recovery := worker.NewRecovery(worker.RecoveryConfig{
		Interval: cfg.RecoveryInterval, StuckQueuedAfter: cfg.StuckQueuedAfter,
		WorkerDeadAfter: cfg.WorkerDeadAfter, Backoff: backoff,
	}, store, b, registry, log, metrics)

	// Metrics and probes.
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /health", func(rw http.ResponseWriter, _ *http.Request) {
		mw.WriteJSON(rw, http.StatusOK, map[string]string{"status": "ok", "worker_id": w.ID()})
	})
	mux.HandleFunc("GET /ready", func(rw http.ResponseWriter, r *http.Request) {
		c, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if pool.Ping(c) != nil || rdb.Ping(c).Err() != nil || ctx.Err() != nil {
			mw.WriteJSON(rw, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
			return
		}
		mw.WriteJSON(rw, http.StatusOK, map[string]any{"status": "ready", "inflight": w.Inflight()})
	})
	metricsSrv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "error", err.Error())
		}
	}()

	bgCtx, cancelBG := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); recovery.Run(bgCtx) }()
	if cfg.OutboxEnabled {
		relay := outbox.NewRelay(pool, b, log, metrics, outbox.RelayConfig{
			BatchSize: cfg.OutboxBatchSize, PollInterval: cfg.OutboxPoll, Retention: 72 * time.Hour,
		})
		wg.Add(1)
		go func() { defer wg.Done(); relay.Run(bgCtx) }()
	}

	log.Info("worker process starting", "worker_id", cfg.WorkerID, "version", version, "metrics_addr", cfg.MetricsAddr)
	runErr := w.Run(ctx) // returns after graceful drain once ctx is cancelled
	cancelBG()
	wg.Wait()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)
	return runErr
}
