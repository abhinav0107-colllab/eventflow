// Command api runs the EventFlow REST API and the transactional outbox relay.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/abhinav0107-colllab/eventflow/internal/api"
	"github.com/abhinav0107-colllab/eventflow/internal/auth"
	"github.com/abhinav0107-colllab/eventflow/internal/broker"
	"github.com/abhinav0107-colllab/eventflow/internal/config"
	"github.com/abhinav0107-colllab/eventflow/internal/database"
	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
	"github.com/abhinav0107-colllab/eventflow/internal/jobs/handlers"
	"github.com/abhinav0107-colllab/eventflow/internal/observability"
	"github.com/abhinav0107-colllab/eventflow/internal/outbox"
	"github.com/abhinav0107-colllab/eventflow/internal/worker"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "eventflow-api:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadAPI()
	if err != nil {
		return err
	}
	log := observability.NewLogger("api", cfg.LogLevel)
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

	rdb := broker.NewClient(broker.ClientConfig{Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB})
	defer rdb.Close()
	if err := broker.WaitReady(ctx, rdb, 30, 2*time.Second); err != nil {
		return errors.New("redis unavailable: " + err.Error())
	}
	b := broker.New(rdb, cfg.StreamPrefix, cfg.ConsumerGroup)
	queues := []string{string(jobs.QueueHigh), string(jobs.QueueNormal), string(jobs.QueueLow)}
	if err := b.EnsureGroups(ctx, queues); err != nil {
		return err
	}

	authStore := auth.NewStore(pool, 30*time.Second)
	if err := authStore.Bootstrap(ctx, cfg.APIKeys); err != nil {
		return errors.New("bootstrap API keys: " + err.Error())
	}

	store := jobs.NewStore(pool)
	srv := api.NewServer(api.Config{
		MaxBodyBytes:       cfg.MaxBodyBytes,
		MaxPayloadBytes:    cfg.MaxPayloadBytes,
		DefaultMaxAttempts: cfg.DefaultMaxAttempts,
		DefaultTimeout:     cfg.DefaultTimeout,
		WorkerActiveWindow: cfg.WorkerDeadAfter,
	}, pool, store, handlers.NewRegistry(), b, worker.NewRegistry(pool), authStore, log, metrics)

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}

	bgCtx, cancelBG := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	if cfg.OutboxEnabled {
		relay := outbox.NewRelay(pool, b, log, metrics, outbox.RelayConfig{
			BatchSize: cfg.OutboxBatchSize, PollInterval: cfg.OutboxPollInterval, Retention: cfg.OutboxRetention,
			RetryBaseDelay: cfg.PublishRetry.BaseDelay, RetryMaxDelay: cfg.PublishRetry.MaxDelay,
		})
		wg.Add(1)
		go func() { defer wg.Done(); relay.Run(bgCtx) }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); srv.RunStatsCollector(bgCtx, cfg.StatsInterval) }()

	errCh := make(chan error, 1)
	go func() {
		log.Info("api listening", "addr", cfg.HTTPAddr, "version", version, "outbox_relay", cfg.OutboxEnabled)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		cancelBG()
		wg.Wait()
		return err
	}

	log.Info("shutting down")
	srv.SetDraining()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown", "error", err.Error())
	}
	cancelBG()
	wg.Wait()
	log.Info("shutdown complete")
	return nil
}
