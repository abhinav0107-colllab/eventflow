package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/abhinav0107-colllab/eventflow/internal/broker"
	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
	"github.com/abhinav0107-colllab/eventflow/internal/observability"
)

// RecoveryConfig tunes the recovery manager.
type RecoveryConfig struct {
	Interval         time.Duration
	StuckQueuedAfter time.Duration
	WorkerDeadAfter  time.Duration
	ConsumerIdle     time.Duration
	BatchSize        int
	Backoff          jobs.Backoff
}

// Recovery repairs state left behind by failures. It is safe to run in
// every worker process concurrently (row locks use SKIP LOCKED and every
// transition is conditional).
//
// It handles:
//   - Expired leases: a worker crashed or stalled mid-execution. The attempt
//     is closed as "lost" and the job retried with backoff or dead-lettered.
//   - Lost broker messages: jobs that are "queued" in PostgreSQL while their
//     stream is fully drained (e.g. Redis restarted without persistence) are
//     re-published through the outbox.
//   - Dead workers: registrations without a recent heartbeat.
//   - Stream housekeeping: trimming fully processed entries and removing
//     idle consumers with nothing pending.
type Recovery struct {
	cfg     RecoveryConfig
	store   *jobs.Store
	broker  *broker.Broker
	workers *Registry
	log     *slog.Logger
	metrics *observability.Metrics
}

// NewRecovery constructs a Recovery manager.
func NewRecovery(cfg RecoveryConfig, store *jobs.Store, b *broker.Broker, workers *Registry,
	log *slog.Logger, m *observability.Metrics) *Recovery {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.ConsumerIdle <= 0 {
		cfg.ConsumerIdle = time.Hour
	}
	return &Recovery{cfg: cfg, store: store, broker: b, workers: workers,
		log: log.With("component", "recovery"), metrics: m}
}

// Run executes RunOnce every Interval until ctx is cancelled.
func (r *Recovery) Run(ctx context.Context) {
	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.RunOnce(ctx)
		}
	}
}

// RunOnce performs one recovery pass. Errors are logged, not returned, so
// that one failing step does not block the others.
func (r *Recovery) RunOnce(ctx context.Context) {
	for {
		recovered, err := r.store.RecoverExpiredLeases(ctx, r.cfg.BatchSize, r.cfg.Backoff)
		if err != nil {
			r.metrics.WorkerFailures.WithLabelValues("recovery").Inc()
			r.log.Warn("lease recovery failed", "error", err.Error())
			break
		}
		for _, j := range recovered {
			r.metrics.RecoveredJobs.WithLabelValues("lease_expired").Inc()
			switch j.Status {
			case jobs.StatusPending:
				r.metrics.JobsRetried.WithLabelValues(j.Type).Inc()
				r.metrics.AttemptFailures.WithLabelValues(j.Type, string(jobs.ClassLeaseExpired)).Inc()
			case jobs.StatusFailed:
				r.metrics.JobsDeadLettered.WithLabelValues(j.Type).Inc()
				r.metrics.AttemptFailures.WithLabelValues(j.Type, string(jobs.ClassLeaseExpired)).Inc()
			}
			r.log.Warn("recovered job with expired lease", "job_id", j.JobID.String(), "job_type", j.Type,
				"dead_worker_id", j.WorkerID, "attempt", j.Attempt, "new_status", j.Status)
		}
		if len(recovered) < r.cfg.BatchSize {
			break
		}
	}

	for _, q := range jobs.WorkQueues {
		st, err := r.broker.Stats(ctx, string(q))
		if err != nil {
			r.log.Warn("queue stats failed", "queue", q, "error", err.Error())
			continue
		}
		if st.Depth() == 0 {
			n, err := r.store.RequeueStuck(ctx, q, r.cfg.StuckQueuedAfter, r.cfg.BatchSize)
			if err != nil {
				r.log.Warn("requeue stuck jobs failed", "queue", q, "error", err.Error())
			} else if n > 0 {
				r.metrics.RecoveredJobs.WithLabelValues("lost_message").Add(float64(n))
				r.log.Warn("re-published queued jobs whose broker messages were lost", "queue", q, "count", n)
			}
		}
		if _, err := r.broker.Trim(ctx, string(q)); err != nil {
			r.log.Warn("stream trim failed", "queue", q, "error", err.Error())
		}
		if _, err := r.broker.PruneConsumers(ctx, string(q), r.cfg.ConsumerIdle); err != nil {
			r.log.Warn("consumer pruning failed", "queue", q, "error", err.Error())
		}
	}

	if n, err := r.workers.MarkDead(ctx, r.cfg.WorkerDeadAfter); err != nil {
		r.log.Warn("mark dead workers failed", "error", err.Error())
	} else if n > 0 {
		r.log.Warn("marked workers dead (no heartbeat)", "count", n)
	}
}
