// Package worker implements the concurrent job execution engine and the
// recovery manager.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/abhinav0107-colllab/eventflow/internal/broker"
	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
	"github.com/abhinav0107-colllab/eventflow/internal/observability"
)

// Config tunes a Worker.
type Config struct {
	ID                string
	Version           string
	Concurrency       int
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	BlockTimeout      time.Duration
	ClaimInterval     time.Duration
	ClaimMinIdle      time.Duration
	ShutdownTimeout   time.Duration
	Backoff           jobs.Backoff
}

// NewID returns a unique worker ID of the form hostname-pid-random.
func NewID() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "worker"
	}
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), uuid.NewString()[:8])
}

type execution struct {
	lease  jobs.Lease
	cancel context.CancelCauseFunc
}

// Worker consumes ready messages from Redis and executes jobs.
//
// Processing protocol for each message:
//  1. Claim: conditional queued->running UPDATE in PostgreSQL that bumps the
//     attempt counter (fencing token) and takes a time-bounded lease. If the
//     job is not queued the message is a duplicate/stale and is acked.
//  2. Execute the handler with a context bounded by the job timeout and
//     cancellable by heartbeat feedback (client cancel, lost lease) or
//     shutdown.
//  3. Persist the outcome with a write fenced on (worker_id, attempt).
//  4. Only then XACK. A crash before step 4 leaves the message pending; it is
//     re-claimed later and dropped at step 1 because the job is no longer
//     queued, while the lease-expiry recovery handles crashes during step 2.
type Worker struct {
	cfg      Config
	store    *jobs.Store
	broker   *broker.Broker
	handlers *jobs.Registry
	workers  *Registry
	log      *slog.Logger
	metrics  *observability.Metrics

	mu       sync.Mutex
	inflight map[jobs.Lease]*execution
	lastHB   time.Time
	slots    chan struct{}
	wg       sync.WaitGroup
}

// New constructs a Worker.
func New(cfg Config, store *jobs.Store, b *broker.Broker, handlers *jobs.Registry, workers *Registry,
	log *slog.Logger, m *observability.Metrics) *Worker {
	if cfg.ID == "" {
		cfg.ID = NewID()
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	return &Worker{
		cfg: cfg, store: store, broker: b, handlers: handlers, workers: workers,
		log:      log.With("component", "worker", "worker_id", cfg.ID),
		metrics:  m,
		inflight: map[jobs.Lease]*execution{},
		slots:    make(chan struct{}, cfg.Concurrency),
	}
}

// ID returns the worker ID (also its consumer name).
func (w *Worker) ID() string { return w.cfg.ID }

// Inflight returns the number of executing jobs.
func (w *Worker) Inflight() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.inflight)
}

func queueNames() []string {
	out := make([]string, len(jobs.WorkQueues))
	for i, q := range jobs.WorkQueues {
		out[i] = string(q)
	}
	return out
}

// Run registers the worker and processes messages until ctx is cancelled,
// then shuts down gracefully: it stops fetching, waits up to
// ShutdownTimeout for in-flight jobs, interrupts the rest (they are
// rescheduled), and deregisters.
func (w *Worker) Run(ctx context.Context) error {
	host, _ := os.Hostname()
	bg := context.WithoutCancel(ctx)
	if err := w.workers.Register(ctx, Info{ID: w.cfg.ID, Hostname: host, PID: os.Getpid(),
		Concurrency: w.cfg.Concurrency, Version: w.cfg.Version}); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	if err := w.broker.EnsureGroups(ctx, queueNames()); err != nil {
		return err
	}
	w.metrics.WorkerCapacity.Set(float64(w.cfg.Concurrency))
	w.mu.Lock()
	w.lastHB = time.Now()
	w.mu.Unlock()

	hbCtx, stopHB := context.WithCancel(bg)
	hbDone := make(chan struct{})
	go func() { defer close(hbDone); w.heartbeatLoop(hbCtx) }()

	w.log.Info("worker started", "concurrency", w.cfg.Concurrency, "lease", w.cfg.LeaseDuration.String())
	w.fetchLoop(ctx)

	// Graceful shutdown.
	w.log.Info("worker draining", "inflight", w.Inflight())
	_ = w.workers.SetStatus(bg, w.cfg.ID, "stopping")
	drained := make(chan struct{})
	go func() { w.wg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(w.cfg.ShutdownTimeout):
		w.log.Warn("shutdown timeout reached; interrupting in-flight jobs", "inflight", w.Inflight())
		w.mu.Lock()
		for _, e := range w.inflight {
			e.cancel(jobs.ErrWorkerShutdown)
		}
		w.mu.Unlock()
		<-drained
	}
	stopHB()
	<-hbDone
	_ = w.workers.SetStatus(bg, w.cfg.ID, "stopped")
	w.log.Info("worker stopped")
	return nil
}

// fetchLoop reads messages while capacity is available.
func (w *Worker) fetchLoop(ctx context.Context) {
	queues := queueNames()
	// Reads use a context detached from shutdown so that an in-progress
	// XREADGROUP is never torn down after Redis has already assigned
	// messages to this consumer; BlockTimeout bounds shutdown latency.
	readCtx := context.WithoutCancel(ctx)
	lastClaim := time.Time{}
	brokerFailures := 0
	for {
		// Acquire at least one execution slot.
		select {
		case w.slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		n := 1
	grab:
		for n < w.cfg.Concurrency {
			select {
			case w.slots <- struct{}{}:
				n++
			default:
				break grab
			}
		}

		var msgs []broker.Message
		var err error
		if time.Since(lastClaim) >= w.cfg.ClaimInterval {
			lastClaim = time.Now()
			msgs, err = w.claimStale(readCtx, n)
		}
		if err == nil && len(msgs) == 0 {
			msgs, err = w.broker.Read(readCtx, w.cfg.ID, queues, n, w.cfg.BlockTimeout)
		}
		for i := len(msgs); i < n; i++ {
			<-w.slots
		}
		if err != nil {
			brokerFailures++
			w.metrics.WorkerFailures.WithLabelValues("broker").Inc()
			delay := min(time.Duration(brokerFailures)*500*time.Millisecond, 10*time.Second)
			w.log.Warn("broker read failed", "error", err.Error(), "retry_in", delay.String())
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			continue
		}
		brokerFailures = 0
		for _, m := range msgs {
			w.wg.Add(1)
			go func(m broker.Message) {
				defer w.wg.Done()
				defer func() { <-w.slots }()
				w.process(m)
			}(m)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// claimStale takes over messages left pending by dead consumers.
func (w *Worker) claimStale(ctx context.Context, n int) ([]broker.Message, error) {
	var out []broker.Message
	for _, q := range jobs.WorkQueues {
		if len(out) >= n {
			break
		}
		msgs, err := w.broker.AutoClaim(ctx, string(q), w.cfg.ID, w.cfg.ClaimMinIdle, n-len(out))
		if err != nil {
			return out, err
		}
		if len(msgs) > 0 {
			w.log.Info("claimed stale messages", "queue", q, "count", len(msgs))
		}
		out = append(out, msgs...)
	}
	return out, nil
}

func (w *Worker) ack(m broker.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.broker.Ack(ctx, m.Queue, m.ID); err != nil {
		// Not fatal: the message will be re-claimed and dropped as a
		// duplicate because the job is no longer queued.
		w.metrics.WorkerFailures.WithLabelValues("ack").Inc()
		w.log.Warn("ack failed", "queue", m.Queue, "message_id", m.ID, "error", err.Error())
	}
}

// process handles one delivered message end to end.
func (w *Worker) process(m broker.Message) {
	log := w.log.With("queue", m.Queue, "message_id", m.ID)
	jobID, err := uuid.Parse(m.Fields["job_id"])
	if err != nil {
		log.Error("dropping malformed message", "error", "invalid job_id")
		w.ack(m)
		return
	}
	log = log.With("job_id", jobID.String())

	claimCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	job, err := w.store.Claim(claimCtx, jobID, w.cfg.ID, w.cfg.LeaseDuration)
	cancel()
	if errors.Is(err, jobs.ErrNotClaimable) {
		w.metrics.DuplicateDeliveries.Inc()
		log.Debug("message not claimable; acknowledging as duplicate/stale")
		w.ack(m)
		return
	}
	if err != nil {
		// Leave unacknowledged; it will be re-claimed after ClaimMinIdle.
		w.metrics.WorkerFailures.WithLabelValues("database").Inc()
		log.Error("claim failed", "error", err.Error())
		return
	}
	lease := jobs.Lease{JobID: job.ID, WorkerID: w.cfg.ID, Attempt: job.Attempts}
	log = log.With("job_type", job.Type, "attempt", job.Attempts, "max_attempts", job.MaxAttempts)
	if job.StartedAt != nil {
		w.metrics.QueueWait.WithLabelValues(string(job.Queue)).Observe(max(0, job.StartedAt.Sub(job.ScheduledAt).Seconds()))
	}

	execCtx, cancelExec := context.WithCancelCause(context.Background())
	execCtx, cancelTimeout := context.WithTimeout(execCtx, job.Timeout())
	deadline, _ := execCtx.Deadline()
	w.mu.Lock()
	w.inflight[lease] = &execution{lease: lease, cancel: cancelExec}
	w.metrics.WorkerInflight.Set(float64(len(w.inflight)))
	w.mu.Unlock()
	defer func() {
		cancelTimeout()
		cancelExec(nil)
		w.mu.Lock()
		delete(w.inflight, lease)
		w.metrics.WorkerInflight.Set(float64(len(w.inflight)))
		w.mu.Unlock()
	}()

	log.Info("job started")
	start := time.Now()
	result, herr := w.handlers.Run(execCtx, jobs.Execution{
		JobID: job.ID, Type: job.Type, Attempt: job.Attempts, MaxAttempts: job.MaxAttempts,
		Payload: job.Payload, IdempotencyKey: job.ID.String(), Deadline: deadline, Logger: log,
	})
	dur := time.Since(start)
	var cause error
	if execCtx.Err() != nil {
		cause = context.Cause(execCtx)
	}

	if w.persistOutcome(log, job, lease, result, herr, cause, dur) {
		w.ack(m)
	}
}

// persistOutcome records the attempt result, retrying transient database
// errors for up to one lease duration. It returns whether the message may be
// acknowledged.
func (w *Worker) persistOutcome(log *slog.Logger, job *jobs.Job, lease jobs.Lease, result []byte,
	herr, cause error, dur time.Duration) bool {
	class := jobs.Classify(herr, cause)
	if herr == nil {
		class = jobs.ClassNone
	}
	if class == jobs.ClassLeaseLost {
		w.metrics.WorkerFailures.WithLabelValues("lease_lost").Inc()
		log.Warn("lease lost during execution; result discarded", "duration_ms", dur.Milliseconds())
		return true
	}

	write := func(ctx context.Context) (*jobs.Job, error) {
		switch {
		case herr == nil:
			return w.store.Complete(ctx, lease, result, dur)
		case class == jobs.ClassCancelled:
			return w.store.MarkCancelled(ctx, lease, dur)
		default:
			d := jobs.DecideFailure(job.Attempts, job.MaxAttempts, class, w.cfg.Backoff, time.Now())
			return w.store.Fail(ctx, lease, jobs.Failure{
				Err: herr.Error(), Class: class, Duration: dur, Retry: d.Retry, RetryAt: d.RetryAt,
			})
		}
	}

	deadline := time.Now().Add(w.cfg.LeaseDuration)
	delay := 100 * time.Millisecond
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		updated, err := write(ctx)
		cancel()
		if errors.Is(err, jobs.ErrLeaseLost) {
			w.metrics.WorkerFailures.WithLabelValues("lease_lost").Inc()
			log.Warn("lease lost before outcome was recorded; result discarded")
			return true
		}
		if err == nil {
			w.recordMetrics(log, updated, class, herr, dur)
			return true
		}
		w.metrics.WorkerFailures.WithLabelValues("database").Inc()
		if time.Now().Add(delay).After(deadline) {
			log.Error("giving up recording outcome; lease recovery will reschedule", "error", err.Error())
			return false
		}
		log.Warn("recording outcome failed; retrying", "error", err.Error(), "retry_in", delay.String())
		time.Sleep(delay)
		delay = min(delay*2, 2*time.Second)
	}
}

func (w *Worker) recordMetrics(log *slog.Logger, j *jobs.Job, class jobs.ErrorClass, herr error, dur time.Duration) {
	outcome := string(j.Status)
	switch j.Status {
	case jobs.StatusSucceeded:
		w.metrics.JobsSucceeded.WithLabelValues(j.Type).Inc()
		log.Info("job succeeded", "duration_ms", dur.Milliseconds())
	case jobs.StatusCancelled:
		w.metrics.JobsCancelled.WithLabelValues(j.Type).Inc()
		log.Info("job cancelled during execution", "duration_ms", dur.Milliseconds())
	case jobs.StatusPending:
		outcome = "retry"
		w.metrics.AttemptFailures.WithLabelValues(j.Type, string(class)).Inc()
		w.metrics.JobsRetried.WithLabelValues(j.Type).Inc()
		log.Warn("job attempt failed; retry scheduled", "error", herr.Error(), "error_class", class,
			"retry_at", j.ScheduledAt.Format(time.RFC3339Nano))
	case jobs.StatusFailed:
		w.metrics.AttemptFailures.WithLabelValues(j.Type, string(class)).Inc()
		w.metrics.JobsDeadLettered.WithLabelValues(j.Type).Inc()
		log.Error("job failed permanently; dead-lettered", "error", herr.Error(), "error_class", class)
	}
	w.metrics.JobDuration.WithLabelValues(j.Type, outcome).Observe(dur.Seconds())
}

// heartbeatLoop renews the worker registration and job leases, and relays
// cancellation requests and lease loss to running handlers.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.heartbeat(ctx)
		}
	}
}

func (w *Worker) heartbeat(ctx context.Context) {
	hctx, cancel := context.WithTimeout(ctx, w.cfg.HeartbeatInterval)
	defer cancel()

	w.mu.Lock()
	leases := make([]jobs.Lease, 0, len(w.inflight))
	for l := range w.inflight {
		leases = append(leases, l)
	}
	w.mu.Unlock()

	err := w.workers.Heartbeat(hctx, w.cfg.ID)
	var states []jobs.LeaseState
	if err == nil {
		states, err = w.store.ExtendLeases(hctx, w.cfg.ID, leases, w.cfg.LeaseDuration)
	}
	now := time.Now()
	w.mu.Lock()
	defer w.mu.Unlock()
	if err != nil {
		w.metrics.WorkerFailures.WithLabelValues("heartbeat").Inc()
		w.log.Warn("heartbeat failed", "error", err.Error())
		// Self-fencing: if we cannot renew for a whole lease period the
		// recovery manager may already have handed our jobs to someone
		// else, so stop executing them to limit duplicate side effects.
		if now.Sub(w.lastHB) > w.cfg.LeaseDuration {
			for _, e := range w.inflight {
				e.cancel(jobs.ErrLeaseLost)
			}
		}
		return
	}
	w.lastHB = now
	held := make(map[jobs.Lease]bool, len(states))
	for _, s := range states {
		held[s.Lease] = true
		if s.CancelRequested {
			if e := w.inflight[s.Lease]; e != nil {
				e.cancel(jobs.ErrCancelRequested)
			}
		}
	}
	for _, l := range leases {
		if !held[l] {
			if e := w.inflight[l]; e != nil {
				w.log.Warn("lease no longer held; stopping job", "job_id", l.JobID.String(), "attempt", l.Attempt)
				e.cancel(jobs.ErrLeaseLost)
			}
		}
	}
}
