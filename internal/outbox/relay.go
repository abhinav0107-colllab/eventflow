package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinav0107-colllab/eventflow/internal/broker"
	"github.com/abhinav0107-colllab/eventflow/internal/database"
	"github.com/abhinav0107-colllab/eventflow/internal/observability"
)

// RelayConfig tunes the relay.
type RelayConfig struct {
	BatchSize      int
	PollInterval   time.Duration
	Retention      time.Duration // published/discarded events older than this are deleted
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	PublishTimeout time.Duration
}

// Relay moves due outbox events into Redis Streams.
//
// Guarantees:
//   - A job is moved to "queued" only in the same transaction that records a
//     successful XADD, so the database never claims a publication that did
//     not happen.
//   - If the process dies after XADD but before COMMIT, the event is
//     published again later. Duplicates are harmless: workers only execute a
//     job after winning the conditional queued->running claim in PostgreSQL.
//   - Several relays may run concurrently; FOR UPDATE SKIP LOCKED partitions
//     the work between them.
type Relay struct {
	pool    *pgxpool.Pool
	broker  *broker.Broker
	log     *slog.Logger
	metrics *observability.Metrics
	cfg     RelayConfig
}

// NewRelay constructs a Relay.
func NewRelay(pool *pgxpool.Pool, b *broker.Broker, log *slog.Logger, m *observability.Metrics, cfg RelayConfig) *Relay {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 200
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 200 * time.Millisecond
	}
	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = 500 * time.Millisecond
	}
	if cfg.RetryMaxDelay <= 0 {
		cfg.RetryMaxDelay = 30 * time.Second
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = 5 * time.Second
	}
	return &Relay{pool: pool, broker: b, log: log.With("component", "outbox_relay"), metrics: m, cfg: cfg}
}

// Run publishes until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	r.log.Info("outbox relay started", "batch_size", r.cfg.BatchSize, "poll_interval", r.cfg.PollInterval.String())
	defer r.log.Info("outbox relay stopped")
	cleanup := time.NewTicker(10 * time.Minute)
	defer cleanup.Stop()
	failures := 0
	for {
		n, err := r.PublishOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		wait := r.cfg.PollInterval
		switch {
		case err != nil:
			failures++
			r.log.Warn("outbox publish cycle failed", "error", err.Error(), "consecutive_failures", failures)
			wait = r.retryDelay(failures)
		case n >= r.cfg.BatchSize:
			failures = 0
			wait = 0 // more work is likely waiting
		default:
			failures = 0
		}
		select {
		case <-ctx.Done():
			return
		case <-cleanup.C:
			if r.cfg.Retention > 0 {
				if deleted, err := r.Cleanup(ctx); err != nil {
					r.log.Warn("outbox cleanup failed", "error", err.Error())
				} else if deleted > 0 {
					r.log.Info("outbox cleanup", "deleted", deleted)
				}
			}
		case <-time.After(wait):
		}
	}
}

func (r *Relay) retryDelay(failures int) time.Duration {
	d := float64(r.cfg.RetryBaseDelay) * math.Pow(2, float64(failures-1))
	if d > float64(r.cfg.RetryMaxDelay) {
		d = float64(r.cfg.RetryMaxDelay)
	}
	return time.Duration(d)
}

type pendingEvent struct {
	id          int64
	jobID       uuid.UUID
	eventType   EventType
	queue       string
	payload     map[string]string
	attempts    int
	jobStatus   string
	availableAt time.Time
}

// PublishOnce publishes one batch of due events and returns how many events
// were processed (published or discarded).
func (r *Relay) PublishOnce(ctx context.Context) (int, error) {
	processed := 0
	var publishErr error
	err := database.InTx(ctx, r.pool, func(tx pgx.Tx) error {
		// Lock both the event and its job: a worker that receives the
		// message before this transaction commits blocks on the job row in
		// Claim and then observes status=queued, instead of seeing
		// status=pending and dropping the message.
		rows, err := tx.Query(ctx, `
			SELECT o.id, o.job_id, o.event_type, o.queue, o.payload, o.attempts, j.status, o.available_at
			FROM outbox_events o JOIN jobs j ON j.id = o.job_id
			WHERE o.status = 'pending' AND o.available_at <= now()
			ORDER BY o.available_at, o.id
			LIMIT $1
			FOR UPDATE OF o, j SKIP LOCKED`, r.cfg.BatchSize)
		if err != nil {
			return fmt.Errorf("select due events: %w", err)
		}
		var events []pendingEvent
		for rows.Next() {
			var e pendingEvent
			var et string
			var raw []byte
			if err := rows.Scan(&e.id, &e.jobID, &et, &e.queue, &raw, &e.attempts, &e.jobStatus, &e.availableAt); err != nil {
				rows.Close()
				return err
			}
			e.eventType = EventType(et)
			if err := json.Unmarshal(raw, &e.payload); err != nil {
				e.payload = map[string]string{"job_id": e.jobID.String()}
			}
			events = append(events, e)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}

		var discard []int64
		var toSend []pendingEvent
		for _, e := range events {
			// A job.ready event is only meaningful while the job is still
			// waiting to run; if it was cancelled meanwhile, drop it.
			if e.eventType == EventJobReady && e.jobStatus != "pending" && e.jobStatus != "queued" {
				discard = append(discard, e.id)
				continue
			}
			toSend = append(toSend, e)
		}

		msgs := make([]broker.OutMessage, len(toSend))
		now := time.Now().UTC()
		for i, e := range toSend {
			fields := make(map[string]string, len(e.payload)+3)
			for k, v := range e.payload {
				fields[k] = v
			}
			fields["event_id"] = strconv.FormatInt(e.id, 10)
			fields["event_type"] = string(e.eventType)
			fields["published_at"] = now.Format(time.RFC3339Nano)
			msgs[i] = broker.OutMessage{Queue: e.queue, Fields: fields}
		}
		pubCtx, cancel := context.WithTimeout(ctx, r.cfg.PublishTimeout)
		ids, errs := r.broker.PublishBatch(pubCtx, msgs)
		cancel()

		var okIDs []int64
		var okMsgIDs []string
		var readyJobs []uuid.UUID
		var failIDs []int64
		var lastErr error
		for i, e := range toSend {
			if errs[i] != nil {
				failIDs = append(failIDs, e.id)
				lastErr = errs[i]
				continue
			}
			okIDs = append(okIDs, e.id)
			okMsgIDs = append(okMsgIDs, ids[i])
			if e.eventType == EventJobReady {
				readyJobs = append(readyJobs, e.jobID)
			}
			r.metrics.OutboxPublished.WithLabelValues(string(e.eventType)).Inc()
			r.metrics.OutboxPublishLatency.Observe(now.Sub(e.availableAt).Seconds())
		}

		if len(okIDs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE outbox_events o SET status = 'published', published_at = now(), stream_message_id = u.mid
				FROM unnest($1::bigint[], $2::text[]) AS u(id, mid) WHERE o.id = u.id`, okIDs, okMsgIDs); err != nil {
				return fmt.Errorf("mark published: %w", err)
			}
		}
		if len(readyJobs) > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE jobs SET status = 'queued', queued_at = now(), updated_at = now(), version = version + 1
				WHERE id = ANY($1) AND status IN ('pending', 'queued')`, readyJobs); err != nil {
				return fmt.Errorf("mark jobs queued: %w", err)
			}
		}
		if len(discard) > 0 {
			if _, err := tx.Exec(ctx, `UPDATE outbox_events SET status = 'discarded', published_at = now()
				WHERE id = ANY($1)`, discard); err != nil {
				return fmt.Errorf("discard events: %w", err)
			}
		}
		if len(failIDs) > 0 {
			r.metrics.OutboxPublishErrors.Add(float64(len(failIDs)))
			// Per-event capped exponential backoff based on its own attempts.
			if _, err := tx.Exec(ctx, `
				UPDATE outbox_events SET attempts = attempts + 1, last_error = $2,
				       available_at = now() + LEAST($4::float8, $3::float8 * power(2, LEAST(attempts, 30)))
				                              * interval '1 millisecond'
				WHERE id = ANY($1)`, failIDs, truncateErr(lastErr),
				r.cfg.RetryBaseDelay.Milliseconds(), r.cfg.RetryMaxDelay.Milliseconds()); err != nil {
				return fmt.Errorf("record publish failure: %w", err)
			}
			r.log.Warn("outbox publication failed; will retry", "events", len(failIDs), "error", lastErr.Error())
		}
		processed = len(okIDs) + len(discard)
		if len(failIDs) > 0 && processed == 0 {
			// Commit the failure bookkeeping, but tell the loop to back off.
			publishErr = fmt.Errorf("publish to broker: %w", lastErr)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return processed, publishErr
}

func truncateErr(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if len(s) > 1000 {
		s = s[:1000]
	}
	return s
}

// Cleanup deletes old published/discarded events in bounded batches.
func (r *Relay) Cleanup(ctx context.Context) (int64, error) {
	var total int64
	for {
		tag, err := r.pool.Exec(ctx, `
			DELETE FROM outbox_events WHERE id IN (
				SELECT id FROM outbox_events
				WHERE status <> 'pending' AND published_at < now() - ($1 * interval '1 millisecond')
				LIMIT 5000)`, r.cfg.Retention.Milliseconds())
		if err != nil {
			return total, err
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < 5000 {
			return total, nil
		}
	}
}

// Backlog reports the number of due unpublished events and the age of the
// oldest one.
func Backlog(ctx context.Context, pool *pgxpool.Pool) (count int64, oldest time.Duration, err error) {
	var age *float64
	err = pool.QueryRow(ctx, `
		SELECT count(*), EXTRACT(EPOCH FROM now() - min(available_at))::float8
		FROM outbox_events WHERE status = 'pending' AND available_at <= now()`).Scan(&count, &age)
	if age != nil && *age > 0 {
		oldest = time.Duration(*age * float64(time.Second))
	}
	return count, oldest, err
}
