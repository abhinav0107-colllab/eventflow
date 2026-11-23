package jobs

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinav0107-colllab/eventflow/internal/database"
	"github.com/abhinav0107-colllab/eventflow/internal/outbox"
)

// maxErrorLen bounds persisted error messages.
const maxErrorLen = 4000

// Store persists jobs in PostgreSQL. All state transitions are conditional
// UPDATEs (compare-and-set on status and, for workers, the lease fencing
// token), so concurrent actors can never apply conflicting transitions.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore returns a Store backed by pool.
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// CreateParams are validated inputs for Create.
type CreateParams struct {
	Type           string
	Payload        json.RawMessage
	Priority       int
	MaxAttempts    int
	TimeoutSeconds int
	ScheduledAt    *time.Time // nil: now
	IdempotencyKey *string
	RequestHash    []byte
	APIKeyID       int64
}

const jobColumns = `id, type, queue, payload, priority, status, idempotency_key, attempts,
	max_attempts, timeout_seconds, scheduled_at, created_at, updated_at, queued_at,
	started_at, completed_at, worker_id, lease_expires_at, cancel_requested, last_error,
	last_error_class, result, duration_ms, dead_lettered_at, version`

func scanJob(row pgx.Row) (*Job, error) { return scanJobWithPrefix(row) }

func readyEvent(j *Job, at time.Time) outbox.Event {
	return outbox.Event{
		JobID:       j.ID,
		Type:        outbox.EventJobReady,
		Queue:       string(j.Queue),
		Payload:     map[string]string{"job_id": j.ID.String(), "type": j.Type},
		AvailableAt: at,
	}
}

// Create inserts a job and its job.ready outbox event atomically.
//
// With an idempotency key, a repeated request returns the original job and
// created=false; a different request under the same key yields
// ErrIdempotencyConflict. Concurrent duplicates are resolved by the unique
// index: INSERT ... ON CONFLICT DO NOTHING blocks until the competing
// transaction commits, after which the winner's row is visible.
func (s *Store) Create(ctx context.Context, p CreateParams) (job *Job, created bool, err error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, false, err
	}
	queue := QueueForPriority(p.Priority)
	err = database.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			INSERT INTO jobs (id, type, queue, payload, priority, api_key_id, idempotency_key,
			                  request_hash, max_attempts, timeout_seconds, scheduled_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, COALESCE($11::timestamptz, now()))
			ON CONFLICT (api_key_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
			RETURNING `+jobColumns,
			id, p.Type, string(queue), []byte(p.Payload), p.Priority, p.APIKeyID, p.IdempotencyKey,
			p.RequestHash, p.MaxAttempts, p.TimeoutSeconds, p.ScheduledAt)
		j, err := scanJob(row)
		if errors.Is(err, pgx.ErrNoRows) {
			// Idempotent replay: return the existing job.
			var hash []byte
			row := tx.QueryRow(ctx, `SELECT request_hash, `+jobColumns+` FROM jobs
				WHERE api_key_id = $1 AND idempotency_key = $2`, p.APIKeyID, p.IdempotencyKey)
			existing, err := scanJobWithPrefix(row, &hash)
			if err != nil {
				return fmt.Errorf("load idempotent job: %w", err)
			}
			if !bytes.Equal(hash, p.RequestHash) {
				job = existing
				return ErrIdempotencyConflict
			}
			job, created = existing, false
			return nil
		}
		if err != nil {
			return fmt.Errorf("insert job: %w", err)
		}
		if err := outbox.Insert(ctx, tx, readyEvent(j, j.ScheduledAt)); err != nil {
			return err
		}
		job, created = j, true
		return nil
	})
	if errors.Is(err, ErrIdempotencyConflict) {
		return job, false, err
	}
	if err != nil {
		return nil, false, err
	}
	return job, created, nil
}

func scanJobWithPrefix(row pgx.Row, prefix ...any) (*Job, error) {
	var j Job
	var queue, status string
	var payload, result []byte
	dest := append(prefix, &j.ID, &j.Type, &queue, &payload, &j.Priority, &status, &j.IdempotencyKey,
		&j.Attempts, &j.MaxAttempts, &j.TimeoutSeconds, &j.ScheduledAt, &j.CreatedAt, &j.UpdatedAt,
		&j.QueuedAt, &j.StartedAt, &j.CompletedAt, &j.WorkerID, &j.LeaseExpiresAt, &j.CancelRequested,
		&j.LastError, &j.LastErrorClass, &result, &j.DurationMS, &j.DeadLetteredAt, &j.Version)
	if err := row.Scan(dest...); err != nil {
		return nil, err
	}
	j.Queue, j.Status = Queue(queue), Status(status)
	j.Payload = payload
	if len(result) > 0 {
		j.Result = result
	}
	return &j, nil
}

// Get loads one job.
func (s *Store) Get(ctx context.Context, id uuid.UUID) (*Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return j, err
}

// ListFilter selects jobs for the list endpoint.
type ListFilter struct {
	Status        *Status
	Type          string
	Queue         string
	DeadLettered  *bool
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	Limit         int
	Cursor        string
}

// Cursor encodes a keyset pagination position (created_at, id).
type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// EncodeCursor returns an opaque cursor string.
func EncodeCursor(c Cursor) string {
	raw := strconv.FormatInt(c.CreatedAt.UnixMicro(), 10) + "|" + c.ID.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodeCursor parses a cursor produced by EncodeCursor.
func DecodeCursor(s string) (Cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, errors.New("malformed cursor")
	}
	ts, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return Cursor{}, errors.New("malformed cursor")
	}
	us, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return Cursor{}, errors.New("malformed cursor")
	}
	uid, err := uuid.Parse(id)
	if err != nil {
		return Cursor{}, errors.New("malformed cursor")
	}
	return Cursor{CreatedAt: time.UnixMicro(us).UTC(), ID: uid}, nil
}

// List returns jobs newest-first using keyset pagination, plus the cursor
// for the next page ("" when there are no more results).
func (s *Store) List(ctx context.Context, f ListFilter) ([]*Job, string, error) {
	if f.Limit <= 0 || f.Limit > 100 {
		f.Limit = 20
	}
	var where []string
	var args []any
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	if f.Status != nil {
		where = append(where, "status = "+arg(string(*f.Status)))
	}
	if f.Type != "" {
		where = append(where, "type = "+arg(f.Type))
	}
	if f.Queue != "" {
		where = append(where, "queue = "+arg(f.Queue))
	}
	if f.DeadLettered != nil {
		if *f.DeadLettered {
			where = append(where, "dead_lettered_at IS NOT NULL")
		} else {
			where = append(where, "dead_lettered_at IS NULL")
		}
	}
	if f.CreatedAfter != nil {
		where = append(where, "created_at >= "+arg(*f.CreatedAfter))
	}
	if f.CreatedBefore != nil {
		where = append(where, "created_at < "+arg(*f.CreatedBefore))
	}
	if f.Cursor != "" {
		c, err := DecodeCursor(f.Cursor)
		if err != nil {
			return nil, "", err
		}
		where = append(where, fmt.Sprintf("(created_at, id) < (%s, %s)", arg(c.CreatedAt), arg(c.ID)))
	}
	q := `SELECT ` + jobColumns + ` FROM jobs`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += " ORDER BY created_at DESC, id DESC LIMIT " + arg(f.Limit+1)

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > f.Limit {
		out = out[:f.Limit]
		last := out[len(out)-1]
		next = EncodeCursor(Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
	}
	return out, next, nil
}

// ListAttempts returns the execution history of a job, oldest first.
func (s *Store) ListAttempts(ctx context.Context, id uuid.UUID) ([]Attempt, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM jobs WHERE id = $1)`, id).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, job_id, attempt_number, worker_id, status, started_at, finished_at,
		       duration_ms, error, error_class
		FROM job_attempts WHERE job_id = $1 ORDER BY attempt_number`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Attempt{}
	for rows.Next() {
		var a Attempt
		var st string
		if err := rows.Scan(&a.ID, &a.JobID, &a.AttemptNumber, &a.WorkerID, &st, &a.StartedAt,
			&a.FinishedAt, &a.DurationMS, &a.Error, &a.ErrorClass); err != nil {
			return nil, err
		}
		a.Status = AttemptStatus(st)
		out = append(out, a)
	}
	return out, rows.Err()
}

// CancelOutcome describes what Cancel did.
type CancelOutcome int

// Cancel outcomes.
const (
	// Cancelled: the job had not started and is now cancelled.
	Cancelled CancelOutcome = iota + 1
	// CancelRequested: the job is running; the worker will observe the
	// request at its next heartbeat and stop the handler. The job may still
	// complete successfully if it finishes first.
	CancelRequested
)

func lockJob(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*Job, error) {
	j, err := scanJob(tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return j, err
}

// Cancel cancels a job that has not started, or requests cancellation of a
// running one. Terminal jobs yield ErrInvalidState (the job is returned).
func (s *Store) Cancel(ctx context.Context, id uuid.UUID) (*Job, CancelOutcome, error) {
	var out *Job
	var outcome CancelOutcome
	err := database.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		j, err := lockJob(ctx, tx, id)
		if err != nil {
			return err
		}
		switch j.Status {
		case StatusPending, StatusQueued:
			out, err = scanJob(tx.QueryRow(ctx, `
				UPDATE jobs SET status = 'cancelled', completed_at = now(), updated_at = now(),
				       last_error = 'cancelled by client', last_error_class = 'cancelled',
				       version = version + 1
				WHERE id = $1 RETURNING `+jobColumns, id))
			if err != nil {
				return err
			}
			// Any queued notification becomes a no-op for workers, but
			// unpublished ones are discarded so they never hit the broker.
			_, err = tx.Exec(ctx, `UPDATE outbox_events SET status = 'discarded'
				WHERE job_id = $1 AND status = 'pending' AND event_type = 'job.ready'`, id)
			outcome = Cancelled
			return err
		case StatusRunning:
			out, err = scanJob(tx.QueryRow(ctx, `
				UPDATE jobs SET cancel_requested = true, updated_at = now(), version = version + 1
				WHERE id = $1 RETURNING `+jobColumns, id))
			outcome = CancelRequested
			return err
		default:
			out = j
			return ErrInvalidState
		}
	})
	return out, outcome, err
}

// Retry re-submits a failed or cancelled job, granting it extraAttempts more
// attempts on top of those already used. Attempt history is preserved.
func (s *Store) Retry(ctx context.Context, id uuid.UUID, extraAttempts int) (*Job, error) {
	if extraAttempts < 1 || extraAttempts > MaxAttemptsLimit {
		return nil, fmt.Errorf("extra attempts must be between 1 and %d", MaxAttemptsLimit)
	}
	var out *Job
	err := database.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		j, err := lockJob(ctx, tx, id)
		if err != nil {
			return err
		}
		// Manual retry is only meaningful for terminal, unsuccessful jobs.
		if j.Status != StatusFailed && j.Status != StatusCancelled {
			out = j
			return ErrInvalidState
		}
		out, err = scanJob(tx.QueryRow(ctx, `
			UPDATE jobs SET status = 'pending', max_attempts = attempts + $2, scheduled_at = now(),
			       completed_at = NULL, dead_lettered_at = NULL, cancel_requested = false,
			       result = NULL, lease_expires_at = NULL, updated_at = now(), version = version + 1
			WHERE id = $1 RETURNING `+jobColumns, id, extraAttempts))
		if err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, readyEvent(out, time.Time{}))
	})
	return out, err
}

// Claim transitions a queued job to running for workerID, incrementing the
// attempt counter (the fencing token) and opening an attempt record. It
// returns ErrNotClaimable if the job is not queued, which is how duplicate,
// stale and cancelled deliveries are filtered out.
func (s *Store) Claim(ctx context.Context, id uuid.UUID, workerID string, lease time.Duration) (*Job, error) {
	var out *Job
	err := database.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		// Lock first, then check. The outbox relay holds this row lock from
		// before XADD until it commits status=queued, so a worker that
		// receives the message early waits here and then reads the
		// committed state. A plain conditional UPDATE would not wait: in
		// READ COMMITTED it skips rows whose snapshot version (still
		// "pending") fails the WHERE clause, and the message would be lost.
		var status string
		err := tx.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1 FOR UPDATE`, id).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotClaimable
		}
		if err != nil {
			return err
		}
		if Status(status) != StatusQueued {
			return ErrNotClaimable
		}
		j, err := scanJob(tx.QueryRow(ctx, `
			UPDATE jobs SET status = 'running', attempts = attempts + 1, worker_id = $2,
			       lease_expires_at = now() + ($3 * interval '1 millisecond'),
			       started_at = now(), updated_at = now(), version = version + 1
			WHERE id = $1 AND status = 'queued' AND attempts < max_attempts
			RETURNING `+jobColumns, id, workerID, lease.Milliseconds()))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotClaimable
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO job_attempts (job_id, attempt_number, worker_id, status, started_at)
			VALUES ($1, $2, $3, 'running', $4)`, j.ID, j.Attempts, workerID, j.StartedAt)
		out = j
		return err
	})
	return out, err
}

const fenced = `id = $1 AND status = 'running' AND worker_id = $2 AND attempts = $3`

func truncate(s string) string {
	if len(s) > maxErrorLen {
		return s[:maxErrorLen]
	}
	return s
}

func finishAttempt(ctx context.Context, tx pgx.Tx, l Lease, st AttemptStatus, durMS int64, errMsg *string, class *string) error {
	_, err := tx.Exec(ctx, `
		UPDATE job_attempts SET status = $3, finished_at = now(), duration_ms = $4, error = $5, error_class = $6
		WHERE job_id = $1 AND attempt_number = $2`, l.JobID, l.Attempt, string(st), durMS, errMsg, class)
	return err
}

// Complete records success. It fails with ErrLeaseLost when the lease no
// longer matches, guaranteeing at most one attempt is ever recorded as the
// successful completion of a job.
func (s *Store) Complete(ctx context.Context, l Lease, result json.RawMessage, dur time.Duration) (*Job, error) {
	var out *Job
	err := database.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		var res any
		if len(result) > 0 {
			res = []byte(result)
		}
		j, err := scanJob(tx.QueryRow(ctx, `
			UPDATE jobs SET status = 'succeeded', completed_at = now(), result = $4, duration_ms = $5,
			       lease_expires_at = NULL, cancel_requested = false, updated_at = now(), version = version + 1
			WHERE `+fenced+` RETURNING `+jobColumns, l.JobID, l.WorkerID, l.Attempt, res, dur.Milliseconds()))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseLost
		}
		if err != nil {
			return err
		}
		out = j
		return finishAttempt(ctx, tx, l, AttemptSucceeded, dur.Milliseconds(), nil, nil)
	})
	return out, err
}

// Failure describes a failed attempt.
type Failure struct {
	Err      string
	Class    ErrorClass
	Duration time.Duration
	// Retry, when true, re-schedules the job at RetryAt; otherwise the job
	// is dead-lettered.
	Retry   bool
	RetryAt time.Time
}

// Fail records a failed attempt and either schedules a retry (pending + a
// delayed job.ready outbox event) or dead-letters the job (failed + a
// job.dead_lettered outbox event), atomically.
func (s *Store) Fail(ctx context.Context, l Lease, f Failure) (*Job, error) {
	var out *Job
	err := database.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		var err error
		out, err = failInTx(ctx, tx, l, f, AttemptFailed)
		return err
	})
	return out, err
}

func failInTx(ctx context.Context, tx pgx.Tx, l Lease, f Failure, attemptStatus AttemptStatus) (*Job, error) {
	msg, class := truncate(f.Err), string(f.Class)
	var row pgx.Row
	if f.Retry {
		row = tx.QueryRow(ctx, `
			UPDATE jobs SET status = 'pending', scheduled_at = $4, lease_expires_at = NULL,
			       last_error = $5, last_error_class = $6, duration_ms = $7,
			       updated_at = now(), version = version + 1
			WHERE `+fenced+` RETURNING `+jobColumns,
			l.JobID, l.WorkerID, l.Attempt, f.RetryAt, msg, class, f.Duration.Milliseconds())
	} else {
		row = tx.QueryRow(ctx, `
			UPDATE jobs SET status = 'failed', completed_at = now(), dead_lettered_at = now(),
			       lease_expires_at = NULL, last_error = $4, last_error_class = $5, duration_ms = $6,
			       updated_at = now(), version = version + 1
			WHERE `+fenced+` RETURNING `+jobColumns,
			l.JobID, l.WorkerID, l.Attempt, msg, class, f.Duration.Milliseconds())
	}
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrLeaseLost
	}
	if err != nil {
		return nil, err
	}
	if err := finishAttempt(ctx, tx, l, attemptStatus, f.Duration.Milliseconds(), &msg, &class); err != nil {
		return nil, err
	}
	if f.Retry {
		return j, outbox.Insert(ctx, tx, readyEvent(j, f.RetryAt))
	}
	return j, outbox.Insert(ctx, tx, outbox.Event{
		JobID: j.ID,
		Type:  outbox.EventJobDeadLettered,
		Queue: string(QueueDeadLetter),
		Payload: map[string]string{
			"job_id":      j.ID.String(),
			"type":        j.Type,
			"attempts":    strconv.Itoa(j.Attempts),
			"error":       msg,
			"error_class": class,
		},
	})
}

// MarkCancelled finalises a running job whose handler stopped because of a
// client cancellation request.
func (s *Store) MarkCancelled(ctx context.Context, l Lease, dur time.Duration) (*Job, error) {
	var out *Job
	err := database.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		j, err := scanJob(tx.QueryRow(ctx, `
			UPDATE jobs SET status = 'cancelled', completed_at = now(), lease_expires_at = NULL,
			       last_error = 'cancelled by client', last_error_class = 'cancelled', duration_ms = $4,
			       updated_at = now(), version = version + 1
			WHERE `+fenced+` RETURNING `+jobColumns, l.JobID, l.WorkerID, l.Attempt, dur.Milliseconds()))
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseLost
		}
		if err != nil {
			return err
		}
		out = j
		msg, class := "cancelled by client", string(ClassCancelled)
		return finishAttempt(ctx, tx, l, AttemptCancelled, dur.Milliseconds(), &msg, &class)
	})
	return out, err
}

// LeaseState is returned by ExtendLeases for each lease still held.
type LeaseState struct {
	Lease           Lease
	CancelRequested bool
}

// ExtendLeases renews the leases a worker still holds and reports pending
// cancellation requests. Leases absent from the result have been lost.
func (s *Store) ExtendLeases(ctx context.Context, workerID string, leases []Lease, d time.Duration) ([]LeaseState, error) {
	if len(leases) == 0 {
		return nil, nil
	}
	ids := make([]uuid.UUID, len(leases))
	attempts := make([]int32, len(leases))
	for i, l := range leases {
		ids[i], attempts[i] = l.JobID, int32(l.Attempt)
	}
	rows, err := s.pool.Query(ctx, `
		UPDATE jobs SET lease_expires_at = now() + ($2 * interval '1 millisecond')
		WHERE status = 'running' AND worker_id = $1
		  AND (id, attempts) IN (SELECT * FROM unnest($3::uuid[], $4::int[]))
		RETURNING id, attempts, cancel_requested`, workerID, d.Milliseconds(), ids, attempts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LeaseState
	for rows.Next() {
		var ls LeaseState
		ls.Lease.WorkerID = workerID
		if err := rows.Scan(&ls.Lease.JobID, &ls.Lease.Attempt, &ls.CancelRequested); err != nil {
			return nil, err
		}
		out = append(out, ls)
	}
	return out, rows.Err()
}

// RecoveredJob describes one job handled by RecoverExpiredLeases.
type RecoveredJob struct {
	JobID    uuid.UUID
	Type     string
	WorkerID string
	Attempt  int
	Status   Status // resulting status: pending (retry), failed (DLQ) or cancelled
}

// RecoverExpiredLeases closes attempts whose lease expired (the worker
// crashed, hung or was partitioned) and retries or dead-letters the jobs.
// SKIP LOCKED lets every worker run recovery concurrently without blocking.
func (s *Store) RecoverExpiredLeases(ctx context.Context, limit int, b Backoff) ([]RecoveredJob, error) {
	var out []RecoveredJob
	err := database.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, type, worker_id, attempts, max_attempts, cancel_requested FROM jobs
			WHERE status = 'running' AND lease_expires_at < now()
			ORDER BY lease_expires_at LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
		if err != nil {
			return err
		}
		type cand struct {
			l         Lease
			typ       string
			max       int
			cancelReq bool
		}
		var cands []cand
		for rows.Next() {
			var c cand
			if err := rows.Scan(&c.l.JobID, &c.typ, &c.l.WorkerID, &c.l.Attempt, &c.max, &c.cancelReq); err != nil {
				rows.Close()
				return err
			}
			cands = append(cands, c)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		now := time.Now()
		for _, c := range cands {
			rj := RecoveredJob{JobID: c.l.JobID, Type: c.typ, WorkerID: c.l.WorkerID, Attempt: c.l.Attempt}
			if c.cancelReq {
				if _, err := tx.Exec(ctx, `
					UPDATE jobs SET status = 'cancelled', completed_at = now(), lease_expires_at = NULL,
					       last_error = 'cancelled by client', last_error_class = 'cancelled',
					       updated_at = now(), version = version + 1
					WHERE `+fenced, c.l.JobID, c.l.WorkerID, c.l.Attempt); err != nil {
					return err
				}
				msg, class := "worker lease expired after cancellation request", string(ClassLeaseExpired)
				if err := finishAttempt(ctx, tx, c.l, AttemptLost, 0, &msg, &class); err != nil {
					return err
				}
				rj.Status = StatusCancelled
			} else {
				d := DecideFailure(c.l.Attempt, c.max, ClassLeaseExpired, b, now)
				j, err := failInTx(ctx, tx, c.l, Failure{
					Err:     fmt.Sprintf("worker %s lease expired (worker crashed or stalled)", c.l.WorkerID),
					Class:   ClassLeaseExpired,
					Retry:   d.Retry,
					RetryAt: d.RetryAt,
				}, AttemptLost)
				if err != nil {
					return err
				}
				rj.Status = j.Status
			}
			out = append(out, rj)
		}
		return nil
	})
	return out, err
}

// RequeueStuck re-publishes jobs of queue that have been in the queued state
// longer than olderThan. The caller must only invoke this when the broker
// stream for queue is drained, meaning such jobs' messages were lost (e.g. a
// Redis restart without persistence). Duplicate publication is harmless
// because Claim only succeeds once per queued period.
func (s *Store) RequeueStuck(ctx context.Context, queue Queue, olderThan time.Duration, limit int) (int, error) {
	n := 0
	err := database.InTx(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			UPDATE jobs SET queued_at = now(), updated_at = now()
			WHERE id IN (
				SELECT id FROM jobs
				WHERE status = 'queued' AND queue = $1 AND queued_at < now() - ($2 * interval '1 millisecond')
				ORDER BY queued_at LIMIT $3 FOR UPDATE SKIP LOCKED)
			RETURNING `+jobColumns, string(queue), olderThan.Milliseconds(), limit)
		if err != nil {
			return err
		}
		var js []*Job
		for rows.Next() {
			j, err := scanJob(rows)
			if err != nil {
				rows.Close()
				return err
			}
			js = append(js, j)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, j := range js {
			if err := outbox.Insert(ctx, tx, readyEvent(j, time.Time{})); err != nil {
				return err
			}
		}
		n = len(js)
		return nil
	})
	return n, err
}

// QueueCounts are non-terminal job counts for one queue.
type QueueCounts struct {
	Pending int64 `json:"pending"`
	Queued  int64 `json:"queued"`
	Running int64 `json:"running"`
}

// Stats summarises job state for dashboards. Only non-terminal states are
// counted (they are index-backed and bounded); terminal totals come from
// Prometheus counters.
type Stats struct {
	ByQueue      map[Queue]*QueueCounts
	DeadLettered int64
}

// Stats computes the current summary.
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	st := Stats{ByQueue: map[Queue]*QueueCounts{}}
	for _, q := range WorkQueues {
		st.ByQueue[q] = &QueueCounts{}
	}
	rows, err := s.pool.Query(ctx, `
		SELECT queue, status, count(*) FROM jobs
		WHERE status IN ('pending', 'queued', 'running') GROUP BY queue, status`)
	if err != nil {
		return st, err
	}
	defer rows.Close()
	for rows.Next() {
		var q, status string
		var n int64
		if err := rows.Scan(&q, &status, &n); err != nil {
			return st, err
		}
		c := st.ByQueue[Queue(q)]
		if c == nil {
			continue
		}
		switch Status(status) {
		case StatusPending:
			c.Pending = n
		case StatusQueued:
			c.Queued = n
		case StatusRunning:
			c.Running = n
		}
	}
	if err := rows.Err(); err != nil {
		return st, err
	}
	err = s.pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE dead_lettered_at IS NOT NULL AND status = 'failed'`).Scan(&st.DeadLettered)
	return st, err
}
