// Package jobs contains the job domain model, its state machine, retry policy,
// handler contract and the PostgreSQL-backed store.
package jobs

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Status is the lifecycle state of a job. PostgreSQL is authoritative.
type Status string

// Job lifecycle states.
const (
	// StatusPending: persisted, waiting for its scheduled time and/or for the
	// outbox relay to publish it to the broker. Retries also return here.
	StatusPending Status = "pending"
	// StatusQueued: a ready notification has been published to Redis.
	StatusQueued Status = "queued"
	// StatusRunning: claimed by a worker holding a valid lease.
	StatusRunning Status = "running"
	// StatusSucceeded: terminal, the handler completed successfully.
	StatusSucceeded Status = "succeeded"
	// StatusFailed: terminal, attempts exhausted or a permanent error; the
	// job is dead-lettered.
	StatusFailed Status = "failed"
	// StatusCancelled: terminal, cancelled by a client.
	StatusCancelled Status = "cancelled"
)

// AllStatuses lists every status in lifecycle order.
var AllStatuses = []Status{StatusPending, StatusQueued, StatusRunning, StatusSucceeded, StatusFailed, StatusCancelled}

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	for _, v := range AllStatuses {
		if v == s {
			return true
		}
	}
	return false
}

// Terminal reports whether no further automatic transitions can happen.
func (s Status) Terminal() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusCancelled
}

// transitions is the complete state machine. Every UPDATE in the store is
// written as a conditional transition that matches one of these edges.
var transitions = map[Status][]Status{
	StatusPending:   {StatusQueued, StatusCancelled},
	StatusQueued:    {StatusRunning, StatusCancelled, StatusQueued}, // queued->queued: re-publish of a lost message
	StatusRunning:   {StatusSucceeded, StatusPending, StatusFailed, StatusCancelled},
	StatusFailed:    {StatusPending}, // manual retry
	StatusCancelled: {StatusPending}, // manual retry
	StatusSucceeded: {},
}

// CanTransition reports whether from -> to is a legal transition.
func CanTransition(from, to Status) bool {
	for _, s := range transitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Queue is a broker queue (a Redis stream). Priorities map onto three
// bands; within a band delivery is FIFO.
type Queue string

// Queues.
const (
	QueueHigh       Queue = "high"
	QueueNormal     Queue = "normal"
	QueueLow        Queue = "low"
	QueueDeadLetter Queue = "dead_letter"
)

// WorkQueues are consumed by workers in strict priority order.
var WorkQueues = []Queue{QueueHigh, QueueNormal, QueueLow}

// Priority bounds.
const (
	MinPriority     = 1
	MaxPriority     = 10
	DefaultPriority = 5
)

// QueueForPriority maps a 1..10 priority to its queue.
func QueueForPriority(p int) Queue {
	switch {
	case p >= 8:
		return QueueHigh
	case p >= 4:
		return QueueNormal
	default:
		return QueueLow
	}
}

// Job is the persisted representation of a job.
type Job struct {
	ID              uuid.UUID       `json:"id"`
	Type            string          `json:"type"`
	Queue           Queue           `json:"queue"`
	Payload         json.RawMessage `json:"payload"`
	Priority        int             `json:"priority"`
	Status          Status          `json:"status"`
	IdempotencyKey  *string         `json:"idempotency_key,omitempty"`
	Attempts        int             `json:"attempts"`
	MaxAttempts     int             `json:"max_attempts"`
	TimeoutSeconds  int             `json:"timeout_seconds"`
	ScheduledAt     time.Time       `json:"scheduled_at"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	QueuedAt        *time.Time      `json:"queued_at,omitempty"`
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	CompletedAt     *time.Time      `json:"completed_at,omitempty"`
	WorkerID        *string         `json:"worker_id,omitempty"`
	LeaseExpiresAt  *time.Time      `json:"lease_expires_at,omitempty"`
	CancelRequested bool            `json:"cancel_requested"`
	LastError       *string         `json:"last_error,omitempty"`
	LastErrorClass  *string         `json:"last_error_class,omitempty"`
	Result          json.RawMessage `json:"result,omitempty"`
	DurationMS      *int64          `json:"duration_ms,omitempty"`
	DeadLetteredAt  *time.Time      `json:"dead_lettered_at,omitempty"`
	Version         int64           `json:"version"`
}

// Timeout returns the per-attempt execution timeout.
func (j *Job) Timeout() time.Duration { return time.Duration(j.TimeoutSeconds) * time.Second }

// AttemptStatus is the outcome of a single execution attempt.
type AttemptStatus string

// Attempt outcomes.
const (
	AttemptRunning   AttemptStatus = "running"
	AttemptSucceeded AttemptStatus = "succeeded"
	AttemptFailed    AttemptStatus = "failed"
	AttemptCancelled AttemptStatus = "cancelled"
	// AttemptLost: the worker stopped heart-beating (crash, partition) and
	// the lease expired; the recovery manager closed the attempt.
	AttemptLost AttemptStatus = "lost"
)

// Attempt is one row of execution history.
type Attempt struct {
	ID            int64         `json:"id"`
	JobID         uuid.UUID     `json:"job_id"`
	AttemptNumber int           `json:"attempt_number"`
	WorkerID      string        `json:"worker_id"`
	Status        AttemptStatus `json:"status"`
	StartedAt     time.Time     `json:"started_at"`
	FinishedAt    *time.Time    `json:"finished_at,omitempty"`
	DurationMS    *int64        `json:"duration_ms,omitempty"`
	Error         *string       `json:"error,omitempty"`
	ErrorClass    *string       `json:"error_class,omitempty"`
}

// Lease identifies a worker's claim on one attempt of a job. The attempt
// number acts as a fencing token: every write a worker makes after claiming
// is conditional on (id, status=running, worker_id, attempts) still matching,
// so a worker whose lease was revoked cannot overwrite newer state.
type Lease struct {
	JobID    uuid.UUID
	WorkerID string
	Attempt  int
}
