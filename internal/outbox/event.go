// Package outbox implements the transactional outbox: events are inserted in
// the same database transaction as the state change they describe, and a
// relay publishes them to Redis Streams afterwards.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// EventType names an outbox event.
type EventType string

// Event types.
const (
	// EventJobReady: the job may now be executed; published to its work queue.
	EventJobReady EventType = "job.ready"
	// EventJobDeadLettered: the job failed terminally; published to the
	// dead-letter stream for operators and downstream consumers.
	EventJobDeadLettered EventType = "job.dead_lettered"
)

// Event is an outbox row to be inserted inside a caller's transaction.
type Event struct {
	JobID       uuid.UUID
	Type        EventType
	Queue       string
	Payload     map[string]string
	AvailableAt time.Time // zero means now()
}

// Insert writes e using tx. It must be called in the same transaction as the
// job state change it announces; that is the whole point of the pattern.
func Insert(ctx context.Context, tx pgx.Tx, e Event) error {
	payload, err := json.Marshal(e.Payload)
	if err != nil {
		return fmt.Errorf("outbox: encode payload: %w", err)
	}
	var avail any
	if !e.AvailableAt.IsZero() {
		avail = e.AvailableAt
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (job_id, event_type, queue, payload, available_at)
		VALUES ($1, $2, $3, $4, COALESCE($5::timestamptz, now()))`,
		e.JobID, string(e.Type), e.Queue, payload, avail)
	if err != nil {
		return fmt.Errorf("outbox: insert event: %w", err)
	}
	return nil
}
