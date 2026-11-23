package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
)

// DelayedPayload is the DELAYED_TASK input.
type DelayedPayload struct {
	DurationMS int    `json:"duration_ms"`
	Message    string `json:"message"`
}

// Delayed simulates a long-running task (e.g. a report export). It sleeps
// cooperatively so cancellation, timeouts, leases and graceful shutdown can
// be observed. Combine with scheduled_at/delay_seconds for delayed starts.
type Delayed struct{}

func (Delayed) parse(raw json.RawMessage) (DelayedPayload, error) {
	var p DelayedPayload
	if err := decode(raw, &p); err != nil {
		return p, err
	}
	if p.DurationMS < 0 || p.DurationMS > maxDelayedMS {
		return p, errors.New("duration_ms must be between 0 and 3600000")
	}
	if len(p.Message) > 1000 {
		return p, errors.New("message must be at most 1000 characters")
	}
	return p, nil
}

// Validate implements jobs.Handler.
func (d Delayed) Validate(raw json.RawMessage) error { _, err := d.parse(raw); return err }

// Handle implements jobs.Handler.
func (d Delayed) Handle(ctx context.Context, x jobs.Execution) (json.RawMessage, error) {
	p, err := d.parse(x.Payload)
	if err != nil {
		return nil, jobs.Permanent(err)
	}
	start := time.Now()
	if err := sleep(ctx, time.Duration(p.DurationMS)*time.Millisecond); err != nil {
		return nil, err
	}
	return mustJSON(map[string]any{"slept_ms": time.Since(start).Milliseconds(), "message": p.Message}), nil
}
