package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
)

// FailurePayload is the DELIBERATE_FAILURE input.
type FailurePayload struct {
	// Mode: "always" (retryable error every attempt), "until_attempt"
	// (fail until SucceedOnAttempt), "permanent" (non-retryable) or "panic".
	Mode             string `json:"mode"`
	SucceedOnAttempt int    `json:"succeed_on_attempt"`
	Message          string `json:"message"`
}

// Failure fails on purpose to exercise retries, backoff, panics and the
// dead-letter queue.
type Failure struct{}

func (Failure) parse(raw json.RawMessage) (FailurePayload, error) {
	var p FailurePayload
	if err := decode(raw, &p); err != nil {
		return p, err
	}
	switch p.Mode {
	case "always", "permanent", "panic":
	case "until_attempt":
		if p.SucceedOnAttempt < 2 || p.SucceedOnAttempt > jobs.MaxAttemptsLimit {
			return p, fmt.Errorf("succeed_on_attempt must be between 2 and %d", jobs.MaxAttemptsLimit)
		}
	default:
		return p, errors.New(`mode must be one of "always", "until_attempt", "permanent", "panic"`)
	}
	if len(p.Message) > 500 {
		return p, errors.New("message must be at most 500 characters")
	}
	if p.Message == "" {
		p.Message = "deliberate failure"
	}
	return p, nil
}

// Validate implements jobs.Handler.
func (f Failure) Validate(raw json.RawMessage) error { _, err := f.parse(raw); return err }

// Handle implements jobs.Handler.
func (f Failure) Handle(ctx context.Context, x jobs.Execution) (json.RawMessage, error) {
	p, err := f.parse(x.Payload)
	if err != nil {
		return nil, jobs.Permanent(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch p.Mode {
	case "permanent":
		return nil, jobs.Permanentf("%s", p.Message)
	case "panic":
		panic(p.Message)
	case "until_attempt":
		if x.Attempt >= p.SucceedOnAttempt {
			return mustJSON(map[string]any{"succeeded_on_attempt": x.Attempt}), nil
		}
	}
	return nil, fmt.Errorf("%s (attempt %d)", p.Message, x.Attempt)
}
