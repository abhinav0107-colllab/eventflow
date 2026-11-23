// Package handlers provides the built-in example job handlers. None of them
// perform real network I/O: email and webhook delivery are simulated so the
// platform is safe to run anywhere.
package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
)

// Job type names.
const (
	TypeEmail    = "EMAIL_SIMULATION"
	TypeWebhook  = "WEBHOOK_SIMULATION"
	TypeCPU      = "CPU_INTENSIVE_COMPUTATION"
	TypeFailure  = "DELIBERATE_FAILURE"
	TypeDelayed  = "DELAYED_TASK"
	maxLatencyMS = 60_000
	maxDelayedMS = 3_600_000
)

// Register adds every built-in handler to r.
func Register(r *jobs.Registry) {
	r.MustRegister(TypeEmail, Email{})
	r.MustRegister(TypeWebhook, Webhook{})
	r.MustRegister(TypeCPU, CPU{})
	r.MustRegister(TypeFailure, Failure{})
	r.MustRegister(TypeDelayed, Delayed{})
}

// NewRegistry returns a registry with all built-in handlers.
func NewRegistry() *jobs.Registry {
	r := jobs.NewRegistry()
	Register(r)
	return r
}

// decode strictly unmarshals a payload, rejecting unknown fields so typos
// surface as validation errors instead of silently ignored options.
func decode(raw json.RawMessage, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("invalid payload: %s", cleanJSONError(err))
	}
	if dec.More() {
		return errors.New("invalid payload: trailing data")
	}
	return nil
}

func cleanJSONError(err error) string {
	var te *json.UnmarshalTypeError
	if errors.As(err, &te) {
		return fmt.Sprintf("field %q must be of type %s", te.Field, te.Type.String())
	}
	return err.Error()
}

// sleep waits for d or until ctx is done.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // only called with static, marshalable types
	}
	return b
}

func checkLatency(ms int) error {
	if ms < 0 || ms > maxLatencyMS {
		return fmt.Errorf("simulate_latency_ms must be between 0 and %d", maxLatencyMS)
	}
	return nil
}
