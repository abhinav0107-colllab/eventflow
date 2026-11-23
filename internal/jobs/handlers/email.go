package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/mail"
	"time"

	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
)

// EmailPayload is the EMAIL_SIMULATION input.
type EmailPayload struct {
	To                string `json:"to"`
	Subject           string `json:"subject"`
	Body              string `json:"body"`
	SimulateLatencyMS int    `json:"simulate_latency_ms"`
}

// Email simulates sending an email. No mail is ever sent. The simulated
// message ID is derived from the job's idempotency key, illustrating how a
// real provider integration would deduplicate retries.
type Email struct{}

func (Email) parse(raw json.RawMessage) (EmailPayload, error) {
	var p EmailPayload
	if err := decode(raw, &p); err != nil {
		return p, err
	}
	addr, err := mail.ParseAddress(p.To)
	if err != nil || addr.Address != p.To {
		return p, errors.New("to must be a bare email address such as user@example.com")
	}
	if l := len(p.Subject); l == 0 || l > 200 {
		return p, errors.New("subject must be 1-200 characters")
	}
	if len(p.Body) > 64<<10 {
		return p, errors.New("body must be at most 64 KiB")
	}
	if p.SimulateLatencyMS == 0 {
		p.SimulateLatencyMS = 50
	}
	return p, checkLatency(p.SimulateLatencyMS)
}

// Validate implements jobs.Handler.
func (e Email) Validate(raw json.RawMessage) error { _, err := e.parse(raw); return err }

// Handle implements jobs.Handler.
func (e Email) Handle(ctx context.Context, x jobs.Execution) (json.RawMessage, error) {
	p, err := e.parse(x.Payload)
	if err != nil {
		return nil, jobs.Permanent(err)
	}
	if err := sleep(ctx, time.Duration(p.SimulateLatencyMS)*time.Millisecond); err != nil {
		return nil, err
	}
	msgID := fmt.Sprintf("<%s@eventflow.simulated>", x.IdempotencyKey)
	x.Logger.Info("simulated email delivery", "message_id", msgID, "subject_len", len(p.Subject))
	return mustJSON(map[string]any{
		"simulated":  true,
		"message_id": msgID,
		"recipient":  p.To,
	}), nil
}
