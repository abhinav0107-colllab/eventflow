package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
)

// WebhookPayload is the WEBHOOK_SIMULATION input.
type WebhookPayload struct {
	URL               string          `json:"url"`
	Method            string          `json:"method"`
	Body              json.RawMessage `json:"body"`
	SimulateStatus    int             `json:"simulate_status"`
	SimulateLatencyMS int             `json:"simulate_latency_ms"`
}

// Webhook simulates delivering an HTTP webhook. It never opens a network
// connection; the downstream response is controlled by simulate_status so
// that retry and dead-letter behaviour can be exercised:
// 2xx succeeds, 408/429/5xx are retryable, other 4xx are permanent.
type Webhook struct{}

func (Webhook) parse(raw json.RawMessage) (WebhookPayload, error) {
	var p WebhookPayload
	if err := decode(raw, &p); err != nil {
		return p, err
	}
	u, err := url.Parse(p.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return p, errors.New("url must be an absolute http(s) URL")
	}
	if len(p.URL) > 2048 {
		return p, errors.New("url must be at most 2048 characters")
	}
	p.Method = strings.ToUpper(p.Method)
	switch p.Method {
	case "":
		p.Method = "POST"
	case "POST", "PUT", "PATCH":
	default:
		return p, errors.New("method must be POST, PUT or PATCH")
	}
	if p.SimulateStatus == 0 {
		p.SimulateStatus = 200
	}
	if p.SimulateStatus < 100 || p.SimulateStatus > 599 {
		return p, errors.New("simulate_status must be a valid HTTP status code")
	}
	if p.SimulateLatencyMS == 0 {
		p.SimulateLatencyMS = 100
	}
	return p, checkLatency(p.SimulateLatencyMS)
}

// Validate implements jobs.Handler.
func (w Webhook) Validate(raw json.RawMessage) error { _, err := w.parse(raw); return err }

// Handle implements jobs.Handler.
func (w Webhook) Handle(ctx context.Context, x jobs.Execution) (json.RawMessage, error) {
	p, err := w.parse(x.Payload)
	if err != nil {
		return nil, jobs.Permanent(err)
	}
	if err := sleep(ctx, time.Duration(p.SimulateLatencyMS)*time.Millisecond); err != nil {
		return nil, err
	}
	s := p.SimulateStatus
	switch {
	case s >= 200 && s < 300:
		x.Logger.Info("simulated webhook delivered", "status", s, "method", p.Method)
		return mustJSON(map[string]any{
			"simulated":       true,
			"status":          s,
			"method":          p.Method,
			"idempotency_key": x.IdempotencyKey, // would be sent as an Idempotency-Key header
		}), nil
	case s == 408 || s == 429 || s >= 500:
		return nil, fmt.Errorf("webhook endpoint responded %d (retryable)", s)
	default:
		return nil, jobs.Permanentf("webhook endpoint responded %d", s)
	}
}
