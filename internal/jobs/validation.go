package jobs

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"
)

// Submission limits.
const (
	MaxIdempotencyKeyLen = 255
	MaxTimeoutSeconds    = 86400
	MaxAttemptsLimit     = 100
	MaxScheduleAhead     = 365 * 24 * time.Hour
)

// CreateRequest is the client-facing submission payload.
type CreateRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       *int            `json:"priority,omitempty"`
	MaxAttempts    *int            `json:"max_attempts,omitempty"`
	TimeoutSeconds *int            `json:"timeout_seconds,omitempty"`
	ScheduledAt    *time.Time      `json:"scheduled_at,omitempty"`
	DelaySeconds   *int            `json:"delay_seconds,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

// FieldError describes one invalid field.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationError aggregates field errors.
type ValidationError struct{ Fields []FieldError }

func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Fields))
	for i, f := range e.Fields {
		parts[i] = f.Field + ": " + f.Message
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

func (e *ValidationError) add(field, format string, args ...any) {
	e.Fields = append(e.Fields, FieldError{Field: field, Message: fmt.Sprintf(format, args...)})
}

// Defaults applied when the client omits optional fields.
type Defaults struct {
	MaxAttempts     int
	TimeoutSeconds  int
	MaxPayloadBytes int
}

// Normalize validates req and converts it into CreateParams (without the
// API key, which the caller supplies). now is injected for testability.
func Normalize(req CreateRequest, reg *Registry, def Defaults, now time.Time) (CreateParams, error) {
	ve := &ValidationError{}
	p := CreateParams{Type: req.Type}

	if req.Type == "" {
		ve.add("type", "is required")
	} else if h, err := reg.Get(req.Type); err != nil {
		ve.add("type", "unknown job type; supported: %s", strings.Join(reg.Types(), ", "))
	} else {
		payload := bytes.TrimSpace(req.Payload)
		switch {
		case len(payload) == 0 || bytes.Equal(payload, []byte("null")):
			ve.add("payload", "is required and must be a JSON object")
		case payload[0] != '{':
			ve.add("payload", "must be a JSON object")
		case def.MaxPayloadBytes > 0 && len(payload) > def.MaxPayloadBytes:
			ve.add("payload", "exceeds the maximum size of %d bytes", def.MaxPayloadBytes)
		default:
			if err := h.Validate(payload); err != nil {
				ve.add("payload", "%s", err.Error())
			}
			p.Payload = compactJSON(payload)
		}
	}

	p.Priority = DefaultPriority
	if req.Priority != nil {
		if *req.Priority < MinPriority || *req.Priority > MaxPriority {
			ve.add("priority", "must be between %d and %d", MinPriority, MaxPriority)
		}
		p.Priority = *req.Priority
	}

	p.MaxAttempts = def.MaxAttempts
	if req.MaxAttempts != nil {
		if *req.MaxAttempts < 1 || *req.MaxAttempts > MaxAttemptsLimit {
			ve.add("max_attempts", "must be between 1 and %d", MaxAttemptsLimit)
		}
		p.MaxAttempts = *req.MaxAttempts
	}

	p.TimeoutSeconds = def.TimeoutSeconds
	if req.TimeoutSeconds != nil {
		if *req.TimeoutSeconds < 1 || *req.TimeoutSeconds > MaxTimeoutSeconds {
			ve.add("timeout_seconds", "must be between 1 and %d", MaxTimeoutSeconds)
		}
		p.TimeoutSeconds = *req.TimeoutSeconds
	}

	switch {
	case req.ScheduledAt != nil && req.DelaySeconds != nil:
		ve.add("scheduled_at", "cannot be combined with delay_seconds")
	case req.ScheduledAt != nil:
		if req.ScheduledAt.After(now.Add(MaxScheduleAhead)) {
			ve.add("scheduled_at", "must be within one year")
		}
		t := req.ScheduledAt.UTC()
		p.ScheduledAt = &t
	case req.DelaySeconds != nil:
		d := time.Duration(*req.DelaySeconds) * time.Second
		if *req.DelaySeconds < 0 || d > MaxScheduleAhead {
			ve.add("delay_seconds", "must be between 0 and %d", int(MaxScheduleAhead/time.Second))
		}
		t := now.Add(d).UTC()
		p.ScheduledAt = &t
	}

	if req.IdempotencyKey != "" {
		if err := ValidateIdempotencyKey(req.IdempotencyKey); err != nil {
			ve.add("idempotency_key", "%s", err.Error())
		}
		k := req.IdempotencyKey
		p.IdempotencyKey = &k
		p.RequestHash = RequestFingerprint(req)
	}

	if len(ve.Fields) > 0 {
		return CreateParams{}, ve
	}
	return p, nil
}

// ValidateIdempotencyKey enforces a printable, bounded key.
func ValidateIdempotencyKey(k string) error {
	if len(k) > MaxIdempotencyKeyLen {
		return fmt.Errorf("must be at most %d characters", MaxIdempotencyKeyLen)
	}
	for _, r := range k {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) {
			return fmt.Errorf("must contain printable ASCII characters only")
		}
	}
	return nil
}

// RequestFingerprint hashes the semantically relevant parts of a request so
// that reusing an idempotency key with a different body can be detected.
// Relative delays are excluded because a legitimate client retry of the same
// request arrives later in time.
func RequestFingerprint(req CreateRequest) []byte {
	h := sha256.New()
	fmt.Fprintf(h, "type=%s\n", req.Type)
	h.Write(canonicalJSON(req.Payload))
	writeOpt := func(name string, v *int) {
		if v != nil {
			fmt.Fprintf(h, "\n%s=%d", name, *v)
		}
	}
	writeOpt("priority", req.Priority)
	writeOpt("max_attempts", req.MaxAttempts)
	writeOpt("timeout_seconds", req.TimeoutSeconds)
	writeOpt("delay_seconds", req.DelaySeconds)
	if req.ScheduledAt != nil {
		fmt.Fprintf(h, "\nscheduled_at=%d", req.ScheduledAt.UnixNano())
	}
	return h.Sum(nil)
}

// canonicalJSON re-encodes JSON so that whitespace and key order do not
// affect fingerprints (encoding/json sorts map keys).
func canonicalJSON(raw json.RawMessage) []byte {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return raw
	}
	out, err := json.Marshal(v)
	if err != nil {
		return raw
	}
	return out
}

func compactJSON(raw []byte) json.RawMessage {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return json.RawMessage(raw)
	}
	return buf.Bytes()
}

// FailureDecision is the result of DecideFailure.
type FailureDecision struct {
	Retry   bool
	RetryAt time.Time
}

// DecideFailure determines whether a failed attempt is retried and when.
// attempts is the number of attempts made so far, including the failed one.
func DecideFailure(attempts, maxAttempts int, class ErrorClass, b Backoff, now time.Time) FailureDecision {
	if !class.Retryable() || attempts >= maxAttempts {
		return FailureDecision{}
	}
	return FailureDecision{Retry: true, RetryAt: now.Add(b.Delay(attempts))}
}
