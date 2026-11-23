package jobs

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func testRegistry() *Registry {
	r := NewRegistry()
	r.MustRegister("ECHO", stubHandler{validate: func(p json.RawMessage) error {
		var v map[string]any
		if err := json.Unmarshal(p, &v); err != nil {
			return err
		}
		if _, ok := v["bad"]; ok {
			return errors.New("bad field present")
		}
		return nil
	}})
	return r
}

var defaults = Defaults{MaxAttempts: 3, TimeoutSeconds: 30, MaxPayloadBytes: 1024}

func ptr[T any](v T) *T { return &v }

func fieldNames(err error) []string {
	var ve *ValidationError
	if !errors.As(err, &ve) {
		return nil
	}
	var out []string
	for _, f := range ve.Fields {
		out = append(out, f.Field)
	}
	return out
}

func TestNormalizeDefaults(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	p, err := Normalize(CreateRequest{Type: "ECHO", Payload: json.RawMessage(` { "a" : 1 } `)}, testRegistry(), defaults, now)
	if err != nil {
		t.Fatal(err)
	}
	if p.Priority != DefaultPriority || p.MaxAttempts != 3 || p.TimeoutSeconds != 30 {
		t.Fatalf("defaults not applied: %+v", p)
	}
	if p.ScheduledAt != nil || p.IdempotencyKey != nil || p.RequestHash != nil {
		t.Fatalf("unexpected optional values: %+v", p)
	}
	if string(p.Payload) != `{"a":1}` {
		t.Fatalf("payload not compacted: %s", p.Payload)
	}
}

func TestNormalizeValidationErrors(t *testing.T) {
	now := time.Now()
	reg := testRegistry()
	cases := []struct {
		name  string
		req   CreateRequest
		field string
	}{
		{"missing type", CreateRequest{Payload: json.RawMessage(`{}`)}, "type"},
		{"unknown type", CreateRequest{Type: "NOPE", Payload: json.RawMessage(`{}`)}, "type"},
		{"missing payload", CreateRequest{Type: "ECHO"}, "payload"},
		{"null payload", CreateRequest{Type: "ECHO", Payload: json.RawMessage(`null`)}, "payload"},
		{"array payload", CreateRequest{Type: "ECHO", Payload: json.RawMessage(`[1]`)}, "payload"},
		{"handler rejects", CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{"bad":1}`)}, "payload"},
		{"payload too big", CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{"a":"` + strings.Repeat("x", 2000) + `"}`)}, "payload"},
		{"priority low", CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{}`), Priority: ptr(0)}, "priority"},
		{"priority high", CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{}`), Priority: ptr(11)}, "priority"},
		{"attempts zero", CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{}`), MaxAttempts: ptr(0)}, "max_attempts"},
		{"attempts huge", CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{}`), MaxAttempts: ptr(101)}, "max_attempts"},
		{"timeout zero", CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{}`), TimeoutSeconds: ptr(0)}, "timeout_seconds"},
		{"negative delay", CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{}`), DelaySeconds: ptr(-1)}, "delay_seconds"},
		{"both schedules", CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{}`), DelaySeconds: ptr(1), ScheduledAt: ptr(now)}, "scheduled_at"},
		{"too far ahead", CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{}`), ScheduledAt: ptr(now.Add(400 * 24 * time.Hour))}, "scheduled_at"},
		{"key too long", CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{}`), IdempotencyKey: strings.Repeat("k", 256)}, "idempotency_key"},
		{"key non-ascii", CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{}`), IdempotencyKey: "клю\n"}, "idempotency_key"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Normalize(c.req, reg, defaults, now)
			fields := fieldNames(err)
			found := false
			for _, f := range fields {
				found = found || f == c.field
			}
			if !found {
				t.Fatalf("expected error on %q, got %v (%v)", c.field, fields, err)
			}
		})
	}
}

func TestNormalizeCollectsAllErrors(t *testing.T) {
	_, err := Normalize(CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{}`), Priority: ptr(99), MaxAttempts: ptr(0)},
		testRegistry(), defaults, time.Now())
	if got := fieldNames(err); len(got) != 2 {
		t.Fatalf("expected 2 field errors, got %v", got)
	}
}

func TestNormalizeScheduling(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	p, err := Normalize(CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{}`), DelaySeconds: ptr(90)}, testRegistry(), defaults, now)
	if err != nil || p.ScheduledAt == nil || !p.ScheduledAt.Equal(now.Add(90*time.Second)) {
		t.Fatalf("delay_seconds not applied: %v %v", p.ScheduledAt, err)
	}
	at := now.Add(time.Hour).In(time.FixedZone("X", 3600))
	p, err = Normalize(CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{}`), ScheduledAt: &at}, testRegistry(), defaults, now)
	if err != nil || !p.ScheduledAt.Equal(at) || p.ScheduledAt.Location() != time.UTC {
		t.Fatalf("scheduled_at not normalized to UTC: %v %v", p.ScheduledAt, err)
	}
}

func TestRequestFingerprint(t *testing.T) {
	base := CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{"a":1,"b":[1,2]}`), Priority: ptr(5)}
	reordered := CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{ "b": [1, 2], "a": 1 }`), Priority: ptr(5)}
	if !bytes.Equal(RequestFingerprint(base), RequestFingerprint(reordered)) {
		t.Fatal("fingerprint must ignore key order and whitespace")
	}
	variants := []CreateRequest{
		{Type: "OTHER", Payload: base.Payload, Priority: ptr(5)},
		{Type: "ECHO", Payload: json.RawMessage(`{"a":2,"b":[1,2]}`), Priority: ptr(5)},
		{Type: "ECHO", Payload: base.Payload, Priority: ptr(6)},
		{Type: "ECHO", Payload: base.Payload},
		{Type: "ECHO", Payload: base.Payload, Priority: ptr(5), MaxAttempts: ptr(2)},
	}
	for i, v := range variants {
		if bytes.Equal(RequestFingerprint(base), RequestFingerprint(v)) {
			t.Errorf("variant %d should have a different fingerprint", i)
		}
	}
}

func TestNormalizeSetsIdempotencyHash(t *testing.T) {
	p, err := Normalize(CreateRequest{Type: "ECHO", Payload: json.RawMessage(`{}`), IdempotencyKey: "order-42"},
		testRegistry(), defaults, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if p.IdempotencyKey == nil || *p.IdempotencyKey != "order-42" || len(p.RequestHash) != 32 {
		t.Fatalf("idempotency not populated: %+v", p)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	c := Cursor{CreatedAt: time.Date(2026, 3, 4, 5, 6, 7, 123456000, time.UTC), ID: [16]byte{1, 2, 3}}
	got, err := DecodeCursor(EncodeCursor(c))
	if err != nil || !got.CreatedAt.Equal(c.CreatedAt) || got.ID != c.ID {
		t.Fatalf("round trip failed: %+v %v", got, err)
	}
	for _, bad := range []string{"!!!", "bm9waXBl", EncodeCursor(c)[:5]} {
		if _, err := DecodeCursor(bad); err == nil {
			t.Errorf("DecodeCursor(%q) should fail", bad)
		}
	}
}
