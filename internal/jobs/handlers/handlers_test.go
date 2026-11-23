package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
)

func exec(typ, payload string, attempt int) jobs.Execution {
	id := uuid.New()
	return jobs.Execution{JobID: id, Type: typ, Attempt: attempt, MaxAttempts: 5, Payload: json.RawMessage(payload),
		IdempotencyKey: id.String(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestValidation(t *testing.T) {
	reg := NewRegistry()
	cases := []struct {
		typ, payload string
		ok           bool
	}{
		{TypeEmail, `{"to":"a@example.com","subject":"s","body":"b"}`, true},
		{TypeEmail, `{"to":"Bob <a@example.com>","subject":"s"}`, false},
		{TypeEmail, `{"to":"not-an-email","subject":"s"}`, false},
		{TypeEmail, `{"to":"a@example.com","subject":""}`, false},
		{TypeEmail, `{"to":"a@example.com","subject":"s","cc":"x"}`, false}, // unknown field
		{TypeEmail, `{"to":"a@example.com","subject":"s","simulate_latency_ms":999999}`, false},
		{TypeWebhook, `{"url":"https://example.com/hook"}`, true},
		{TypeWebhook, `{"url":"ftp://example.com"}`, false},
		{TypeWebhook, `{"url":"/relative"}`, false},
		{TypeWebhook, `{"url":"https://example.com","method":"DELETE"}`, false},
		{TypeWebhook, `{"url":"https://example.com","simulate_status":700}`, false},
		{TypeCPU, `{"operation":"prime_count","n":1000}`, true},
		{TypeCPU, `{"operation":"hash_chain","n":10,"seed":"x"}`, true},
		{TypeCPU, `{"operation":"prime_count","n":1}`, false},
		{TypeCPU, `{"operation":"prime_count","n":999999999}`, false},
		{TypeCPU, `{"operation":"mine_bitcoin","n":10}`, false},
		{TypeCPU, `{"operation":"prime_count","n":"10"}`, false},
		{TypeFailure, `{"mode":"always"}`, true},
		{TypeFailure, `{"mode":"until_attempt","succeed_on_attempt":3}`, true},
		{TypeFailure, `{"mode":"until_attempt"}`, false},
		{TypeFailure, `{"mode":"sometimes"}`, false},
		{TypeDelayed, `{"duration_ms":10}`, true},
		{TypeDelayed, `{"duration_ms":-1}`, false},
		{TypeDelayed, `{"duration_ms":99999999}`, false},
	}
	for _, c := range cases {
		h, err := reg.Get(c.typ)
		if err != nil {
			t.Fatal(err)
		}
		err = h.Validate(json.RawMessage(c.payload))
		if (err == nil) != c.ok {
			t.Errorf("%s %s: err=%v, want ok=%v", c.typ, c.payload, err, c.ok)
		}
	}
}

func TestEmailIsIdempotentlyKeyed(t *testing.T) {
	x := exec(TypeEmail, `{"to":"a@example.com","subject":"s","simulate_latency_ms":1}`, 1)
	out1, err := Email{}.Handle(context.Background(), x)
	if err != nil {
		t.Fatal(err)
	}
	x.Attempt = 2
	out2, _ := Email{}.Handle(context.Background(), x)
	if string(out1) != string(out2) || !strings.Contains(string(out1), x.IdempotencyKey) {
		t.Fatalf("message id must be stable across attempts: %s vs %s", out1, out2)
	}
}

func TestWebhookStatusClassification(t *testing.T) {
	cases := map[int]jobs.ErrorClass{200: jobs.ClassNone, 204: jobs.ClassNone, 500: jobs.ClassError,
		503: jobs.ClassError, 429: jobs.ClassError, 408: jobs.ClassError, 400: jobs.ClassPermanent, 404: jobs.ClassPermanent}
	for status, want := range cases {
		p := `{"url":"https://example.com","simulate_latency_ms":1,"simulate_status":` + itoa(status) + `}`
		_, err := Webhook{}.Handle(context.Background(), exec(TypeWebhook, p, 1))
		if got := jobs.Classify(err, nil); got != want {
			t.Errorf("status %d classified %q, want %q (err=%v)", status, got, want, err)
		}
	}
}

func itoa(i int) string { b, _ := json.Marshal(i); return string(b) }

func TestCPU(t *testing.T) {
	out, err := CPU{}.Handle(context.Background(), exec(TypeCPU, `{"operation":"prime_count","n":100000}`, 1))
	if err != nil || !strings.Contains(string(out), `"prime_count":9592`) {
		t.Fatalf("prime_count(100000) = %s, %v", out, err)
	}
	d1, _ := HashChain(context.Background(), "seed", 3)
	d2, _ := HashChain(context.Background(), "seed", 3)
	d3, _ := HashChain(context.Background(), "seed", 4)
	if d1 != d2 || d1 == d3 || len(d1) != 64 {
		t.Fatalf("hash chain not deterministic/distinct: %s %s %s", d1, d2, d3)
	}
}

func TestCPUHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CountPrimes(ctx, 10_000_000); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if _, err := HashChain(ctx, "x", 10_000_000); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func TestDeliberateFailureModes(t *testing.T) {
	_, err := Failure{}.Handle(context.Background(), exec(TypeFailure, `{"mode":"always"}`, 1))
	if jobs.Classify(err, nil) != jobs.ClassError {
		t.Fatalf("always: %v", err)
	}
	_, err = Failure{}.Handle(context.Background(), exec(TypeFailure, `{"mode":"permanent"}`, 1))
	if jobs.Classify(err, nil) != jobs.ClassPermanent {
		t.Fatalf("permanent: %v", err)
	}
	p := `{"mode":"until_attempt","succeed_on_attempt":3}`
	if _, err := (Failure{}).Handle(context.Background(), exec(TypeFailure, p, 2)); err == nil {
		t.Fatal("attempt 2 should fail")
	}
	if _, err := (Failure{}).Handle(context.Background(), exec(TypeFailure, p, 3)); err != nil {
		t.Fatalf("attempt 3 should succeed: %v", err)
	}
	// Panics are converted by the registry.
	_, err = NewRegistry().Run(context.Background(), exec(TypeFailure, `{"mode":"panic"}`, 1))
	if jobs.Classify(err, nil) != jobs.ClassPanic {
		t.Fatalf("panic mode: %v", err)
	}
}

func TestDelayedTaskTimeoutAndCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Delayed{}.Handle(ctx, exec(TypeDelayed, `{"duration_ms":5000}`, 1))
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("expected prompt deadline error, got %v after %s", err, time.Since(start))
	}
	if jobs.Classify(err, context.Cause(ctx)) != jobs.ClassTimeout {
		t.Fatal("should classify as timeout")
	}
	out, err := Delayed{}.Handle(context.Background(), exec(TypeDelayed, `{"duration_ms":5,"message":"hi"}`, 1))
	if err != nil || !strings.Contains(string(out), `"message":"hi"`) {
		t.Fatalf("delayed success: %s %v", out, err)
	}
}

func TestHandleRejectsInvalidPayloadPermanently(t *testing.T) {
	for _, typ := range NewRegistry().Types() {
		h, _ := NewRegistry().Get(typ)
		_, err := h.Handle(context.Background(), exec(typ, `{"definitely_unknown":true}`, 1))
		if jobs.Classify(err, nil) != jobs.ClassPermanent {
			t.Errorf("%s: invalid payload should be permanent, got %v", typ, err)
		}
	}
}
