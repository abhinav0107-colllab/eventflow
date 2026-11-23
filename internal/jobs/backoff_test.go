package jobs

import (
	"testing"
	"time"
)

func TestBackoffBounds(t *testing.T) {
	b := Backoff{Base: time.Second, Max: 30 * time.Second}
	for attempt := 1; attempt <= 20; attempt++ {
		exp := time.Second << (attempt - 1)
		if exp > 30*time.Second || exp <= 0 {
			exp = 30 * time.Second
		}
		for i := 0; i < 200; i++ {
			d := b.Delay(attempt)
			if d < exp/2 || d > exp {
				t.Fatalf("attempt %d: delay %s outside [%s, %s]", attempt, d, exp/2, exp)
			}
		}
	}
}

func TestBackoffDeterministicWithRand(t *testing.T) {
	low := Backoff{Base: 100 * time.Millisecond, Max: time.Minute, Rand: func() float64 { return 0 }}
	high := Backoff{Base: 100 * time.Millisecond, Max: time.Minute, Rand: func() float64 { return 0.999999 }}
	want := []time.Duration{50, 100, 200, 400, 800}
	for i, w := range want {
		if got := low.Delay(i + 1); got != w*time.Millisecond {
			t.Errorf("min delay attempt %d = %s, want %s", i+1, got, w*time.Millisecond)
		}
		if got := high.Delay(i + 1); got < 2*w*time.Millisecond-time.Millisecond {
			t.Errorf("max delay attempt %d = %s, want ~%s", i+1, got, 2*w*time.Millisecond)
		}
	}
}

func TestBackoffIsJittered(t *testing.T) {
	b := Backoff{Base: time.Second, Max: time.Hour}
	seen := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		seen[b.Delay(5)] = true
	}
	if len(seen) < 10 {
		t.Fatalf("expected jittered delays, got only %d distinct values", len(seen))
	}
}

func TestBackoffHugeAttemptDoesNotOverflow(t *testing.T) {
	b := Backoff{Base: time.Second, Max: 10 * time.Minute}
	if d := b.Delay(10_000); d <= 0 || d > 10*time.Minute {
		t.Fatalf("delay overflowed: %s", d)
	}
	if d := b.Delay(0); d <= 0 {
		t.Fatalf("attempt 0 should be treated as 1, got %s", d)
	}
}

func TestDecideFailure(t *testing.T) {
	b := Backoff{Base: time.Second, Max: time.Minute, Rand: func() float64 { return 0 }}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name          string
		attempts, max int
		class         ErrorClass
		retry         bool
	}{
		{"retryable under budget", 1, 3, ClassError, true},
		{"timeout under budget", 2, 3, ClassTimeout, true},
		{"lease expired under budget", 1, 2, ClassLeaseExpired, true},
		{"budget exhausted", 3, 3, ClassError, false},
		{"permanent never retries", 1, 10, ClassPermanent, false},
		{"unknown type never retries", 1, 10, ClassUnknownType, false},
		{"cancelled never retries", 1, 10, ClassCancelled, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := DecideFailure(c.attempts, c.max, c.class, b, now)
			if d.Retry != c.retry {
				t.Fatalf("retry = %v, want %v", d.Retry, c.retry)
			}
			if d.Retry && !d.RetryAt.After(now) {
				t.Fatalf("retry_at %s must be after now", d.RetryAt)
			}
		})
	}
}
