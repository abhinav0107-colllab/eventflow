package jobs

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestClassify(t *testing.T) {
	plain := errors.New("boom")
	cases := []struct {
		name  string
		err   error
		cause error
		want  ErrorClass
	}{
		{"nil", nil, nil, ClassNone},
		{"generic", plain, nil, ClassError},
		{"wrapped permanent", fmt.Errorf("ctx: %w", Permanent(plain)), nil, ClassPermanent},
		{"panic", &PanicError{Value: "x"}, nil, ClassPanic},
		{"deadline error", context.DeadlineExceeded, nil, ClassTimeout},
		{"deadline cause wins", context.Canceled, context.DeadlineExceeded, ClassTimeout},
		{"cancel request cause", context.Canceled, ErrCancelRequested, ClassCancelled},
		{"lease lost cause", context.Canceled, ErrLeaseLost, ClassLeaseLost},
		{"shutdown cause", context.Canceled, ErrWorkerShutdown, ClassInterrupted},
		{"cause beats permanent", Permanent(plain), ErrCancelRequested, ClassCancelled},
		{"unknown type", fmt.Errorf("%w: X", ErrUnknownType), nil, ClassUnknownType},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Classify(c.err, c.cause); got != c.want {
				t.Fatalf("Classify = %q, want %q", got, c.want)
			}
		})
	}
}

func TestErrorClassRetryable(t *testing.T) {
	retryable := []ErrorClass{ClassError, ClassTimeout, ClassPanic, ClassInterrupted, ClassLeaseLost, ClassLeaseExpired}
	for _, c := range retryable {
		if !c.Retryable() {
			t.Errorf("%s should be retryable", c)
		}
	}
	for _, c := range []ErrorClass{ClassPermanent, ClassCancelled, ClassUnknownType, ClassNone} {
		if c.Retryable() {
			t.Errorf("%s should not be retryable", c)
		}
	}
}

func TestPermanentNil(t *testing.T) {
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must be nil")
	}
	err := Permanentf("bad %d", 4)
	if err.Error() != "permanent: bad 4" {
		t.Fatalf("unexpected message %q", err.Error())
	}
}
