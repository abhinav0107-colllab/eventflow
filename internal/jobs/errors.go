package jobs

import (
	"context"
	"errors"
	"fmt"
)

// Store-level sentinel errors.
var (
	ErrNotFound = errors.New("job not found")
	// ErrNotClaimable is returned when a delivered message refers to a job
	// that is not in the queued state (duplicate delivery, cancelled, already
	// running elsewhere). The message should be acknowledged and dropped.
	ErrNotClaimable = errors.New("job is not claimable")
	// ErrLeaseLost is returned when a fenced write no longer matches: the
	// worker's lease was revoked and another actor owns the job now.
	ErrLeaseLost = errors.New("job lease lost")
	// ErrIdempotencyConflict: the idempotency key was reused with a
	// different request body.
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")
	// ErrInvalidState: the requested operation is not valid for the job's
	// current status.
	ErrInvalidState = errors.New("operation not allowed in current job state")
)

// Execution-time causes attached to a handler context via
// context.WithCancelCause, used to classify why a handler stopped.
var (
	ErrCancelRequested = errors.New("job cancellation requested")
	ErrWorkerShutdown  = errors.New("worker shutting down")
)

// PermanentError marks a handler failure that must not be retried (for
// example invalid input or a 4xx from a downstream API).
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return "permanent: " + e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err so that the job is dead-lettered without retries.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// Permanentf is a fmt.Errorf shorthand for Permanent.
func Permanentf(format string, args ...any) error { return Permanent(fmt.Errorf(format, args...)) }

// PanicError is produced when a handler panics.
type PanicError struct{ Value any }

func (e *PanicError) Error() string { return fmt.Sprintf("handler panic: %v", e.Value) }

// ErrorClass names a failure category. Values are bounded so they can be used
// as metric labels.
type ErrorClass string

// Error classes.
const (
	ClassNone         ErrorClass = ""
	ClassError        ErrorClass = "error"         // generic, retryable
	ClassPermanent    ErrorClass = "permanent"     // not retryable
	ClassTimeout      ErrorClass = "timeout"       // attempt exceeded timeout_seconds
	ClassPanic        ErrorClass = "panic"         // handler panicked
	ClassCancelled    ErrorClass = "cancelled"     // client cancellation
	ClassInterrupted  ErrorClass = "interrupted"   // worker shutdown interrupted the attempt
	ClassLeaseLost    ErrorClass = "lease_lost"    // lease revoked while running
	ClassLeaseExpired ErrorClass = "lease_expired" // worker died; recovery closed the attempt
	ClassUnknownType  ErrorClass = "unknown_type"  // no handler registered
)

// Retryable reports whether a failure of this class may be retried.
func (c ErrorClass) Retryable() bool {
	switch c {
	case ClassPermanent, ClassCancelled, ClassUnknownType, ClassNone:
		return false
	}
	return true
}

// Classify maps a handler error, together with the cause recorded on the
// execution context, to an ErrorClass. The context cause wins over the error
// value because handlers commonly wrap or replace ctx.Err().
func Classify(err error, ctxCause error) ErrorClass {
	if err == nil {
		return ClassNone
	}
	switch {
	case errors.Is(ctxCause, ErrCancelRequested):
		return ClassCancelled
	case errors.Is(ctxCause, ErrLeaseLost):
		return ClassLeaseLost
	case errors.Is(ctxCause, ErrWorkerShutdown):
		return ClassInterrupted
	case errors.Is(ctxCause, context.DeadlineExceeded):
		return ClassTimeout
	}
	var perm *PermanentError
	var pe *PanicError
	switch {
	case errors.As(err, &perm):
		return ClassPermanent
	case errors.As(err, &pe):
		return ClassPanic
	case errors.Is(err, context.DeadlineExceeded):
		return ClassTimeout
	case errors.Is(err, ErrUnknownType):
		return ClassUnknownType
	}
	return ClassError
}
