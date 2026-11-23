package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ErrUnknownType is returned for job types with no registered handler.
var ErrUnknownType = errors.New("unknown job type")

// Execution is the context passed to a handler for one attempt.
//
// Idempotency contract: delivery is at-least-once. A handler may run more
// than once for the same job (e.g. after a worker crash or a lost lease), so
// any external side effect must be keyed by IdempotencyKey, which is stable
// across attempts of the same logical job.
type Execution struct {
	JobID          uuid.UUID
	Type           string
	Attempt        int
	MaxAttempts    int
	Payload        json.RawMessage
	IdempotencyKey string
	Deadline       time.Time
	Logger         *slog.Logger
}

// Handler executes one job type.
type Handler interface {
	// Validate checks a payload at submission time so that malformed jobs
	// are rejected synchronously with a 422 instead of failing later.
	Validate(payload json.RawMessage) error
	// Handle runs one attempt. It must honour ctx cancellation. Returning
	// an error wrapped with Permanent stops retries. The returned JSON (may
	// be nil) is stored as the job result.
	Handle(ctx context.Context, exec Execution) (json.RawMessage, error)
}

var typeName = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)

// Registry maps job type names to handlers. It is safe for concurrent use.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{handlers: map[string]Handler{}} }

// Register adds a handler. Type names are UPPER_SNAKE_CASE.
func (r *Registry) Register(name string, h Handler) error {
	if !typeName.MatchString(name) {
		return fmt.Errorf("invalid job type name %q", name)
	}
	if h == nil {
		return errors.New("nil handler")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.handlers[name]; dup {
		return fmt.Errorf("job type %q already registered", name)
	}
	r.handlers[name] = h
	return nil
}

// MustRegister is Register that panics on error (for static wiring).
func (r *Registry) MustRegister(name string, h Handler) {
	if err := r.Register(name, h); err != nil {
		panic(err)
	}
}

// Get returns the handler for a type.
func (r *Registry) Get(name string) (Handler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownType, name)
	}
	return h, nil
}

// Types returns the registered type names, sorted.
func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.handlers))
	for k := range r.handlers {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Run executes the handler for exec.Type, converting panics into
// *PanicError so that one misbehaving handler cannot crash the worker.
func (r *Registry) Run(ctx context.Context, exec Execution) (result json.RawMessage, err error) {
	h, err := r.Get(exec.Type)
	if err != nil {
		return nil, err
	}
	defer func() {
		if p := recover(); p != nil {
			result, err = nil, &PanicError{Value: p}
		}
	}()
	return h.Handle(ctx, exec)
}
