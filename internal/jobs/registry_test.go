package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type stubHandler struct {
	validate func(json.RawMessage) error
	handle   func(context.Context, Execution) (json.RawMessage, error)
}

func (s stubHandler) Validate(p json.RawMessage) error {
	if s.validate == nil {
		return nil
	}
	return s.validate(p)
}

func (s stubHandler) Handle(ctx context.Context, e Execution) (json.RawMessage, error) {
	return s.handle(ctx, e)
}

func TestRegistryRegisterAndRun(t *testing.T) {
	r := NewRegistry()
	if err := r.Register("lower_case", stubHandler{}); err == nil {
		t.Fatal("expected invalid name to be rejected")
	}
	if err := r.Register("OK_TYPE", nil); err == nil {
		t.Fatal("expected nil handler to be rejected")
	}
	h := stubHandler{handle: func(_ context.Context, e Execution) (json.RawMessage, error) {
		return json.RawMessage(`{"attempt":` + string(rune('0'+e.Attempt)) + `}`), nil
	}}
	if err := r.Register("OK_TYPE", h); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("OK_TYPE", h); err == nil {
		t.Fatal("expected duplicate registration to fail")
	}
	out, err := r.Run(context.Background(), Execution{Type: "OK_TYPE", Attempt: 2})
	if err != nil || string(out) != `{"attempt":2}` {
		t.Fatalf("Run = %s, %v", out, err)
	}
	if _, err := r.Run(context.Background(), Execution{Type: "MISSING"}); !errors.Is(err, ErrUnknownType) {
		t.Fatalf("expected ErrUnknownType, got %v", err)
	}
	if got := r.Types(); len(got) != 1 || got[0] != "OK_TYPE" {
		t.Fatalf("Types = %v", got)
	}
}

func TestRegistryRecoversPanics(t *testing.T) {
	r := NewRegistry()
	r.MustRegister("PANICS", stubHandler{handle: func(context.Context, Execution) (json.RawMessage, error) {
		panic("kaboom")
	}})
	_, err := r.Run(context.Background(), Execution{Type: "PANICS"})
	var pe *PanicError
	if !errors.As(err, &pe) || pe.Value != "kaboom" {
		t.Fatalf("expected PanicError, got %v", err)
	}
	if Classify(err, nil) != ClassPanic {
		t.Fatal("panic should classify as panic")
	}
}
