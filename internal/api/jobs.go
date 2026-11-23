package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
	mw "github.com/abhinav0107-colllab/eventflow/internal/middleware"
)

// IdempotencyHeader may carry the idempotency key instead of the body field.
const IdempotencyHeader = "Idempotency-Key"

// decodeJSON strictly decodes a JSON request body into v. It writes the
// error response itself and returns false on failure.
func (s *Server) decodeJSON(w http.ResponseWriter, r *http.Request, v any, allowEmpty bool) bool {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			mw.WriteError(w, r, http.StatusRequestEntityTooLarge, "payload_too_large",
				fmt.Sprintf("request body exceeds %d bytes", mbe.Limit), nil)
			return false
		}
		mw.WriteError(w, r, http.StatusBadRequest, "invalid_request", "could not read request body", nil)
		return false
	}
	if len(bytes.TrimSpace(body)) == 0 {
		if allowEmpty {
			return true
		}
		mw.WriteError(w, r, http.StatusBadRequest, "invalid_json", "request body is required", nil)
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		mw.WriteError(w, r, http.StatusBadRequest, "invalid_json", jsonErrorMessage(err), nil)
		return false
	}
	if dec.More() {
		mw.WriteError(w, r, http.StatusBadRequest, "invalid_json", "request body must contain a single JSON object", nil)
		return false
	}
	return true
}

func jsonErrorMessage(err error) string {
	var se *json.SyntaxError
	var te *json.UnmarshalTypeError
	switch {
	case errors.As(err, &se):
		return fmt.Sprintf("malformed JSON at offset %d", se.Offset)
	case errors.As(err, &te):
		return fmt.Sprintf("field %q must be of type %s", te.Field, te.Type.String())
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "malformed JSON: unexpected end of input"
	}
	// Unknown-field errors from encoding/json are safe to echo.
	return "invalid JSON: " + err.Error()
}

func (s *Server) internalError(w http.ResponseWriter, r *http.Request, op string, err error) {
	s.log.Error("request failed", "request_id", mw.RequestID(r.Context()), "op", op, "error", err.Error())
	mw.WriteError(w, r, http.StatusInternalServerError, "internal_error", "internal server error", nil)
}

func (s *Server) jobID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		mw.WriteError(w, r, http.StatusBadRequest, "invalid_id", "job id must be a UUID", nil)
		return uuid.Nil, false
	}
	return id, true
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	principal, ok := mw.PrincipalFrom(r.Context())
	if !ok {
		mw.WriteError(w, r, http.StatusUnauthorized, "unauthorized", "missing API key", nil)
		return
	}
	var req jobs.CreateRequest
	if !s.decodeJSON(w, r, &req, false) {
		return
	}
	if hk := r.Header.Get(IdempotencyHeader); hk != "" {
		if req.IdempotencyKey != "" && req.IdempotencyKey != hk {
			mw.WriteError(w, r, http.StatusBadRequest, "invalid_request",
				"Idempotency-Key header and idempotency_key field differ", nil)
			return
		}
		req.IdempotencyKey = hk
	}
	params, err := jobs.Normalize(req, s.handlers, jobs.Defaults{
		MaxAttempts:     s.cfg.DefaultMaxAttempts,
		TimeoutSeconds:  int(s.cfg.DefaultTimeout / time.Second),
		MaxPayloadBytes: s.cfg.MaxPayloadBytes,
	}, s.now())
	var ve *jobs.ValidationError
	if errors.As(err, &ve) {
		mw.WriteError(w, r, http.StatusUnprocessableEntity, "validation_failed", "request validation failed", ve.Fields)
		return
	}
	if err != nil {
		s.internalError(w, r, "normalize", err)
		return
	}
	params.APIKeyID = principal.KeyID

	job, created, err := s.store.Create(r.Context(), params)
	switch {
	case errors.Is(err, jobs.ErrIdempotencyConflict):
		mw.WriteError(w, r, http.StatusConflict, "idempotency_conflict",
			"idempotency key was already used with a different request", map[string]string{"job_id": job.ID.String()})
		return
	case err != nil:
		s.internalError(w, r, "create job", err)
		return
	}
	w.Header().Set("Location", "/api/v1/jobs/"+job.ID.String())
	if !created {
		w.Header().Set("Idempotent-Replayed", "true")
		mw.WriteJSON(w, http.StatusOK, job)
		return
	}
	s.metrics.JobsSubmitted.WithLabelValues(job.Type).Inc()
	s.log.Info("job submitted", "request_id", mw.RequestID(r.Context()), "job_id", job.ID.String(),
		"job_type", job.Type, "priority", job.Priority, "scheduled_at", job.ScheduledAt.Format(time.RFC3339Nano))
	mw.WriteJSON(w, http.StatusCreated, job)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, ok := s.jobID(w, r)
	if !ok {
		return
	}
	job, err := s.store.Get(r.Context(), id)
	if errors.Is(err, jobs.ErrNotFound) {
		mw.WriteError(w, r, http.StatusNotFound, "not_found", "job not found", nil)
		return
	}
	if err != nil {
		s.internalError(w, r, "get job", err)
		return
	}
	mw.WriteJSON(w, http.StatusOK, job)
}

// ListResponse is the paginated list envelope.
type ListResponse struct {
	Data       []*jobs.Job `json:"data"`
	NextCursor string      `json:"next_cursor,omitempty"`
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := jobs.ListFilter{Type: q.Get("type"), Queue: q.Get("queue"), Cursor: q.Get("cursor"), Limit: 20}
	var problems []jobs.FieldError
	bad := func(field, msg string) { problems = append(problems, jobs.FieldError{Field: field, Message: msg}) }

	if v := q.Get("status"); v != "" {
		st := jobs.Status(v)
		if !st.Valid() {
			bad("status", "must be one of pending, queued, running, succeeded, failed, cancelled")
		}
		f.Status = &st
	}
	if f.Queue != "" {
		switch jobs.Queue(f.Queue) {
		case jobs.QueueHigh, jobs.QueueNormal, jobs.QueueLow:
		default:
			bad("queue", "must be one of high, normal, low")
		}
	}
	if len(f.Type) > 64 {
		bad("type", "must be at most 64 characters")
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 100 {
			bad("limit", "must be an integer between 1 and 100")
		}
		f.Limit = n
	}
	if v := q.Get("dead_lettered"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			bad("dead_lettered", "must be true or false")
		}
		f.DeadLettered = &b
	}
	for _, p := range []struct {
		name string
		dst  **time.Time
	}{{"created_after", &f.CreatedAfter}, {"created_before", &f.CreatedBefore}} {
		if v := q.Get(p.name); v != "" {
			t, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				bad(p.name, "must be an RFC 3339 timestamp")
			}
			*p.dst = &t
		}
	}
	if f.Cursor != "" {
		if _, err := jobs.DecodeCursor(f.Cursor); err != nil {
			bad("cursor", "is malformed")
		}
	}
	if len(problems) > 0 {
		mw.WriteError(w, r, http.StatusBadRequest, "invalid_query", "invalid query parameters", problems)
		return
	}
	list, next, err := s.store.List(r.Context(), f)
	if err != nil {
		s.internalError(w, r, "list jobs", err)
		return
	}
	if list == nil {
		list = []*jobs.Job{}
	}
	mw.WriteJSON(w, http.StatusOK, ListResponse{Data: list, NextCursor: next})
}

// RetryRequest is the optional body for POST /jobs/{id}/retry.
type RetryRequest struct {
	AdditionalAttempts *int `json:"additional_attempts,omitempty"`
}

func (s *Server) retryJob(w http.ResponseWriter, r *http.Request) {
	id, ok := s.jobID(w, r)
	if !ok {
		return
	}
	var req RetryRequest
	if !s.decodeJSON(w, r, &req, true) {
		return
	}
	extra := s.cfg.DefaultMaxAttempts
	if req.AdditionalAttempts != nil {
		if *req.AdditionalAttempts < 1 || *req.AdditionalAttempts > jobs.MaxAttemptsLimit {
			mw.WriteError(w, r, http.StatusUnprocessableEntity, "validation_failed", "request validation failed",
				[]jobs.FieldError{{Field: "additional_attempts", Message: fmt.Sprintf("must be between 1 and %d", jobs.MaxAttemptsLimit)}})
			return
		}
		extra = *req.AdditionalAttempts
	}
	job, err := s.store.Retry(r.Context(), id, extra)
	switch {
	case errors.Is(err, jobs.ErrNotFound):
		mw.WriteError(w, r, http.StatusNotFound, "not_found", "job not found", nil)
	case errors.Is(err, jobs.ErrInvalidState):
		mw.WriteError(w, r, http.StatusConflict, "invalid_state",
			fmt.Sprintf("only failed or cancelled jobs can be retried (current status: %s)", job.Status),
			map[string]string{"status": string(job.Status)})
	case err != nil:
		s.internalError(w, r, "retry job", err)
	default:
		s.log.Info("job manually retried", "request_id", mw.RequestID(r.Context()), "job_id", id.String(),
			"additional_attempts", extra)
		mw.WriteJSON(w, http.StatusOK, job)
	}
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	id, ok := s.jobID(w, r)
	if !ok {
		return
	}
	job, outcome, err := s.store.Cancel(r.Context(), id)
	switch {
	case errors.Is(err, jobs.ErrNotFound):
		mw.WriteError(w, r, http.StatusNotFound, "not_found", "job not found", nil)
	case errors.Is(err, jobs.ErrInvalidState):
		mw.WriteError(w, r, http.StatusConflict, "invalid_state",
			fmt.Sprintf("job is already %s", job.Status), map[string]string{"status": string(job.Status)})
	case err != nil:
		s.internalError(w, r, "cancel job", err)
	case outcome == jobs.CancelRequested:
		s.log.Info("job cancellation requested", "request_id", mw.RequestID(r.Context()), "job_id", id.String())
		mw.WriteJSON(w, http.StatusAccepted, job)
	default:
		s.log.Info("job cancelled", "request_id", mw.RequestID(r.Context()), "job_id", id.String())
		mw.WriteJSON(w, http.StatusOK, job)
	}
}

func (s *Server) listAttempts(w http.ResponseWriter, r *http.Request) {
	id, ok := s.jobID(w, r)
	if !ok {
		return
	}
	attempts, err := s.store.ListAttempts(r.Context(), id)
	if errors.Is(err, jobs.ErrNotFound) {
		mw.WriteError(w, r, http.StatusNotFound, "not_found", "job not found", nil)
		return
	}
	if err != nil {
		s.internalError(w, r, "list attempts", err)
		return
	}
	mw.WriteJSON(w, http.StatusOK, map[string]any{"data": attempts})
}

func (s *Server) listJobTypes(w http.ResponseWriter, _ *http.Request) {
	mw.WriteJSON(w, http.StatusOK, map[string]any{"data": s.handlers.Types()})
}

func (s *Server) listWorkers(w http.ResponseWriter, r *http.Request) {
	list, err := s.workers.List(r.Context(), 100)
	if err != nil {
		s.internalError(w, r, "list workers", err)
		return
	}
	mw.WriteJSON(w, http.StatusOK, map[string]any{"data": list})
}
