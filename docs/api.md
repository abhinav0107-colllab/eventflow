# API guide

The complete contract is in [`api/openapi.yaml`](../api/openapi.yaml), which is
also served by the running API at `GET /openapi.yaml`. This page shows
practical examples.

```bash
export BASE=http://localhost:8080
export KEY=$(grep ^EVENTFLOW_API_KEYS .env | cut -d= -f2 | cut -d, -f1)
```

All `/api/v1` routes require `X-API-Key: $KEY` (or `Authorization: Bearer $KEY`).
Every response carries `X-Request-ID`; send your own (`[A-Za-z0-9._-]{1,64}`)
to correlate client and server logs.

## Submit jobs

```bash
curl -s -X POST $BASE/api/v1/jobs -H "X-API-Key: $KEY" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: signup-42-welcome' \
  -d '{
        "type": "EMAIL_SIMULATION",
        "payload": {"to": "user@example.com", "subject": "Welcome", "body": "Hello!"},
        "priority": 8,
        "max_attempts": 5,
        "timeout_seconds": 20
      }'
```

```json
{
  "id": "01a207c9-6a6c-7871-b051-bdf88e005f10",
  "type": "EMAIL_SIMULATION",
  "queue": "high",
  "payload": {"to": "user@example.com", "body": "Hello!", "subject": "Welcome"},
  "priority": 8,
  "status": "pending",
  "idempotency_key": "signup-42-welcome",
  "attempts": 0,
  "max_attempts": 5,
  "timeout_seconds": 20,
  "scheduled_at": "2026-11-23T09:42:50.220696Z",
  "created_at": "2026-11-23T09:42:50.220696Z",
  "updated_at": "2026-11-23T09:42:50.220696Z",
  "cancel_requested": false,
  "version": 1
}
```

| Field | Default | Notes |
|-------|---------|-------|
| `type` | required | `EMAIL_SIMULATION`, `WEBHOOK_SIMULATION`, `CPU_INTENSIVE_COMPUTATION`, `DELIBERATE_FAILURE`, `DELAYED_TASK` (`GET /api/v1/job-types`) |
| `payload` | required | JSON object, validated by the handler at submission time |
| `priority` | 5 | 1–10. 8–10 → `high`, 4–7 → `normal`, 1–3 → `low` |
| `max_attempts` | 3 | 1–100 |
| `timeout_seconds` | 30 | per attempt, 1–86400 |
| `scheduled_at` / `delay_seconds` | now | mutually exclusive; at most one year ahead |
| `idempotency_key` | – | or `Idempotency-Key` header (both must match if both are sent) |

### More examples

```bash
# Delayed: run in 30 seconds
curl -s -X POST $BASE/api/v1/jobs -H "X-API-Key: $KEY" \
  -d '{"type":"DELAYED_TASK","payload":{"duration_ms":2000,"message":"report"},"delay_seconds":30}'

# Scheduled at an absolute time
curl -s -X POST $BASE/api/v1/jobs -H "X-API-Key: $KEY" \
  -d '{"type":"WEBHOOK_SIMULATION","payload":{"url":"https://example.com/hook"},"scheduled_at":"2030-01-01T00:00:00Z"}'

# CPU-bound work
curl -s -X POST $BASE/api/v1/jobs -H "X-API-Key: $KEY" \
  -d '{"type":"CPU_INTENSIVE_COMPUTATION","payload":{"operation":"prime_count","n":5000000},"priority":2}'

# Retries then success on attempt 3
curl -s -X POST $BASE/api/v1/jobs -H "X-API-Key: $KEY" \
  -d '{"type":"DELIBERATE_FAILURE","payload":{"mode":"until_attempt","succeed_on_attempt":3},"max_attempts":5}'

# Straight to the dead-letter queue (non-retryable)
curl -s -X POST $BASE/api/v1/jobs -H "X-API-Key: $KEY" \
  -d '{"type":"WEBHOOK_SIMULATION","payload":{"url":"https://example.com","simulate_status":404}}'
```

### Idempotency semantics

| Situation | Response |
|-----------|----------|
| First request with key `k` | `201 Created` |
| Same request again with `k` (whitespace and key order in the payload do not matter) | `200 OK`, `Idempotent-Replayed: true`, original job |
| Different request with `k` | `409 idempotency_conflict` (`details.job_id` names the original) |
| Same `k` from a different API key | independent: new job |

## Read jobs

```bash
curl -s $BASE/api/v1/jobs/$ID -H "X-API-Key: $KEY"
curl -s $BASE/api/v1/jobs/$ID/attempts -H "X-API-Key: $KEY"

# Filters: status, type, queue, dead_lettered, created_after, created_before; limit 1-100
curl -s "$BASE/api/v1/jobs?status=failed&dead_lettered=true&limit=50" -H "X-API-Key: $KEY"
# Next page
curl -s "$BASE/api/v1/jobs?status=failed&dead_lettered=true&limit=50&cursor=$NEXT" -H "X-API-Key: $KEY"
```

Attempt history example (retry → success):

```json
{"data": [
  {"attempt_number": 1, "worker_id": "worker-1-ab12cd34", "status": "failed",  "duration_ms": 0, "error": "deliberate failure (attempt 1)", "error_class": "error"},
  {"attempt_number": 2, "worker_id": "worker-2-ef56aa01", "status": "failed",  "duration_ms": 0, "error": "deliberate failure (attempt 2)", "error_class": "error"},
  {"attempt_number": 3, "worker_id": "worker-1-ab12cd34", "status": "succeeded", "duration_ms": 0}
]}
```

## Cancel and retry

```bash
curl -s -X POST $BASE/api/v1/jobs/$ID/cancel -H "X-API-Key: $KEY"
#  200 -> was pending/queued, now cancelled
#  202 -> running; cancellation requested (worker stops it at its next heartbeat)
#  409 -> already terminal

curl -s -X POST $BASE/api/v1/jobs/$ID/retry -H "X-API-Key: $KEY" -d '{"additional_attempts": 2}'
#  200 -> failed/cancelled job re-submitted; 409 otherwise
```

## Operations

```bash
curl -s $BASE/api/v1/queues  -H "X-API-Key: $KEY"   # stream + DB stats, DLQ, outbox, workers
curl -s $BASE/api/v1/workers -H "X-API-Key: $KEY"
curl -s $BASE/health      # liveness
curl -s $BASE/ready       # 200 when PostgreSQL and Redis are reachable, 503 otherwise or while draining
curl -s $BASE/metrics     # Prometheus
```

## Errors

```json
{"error": {
  "code": "validation_failed",
  "message": "request validation failed",
  "request_id": "6f1c2a9e0b7d4c3e8a5f1b2d",
  "details": [{"field": "priority", "message": "must be between 1 and 10"}]
}}
```

| HTTP | `code` |
|------|--------|
| 400 | `invalid_json`, `invalid_request`, `invalid_query`, `invalid_id` |
| 401 | `unauthorized` |
| 404 | `not_found` |
| 405 | `method_not_allowed` |
| 409 | `idempotency_conflict`, `invalid_state` |
| 413 | `payload_too_large` |
| 422 | `validation_failed` |
| 500 | `internal_error` (details are logged server-side with the request ID, never returned) |
| 503 | `unavailable` |
