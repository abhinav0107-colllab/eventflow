# Operations guide

## Configuration reference

All settings are environment variables with the `EVENTFLOW_` prefix, and they
are validated at start-up: the process exits with a list of every problem
rather than failing later.

### Shared (api, worker, migrate)

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | **required** | PostgreSQL DSN (never logged) |
| `DB_MAX_CONNS` / `DB_MIN_CONNS` | 20 / 2 | pgx pool bounds |
| `REDIS_ADDR` | `localhost:6379` | |
| `REDIS_PASSWORD` / `REDIS_DB` | – / 0 | |
| `STREAM_PREFIX` | `eventflow` | Redis key prefix (`<prefix>:jobs:<queue>`) |
| `CONSUMER_GROUP` | `eventflow-workers` | |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `SHUTDOWN_TIMEOUT` | 30s | HTTP drain (api) / in-flight job grace period (worker) |

### API (`cmd/api`)

| Variable | Default | Description |
|----------|---------|-------------|
| `API_KEYS` | **required** | Comma-separated keys (≥ 16 chars), stored as SHA-256 in `api_keys` |
| `HTTP_ADDR` | `:8080` | |
| `MAX_BODY_BYTES` | 1 MiB | Request body limit (413 above) |
| `MAX_PAYLOAD_BYTES` | 256 KiB | Job payload limit (422 above) |
| `DEFAULT_MAX_ATTEMPTS` | 3 | Also the default for manual retry |
| `DEFAULT_TIMEOUT` | 30s | Default `timeout_seconds` |
| `HTTP_READ_TIMEOUT` / `HTTP_WRITE_TIMEOUT` | 15s / 30s | |
| `OUTBOX_ENABLED` | true | Run the outbox relay in this process |
| `OUTBOX_POLL_INTERVAL` | 200ms | Relay poll interval when idle (it loops immediately while busy) |
| `OUTBOX_BATCH_SIZE` | 200 | Events per relay transaction |
| `OUTBOX_RETRY_BASE_DELAY` / `OUTBOX_RETRY_MAX_DELAY` | 500ms / 30s | Publish failure backoff |
| `OUTBOX_RETENTION` | 72h | Published/discarded events older than this are deleted |
| `STATS_INTERVAL` | 5s | Refresh of queue/outbox/worker gauges |
| `WORKER_DEAD_AFTER` | 45s | Heartbeat age after which a worker no longer counts as active |

### Worker (`cmd/worker`)

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_ID` | `host-pid-random` | Also the Redis consumer name |
| `WORKER_CONCURRENCY` | 8 | Executor goroutines |
| `WORKER_METRICS_ADDR` | `:9091` | `/metrics`, `/health`, `/ready` |
| `LEASE_DURATION` | 30s | Job lease; must be at least 2× the heartbeat interval |
| `HEARTBEAT_INTERVAL` | 10s | Lease renewal and cancellation polling |
| `BLOCK_TIMEOUT` | 2s | `XREADGROUP BLOCK` (bounds shutdown latency) |
| `CLAIM_INTERVAL` / `CLAIM_MIN_IDLE` | 15s / 60s | `XAUTOCLAIM` cadence and idle threshold |
| `RECOVERY_INTERVAL` | 5s | Recovery manager cadence |
| `STUCK_QUEUED_AFTER` | 2m | Age after which a `queued` job with a drained stream is re-published |
| `WORKER_DEAD_AFTER` | 45s | Mark workers dead after this heartbeat age |
| `RETRY_BASE_DELAY` / `RETRY_MAX_DELAY` | 2s / 10m | Job retry backoff |
| `WORKER_OUTBOX_ENABLED` | false | Also run an outbox relay in the worker |

**Choosing lease and heartbeat.** The lease bounds how long a crashed
worker's job stays stuck: worst case about `LEASE_DURATION + RECOVERY_INTERVAL`.
A shorter lease recovers faster, but a GC pause or brief network blip longer
than the lease causes an unnecessary re-execution. Keep
`HEARTBEAT_INTERVAL ≤ LEASE_DURATION / 3` in production.

## Running

```bash
make env            # .env with random secrets (or copy .env.example and edit)
make up             # build + start everything
make smoke          # end-to-end check
make logs
docker compose up -d --scale worker=4   # scale workers horizontally
make down           # stop (keep data)   |   make clean-stack  # stop and DELETE volumes
```

| URL | What |
|-----|------|
| http://localhost:8080 | API (`/openapi.yaml`, `/metrics`, `/ready`) |
| http://localhost:9090 | Prometheus (targets: api + every worker replica) |
| http://localhost:3000 | Grafana: user `admin`, password `GRAFANA_ADMIN_PASSWORD` from `.env`; the EventFlow dashboard is the home page |

Running the binaries directly (with PostgreSQL and Redis available):

```bash
export EVENTFLOW_DATABASE_URL=postgres://user:pass@localhost:5432/eventflow?sslmode=disable
export EVENTFLOW_API_KEYS=dev-key-at-least-16-chars
make build
./bin/migrate up
./bin/api &
EVENTFLOW_WORKER_METRICS_ADDR=:9191 ./bin/worker
```

## Security notes

* API keys: only SHA-256 digests are stored. Keys from `EVENTFLOW_API_KEYS`
  are inserted at start-up as `env-<digest prefix>`. To revoke one, run
  `UPDATE api_keys SET revoked_at = now() WHERE name = 'env-…'` (effective
  within the 30 s auth cache TTL), then remove it from the environment.
* Database roles: the API and workers use `eventflow_app` (DML only).
  Migrations use the owner role. New tables get default privileges for the
  app role, and migration `0002` revokes writes to `schema_migrations` and
  deletes on `api_keys`.
* Containers: distroless static images, non-root user, read-only root
  filesystem, `no-new-privileges`, all capabilities dropped. PostgreSQL,
  Redis, Prometheus and Grafana bind to `127.0.0.1` only.
* Errors returned to clients never include internal error text. Logs never
  include API keys, DSNs or job payloads.
* TLS is expected to be terminated by a reverse proxy or load balancer in
  front of the API. For managed databases use `sslmode=verify-full`.

## Troubleshooting

| Symptom | Likely cause | What to check / do |
|---------|--------------|--------------------|
| Jobs stay `pending` past `scheduled_at` | Outbox relay not running or Redis unreachable | `eventflow_outbox_backlog`, `eventflow_outbox_oldest_due_age_seconds`; API logs for `outbox publication failed`; `SELECT attempts, last_error FROM outbox_events WHERE status='pending' ORDER BY available_at LIMIT 5;` |
| Jobs stay `queued` | No workers, or workers can't reach Redis or PostgreSQL | `GET /api/v1/queues` (`active_workers`, `undelivered`); worker logs for `broker read failed`/`claim failed`. If Redis lost data, recovery re-publishes after `STUCK_QUEUED_AFTER` (look for `re-published queued jobs` in logs) |
| Jobs stuck `running` | Worker crashed or hung | Recovered automatically once `lease_expires_at` passes; attempts show `lost` / `lease_expired`. Check `eventflow_recovered_jobs_total` |
| Same job executed twice | Expected under at-least-once after crashes or lease expiry | Attempt history shows why (`lost`, `interrupted`). Make the handler idempotent using `IdempotencyKey` (see [architecture](architecture.md#idempotency-contract-and-the-limits-of-exactly-once)) |
| Many `lease_lost` worker failures | Lease too short for heartbeat latency, or DB overloaded | Increase `LEASE_DURATION`, check PostgreSQL latency |
| `unacknowledged` grows in `/queues` | Consumers died with messages in their PEL | Re-claimed after `CLAIM_MIN_IDLE` automatically. Inspect with `XPENDING eventflow:jobs:normal eventflow-workers` |
| Dead-letter count rising | Downstream failures or bad payloads | `GET /api/v1/jobs?dead_lettered=true`, then each job's `/attempts`; fix the cause, then `POST /jobs/{id}/retry` |
| `401 unauthorized` | Wrong or revoked key, or key shorter than 16 chars rejected at start-up | Check `api_keys.revoked_at`; the API logs never print keys |
| `/ready` returns 503 | DB or Redis unreachable, or the process is draining | Response body names the failing check |
| API exits at start-up | Invalid configuration | The error lists every invalid variable |
| `migrate` fails in Compose | DB not ready or wrong owner password | `docker compose logs migrate`; `.env` passwords only apply when the volume is first created (`make clean-stack` resets it, **deleting data**) |
| `docker compose build` fails with x509 errors | TLS-intercepting proxy | Pass the proxy CA as a build secret: `docker build --secret id=extra_ca,src=ca.crt ...` (see the Dockerfile) |

Useful SQL:

```sql
-- Jobs per state
SELECT status, count(*) FROM jobs GROUP BY 1;
-- Slowest recent attempts
SELECT job_id, attempt_number, duration_ms, status FROM job_attempts ORDER BY finished_at DESC NULLS LAST LIMIT 20;
-- Workers and heartbeat age
SELECT id, status, now() - last_heartbeat_at AS age FROM workers ORDER BY last_heartbeat_at DESC;
```
