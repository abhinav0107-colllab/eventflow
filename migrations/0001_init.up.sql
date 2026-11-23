-- EventFlow core schema.
-- PostgreSQL is the authoritative source of job state; Redis only transports
-- "this job is ready" notifications.

CREATE TABLE api_keys (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT        NOT NULL UNIQUE CHECK (char_length(name) BETWEEN 1 AND 128),
    key_hash    BYTEA       NOT NULL UNIQUE CHECK (octet_length(key_hash) = 32), -- SHA-256 of the raw key
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at  TIMESTAMPTZ
);

CREATE TABLE jobs (
    id               UUID        PRIMARY KEY,
    type             TEXT        NOT NULL CHECK (char_length(type) BETWEEN 1 AND 64),
    queue            TEXT        NOT NULL CHECK (queue IN ('high', 'normal', 'low')),
    payload          JSONB       NOT NULL,
    priority         SMALLINT    NOT NULL CHECK (priority BETWEEN 1 AND 10),
    status           TEXT        NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'queued', 'running', 'succeeded', 'failed', 'cancelled')),
    api_key_id       BIGINT      NOT NULL REFERENCES api_keys (id),
    idempotency_key  TEXT        CHECK (char_length(idempotency_key) BETWEEN 1 AND 255),
    request_hash     BYTEA,
    attempts         INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts     INTEGER     NOT NULL CHECK (max_attempts BETWEEN 1 AND 1000),
    timeout_seconds  INTEGER     NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 86400),
    scheduled_at     TIMESTAMPTZ NOT NULL,           -- earliest time the next attempt may run
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    queued_at        TIMESTAMPTZ,
    started_at       TIMESTAMPTZ,                    -- start of the most recent attempt
    completed_at     TIMESTAMPTZ,
    worker_id        TEXT,
    lease_expires_at TIMESTAMPTZ,
    cancel_requested BOOLEAN     NOT NULL DEFAULT false,
    last_error       TEXT,
    last_error_class TEXT,
    result           JSONB,
    duration_ms      BIGINT,                         -- duration of the most recent attempt
    dead_lettered_at TIMESTAMPTZ,
    version          BIGINT      NOT NULL DEFAULT 1, -- optimistic concurrency / change counter
    CONSTRAINT jobs_running_has_lease
        CHECK (status <> 'running' OR (worker_id IS NOT NULL AND lease_expires_at IS NOT NULL)),
    CONSTRAINT jobs_attempts_within_budget CHECK (attempts <= max_attempts)
);

-- Idempotency keys are scoped to the API credential that submitted them.
CREATE UNIQUE INDEX jobs_idempotency_uniq ON jobs (api_key_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
-- Keyset pagination for the list endpoint (newest first).
CREATE INDEX jobs_created_idx ON jobs (created_at DESC, id DESC);
CREATE INDEX jobs_status_created_idx ON jobs (status, created_at DESC, id DESC);
CREATE INDEX jobs_type_created_idx ON jobs (type, created_at DESC, id DESC);
-- Recovery scans.
CREATE INDEX jobs_running_lease_idx ON jobs (lease_expires_at) WHERE status = 'running';
CREATE INDEX jobs_queued_idx ON jobs (queue, queued_at) WHERE status = 'queued';
CREATE INDEX jobs_dead_lettered_idx ON jobs (dead_lettered_at DESC) WHERE dead_lettered_at IS NOT NULL;

CREATE TABLE job_attempts (
    id             BIGSERIAL   PRIMARY KEY,
    job_id         UUID        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt_number INTEGER     NOT NULL CHECK (attempt_number >= 1),
    worker_id      TEXT        NOT NULL,
    status         TEXT        NOT NULL
                   CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled', 'lost')),
    started_at     TIMESTAMPTZ NOT NULL,
    finished_at    TIMESTAMPTZ,
    duration_ms    BIGINT,
    error          TEXT,
    error_class    TEXT,
    UNIQUE (job_id, attempt_number)
);

CREATE TABLE outbox_events (
    id                BIGSERIAL   PRIMARY KEY,
    job_id            UUID        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    event_type        TEXT        NOT NULL CHECK (event_type IN ('job.ready', 'job.dead_lettered')),
    queue             TEXT        NOT NULL CHECK (queue IN ('high', 'normal', 'low', 'dead_letter')),
    payload           JSONB       NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending', 'published', 'discarded')),
    attempts          INTEGER     NOT NULL DEFAULT 0,
    last_error        TEXT,
    available_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at      TIMESTAMPTZ,
    stream_message_id TEXT
);

CREATE INDEX outbox_pending_idx ON outbox_events (available_at, id) WHERE status = 'pending';
CREATE INDEX outbox_job_idx ON outbox_events (job_id);
CREATE INDEX outbox_published_idx ON outbox_events (published_at) WHERE status <> 'pending';

CREATE TABLE workers (
    id                TEXT        PRIMARY KEY,
    hostname          TEXT        NOT NULL,
    pid               INTEGER     NOT NULL,
    concurrency       INTEGER     NOT NULL CHECK (concurrency >= 1),
    status            TEXT        NOT NULL CHECK (status IN ('active', 'stopping', 'stopped', 'dead')),
    version           TEXT        NOT NULL,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    stopped_at        TIMESTAMPTZ
);

CREATE INDEX workers_status_heartbeat_idx ON workers (status, last_heartbeat_at);
