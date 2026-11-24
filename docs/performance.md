# Performance and test report

Every number on this page was measured, not estimated. The raw k6 summaries
are written to `loadtest/results/*.json` (gitignored) when the scripts run.

## Environment

| | |
|---|---|
| Host | single cloud VM, **4 vCPU** Intel Xeon @ 2.10 GHz (1 thread/core), 15 GB RAM, Linux 6.18 |
| Runtime | Docker 29.10, Docker Compose v2 |
| Topology | Full `docker-compose.yml` stack: PostgreSQL 16 (`shared_buffers=256MB`, `synchronous_commit=on`, fsync on), Redis 7.2 (AOF `everysec`), 1 API container (outbox relay enabled, `DB_MAX_CONNS=40`), 2 worker containers × 16 concurrency (unless stated otherwise), Prometheus (5 s scrape), Grafana |
| Load generator | k6 0.54 in Docker (`--network host`), **on the same 4 vCPUs** |
| Build | Go 1.24.7, `CGO_ENABLED=0`, `-trimpath -ldflags "-s -w"` (measured before the security upgrade to Go 1.26.11; see the changelog note below) |
| Date | 2026-11-23 |

**Read these numbers as a lower bound.** The database, broker, API, workers,
monitoring and the load generator all compete for 4 shared vCPUs. A real
deployment would put PostgreSQL on dedicated hardware.

## Workload and methodology

| Script | What it does | Executor |
|--------|--------------|----------|
| `loadtest/submit.js` | `POST /api/v1/jobs` with a cheap `EMAIL_SIMULATION` job (5 ms simulated latency) | constant arrival rate (`RATE`, `DURATION`) |
| `loadtest/read.js` | Seeds 200–500 jobs, then 60% get-by-id, 20% list, 10% filtered list, 7% attempts, 3% `/queues` | constant arrival rate |
| `loadtest/concurrent.js` | Closed loop: each VU submits a mixed job, reads it back and lists jobs; ramps to 100 VUs, holds 45 s | ramping VUs |
| `loadtest/processing.js` | Submits `JOBS` jobs with 50 VUs, then polls `/queues` until no job is pending, queued or running. Reports `JOBS / elapsed` (end-to-end) | shared iterations |
| `scripts/bench_drain.sh` | Stops workers, submits `JOBS` jobs, waits until all are `queued`, starts `WORKERS` workers and times the drain (worker start-up included) | – |
| `loadtest/sustained.js` | 150 submissions/s (mixed job types) + 150 list reads/s for 10 minutes | constant arrival rate |

Commands, run in this order against one long-lived stack:

```bash
make loadtest-submit                                         # 300 req/s, 60 s
make loadtest-submit K6_ARGS="-e RATE=1000 -e DURATION=60s"
make loadtest-submit K6_ARGS="-e RATE=600 -e DURATION=60s"
make loadtest-read                                           # 1000 req/s, 60 s (workers still busy, see below)
make loadtest-read K6_ARGS="-e DURATION=40s -e SEED=200"     # 1000 req/s, workers idle
make loadtest-processing K6_ARGS="-e JOBS=20000"             # 2 workers x 16
JOBS=20000 WORKERS=1 scripts/bench_drain.sh                  # then WORKERS=2, WORKERS=4
EVENTFLOW_WORKER_CONCURRENCY=64 JOBS=20000 WORKERS=1 scripts/bench_drain.sh
make loadtest-concurrent                                     # 100 VUs
make loadtest-sustained                                      # 10 minutes
```

Latencies are k6 client-side `http_req_duration` (request sent → response
received). The database held between roughly 0 and 300k job rows over the
course of the runs.

## Results

### Job submission (`POST /api/v1/jobs`)

| Target rate | Achieved | p50 | p95 | p99 | max | Errors | Dropped iterations |
|------------:|---------:|----:|----:|----:|----:|-------:|-------------------:|
| 300/s | 299.6/s | 2.47 ms | 23.63 ms | 64.20 ms | 279 ms | 0 / 17,980 | 21 |
| 600/s | 598.8/s | 17.35 ms | 63.99 ms | 107.10 ms | 287 ms | 0 / 35,939 | 62 |
| 1000/s | 983.7/s | 105.05 ms | 309.92 ms | 443.92 ms | 677 ms | 0 / 59,068 | 933 |

Every submission is a PostgreSQL transaction (job + outbox row), and the
workers were executing the submitted jobs concurrently on the same CPUs. At
1000/s the host saturates: no request failed, but latency thresholds were
exceeded and k6 dropped 1.6% of scheduled iterations. **About 600 submissions
per second is the comfortable ceiling on this machine.**

### Read API (1000 req/s target)

| Run | Endpoint | p50 | p95 | p99 | Errors |
|-----|----------|----:|----:|----:|-------:|
| Workers idle (998 req/s achieved) | `GET /jobs/{id}` | 0.77 ms | 1.83 ms | 5.64 ms | 0 |
| | `GET /jobs?limit=50` | 1.75 ms | 4.09 ms | 9.98 ms | 0 |
| Workers draining ~113k jobs (956 req/s achieved) | `GET /jobs/{id}` | 14.43 ms | 440.81 ms | 555.23 ms | 0 |
| | `GET /jobs?limit=50` | 19.86 ms | 440.28 ms | 564.35 ms | 0 |

Both runs are reported. The first read run started while the workers were
still processing the backlog from the submission tests, and CPU contention
produced the long tail. During the idle run, container CPU was about 70% for
the API, 36% for PostgreSQL and 53% for k6, with only 2 active PostgreSQL
backends, so the API and database are not the read bottleneck.

### Concurrent clients (100 VUs, mixed submit + get + list)

| Requests | Rate | Iterations/s | p50 | p95 | p99 | max | Errors |
|---------:|-----:|-------------:|----:|----:|----:|----:|-------:|
| 115,968 | 1656.5 req/s | 552.2 | 41.40 ms | 115.78 ms | 163.86 ms | 472 ms | 0 |

### Job processing throughput

| Measurement | Workers × concurrency | Jobs | Time | Throughput |
|-------------|----------------------|-----:|-----:|-----------:|
| End-to-end (submit + process, overlapping) | 2 × 16 | 20,000 | 40.5 s | **494 jobs/s** |
| End-to-end | 4 × 16 | 20,000 | 40.0 s | 500 jobs/s |
| Drain of pre-queued jobs | 1 × 16 | 20,000 | 40.4 s | 495 jobs/s |
| Drain of pre-queued jobs | 2 × 16 | 20,000 | 22.0 s | **910 jobs/s** |
| Drain of pre-queued jobs | 4 × 16 | 20,000 | 24.2 s | 825 jobs/s |
| Drain of pre-queued jobs | 1 × 64 | 20,000 | 16.8 s | **1190 jobs/s** |

Jobs were `EMAIL_SIMULATION` with a 10 ms simulated handler latency. Each job
costs two PostgreSQL transactions on the worker side (claim, completion), one
on submission, and a share of a batched relay transaction, plus two Redis
round trips (read, ack).

Analysis:

* **End-to-end throughput did not change between 2 and 4 workers** (494 vs
  500 jobs/s), so in that test the limit was submission (50 closed-loop VUs
  through the API), not execution. That is why the drain benchmark exists.
* **Per worker, throughput is bound by per-job latency, not the fetcher.**
  Measured attempt durations averaged 10.1 ms, yet one 16-slot worker
  completed about 31 jobs/s per slot (about 32 ms per job), which means
  about 20 ms of database round trips and commits per job under load. Raising
  one worker to 64 slots more than doubled its throughput (495 → 1190
  jobs/s), which rules out the single fetch loop as the bottleneck.
* **Horizontal scaling** works up to the host's limit: 1 → 2 workers scaled
  1.84×. At 4 workers the 4 vCPUs (shared with PostgreSQL, which does every
  state transition) were saturated and throughput fell slightly, to 825 jobs/s.
* The next optimization would be batching claims and completions to cut
  commits per job. See *Future improvements* in the README.

### Sustained load (10 minutes)

| Scenario | Rate | Requests | p50 | p95 | p99 | max | Errors |
|----------|-----:|---------:|----:|----:|----:|----:|-------:|
| Writes (mixed job types) | 150/s | ≈ 90,000 | 2.39 ms | 9.91 ms | 23.30 ms | 207 ms | 0 |
| Reads (list) | 150/s | ≈ 90,000 | 1.23 ms | 3.95 ms | 9.85 ms | 81 ms | 0 |
| **Total** | 300/s | 180,002 | 2.06 ms | 7.16 ms | 17.86 ms | 207 ms | **0** |

After the run, 0 jobs were outstanding (every submitted job was processed
during the run), the outbox backlog was 0, and memory stayed flat (worker
containers about 11 MB each, API about 40 MB). The Grafana screenshot in the
README was taken right after this run and shows the whole session.

## Test execution record

| Suite | Command | Result |
|-------|---------|--------|
| Unit (43 tests, 6 packages) | `go test -race -count=1 ./...` | pass |
| Integration and failure (32 tests) | `go test -tags integration -race -count=1 ./tests/integration/` | pass (102 s) |
| Integration repeated for flakiness | `-race` run, `-count=2` run, coverage run | pass: 4 of 4 full runs, no flakes |
| Coverage of `internal/...` (unit + integration, merged) | `make cover` | **82.8%** of statements (integration alone: 73.0%) |
| Static analysis | `gofmt`, `go vet` (with and without `integration` tag), `golangci-lint` v2.5 | clean |
| Docker Compose | `make up && make smoke` | all services healthy, smoke test passed |
| Vulnerability scan | `govulncheck` | **not run locally**: this environment's network policy blocks `vuln.go.dev`. It runs in CI (`lint` job) and via `make vuln` |

## Reproducing

```bash
make env && make up
make smoke
make loadtest-submit            # see the command list above for the other scenarios
```

Results vary with hardware. When comparing changes, run the same command on
the same machine, with Prometheus and Grafana open, and watch the
*EventFlow* dashboard for the cause of any regression.

## Note: security upgrade after measurement

The measurements above were taken with Go 1.24.7, pgx v5.7.1 and go-redis
v9.7.0. The first CI run of `govulncheck` (which could not run in the
development sandbox) then reported known vulnerabilities in the Go 1.24
standard library (`crypto/tls`, `net/http`, `net/url`, ...), pgx, go-redis
and `golang.org/x/text`. The project was upgraded to Go 1.26.11, pgx v5.9.2,
go-redis v9.7.3 and `golang.org/x/text` v0.39.0. The full unit and
integration suites were re-run on the upgraded build. The load tests were not
repeated, because the upgrade changes no EventFlow code paths.
