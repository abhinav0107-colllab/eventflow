//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/abhinav0107-colllab/eventflow/internal/database"
	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
	"github.com/abhinav0107-colllab/eventflow/migrations"
)

const emailJob = `{"type":"EMAIL_SIMULATION","payload":{"to":"user@example.com","subject":"Hi","simulate_latency_ms":5}}`

func TestMigrationsRollBackAndReapply(t *testing.T) {
	h := newHarness(t, harnessOpts{noRelay: true})
	if _, err := h.pool.Exec(h.ctx, `DROP DATABASE IF EXISTS migration_check`); err != nil {
		t.Fatal(err)
	}
	if _, err := h.pool.Exec(h.ctx, `CREATE DATABASE migration_check`); err != nil {
		t.Fatal(err)
	}
	dsn := strings.Replace(pgURL, "/eventflow_test?", "/migration_check?", 1)
	pool, err := database.Connect(h.ctx, database.PoolConfig{URL: dsn, MaxConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	m, err := database.NewMigrator(pool, migrations.FS)
	if err != nil {
		t.Fatal(err)
	}
	up, err := m.Up(h.ctx)
	if err != nil || len(up) != 2 {
		t.Fatalf("up: %v %v", up, err)
	}
	if again, err := m.Up(h.ctx); err != nil || len(again) != 0 {
		t.Fatalf("second up must be a no-op: %v %v", again, err)
	}
	down, err := m.Down(h.ctx, 2)
	if err != nil || len(down) != 2 {
		t.Fatalf("down: %v %v", down, err)
	}
	var tables int
	_ = pool.QueryRow(h.ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name IN ('jobs','job_attempts','outbox_events','workers','api_keys')`).Scan(&tables)
	if tables != 0 {
		t.Fatalf("down left %d tables behind", tables)
	}
	if up, err := m.Up(h.ctx); err != nil || len(up) != 2 {
		t.Fatalf("re-apply: %v %v", up, err)
	}
	st, err := m.Status(h.ctx)
	if err != nil || len(st) != 2 || st[0].AppliedAt == nil || st[1].AppliedAt == nil {
		t.Fatalf("status: %+v %v", st, err)
	}
}

func TestCreateJobPersistsJobAndOutboxEventAtomically(t *testing.T) {
	h := newHarness(t, harnessOpts{noRelay: true})
	r := h.request("POST", "/api/v1/jobs",
		`{"type":"EMAIL_SIMULATION","payload":{"to":"a@example.com","subject":"s"},"priority":9,"max_attempts":5,"timeout_seconds":12}`,
		testAPIKey)
	if r.Status != http.StatusCreated {
		t.Fatalf("status %d: %s", r.Status, r.Body)
	}
	var j jobs.Job
	r.decode(t, &j)
	if r.Header.Get("Location") != "/api/v1/jobs/"+j.ID.String() {
		t.Fatalf("location header %q", r.Header.Get("Location"))
	}
	if j.Status != jobs.StatusPending || j.Queue != jobs.QueueHigh || j.MaxAttempts != 5 || j.TimeoutSeconds != 12 || j.Version != 1 {
		t.Fatalf("unexpected job: %+v", j)
	}
	var evType, queue, status string
	if err := h.pool.QueryRow(h.ctx, `SELECT event_type, queue, status FROM outbox_events WHERE job_id = $1`, j.ID).
		Scan(&evType, &queue, &status); err != nil {
		t.Fatalf("outbox event missing: %v", err)
	}
	if evType != "job.ready" || queue != "high" || status != "pending" {
		t.Fatalf("outbox event: %s %s %s", evType, queue, status)
	}
	got := h.request("GET", "/api/v1/jobs/"+j.ID.String(), "", testAPIKey)
	var fetched jobs.Job
	got.decode(t, &fetched)
	var payload map[string]string
	_ = json.Unmarshal(fetched.Payload, &payload)
	if got.Status != 200 || fetched.ID != j.ID || payload["to"] != "a@example.com" || payload["subject"] != "s" {
		t.Fatalf("get: %d %s", got.Status, got.Body)
	}
	if r := h.request("GET", "/api/v1/jobs/"+uuid.NewString(), "", testAPIKey); r.Status != 404 {
		t.Fatalf("unknown job: %d", r.Status)
	}
	if r := h.request("GET", "/api/v1/jobs/"+uuid.NewString()+"/attempts", "", testAPIKey); r.Status != 404 {
		t.Fatalf("unknown job attempts: %d", r.Status)
	}
}

func TestIdempotencyKeys(t *testing.T) {
	h := newHarness(t, harnessOpts{noRelay: true})
	first := h.request("POST", "/api/v1/jobs", emailJob, testAPIKey, "Idempotency-Key", "order-1")
	if first.Status != 201 {
		t.Fatalf("first: %d %s", first.Status, first.Body)
	}
	var a jobs.Job
	first.decode(t, &a)

	// Same request (different whitespace/key order) replays the job.
	replay := h.request("POST", "/api/v1/jobs",
		`{ "payload": {"subject":"Hi", "simulate_latency_ms":5, "to":"user@example.com"}, "type":"EMAIL_SIMULATION",
		   "idempotency_key":"order-1" }`, testAPIKey)
	var b jobs.Job
	replay.decode(t, &b)
	if replay.Status != 200 || replay.Header.Get("Idempotent-Replayed") != "true" || b.ID != a.ID {
		t.Fatalf("replay: %d %s", replay.Status, replay.Body)
	}

	// Different body under the same key conflicts.
	conflict := h.request("POST", "/api/v1/jobs",
		`{"type":"EMAIL_SIMULATION","payload":{"to":"other@example.com","subject":"Hi"}}`, testAPIKey, "Idempotency-Key", "order-1")
	if conflict.Status != 409 || !strings.Contains(string(conflict.Body), "idempotency_conflict") {
		t.Fatalf("conflict: %d %s", conflict.Status, conflict.Body)
	}

	// Keys are scoped per API credential.
	other := h.request("POST", "/api/v1/jobs", emailJob, otherAPIKey, "Idempotency-Key", "order-1")
	var c jobs.Job
	other.decode(t, &c)
	if other.Status != 201 || c.ID == a.ID {
		t.Fatalf("other key: %d %s", other.Status, other.Body)
	}
	if n := h.queryInt(`SELECT count(*) FROM jobs`); n != 2 {
		t.Fatalf("expected 2 jobs, found %d", n)
	}
	if n := h.queryInt(`SELECT count(*) FROM outbox_events`); n != 2 {
		t.Fatalf("expected 2 outbox events, found %d", n)
	}
}

func TestConcurrentIdempotentSubmissionsCreateOneJob(t *testing.T) {
	h := newHarness(t, harnessOpts{noRelay: true})
	const n = 40
	var wg sync.WaitGroup
	statuses := make([]int, n)
	ids := make([]uuid.UUID, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := h.request("POST", "/api/v1/jobs", emailJob, testAPIKey, "Idempotency-Key", "same-key")
			statuses[i] = r.Status
			var j jobs.Job
			_ = json.Unmarshal(r.Body, &j)
			ids[i] = j.ID
		}(i)
	}
	wg.Wait()
	created := 0
	for i := range statuses {
		switch statuses[i] {
		case 201:
			created++
		case 200:
		default:
			t.Fatalf("request %d: unexpected status %d", i, statuses[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("request %d returned a different job id", i)
		}
	}
	if created != 1 {
		t.Fatalf("expected exactly one 201, got %d", created)
	}
	if c := h.queryInt(`SELECT count(*) FROM jobs`); c != 1 {
		t.Fatalf("expected 1 job row, found %d", c)
	}
	if c := h.queryInt(`SELECT count(*) FROM outbox_events`); c != 1 {
		t.Fatalf("expected 1 outbox event, found %d", c)
	}
}

func TestConcurrentSubmissions(t *testing.T) {
	h := newHarness(t, harnessOpts{noRelay: true})
	const goroutines, perG = 20, 15
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := map[uuid.UUID]bool{}
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				r := h.request("POST", "/api/v1/jobs", emailJob, testAPIKey)
				if r.Status != 201 {
					t.Errorf("status %d", r.Status)
					return
				}
				var j jobs.Job
				_ = json.Unmarshal(r.Body, &j)
				mu.Lock()
				seen[j.ID] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != goroutines*perG {
		t.Fatalf("expected %d distinct jobs, got %d", goroutines*perG, len(seen))
	}
	if c := h.queryInt(`SELECT count(*) FROM jobs j JOIN outbox_events o ON o.job_id = j.id`); c != goroutines*perG {
		t.Fatalf("every job needs exactly one outbox event, found %d", c)
	}
}

func TestListJobsFiltersAndPagination(t *testing.T) {
	h := newHarness(t, harnessOpts{noRelay: true})
	var ids []uuid.UUID
	for i := 0; i < 7; i++ {
		ids = append(ids, h.submit(emailJob).ID)
	}
	h.submit(`{"type":"DELAYED_TASK","payload":{"duration_ms":1},"priority":2}`)

	var all []uuid.UUID
	cursor := ""
	for page := 0; page < 10; page++ {
		path := "/api/v1/jobs?type=EMAIL_SIMULATION&limit=3"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		r := h.request("GET", path, "", testAPIKey)
		var body struct {
			Data       []jobs.Job `json:"data"`
			NextCursor string     `json:"next_cursor"`
		}
		r.decode(t, &body)
		for _, j := range body.Data {
			all = append(all, j.ID)
		}
		if body.NextCursor == "" {
			break
		}
		cursor = body.NextCursor
	}
	if len(all) != 7 {
		t.Fatalf("pagination returned %d jobs, want 7", len(all))
	}
	for i := range all { // newest first; UUIDv7 ids are time ordered
		if all[i] != ids[len(ids)-1-i] {
			t.Fatalf("page order mismatch at %d", i)
		}
	}
	var low struct{ Data []jobs.Job }
	h.request("GET", "/api/v1/jobs?queue=low&status=pending", "", testAPIKey).decode(t, &low)
	if len(low.Data) != 1 || low.Data[0].Type != "DELAYED_TASK" {
		t.Fatalf("queue filter: %+v", low.Data)
	}
	var none struct{ Data []jobs.Job }
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	h.request("GET", "/api/v1/jobs?created_after="+future, "", testAPIKey).decode(t, &none)
	if len(none.Data) != 0 {
		t.Fatalf("created_after filter returned %d", len(none.Data))
	}
}

func TestOperationalEndpoints(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	w := h.startWorker(h.workerConfig("ops-worker"), nil, nil)
	j := h.submit(emailJob)
	h.waitStatus(j.ID, 10*time.Second, jobs.StatusSucceeded)

	var q struct {
		Queues []struct {
			Name string
			Jobs struct{ Pending, Queued, Running int }
		}
		Outbox        struct{ Backlog int }
		ActiveWorkers int `json:"active_workers"`
	}
	r := h.request("GET", "/api/v1/queues", "", testAPIKey)
	r.decode(t, &q)
	if r.Status != 200 || len(q.Queues) != 3 || q.ActiveWorkers != 1 || q.Outbox.Backlog != 0 {
		t.Fatalf("queues: %d %s", r.Status, r.Body)
	}
	var ws struct{ Data []struct{ ID, Status string } }
	h.request("GET", "/api/v1/workers", "", testAPIKey).decode(t, &ws)
	if len(ws.Data) != 1 || ws.Data[0].ID != "ops-worker" || ws.Data[0].Status != "active" {
		t.Fatalf("workers: %+v", ws)
	}
	if r := h.request("GET", "/ready", "", ""); r.Status != 200 {
		t.Fatalf("ready: %d %s", r.Status, r.Body)
	}
	h.apiSrv.SetDraining()
	if r := h.request("GET", "/ready", "", ""); r.Status != 503 || !strings.Contains(string(r.Body), "draining") {
		t.Fatalf("draining ready: %d %s", r.Status, r.Body)
	}
	w.Stop(t)
	var after struct{ Data []struct{ Status string } }
	h.request("GET", "/api/v1/workers", "", testAPIKey).decode(t, &after)
	if after.Data[0].Status != "stopped" {
		t.Fatalf("worker should be stopped after graceful shutdown: %+v", after)
	}
	metrics := string(h.request("GET", "/metrics", "", "").Body)
	for _, m := range []string{`eventflow_jobs_submitted_total{type="EMAIL_SIMULATION"} 1`,
		`eventflow_jobs_succeeded_total{type="EMAIL_SIMULATION"} 1`, `eventflow_http_requests_total{code="201",method="POST",route="/api/v1/jobs"} 1`} {
		if !strings.Contains(metrics, m) {
			t.Errorf("metrics missing %s", m)
		}
	}
}

func TestReadinessReportsRedisOutage(t *testing.T) {
	p := newFaultProxy(t, redisAddr)
	h := newHarness(t, harnessOpts{noRelay: true, apiRedis: p.Addr()})
	if r := h.request("GET", "/ready", "", ""); r.Status != 200 {
		t.Fatalf("ready: %d %s", r.Status, r.Body)
	}
	p.Disable()
	r := h.request("GET", "/ready", "", "")
	if r.Status != 503 || !strings.Contains(string(r.Body), `"redis":"unavailable"`) {
		t.Fatalf("expected redis unavailable: %d %s", r.Status, r.Body)
	}
	p.Enable()
	waitFor(t, 10*time.Second, "readiness to recover", func() bool { return h.request("GET", "/ready", "", "").Status == 200 })
}

func TestPayloadLimitsAndAuth(t *testing.T) {
	h := newHarness(t, harnessOpts{noRelay: true})
	big := fmt.Sprintf(`{"type":"EMAIL_SIMULATION","payload":{"to":"a@example.com","subject":"s","body":"%s"}}`,
		strings.Repeat("x", 2<<20))
	if r := h.request("POST", "/api/v1/jobs", big, testAPIKey); r.Status != 413 {
		t.Fatalf("oversized body: %d", r.Status)
	}
	if r := h.request("POST", "/api/v1/jobs", emailJob, "revoked-or-unknown-key"); r.Status != 401 {
		t.Fatalf("bad key: %d", r.Status)
	}
	// Revocation takes effect for keys not yet cached.
	if _, err := h.pool.Exec(h.ctx, `UPDATE api_keys SET revoked_at = now()`); err != nil {
		t.Fatal(err)
	}
	if r := h.request("POST", "/api/v1/jobs", emailJob, otherAPIKey); r.Status != 401 {
		t.Fatalf("revoked key accepted: %d", r.Status)
	}
	if n := h.queryInt(`SELECT count(*) FROM jobs`); n != 0 {
		t.Fatalf("rejected requests must not create jobs, found %d", n)
	}
}
