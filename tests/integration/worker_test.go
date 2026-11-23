//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
)

func TestWorkersExecuteAllJobTypes(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	h.startWorker(h.workerConfig("w-a"), nil, nil)
	h.startWorker(h.workerConfig("w-b"), nil, nil)

	bodies := []string{
		emailJob,
		`{"type":"WEBHOOK_SIMULATION","payload":{"url":"https://example.com/hook","simulate_latency_ms":5}}`,
		`{"type":"CPU_INTENSIVE_COMPUTATION","payload":{"operation":"prime_count","n":200000}}`,
		`{"type":"CPU_INTENSIVE_COMPUTATION","payload":{"operation":"hash_chain","n":1000,"seed":"x"}}`,
		`{"type":"DELAYED_TASK","payload":{"duration_ms":50,"message":"done"}}`,
	}
	var ids []uuid.UUID
	for i := 0; i < 8; i++ {
		for _, b := range bodies {
			ids = append(ids, h.submit(b).ID)
		}
	}
	workersSeen := map[string]int{}
	for _, id := range ids {
		j := h.waitStatus(id, 20*time.Second, jobs.StatusSucceeded)
		if j.Attempts != 1 || len(j.Result) == 0 || j.CompletedAt == nil || j.DurationMS == nil {
			t.Fatalf("job %s: %+v", id, j)
		}
		workersSeen[*j.WorkerID]++
		at := h.attempts(id)
		if len(at) != 1 || at[0].Status != jobs.AttemptSucceeded || at[0].FinishedAt == nil {
			t.Fatalf("attempts for %s: %+v", id, at)
		}
	}
	if len(workersSeen) != 2 {
		t.Fatalf("expected both workers to process jobs, distribution: %v", workersSeen)
	}
	var cpu jobs.Job
	for _, id := range ids {
		if j := h.get(id); j.Type == "CPU_INTENSIVE_COMPUTATION" && strings.Contains(string(j.Payload), "prime_count") {
			cpu = j
			break
		}
	}
	if !strings.Contains(string(cpu.Result), `"prime_count": 17984`) {
		t.Fatalf("prime count result wrong: %s", cpu.Result)
	}
	// Every message was acknowledged.
	waitFor(t, 5*time.Second, "all messages acknowledged", func() bool {
		for _, q := range []string{"high", "normal", "low"} {
			if st, _ := h.broker.Stats(h.ctx, q); st.Depth() != 0 {
				return false
			}
		}
		return true
	})
}

func TestStrictPriorityOrdering(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	var ids []uuid.UUID
	for i := 0; i < 5; i++ {
		ids = append(ids, h.submit(`{"type":"DELAYED_TASK","payload":{"duration_ms":20},"priority":1}`).ID)
	}
	for i := 0; i < 5; i++ {
		ids = append(ids, h.submit(`{"type":"DELAYED_TASK","payload":{"duration_ms":20},"priority":10}`).ID)
	}
	waitFor(t, 5*time.Second, "all jobs queued", func() bool {
		return h.queryInt(`SELECT count(*) FROM jobs WHERE status = 'queued'`) == 10
	})
	cfg := h.workerConfig("prio")
	cfg.Concurrency = 1
	h.startWorker(cfg, nil, nil)
	for _, id := range ids {
		h.waitStatus(id, 20*time.Second, jobs.StatusSucceeded)
	}
	rows, err := h.pool.Query(h.ctx, `SELECT priority FROM jobs ORDER BY started_at`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var order []int
	for rows.Next() {
		var p int
		_ = rows.Scan(&p)
		order = append(order, p)
	}
	for i, p := range order {
		if (i < 5 && p != 10) || (i >= 5 && p != 1) {
			t.Fatalf("high-priority jobs must run first, got start order %v", order)
		}
	}
}

func TestRetryWithBackoffThenSuccess(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	h.startWorker(h.workerConfig("retry-w"), nil, nil)
	j := h.submit(`{"type":"DELIBERATE_FAILURE","payload":{"mode":"until_attempt","succeed_on_attempt":3},"max_attempts":5}`)
	done := h.waitStatus(j.ID, 20*time.Second, jobs.StatusSucceeded)
	if done.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", done.Attempts)
	}
	at := h.attempts(j.ID)
	if len(at) != 3 || at[0].Status != jobs.AttemptFailed || at[1].Status != jobs.AttemptFailed || at[2].Status != jobs.AttemptSucceeded {
		t.Fatalf("attempt history: %+v", at)
	}
	if *at[0].ErrorClass != "error" || !strings.Contains(*at[0].Error, "attempt 1") {
		t.Fatalf("error details not persisted: %+v", at[0])
	}
	// Backoff: base 100ms with equal jitter => at least 50ms, then 100ms.
	gap1 := at[1].StartedAt.Sub(*at[0].FinishedAt)
	gap2 := at[2].StartedAt.Sub(*at[1].FinishedAt)
	if gap1 < 50*time.Millisecond || gap2 < 100*time.Millisecond {
		t.Fatalf("backoff not respected: gaps %s, %s", gap1, gap2)
	}
	if v := testutil.ToFloat64(h.metrics.JobsRetried.WithLabelValues("DELIBERATE_FAILURE")); v != 2 {
		t.Fatalf("retried metric = %v, want 2", v)
	}
}

func TestDeadLetterAndManualRetry(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	h.startWorker(h.workerConfig("dlq-w"), nil, nil)
	j := h.submit(`{"type":"DELIBERATE_FAILURE","payload":{"mode":"always","message":"downstream broken"},"max_attempts":2}`)
	failed := h.waitStatus(j.ID, 20*time.Second, jobs.StatusFailed)
	if failed.Attempts != 2 || failed.DeadLetteredAt == nil || failed.CompletedAt == nil ||
		!strings.Contains(deref(failed.LastError), "downstream broken") || deref(failed.LastErrorClass) != "error" {
		t.Fatalf("dead-lettered job: %+v", failed)
	}
	// The dead-letter event reaches the DLQ stream via the outbox.
	waitFor(t, 5*time.Second, "DLQ stream entry", func() bool {
		msgs, _ := h.rdb.XRange(h.ctx, h.broker.StreamKey("dead_letter"), "-", "+").Result()
		return len(msgs) == 1 && msgs[0].Values["job_id"] == j.ID.String() && msgs[0].Values["attempts"] == "2"
	})
	var listed struct{ Data []jobs.Job }
	h.request("GET", "/api/v1/jobs?dead_lettered=true", "", testAPIKey).decode(t, &listed)
	if len(listed.Data) != 1 || listed.Data[0].ID != j.ID {
		t.Fatalf("dead-letter listing: %+v", listed)
	}
	if v := testutil.ToFloat64(h.metrics.JobsDeadLettered.WithLabelValues("DELIBERATE_FAILURE")); v != 1 {
		t.Fatalf("dead-lettered metric = %v", v)
	}

	// Manual retry grants extra attempts and keeps history.
	r := h.request("POST", "/api/v1/jobs/"+j.ID.String()+"/retry", `{"additional_attempts":2}`, testAPIKey)
	var retried jobs.Job
	r.decode(t, &retried)
	if r.Status != 200 || retried.Status != jobs.StatusPending || retried.MaxAttempts != 4 || retried.DeadLetteredAt != nil {
		t.Fatalf("retry: %d %s", r.Status, r.Body)
	}
	again := h.waitStatus(j.ID, 20*time.Second, jobs.StatusFailed)
	if again.Attempts != 4 || len(h.attempts(j.ID)) != 4 {
		t.Fatalf("after manual retry: attempts=%d history=%d", again.Attempts, len(h.attempts(j.ID)))
	}
	// Retrying a non-terminal job is rejected.
	ok := h.submit(`{"type":"DELAYED_TASK","payload":{"duration_ms":2000}}`)
	if r := h.request("POST", "/api/v1/jobs/"+ok.ID.String()+"/retry", "", testAPIKey); r.Status != 409 {
		t.Fatalf("retry of active job: %d %s", r.Status, r.Body)
	}
}

func TestPermanentFailureAndPanicHandling(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	h.startWorker(h.workerConfig("perm-w"), nil, nil)
	perm := h.submit(`{"type":"WEBHOOK_SIMULATION","payload":{"url":"https://example.com","simulate_status":404,"simulate_latency_ms":1},"max_attempts":5}`)
	p := h.waitStatus(perm.ID, 10*time.Second, jobs.StatusFailed)
	if p.Attempts != 1 || deref(p.LastErrorClass) != "permanent" {
		t.Fatalf("permanent error must not retry: %+v", p)
	}
	pan := h.submit(`{"type":"DELIBERATE_FAILURE","payload":{"mode":"panic"},"max_attempts":2}`)
	pj := h.waitStatus(pan.ID, 10*time.Second, jobs.StatusFailed)
	if pj.Attempts != 2 || deref(pj.LastErrorClass) != "panic" {
		t.Fatalf("panics are retried then dead-lettered: %+v", pj)
	}
	// The worker survived the panics and still processes work.
	ok := h.submit(emailJob)
	h.waitStatus(ok.ID, 10*time.Second, jobs.StatusSucceeded)
}

func TestExecutionTimeout(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	h.startWorker(h.workerConfig("timeout-w"), nil, nil)
	j := h.submit(`{"type":"DELAYED_TASK","payload":{"duration_ms":10000},"timeout_seconds":1,"max_attempts":1}`)
	f := h.waitStatus(j.ID, 10*time.Second, jobs.StatusFailed)
	if deref(f.LastErrorClass) != "timeout" || *f.DurationMS < 900 || *f.DurationMS > 3000 {
		t.Fatalf("timeout: class=%s duration=%d", deref(f.LastErrorClass), *f.DurationMS)
	}
}

func TestDuplicateDeliveriesExecuteOnce(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	j := h.submit(`{"type":"DELAYED_TASK","payload":{"duration_ms":300}}`)
	h.waitStatus(j.ID, 5*time.Second, jobs.StatusQueued)
	// Inject duplicate notifications, as produced by a relay crash between
	// XADD and COMMIT or by a client-side retry.
	for i := 0; i < 4; i++ {
		if _, err := h.broker.Publish(h.ctx, "normal", map[string]string{"job_id": j.ID.String(), "event_type": "job.ready"}); err != nil {
			t.Fatal(err)
		}
	}
	h.startWorker(h.workerConfig("dup-a"), nil, nil)
	h.startWorker(h.workerConfig("dup-b"), nil, nil)
	done := h.waitStatus(j.ID, 10*time.Second, jobs.StatusSucceeded)
	if done.Attempts != 1 || len(h.attempts(j.ID)) != 1 {
		t.Fatalf("duplicate deliveries caused re-execution: attempts=%d", done.Attempts)
	}
	waitFor(t, 5*time.Second, "duplicates acknowledged", func() bool {
		st, _ := h.broker.Stats(h.ctx, "normal")
		return st.Depth() == 0
	})
	if v := testutil.ToFloat64(h.metrics.DuplicateDeliveries); v != 4 {
		t.Fatalf("duplicate metric = %v, want 4", v)
	}
	// Malformed messages are dropped and acknowledged, not retried forever.
	_, _ = h.broker.Publish(h.ctx, "normal", map[string]string{"job_id": "garbage"})
	_, _ = h.broker.Publish(h.ctx, "normal", map[string]string{"job_id": uuid.NewString()})
	waitFor(t, 5*time.Second, "poison messages acknowledged", func() bool {
		st, _ := h.broker.Stats(h.ctx, "normal")
		return st.Depth() == 0
	})
}

func TestCancellation(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	// Pending (delayed) job: cancelled immediately, outbox event discarded.
	delayed := h.submit(`{"type":"DELAYED_TASK","payload":{"duration_ms":1},"delay_seconds":60}`)
	r := h.request("POST", "/api/v1/jobs/"+delayed.ID.String()+"/cancel", "", testAPIKey)
	if r.Status != 200 || h.get(delayed.ID).Status != jobs.StatusCancelled {
		t.Fatalf("cancel pending: %d %s", r.Status, r.Body)
	}
	var evStatus string
	_ = h.pool.QueryRow(h.ctx, `SELECT status FROM outbox_events WHERE job_id = $1`, delayed.ID).Scan(&evStatus)
	if evStatus != "discarded" {
		t.Fatalf("outbox event should be discarded, is %s", evStatus)
	}
	if r := h.request("POST", "/api/v1/jobs/"+delayed.ID.String()+"/cancel", "", testAPIKey); r.Status != 409 {
		t.Fatalf("double cancel: %d", r.Status)
	}

	// Queued job: cancelled before any worker picks it up; never executes.
	queued := h.submit(`{"type":"DELAYED_TASK","payload":{"duration_ms":1}}`)
	h.waitStatus(queued.ID, 5*time.Second, jobs.StatusQueued)
	if r := h.request("POST", "/api/v1/jobs/"+queued.ID.String()+"/cancel", "", testAPIKey); r.Status != 200 {
		t.Fatalf("cancel queued: %d", r.Status)
	}

	// Running job: cancellation requested, handler stopped via context.
	h.startWorker(h.workerConfig("cancel-w"), nil, nil)
	running := h.submit(`{"type":"DELAYED_TASK","payload":{"duration_ms":20000}}`)
	h.waitStatus(running.ID, 5*time.Second, jobs.StatusRunning)
	r = h.request("POST", "/api/v1/jobs/"+running.ID.String()+"/cancel", "", testAPIKey)
	var accepted jobs.Job
	r.decode(t, &accepted)
	if r.Status != 202 || !accepted.CancelRequested {
		t.Fatalf("cancel running: %d %s", r.Status, r.Body)
	}
	c := h.waitStatus(running.ID, 5*time.Second, jobs.StatusCancelled)
	if *c.DurationMS > 3000 {
		t.Fatalf("handler was not interrupted promptly: %dms", *c.DurationMS)
	}
	at := h.attempts(running.ID)
	if len(at) != 1 || at[0].Status != jobs.AttemptCancelled {
		t.Fatalf("attempt: %+v", at)
	}
	time.Sleep(300 * time.Millisecond)
	if n := len(h.attempts(queued.ID)); n != 0 {
		t.Fatalf("cancelled queued job was executed %d times", n)
	}
}

// TestConcurrentCancelAndCompletion races client cancellation against job
// completion many times and checks that exactly one outcome is recorded
// consistently in the job row and its attempt history.
func TestConcurrentCancelAndCompletion(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	cfg := h.workerConfig("race-w")
	cfg.HeartbeatInterval = 20 * time.Millisecond
	cfg.Concurrency = 8
	h.startWorker(cfg, nil, nil)

	const n = 30
	var wg sync.WaitGroup
	codes := make([]int, n)
	ids := make([]uuid.UUID, n)
	for i := 0; i < n; i++ {
		ids[i] = h.submit(fmt.Sprintf(`{"type":"DELAYED_TASK","payload":{"duration_ms":%d}}`, 40+i*3)).ID
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(30+i*4) * time.Millisecond)
			codes[i] = h.request("POST", "/api/v1/jobs/"+ids[i].String()+"/cancel", "", testAPIKey).Status
		}(i)
	}
	wg.Wait()
	outcomes := map[jobs.Status]int{}
	for i, id := range ids {
		j := h.waitStatus(id, 10*time.Second, jobs.StatusSucceeded, jobs.StatusCancelled)
		outcomes[j.Status]++
		at := h.attempts(id)
		switch j.Status {
		case jobs.StatusSucceeded:
			if codes[i] == 200 {
				t.Fatalf("job %d: API reported immediate cancellation but job succeeded", i)
			}
			if len(at) != 1 || at[0].Status != jobs.AttemptSucceeded || len(j.Result) == 0 {
				t.Fatalf("job %d succeeded with inconsistent history: %+v", i, at)
			}
		case jobs.StatusCancelled:
			if codes[i] == 409 {
				t.Fatalf("job %d: API said terminal but job ended cancelled", i)
			}
			if len(at) > 1 || (len(at) == 1 && at[0].Status != jobs.AttemptCancelled) || j.Result != nil {
				t.Fatalf("job %d cancelled with inconsistent history: %+v", i, at)
			}
		}
	}
	t.Logf("race outcomes: %v (cancel HTTP codes: %v)", outcomes, codes)
}

func TestResultIsStoredAsJSON(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	h.startWorker(h.workerConfig("res-w"), nil, nil)
	j := h.submit(`{"type":"EMAIL_SIMULATION","payload":{"to":"x@example.com","subject":"s","simulate_latency_ms":1}}`)
	h.waitStatus(j.ID, 10*time.Second, jobs.StatusSucceeded)
	var got jobs.Job
	h.request("GET", "/api/v1/jobs/"+j.ID.String(), "", testAPIKey).decode(t, &got)
	var res map[string]any
	if err := json.Unmarshal(got.Result, &res); err != nil || res["message_id"] != "<"+j.ID.String()+"@eventflow.simulated>" {
		t.Fatalf("result: %s %v", got.Result, err)
	}
}
