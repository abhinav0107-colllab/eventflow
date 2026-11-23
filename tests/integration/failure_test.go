//go:build integration

package integration

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/abhinav0107-colllab/eventflow/internal/broker"
	"github.com/abhinav0107-colllab/eventflow/internal/database"
	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
)

// startWorkerProcess runs the real cmd/worker binary as a child process.
func (h *harness) startWorkerProcess(id string, extraEnv ...string) *exec.Cmd {
	h.t.Helper()
	cmd := exec.Command(workerBinary)
	cmd.Env = append(os.Environ(),
		"EVENTFLOW_DATABASE_URL="+pgURL,
		"EVENTFLOW_REDIS_ADDR="+redisAddr,
		"EVENTFLOW_STREAM_PREFIX="+h.prefix,
		"EVENTFLOW_CONSUMER_GROUP="+group,
		"EVENTFLOW_WORKER_ID="+id,
		"EVENTFLOW_WORKER_CONCURRENCY=2",
		"EVENTFLOW_WORKER_METRICS_ADDR=127.0.0.1:0",
		"EVENTFLOW_LEASE_DURATION=2s",
		"EVENTFLOW_HEARTBEAT_INTERVAL=300ms",
		"EVENTFLOW_BLOCK_TIMEOUT=100ms",
		"EVENTFLOW_RECOVERY_INTERVAL=1h", // recovery is exercised by the surviving worker
		"EVENTFLOW_LOG_LEVEL=warn",
	)
	cmd.Env = append(cmd.Env, extraEnv...)
	if os.Getenv("EVENTFLOW_TEST_LOG") != "" {
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	}
	if err := cmd.Start(); err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	})
	return cmd
}

// TestWorkerCrashRecovery kills a real worker process with SIGKILL while it
// executes a job, then verifies that a surviving worker's recovery manager
// detects the expired lease, closes the attempt as lost, re-queues the job
// and completes it, and that the dead consumer's pending message is
// re-claimed and acknowledged.
func TestWorkerCrashRecovery(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	proc := h.startWorkerProcess("doomed-worker")
	j := h.submit(`{"type":"DELAYED_TASK","payload":{"duration_ms":4000},"max_attempts":3}`)
	running := h.waitStatus(j.ID, 15*time.Second, jobs.StatusRunning)
	if deref(running.WorkerID) != "doomed-worker" {
		t.Fatalf("job picked up by %s", deref(running.WorkerID))
	}

	if err := proc.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_, _ = proc.Process.Wait()
	killedAt := time.Now()

	// A surviving worker with recovery enabled.
	h.startWorker(h.workerConfig("survivor"), nil, nil)
	h.startRecovery(time.Minute)

	done := h.waitStatus(j.ID, 30*time.Second, jobs.StatusSucceeded)
	if done.Attempts != 2 || deref(done.WorkerID) != "survivor" {
		t.Fatalf("expected completion on attempt 2 by survivor: %+v", done)
	}
	at := h.attempts(j.ID)
	if len(at) != 2 || at[0].Status != jobs.AttemptLost || deref(at[0].ErrorClass) != "lease_expired" ||
		at[0].WorkerID != "doomed-worker" || at[1].Status != jobs.AttemptSucceeded {
		t.Fatalf("attempt history: %+v", at)
	}
	if at[1].StartedAt.Before(killedAt) {
		t.Fatal("second attempt started before the crash")
	}
	waitFor(t, 10*time.Second, "dead consumer's message re-claimed and acked", func() bool {
		st, _ := h.broker.Stats(h.ctx, "normal")
		return st.Depth() == 0
	})
	waitFor(t, 10*time.Second, "crashed worker marked dead", func() bool {
		return h.queryInt(`SELECT count(*) FROM workers WHERE id = 'doomed-worker' AND status = 'dead'`) == 1
	})
	if v := testutil.ToFloat64(h.metrics.RecoveredJobs.WithLabelValues("lease_expired")); v != 1 {
		t.Fatalf("recovered metric = %v", v)
	}
}

// TestWorkerProcessGracefulShutdownAndRestart sends SIGTERM to a real worker
// process mid-job: it must finish the job, deregister, and exit 0; a
// restarted process with the same ID then processes new work.
func TestWorkerProcessGracefulShutdownAndRestart(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	proc := h.startWorkerProcess("restartable", "EVENTFLOW_SHUTDOWN_TIMEOUT=10s")
	j := h.submit(`{"type":"DELAYED_TASK","payload":{"duration_ms":1500}}`)
	h.waitStatus(j.ID, 15*time.Second, jobs.StatusRunning)
	if err := proc.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- proc.Wait() }()
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("worker exited with error: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("worker did not exit")
	}
	done := h.get(j.ID)
	if done.Status != jobs.StatusSucceeded || done.Attempts != 1 {
		t.Fatalf("in-flight job should complete during graceful shutdown: %+v", done)
	}
	if h.queryInt(`SELECT count(*) FROM workers WHERE id = 'restartable' AND status = 'stopped'`) != 1 {
		t.Fatal("worker not marked stopped")
	}
	st, _ := h.broker.Stats(h.ctx, "normal")
	if st.Pending != 0 {
		t.Fatalf("message left unacknowledged: %+v", st)
	}

	h.startWorkerProcess("restartable")
	next := h.submit(emailJob)
	h.waitStatus(next.ID, 15*time.Second, jobs.StatusSucceeded)
	if h.queryInt(`SELECT count(*) FROM workers WHERE id = 'restartable' AND status = 'active'`) != 1 {
		t.Fatal("restarted worker not re-registered as active")
	}
}

// TestShutdownTimeoutInterruptsAndReschedules checks that jobs exceeding the
// shutdown grace period are interrupted, recorded and rescheduled rather
// than lost.
func TestShutdownTimeoutInterruptsAndReschedules(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	cfg := h.workerConfig("impatient")
	cfg.ShutdownTimeout = 300 * time.Millisecond
	w := h.startWorker(cfg, nil, nil)
	j := h.submit(`{"type":"DELAYED_TASK","payload":{"duration_ms":30000},"max_attempts":3}`)
	h.waitStatus(j.ID, 5*time.Second, jobs.StatusRunning)
	start := time.Now()
	w.Stop(t)
	if time.Since(start) > 5*time.Second {
		t.Fatalf("shutdown took %s", time.Since(start))
	}
	got := h.get(j.ID)
	if got.Status != jobs.StatusPending || deref(got.LastErrorClass) != "interrupted" {
		t.Fatalf("interrupted job should be rescheduled: %+v", got)
	}
	at := h.attempts(j.ID)
	if len(at) != 1 || at[0].Status != jobs.AttemptFailed || deref(at[0].ErrorClass) != "interrupted" {
		t.Fatalf("attempt: %+v", at)
	}
	// Another worker picks the job up again.
	if _, err := h.pool.Exec(h.ctx, `UPDATE jobs SET payload = '{"duration_ms": 10}' WHERE id = $1`, j.ID); err != nil {
		t.Fatal(err)
	}
	h.startWorker(h.workerConfig("successor"), nil, nil)
	done := h.waitStatus(j.ID, 10*time.Second, jobs.StatusSucceeded)
	if done.Attempts != 2 {
		t.Fatalf("attempts = %d", done.Attempts)
	}
}

// TestExpiredLeaseFencesStaleWorker simulates a worker that stalls (e.g. a
// long GC pause or network partition) past its lease. Recovery hands the
// job to another worker, and the stale worker's late completion is rejected
// so that only one completion is ever recorded.
func TestExpiredLeaseFencesStaleWorker(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	j := h.submit(`{"type":"EMAIL_SIMULATION","payload":{"to":"a@example.com","subject":"s","simulate_latency_ms":1},"max_attempts":3}`)
	h.waitStatus(j.ID, 5*time.Second, jobs.StatusQueued)

	stale, err := h.store.Claim(h.ctx, j.ID, "stalled-worker", 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	staleLease := jobs.Lease{JobID: j.ID, WorkerID: "stalled-worker", Attempt: stale.Attempts}

	// A second claim of the same job is impossible while it is running.
	if _, err := h.store.Claim(h.ctx, j.ID, "other", time.Second); !errors.Is(err, jobs.ErrNotClaimable) {
		t.Fatalf("double claim: %v", err)
	}
	time.Sleep(400 * time.Millisecond) // lease expires; no heartbeat renews it
	h.recovery(time.Minute).RunOnce(h.ctx)
	if s := h.get(j.ID).Status; s != jobs.StatusPending {
		t.Fatalf("recovery should reschedule the job, status %s", s)
	}

	// Heartbeat from the stale worker no longer extends anything.
	states, err := h.store.ExtendLeases(h.ctx, "stalled-worker", []jobs.Lease{staleLease}, time.Minute)
	if err != nil || len(states) != 0 {
		t.Fatalf("stale lease extended: %v %v", states, err)
	}

	h.startWorker(h.workerConfig("healthy"), nil, nil)
	done := h.waitStatus(j.ID, 10*time.Second, jobs.StatusSucceeded)

	// The stalled worker wakes up and tries to record its outcome.
	if _, err := h.store.Complete(h.ctx, staleLease, []byte(`{"stale":true}`), time.Second); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatalf("stale completion must be rejected, got %v", err)
	}
	if _, err := h.store.Fail(h.ctx, staleLease, jobs.Failure{Err: "late", Class: jobs.ClassError}); !errors.Is(err, jobs.ErrLeaseLost) {
		t.Fatalf("stale failure must be rejected, got %v", err)
	}
	final := h.get(j.ID)
	if final.Version != done.Version || strings.Contains(string(final.Result), "stale") {
		t.Fatalf("stale worker modified the job: %+v", final)
	}
	at := h.attempts(j.ID)
	if len(at) != 2 || at[0].Status != jobs.AttemptLost || at[1].Status != jobs.AttemptSucceeded {
		t.Fatalf("history: %+v", at)
	}
}

// TestLeaseLossStopsRunningHandler verifies that a worker which discovers at
// heartbeat time that its lease was revoked cancels the handler instead of
// continuing to produce side effects.
func TestLeaseLossStopsRunningHandler(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	cfg := h.workerConfig("revoked")
	h.startWorker(cfg, nil, nil)
	j := h.submit(`{"type":"DELAYED_TASK","payload":{"duration_ms":20000},"max_attempts":3}`)
	h.waitStatus(j.ID, 5*time.Second, jobs.StatusRunning)
	// Force-expire the lease (as if the heartbeats had been delayed) and
	// let recovery reassign the job.
	if _, err := h.pool.Exec(h.ctx, `UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, j.ID); err != nil {
		t.Fatal(err)
	}
	h.recovery(time.Minute).RunOnce(h.ctx)
	waitFor(t, 5*time.Second, "handler to stop after lease loss", func() bool {
		return testutil.ToFloat64(h.metrics.WorkerFailures.WithLabelValues("lease_lost")) >= 1
	})
	// The rescheduled attempt runs (on the same worker) and the original,
	// revoked attempt never records a result.
	waitFor(t, 5*time.Second, "job re-run", func() bool { return h.get(j.ID).Attempts == 2 })
	at := h.attempts(j.ID)
	if at[0].Status != jobs.AttemptLost {
		t.Fatalf("first attempt should be lost: %+v", at[0])
	}
}

// TestWorkerSurvivesRedisInterruption cuts the worker's Redis connection,
// keeps submitting work, and restores it: every job must complete exactly
// once.
func TestWorkerSurvivesRedisInterruption(t *testing.T) {
	p := newFaultProxy(t, redisAddr)
	h := newHarness(t, harnessOpts{})
	wb := broker.New(broker.NewClient(broker.ClientConfig{Addr: p.Addr()}), h.prefix, group)
	h.startWorker(h.workerConfig("flaky-redis"), wb, nil)
	h.startRecovery(2 * time.Second)

	first := h.submit(emailJob)
	h.waitStatus(first.ID, 10*time.Second, jobs.StatusSucceeded)

	p.Disable()
	var during []jobs.Job
	for i := 0; i < 5; i++ {
		during = append(during, h.submit(emailJob))
	}
	time.Sleep(1500 * time.Millisecond)
	for _, j := range during {
		if s := h.get(j.ID).Status; s == jobs.StatusSucceeded || s == jobs.StatusRunning {
			t.Fatalf("job ran while the worker had no Redis connection: %s", s)
		}
	}
	p.Enable()
	for _, j := range during {
		done := h.waitStatus(j.ID, 20*time.Second, jobs.StatusSucceeded)
		if done.Attempts != 1 {
			t.Fatalf("job executed %d times", done.Attempts)
		}
	}
	if v := testutil.ToFloat64(h.metrics.WorkerFailures.WithLabelValues("broker")); v < 1 {
		t.Fatalf("broker failures should be counted, got %v", v)
	}
}

// TestDatabaseOutageDuringExecution cuts the worker's PostgreSQL connection
// while a job runs. The worker must retry recording the outcome and, once
// the database is back, record exactly one success.
func TestDatabaseOutageDuringExecution(t *testing.T) {
	p := newFaultProxy(t, pgHostPort())
	h := newHarness(t, harnessOpts{})
	wpool, err := database.Connect(h.ctx, database.PoolConfig{URL: dsnVia(p.Addr()), MaxConns: 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(wpool.Close)
	cfg := h.workerConfig("flaky-db")
	cfg.LeaseDuration = 10 * time.Second
	cfg.HeartbeatInterval = 500 * time.Millisecond
	h.startWorker(cfg, nil, jobs.NewStore(wpool))

	j := h.submit(`{"type":"DELAYED_TASK","payload":{"duration_ms":800}}`)
	h.waitStatus(j.ID, 10*time.Second, jobs.StatusRunning)
	p.Disable()
	time.Sleep(2 * time.Second) // job finishes while the DB is unreachable
	if s := h.get(j.ID).Status; s != jobs.StatusRunning {
		t.Fatalf("status changed while the worker was cut off: %s", s)
	}
	p.Enable()
	done := h.waitStatus(j.ID, 15*time.Second, jobs.StatusSucceeded)
	if done.Attempts != 1 || len(h.attempts(j.ID)) != 1 {
		t.Fatalf("expected a single recorded attempt, got %d", done.Attempts)
	}
	if v := testutil.ToFloat64(h.metrics.WorkerFailures.WithLabelValues("database")); v < 1 {
		t.Fatalf("database failures should be counted, got %v", v)
	}
	waitFor(t, 5*time.Second, "message acknowledged", func() bool {
		st, _ := h.broker.Stats(h.ctx, "normal")
		return st.Depth() == 0
	})
}

// TestLostBrokerMessagesAreRepublished simulates Redis losing its data (e.g.
// restart without persistence) after jobs were marked queued. Recovery
// detects queued jobs with a drained stream and re-publishes them.
func TestLostBrokerMessagesAreRepublished(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	var ids []string
	for i := 0; i < 3; i++ {
		j := h.submit(emailJob)
		h.waitStatus(j.ID, 5*time.Second, jobs.StatusQueued)
		ids = append(ids, j.ID.String())
	}
	if err := h.rdb.FlushAll(h.ctx).Err(); err != nil {
		t.Fatal(err)
	}
	h.startWorker(h.workerConfig("after-flush"), nil, nil) // recreates the consumer group
	time.Sleep(500 * time.Millisecond)
	if n := h.queryInt(`SELECT count(*) FROM jobs WHERE status = 'queued'`); n != 3 {
		t.Fatalf("jobs should be stranded in queued before recovery, found %d", n)
	}
	h.startRecovery(300 * time.Millisecond)
	for _, id := range ids {
		var s string
		waitFor(t, 15*time.Second, "republished job to complete", func() bool {
			_ = h.pool.QueryRow(context.Background(), `SELECT status FROM jobs WHERE id = $1`, id).Scan(&s)
			return s == "succeeded"
		})
	}
	if v := testutil.ToFloat64(h.metrics.RecoveredJobs.WithLabelValues("lost_message")); v != 3 {
		t.Fatalf("lost_message recoveries = %v, want 3", v)
	}
	if n := h.queryInt(`SELECT count(*) FROM job_attempts`); n != 3 {
		t.Fatalf("each job should run exactly once, attempts = %d", n)
	}
}
