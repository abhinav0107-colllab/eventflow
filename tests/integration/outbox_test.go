//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/abhinav0107-colllab/eventflow/internal/broker"
	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
)

func TestRelayPublishesAndMarksJobsQueued(t *testing.T) {
	h := newHarness(t, harnessOpts{noRelay: true})
	hi := h.submit(`{"type":"EMAIL_SIMULATION","payload":{"to":"a@example.com","subject":"s"},"priority":10}`)
	lo := h.submit(`{"type":"EMAIL_SIMULATION","payload":{"to":"a@example.com","subject":"s"},"priority":1}`)

	n, err := h.newRelay(h.broker, time.Second).PublishOnce(h.ctx)
	if err != nil || n != 2 {
		t.Fatalf("PublishOnce = %d, %v", n, err)
	}
	for _, c := range []struct {
		job    jobs.Job
		stream string
	}{{hi, h.broker.StreamKey("high")}, {lo, h.broker.StreamKey("low")}} {
		msgs, err := h.rdb.XRange(h.ctx, c.stream, "-", "+").Result()
		if err != nil || len(msgs) != 1 || msgs[0].Values["job_id"] != c.job.ID.String() {
			t.Fatalf("stream %s: %v %v", c.stream, msgs, err)
		}
		if msgs[0].Values["event_type"] != "job.ready" || msgs[0].Values["event_id"] == "" {
			t.Fatalf("message fields: %v", msgs[0].Values)
		}
		j := h.get(c.job.ID)
		if j.Status != jobs.StatusQueued || j.QueuedAt == nil {
			t.Fatalf("job not queued: %+v", j)
		}
		var status, msgID string
		_ = h.pool.QueryRow(h.ctx, `SELECT status, stream_message_id FROM outbox_events WHERE job_id = $1`, c.job.ID).Scan(&status, &msgID)
		if status != "published" || msgID != msgs[0].ID {
			t.Fatalf("outbox row: %s %s (stream id %s)", status, msgID, msgs[0].ID)
		}
	}
	if n, err := h.newRelay(h.broker, time.Second).PublishOnce(h.ctx); err != nil || n != 0 {
		t.Fatalf("second PublishOnce must be empty: %d %v", n, err)
	}
}

func TestDelayedJobIsPublishedOnlyWhenDue(t *testing.T) {
	h := newHarness(t, harnessOpts{noRelay: true})
	j := h.submit(`{"type":"DELAYED_TASK","payload":{"duration_ms":1},"delay_seconds":2}`)
	relay := h.newRelay(h.broker, time.Second)
	if n, _ := relay.PublishOnce(h.ctx); n != 0 {
		t.Fatalf("delayed job published early")
	}
	if s := h.get(j.ID).Status; s != jobs.StatusPending {
		t.Fatalf("status %s", s)
	}
	waitFor(t, 5*time.Second, "delayed job to be published", func() bool {
		n, _ := relay.PublishOnce(h.ctx)
		return n == 1
	})
	got := h.get(j.ID)
	if got.Status != jobs.StatusQueued || got.QueuedAt.Before(j.ScheduledAt) {
		t.Fatalf("published before scheduled time: queued %v scheduled %v", got.QueuedAt, j.ScheduledAt)
	}
}

func TestScheduledJobRunsAtScheduledTime(t *testing.T) {
	h := newHarness(t, harnessOpts{})
	h.startWorker(h.workerConfig("sched-worker"), nil, nil)
	at := time.Now().Add(1500 * time.Millisecond).UTC()
	j := h.submit(`{"type":"DELAYED_TASK","payload":{"duration_ms":1},"scheduled_at":"` + at.Format(time.RFC3339Nano) + `"}`)
	done := h.waitStatus(j.ID, 10*time.Second, jobs.StatusSucceeded)
	if done.StartedAt.Before(at) {
		t.Fatalf("started %v before scheduled %v", done.StartedAt, at)
	}
}

func TestRelaySurvivesRedisOutage(t *testing.T) {
	p := newFaultProxy(t, redisAddr)
	h := newHarness(t, harnessOpts{noRelay: true})
	viaProxy := broker.New(broker.NewClient(broker.ClientConfig{Addr: p.Addr()}), h.prefix, group)
	relay := h.newRelay(viaProxy, time.Second)

	p.Disable()
	j := h.submit(emailJob)
	if _, err := relay.PublishOnce(h.ctx); err == nil {
		t.Fatal("expected publish error while Redis is unreachable")
	}
	var attempts int
	var lastErr *string
	_ = h.pool.QueryRow(h.ctx, `SELECT attempts, last_error FROM outbox_events WHERE job_id = $1`, j.ID).Scan(&attempts, &lastErr)
	if attempts != 1 || lastErr == nil || *lastErr == "" {
		t.Fatalf("failure not recorded: attempts=%d err=%v", attempts, lastErr)
	}
	// The job must not be marked as published.
	if s := h.get(j.ID).Status; s != jobs.StatusPending {
		t.Fatalf("job marked %s without a successful publication", s)
	}
	if n, _ := h.rdb.XLen(h.ctx, h.broker.StreamKey("normal")).Result(); n != 0 {
		t.Fatalf("nothing should reach Redis, found %d", n)
	}

	p.Enable()
	waitFor(t, 10*time.Second, "relay to recover", func() bool {
		_, _ = relay.PublishOnce(h.ctx)
		return h.get(j.ID).Status == jobs.StatusQueued
	})
	if n, _ := h.rdb.XLen(h.ctx, h.broker.StreamKey("normal")).Result(); n != 1 {
		t.Fatalf("expected exactly one message after recovery, found %d", n)
	}
}

func TestStreamTrimKeepsUndeliveredAndUnacked(t *testing.T) {
	h := newHarness(t, harnessOpts{noRelay: true})
	key := h.broker.StreamKey("normal")
	for i := 0; i < 10; i++ {
		if _, err := h.broker.Publish(h.ctx, "normal", map[string]string{"n": string(rune('0' + i))}); err != nil {
			t.Fatal(err)
		}
	}
	msgs, err := h.broker.Read(h.ctx, "c1", []string{"normal"}, 5, 10*time.Millisecond)
	if err != nil || len(msgs) != 5 {
		t.Fatalf("read: %d %v", len(msgs), err)
	}
	for _, m := range msgs[:3] {
		_ = h.broker.Ack(h.ctx, "normal", m.ID)
	}
	trimmed, err := h.broker.Trim(h.ctx, "normal")
	if err != nil || trimmed != 3 {
		t.Fatalf("trim removed %d (%v), want 3 acknowledged entries", trimmed, err)
	}
	rest, _ := h.rdb.XRange(h.ctx, key, "-", "+").Result()
	if len(rest) != 7 || rest[0].ID != msgs[3].ID {
		t.Fatalf("trim removed unacknowledged or undelivered entries: %d left", len(rest))
	}
	st, _ := h.broker.Stats(h.ctx, "normal")
	if st.Pending != 2 || st.Lag != 5 || st.Depth() != 7 {
		t.Fatalf("stats after trim: %+v", st)
	}
	pend, _ := h.rdb.XPendingExt(h.ctx, &redis.XPendingExtArgs{Stream: key, Group: group, Start: "-", End: "+", Count: 10}).Result()
	if len(pend) != 2 {
		t.Fatalf("pending entries: %d", len(pend))
	}
}
