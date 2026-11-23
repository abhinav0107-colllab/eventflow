package api

import (
	"context"
	"net/http"
	"time"

	"github.com/abhinav0107-colllab/eventflow/internal/jobs"
	mw "github.com/abhinav0107-colllab/eventflow/internal/middleware"
	"github.com/abhinav0107-colllab/eventflow/internal/outbox"
)

// QueueInfo combines broker and database views of one queue.
type QueueInfo struct {
	Name      string            `json:"name"`
	Stream    string            `json:"stream"`
	Length    int64             `json:"stream_length"`
	Lag       int64             `json:"undelivered"`
	Pending   int64             `json:"unacknowledged"`
	Consumers int64             `json:"consumers"`
	Jobs      *jobs.QueueCounts `json:"jobs,omitempty"`
}

// QueuesResponse is returned by GET /api/v1/queues.
type QueuesResponse struct {
	Queues        []QueueInfo `json:"queues"`
	DeadLetter    DLQInfo     `json:"dead_letter"`
	Outbox        OutboxInfo  `json:"outbox"`
	ActiveWorkers int64       `json:"active_workers"`
}

// DLQInfo describes the dead-letter queue.
type DLQInfo struct {
	Stream       string `json:"stream"`
	StreamLength int64  `json:"stream_length"`
	Jobs         int64  `json:"jobs"`
}

// OutboxInfo describes the outbox backlog.
type OutboxInfo struct {
	Backlog          int64   `json:"backlog"`
	OldestAgeSeconds float64 `json:"oldest_age_seconds"`
}

func (s *Server) collect(ctx context.Context) (QueuesResponse, error) {
	var resp QueuesResponse
	st, err := s.store.Stats(ctx)
	if err != nil {
		return resp, err
	}
	for _, q := range jobs.WorkQueues {
		bs, err := s.broker.Stats(ctx, string(q))
		if err != nil {
			return resp, err
		}
		resp.Queues = append(resp.Queues, QueueInfo{
			Name: string(q), Stream: bs.Stream, Length: bs.Length, Lag: bs.Lag, Pending: bs.Pending,
			Consumers: bs.Consumers, Jobs: st.ByQueue[q],
		})
	}
	dl, err := s.broker.Stats(ctx, string(jobs.QueueDeadLetter))
	if err != nil {
		return resp, err
	}
	resp.DeadLetter = DLQInfo{Stream: dl.Stream, StreamLength: dl.Length, Jobs: st.DeadLettered}
	n, age, err := outbox.Backlog(ctx, s.pool)
	if err != nil {
		return resp, err
	}
	resp.Outbox = OutboxInfo{Backlog: n, OldestAgeSeconds: age.Seconds()}
	resp.ActiveWorkers, err = s.workers.CountActive(ctx, s.cfg.WorkerActiveWindow)
	return resp, err
}

func (s *Server) listQueues(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	resp, err := s.collect(ctx)
	if err != nil {
		s.internalError(w, r, "queue stats", err)
		return
	}
	mw.WriteJSON(w, http.StatusOK, resp)
}

// RunStatsCollector refreshes system-level gauges (queue depth, outbox
// backlog, active workers, jobs by state) every interval. Only the API
// process exports these, so they are not double counted across workers.
func (s *Server) RunStatsCollector(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		cctx, cancel := context.WithTimeout(ctx, interval)
		resp, err := s.collect(cctx)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				s.log.Warn("stats collection failed", "error", err.Error())
			}
		} else {
			for _, q := range resp.Queues {
				s.metrics.QueueDepth.WithLabelValues(q.Name).Set(float64(q.Lag + q.Pending))
				s.metrics.QueuePending.WithLabelValues(q.Name).Set(float64(q.Pending))
				if q.Jobs != nil {
					s.metrics.JobsInState.WithLabelValues(q.Name, "pending").Set(float64(q.Jobs.Pending))
					s.metrics.JobsInState.WithLabelValues(q.Name, "queued").Set(float64(q.Jobs.Queued))
					s.metrics.JobsInState.WithLabelValues(q.Name, "running").Set(float64(q.Jobs.Running))
				}
			}
			s.metrics.DeadLettered.Set(float64(resp.DeadLetter.Jobs))
			s.metrics.OutboxBacklog.Set(float64(resp.Outbox.Backlog))
			s.metrics.OutboxOldestAge.Set(resp.Outbox.OldestAgeSeconds)
			s.metrics.ActiveWorkers.Set(float64(resp.ActiveWorkers))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
