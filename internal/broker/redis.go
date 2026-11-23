// Package broker wraps Redis Streams: one stream per work queue, consumed by
// a single consumer group shared by all workers.
//
// Redis is a transport only. Messages carry a job ID; the authoritative job
// state lives in PostgreSQL. Delivery is at-least-once: a message stays in
// the group's Pending Entries List (PEL) until acknowledged and is re-claimed
// by another consumer if its owner disappears.
package broker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Message is one delivered stream entry.
type Message struct {
	Queue  string
	ID     string
	Fields map[string]string
}

// Broker is a Redis Streams client bound to a key prefix and consumer group.
type Broker struct {
	rdb    redis.UniversalClient
	prefix string
	group  string
}

// New returns a Broker.
func New(rdb redis.UniversalClient, prefix, group string) *Broker {
	return &Broker{rdb: rdb, prefix: prefix, group: group}
}

// Client exposes the underlying client (used for readiness checks).
func (b *Broker) Client() redis.UniversalClient { return b.rdb }

// Group returns the consumer group name.
func (b *Broker) Group() string { return b.group }

// StreamKey maps a queue name to its Redis key.
func (b *Broker) StreamKey(queue string) string { return b.prefix + ":jobs:" + queue }

func (b *Broker) queueOf(key string) string {
	return strings.TrimPrefix(key, b.prefix+":jobs:")
}

// Ping checks connectivity.
func (b *Broker) Ping(ctx context.Context) error { return b.rdb.Ping(ctx).Err() }

// EnsureGroups creates the consumer group on each queue's stream, creating
// the stream if needed. Starting at ID 0 means messages published before the
// group existed are still delivered.
func (b *Broker) EnsureGroups(ctx context.Context, queues []string) error {
	for _, q := range queues {
		err := b.rdb.XGroupCreateMkStream(ctx, b.StreamKey(q), b.group, "0").Err()
		if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
			return fmt.Errorf("create consumer group on %s: %w", q, err)
		}
	}
	return nil
}

// OutMessage is a message to publish.
type OutMessage struct {
	Queue  string
	Fields map[string]string
}

// PublishBatch XADDs messages in one pipeline round trip and returns, per
// message, the assigned stream ID or an error.
func (b *Broker) PublishBatch(ctx context.Context, msgs []OutMessage) ([]string, []error) {
	ids := make([]string, len(msgs))
	errs := make([]error, len(msgs))
	if len(msgs) == 0 {
		return ids, errs
	}
	pipe := b.rdb.Pipeline()
	cmds := make([]*redis.StringCmd, len(msgs))
	for i, m := range msgs {
		vals := make([]any, 0, len(m.Fields)*2)
		for k, v := range m.Fields {
			vals = append(vals, k, v)
		}
		cmds[i] = pipe.XAdd(ctx, &redis.XAddArgs{Stream: b.StreamKey(m.Queue), Values: vals})
	}
	_, pipeErr := pipe.Exec(ctx)
	for i, c := range cmds {
		ids[i], errs[i] = c.Result()
		if errs[i] == nil && ids[i] == "" {
			errs[i] = pipeErr
		}
	}
	return ids, errs
}

// Publish XADDs a single message.
func (b *Broker) Publish(ctx context.Context, queue string, fields map[string]string) (string, error) {
	ids, errs := b.PublishBatch(ctx, []OutMessage{{Queue: queue, Fields: fields}})
	return ids[0], errs[0]
}

func toMessages(streams []redis.XStream, b *Broker) []Message {
	var out []Message
	for _, s := range streams {
		q := b.queueOf(s.Stream)
		for _, m := range s.Messages {
			out = append(out, toMessage(q, m))
		}
	}
	return out
}

func toMessage(queue string, m redis.XMessage) Message {
	f := make(map[string]string, len(m.Values))
	for k, v := range m.Values {
		if s, ok := v.(string); ok {
			f[k] = s
		}
	}
	return Message{Queue: queue, ID: m.ID, Fields: f}
}

// Read fetches up to count new messages for consumer, honouring strict queue
// priority: queues earlier in the slice are drained first. If every queue is
// empty it blocks on all of them for up to block.
func (b *Broker) Read(ctx context.Context, consumer string, queues []string, count int, block time.Duration) ([]Message, error) {
	for _, q := range queues {
		res, err := b.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: b.group, Consumer: consumer,
			Streams: []string{b.StreamKey(q), ">"},
			Count:   int64(count), Block: -1, // non-blocking probe
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, err
		}
		if msgs := toMessages(res, b); len(msgs) > 0 {
			return msgs, nil
		}
	}
	streams := make([]string, 0, len(queues)*2)
	for _, q := range queues {
		streams = append(streams, b.StreamKey(q))
	}
	for range queues {
		streams = append(streams, ">")
	}
	// Block on all queues; when several have data at once Redis returns up
	// to count from each, which is acceptable right after an idle period.
	res, err := b.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: b.group, Consumer: consumer, Streams: streams, Count: int64(count), Block: block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return toMessages(res, b), nil
}

// Ack acknowledges a message, removing it from the PEL.
func (b *Broker) Ack(ctx context.Context, queue, id string) error {
	return b.rdb.XAck(ctx, b.StreamKey(queue), b.group, id).Err()
}

// AutoClaim transfers messages that have been pending for longer than
// minIdle (their consumer crashed or stalled) to consumer.
func (b *Broker) AutoClaim(ctx context.Context, queue, consumer string, minIdle time.Duration, count int) ([]Message, error) {
	msgs, _, err := b.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: b.StreamKey(queue), Group: b.group, Consumer: consumer,
		MinIdle: minIdle, Start: "0-0", Count: int64(count),
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	out := make([]Message, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, toMessage(queue, m))
	}
	return out, nil
}

// QueueStats describes one stream from the consumer group's perspective.
type QueueStats struct {
	Queue     string `json:"queue"`
	Stream    string `json:"stream"`
	Length    int64  `json:"length"`    // entries retained in the stream
	Lag       int64  `json:"lag"`       // entries not yet delivered to the group
	Pending   int64  `json:"pending"`   // delivered but unacknowledged
	Consumers int64  `json:"consumers"` // consumers registered in the group
}

// Depth is the number of messages awaiting completion (lag + pending).
func (s QueueStats) Depth() int64 { return s.Lag + s.Pending }

// Stats returns statistics for queue. For streams without a group (the
// dead-letter stream) only Length is populated.
func (b *Broker) Stats(ctx context.Context, queue string) (QueueStats, error) {
	key := b.StreamKey(queue)
	st := QueueStats{Queue: queue, Stream: key}
	n, err := b.rdb.XLen(ctx, key).Result()
	if err != nil {
		return st, err
	}
	st.Length = n
	if n == 0 {
		// Stream may not exist yet; a group may still hold pending entries
		// whose bodies were trimmed, so fall through when it exists.
		if exists, err := b.rdb.Exists(ctx, key).Result(); err != nil || exists == 0 {
			return st, err
		}
	}
	groups, err := b.rdb.XInfoGroups(ctx, key).Result()
	if err != nil {
		if strings.Contains(err.Error(), "no such key") {
			return st, nil
		}
		return st, err
	}
	for _, g := range groups {
		if g.Name != b.group {
			continue
		}
		st.Pending, st.Consumers, st.Lag = g.Pending, g.Consumers, g.Lag
		// go-redis reports an unknown (NULL) lag as 0. Detect that case
		// by checking for entries after the last delivered ID.
		if st.Lag == 0 && st.Length > 0 {
			after, err := b.rdb.XRangeN(ctx, key, "("+g.LastDeliveredID, "+", 1).Result()
			if err != nil {
				return st, err
			}
			if len(after) > 0 {
				st.Lag = st.Length // upper bound when the exact figure is unknown
			}
		}
	}
	return st, nil
}

// Trim deletes stream entries that every consumer has already processed:
// entries older than both the group's last-delivered ID and its oldest
// pending entry. It never removes undelivered or unacknowledged messages.
func (b *Broker) Trim(ctx context.Context, queue string) (int64, error) {
	key := b.StreamKey(queue)
	groups, err := b.rdb.XInfoGroups(ctx, key).Result()
	if err != nil {
		if strings.Contains(err.Error(), "no such key") {
			return 0, nil
		}
		return 0, err
	}
	minID := ""
	for _, g := range groups {
		candidate := g.LastDeliveredID
		if g.Pending > 0 {
			p, err := b.rdb.XPending(ctx, key, g.Name).Result()
			if err != nil {
				return 0, err
			}
			candidate = p.Lower
		}
		if minID == "" || compareIDs(candidate, minID) < 0 {
			minID = candidate
		}
	}
	if minID == "" || minID == "0-0" {
		return 0, nil
	}
	return b.rdb.XTrimMinID(ctx, key, minID).Result()
}

// PruneConsumers removes consumers that have no pending messages and have
// been idle longer than idle (e.g. workers that exited). Returns the number
// removed.
func (b *Broker) PruneConsumers(ctx context.Context, queue string, idle time.Duration) (int, error) {
	key := b.StreamKey(queue)
	cs, err := b.rdb.XInfoConsumers(ctx, key, b.group).Result()
	if err != nil {
		if strings.Contains(err.Error(), "no such key") || strings.Contains(err.Error(), "NOGROUP") {
			return 0, nil
		}
		return 0, err
	}
	n := 0
	for _, c := range cs {
		if c.Pending == 0 && c.Idle > idle {
			if err := b.rdb.XGroupDelConsumer(ctx, key, b.group, c.Name).Err(); err != nil {
				return n, err
			}
			n++
		}
	}
	return n, nil
}

// compareIDs compares two stream IDs ("ms-seq").
func compareIDs(a, b string) int {
	am, as := splitID(a)
	bm, bs := splitID(b)
	switch {
	case am < bm:
		return -1
	case am > bm:
		return 1
	case as < bs:
		return -1
	case as > bs:
		return 1
	}
	return 0
}

func splitID(id string) (uint64, uint64) {
	ts, s, _ := strings.Cut(id, "-")
	// Malformed parts compare as 0; IDs come from Redis and are well formed.
	ms, _ := strconv.ParseUint(ts, 10, 64)
	seq, _ := strconv.ParseUint(s, 10, 64)
	return ms, seq
}
