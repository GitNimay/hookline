// Package queue sets up NATS JetStream streams and consumers and publishes jobs.
package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	"github.com/GitNimay/hookline/internal/domain"
	"github.com/GitNimay/hookline/internal/telemetry"
)

// Stream and consumer names.
const (
	EventsStream       = "EVENTS"
	DeliveriesStream   = "DELIVERIES"
	DispatcherConsumer = "dispatcher"
)

// Queue wraps a NATS connection and its JetStream context.
type Queue struct {
	NC *nats.Conn
	JS jetstream.JetStream
}

// Connect dials NATS with reconnects forever, so a NATS restart only pauses work.
func Connect(url, name string) (*Queue, error) {
	nc, err := nats.Connect(url,
		nats.Name(name),
		nats.Timeout(5*time.Second),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
		nats.RetryOnFailedConnect(true),
	)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &Queue{NC: nc, JS: js}, nil
}

// Close drains the connection.
func (q *Queue) Close() { _ = q.NC.Drain() }

// Ready reports whether NATS is connected.
func (q *Queue) Ready(context.Context) error {
	if !q.NC.IsConnected() {
		return errors.New("nats not connected")
	}
	return nil
}

// EnsureStreams creates or updates both work-queue streams. Idempotent.
func (q *Queue) EnsureStreams(ctx context.Context) error {
	for name, subj := range map[string]string{EventsStream: "events.>", DeliveriesStream: "deliveries.>"} {
		_, err := q.JS.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:       name,
			Subjects:   []string{subj},
			Retention:  jetstream.WorkQueuePolicy,
			Storage:    jetstream.FileStorage,
			MaxAge:     7 * 24 * time.Hour,
			Duplicates: 2 * time.Minute,
		})
		if err != nil {
			return fmt.Errorf("ensure stream %s: %w", name, err)
		}
	}
	return nil
}

// EnsureDispatcherConsumer creates or updates the single EVENTS consumer.
func (q *Queue) EnsureDispatcherConsumer(ctx context.Context) (jetstream.Consumer, error) {
	return q.JS.CreateOrUpdateConsumer(ctx, EventsStream, jetstream.ConsumerConfig{
		Durable:       DispatcherConsumer,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    -1,
		MaxAckPending: 1000,
	})
}

// EnsureTenantConsumer creates or updates a tenant's DELIVERIES consumer.
// MaxAckPending is the tenant's in-flight cap (its bulkhead).
func (q *Queue) EnsureTenantConsumer(ctx context.Context, tenantID string, maxAckPending int) (jetstream.Consumer, error) {
	return q.JS.CreateOrUpdateConsumer(ctx, DeliveriesStream, jetstream.ConsumerConfig{
		Durable:       domain.DeliverConsumer(tenantID),
		FilterSubject: domain.DeliveriesSubject(tenantID),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       60 * time.Second,
		MaxDeliver:    -1,
		MaxAckPending: maxAckPending,
	})
}

// DeleteTenantConsumer removes a disabled tenant's consumer. Missing is fine.
func (q *Queue) DeleteTenantConsumer(ctx context.Context, tenantID string) error {
	err := q.JS.DeleteConsumer(ctx, DeliveriesStream, domain.DeliverConsumer(tenantID))
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		return nil
	}
	return err
}

// Msg builds a NATS message carrying m and ctx's trace context.
func Msg(ctx context.Context, subject string, m proto.Message) (*nats.Msg, error) {
	data, err := proto.Marshal(m)
	if err != nil {
		return nil, err
	}
	msg := nats.NewMsg(subject)
	msg.Data = data
	telemetry.Inject(ctx, msg.Header)
	return msg, nil
}

// PublishDelivery publishes one delivery job and waits for the stream ack.
func (q *Queue) PublishDelivery(ctx context.Context, tenantID string, job proto.Message, msgID string) error {
	msg, err := Msg(ctx, domain.DeliveriesSubject(tenantID), job)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = q.JS.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID))
	return err
}

// PublishAll publishes msgs asynchronously and waits for every ack.
// It returns the indexes that were acked, so callers mark only those as done.
func (q *Queue) PublishAll(ctx context.Context, msgs []*nats.Msg, ids []string) ([]int, error) {
	futures := make([]jetstream.PubAckFuture, len(msgs))
	for i, m := range msgs {
		f, err := q.JS.PublishMsgAsync(m, jetstream.WithMsgID(ids[i]))
		if err != nil {
			futures = futures[:i]
			break
		}
		futures[i] = f
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var ok []int
	var firstErr error
	for i, f := range futures {
		select {
		case <-f.Ok():
			ok = append(ok, i)
		case err := <-f.Err():
			if firstErr == nil {
				firstErr = err
			}
		case <-ctx.Done():
			return ok, ctx.Err()
		}
	}
	if len(futures) < len(msgs) && firstErr == nil {
		firstErr = errors.New("async publish rejected")
	}
	return ok, firstErr
}
