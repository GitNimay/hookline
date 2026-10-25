package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"

	pb "github.com/GitNimay/hookline/gen/hookline/v1"
	"github.com/GitNimay/hookline/internal/domain"
	"github.com/GitNimay/hookline/internal/queue"
	"github.com/GitNimay/hookline/internal/store"
	"github.com/GitNimay/hookline/internal/telemetry"
)

// Relay moves outbox rows to NATS. Rows are claimed with SKIP LOCKED, published,
// and marked published in the same transaction, so a crash at any point only causes a
// re-publish, which NATS deduplicates by event ID.
type Relay struct {
	Pool   *pgxpool.Pool
	Queue  *queue.Queue
	Log    *slog.Logger
	MinAge time.Duration // 0 for ingest; 1 min for the scheduler's safety net
	Notify chan struct{} // optional: wake immediately after an insert
	Every  time.Duration
}

const relayBatch = 500

// Run loops until ctx ends.
func (r *Relay) Run(ctx context.Context) error {
	t := time.NewTicker(r.Every)
	defer t.Stop()
	gauge := time.NewTicker(5 * time.Second)
	defer gauge.Stop()
	backoff := time.Duration(0)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		case <-r.Notify:
		case <-gauge.C:
			r.updateGauge(ctx)
			continue
		}
		for {
			n, err := r.Once(ctx)
			if err != nil {
				backoff = min(max(backoff*2, 100*time.Millisecond), 5*time.Second)
				r.Log.Warn("outbox relay failed; backing off", "err", err, "backoff", backoff)
				if !sleep(ctx, backoff) {
					return nil
				}
				break
			}
			backoff = 0
			if n < relayBatch {
				break
			}
		}
	}
}

func (r *Relay) updateGauge(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if n, err := store.New(r.Pool).CountOutboxPending(ctx); err == nil {
		telemetry.OutboxPending.Set(float64(n))
	}
}

// Once relays one batch and returns how many rows it claimed.
func (r *Relay) Once(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	q := store.New(tx)
	rows, err := q.ClaimOutbox(ctx, store.ClaimOutboxParams{OlderThan: time.Now().Add(-r.MinAge), BatchSize: relayBatch})
	if err != nil || len(rows) == 0 {
		return 0, err
	}
	msgs := make([]*nats.Msg, len(rows))
	ids := make([]string, len(rows))
	for i, row := range rows {
		tenant := domain.FormatID(domain.PrefixTenant, row.TenantID)
		ids[i] = domain.FormatID(domain.PrefixEvent, row.EventID)
		mctx := telemetry.WithTraceParent(ctx, row.TraceParent)
		if msgs[i], err = queue.Msg(mctx, domain.EventsSubject(tenant), &pb.EventJob{EventId: ids[i], TenantId: tenant, EventType: row.EventType}); err != nil {
			return 0, err
		}
	}
	acked, pubErr := r.Queue.PublishAll(ctx, msgs, ids)
	done := make([]int64, len(acked))
	for i, idx := range acked {
		done[i] = rows[idx].ID
		telemetry.OutboxPublishLag.Observe(time.Since(rows[idx].CreatedAt).Seconds())
	}
	if len(done) > 0 {
		if err := q.MarkOutboxPublished(ctx, done); err != nil {
			return 0, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
	}
	if pubErr != nil {
		return len(rows), fmt.Errorf("publish: %w", pubErr)
	}
	return len(rows), nil
}

// sleep waits d or until ctx ends; false if ctx ended.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
