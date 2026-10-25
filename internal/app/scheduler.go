package app

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GitNimay/hookline/internal/config"
	"github.com/GitNimay/hookline/internal/domain"
	"github.com/GitNimay/hookline/internal/queue"
	"github.com/GitNimay/hookline/internal/server"
	"github.com/GitNimay/hookline/internal/store"
	"github.com/GitNimay/hookline/internal/telemetry"
)

// Scheduler republishes due retries and runs maintenance. Work is claimed with
// SKIP LOCKED, so several replicas can run at once.
type Scheduler struct {
	Pool  *pgxpool.Pool
	Queue *queue.Queue
	Log   *slog.Logger
	Cfg   config.Scheduler
}

const (
	dueBatch = 500
	// dueLease pushes next_attempt_at forward while a job is in flight. If the publish
	// or job is lost, the delivery becomes due again after the lease. Longer than NATS's
	// 2-minute dedup window so the republish is not swallowed.
	dueLease = 5 * time.Minute
)

// DueLoop publishes deliveries whose retry time has come, every second.
func (s *Scheduler) DueLoop(ctx context.Context) error {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		for {
			n, err := s.PublishDue(ctx)
			if err != nil {
				s.Log.Warn("publish due deliveries", "err", err)
				break
			}
			if n < dueBatch {
				break
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// PublishDue claims one batch of due retries and publishes their jobs.
func (s *Scheduler) PublishDue(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := store.New(s.Pool).ClaimDueDeliveries(ctx, store.ClaimDueDeliveriesParams{BatchSize: dueBatch, LeaseSecs: dueLease.Seconds()})
	if err != nil {
		return 0, err
	}
	for _, r := range rows {
		nonce := strconv.FormatInt(r.NextAttemptAt.UnixMilli(), 10)
		if err := publishJob(ctx, s.Queue, jobRef{ID: r.ID, TenantID: r.TenantID, EventID: r.EventID, EndpointID: r.EndpointID, AttemptCount: r.AttemptCount}, nonce); err != nil {
			// The lease brings it back; no need to fail the batch.
			s.Log.Warn("publish retry job", "delivery_id", domain.FormatID(domain.PrefixDelivery, r.ID), "err", err)
			continue
		}
		telemetry.SchedulerDuePublished.Inc()
	}
	return len(rows), nil
}

// MaintenanceLoop runs reconciliation, auto-disable, retention and gauges every minute.
func (s *Scheduler) MaintenanceLoop(ctx context.Context) error {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		s.Maintain(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// Maintain runs one maintenance pass. Each step logs its own errors and the rest still run.
func (s *Scheduler) Maintain(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 50*time.Second)
	defer cancel()
	q := store.New(s.Pool)
	now := time.Now()

	// Stuck pending deliveries (a lost publish) get their job again.
	stale, err := q.ClaimStalePending(ctx, store.ClaimStalePendingParams{OlderThan: now.Add(-s.Cfg.ReconcilePendingAt), BatchSize: dueBatch})
	if err != nil {
		s.Log.Warn("reconcile pending", "err", err)
	}
	for _, r := range stale {
		nonce := "p" + strconv.FormatInt(r.UpdatedAt.UnixMilli(), 10)
		if err := publishJob(ctx, s.Queue, jobRef{ID: r.ID, TenantID: r.TenantID, EventID: r.EventID, EndpointID: r.EndpointID, AttemptCount: r.AttemptCount}, nonce); err != nil {
			s.Log.Warn("republish stuck delivery", "err", err)
		}
	}
	if len(stale) > 0 {
		s.Log.Info("republished stuck pending deliveries", "count", len(stale))
	}

	// Endpoints failing with no success for too long are disabled.
	cutoff := now.Add(-s.Cfg.AutoDisableAfter)
	disabled, err := q.AutoDisableEndpoints(ctx, &cutoff)
	if err != nil {
		s.Log.Warn("auto-disable endpoints", "err", err)
	}
	for _, e := range disabled {
		telemetry.EndpointsAutoDisabled.Inc()
		s.Log.Warn("endpoint auto-disabled after continuous failures", "event", "endpoint.auto_disabled",
			"tenant_id", domain.FormatID(domain.PrefixTenant, e.TenantID), "endpoint_id", domain.FormatID(domain.PrefixEndpoint, e.ID), "failing_since", e.FailingSince)
	}

	// Retention, in small batches so locks stay short.
	pubCutoff := now.Add(-s.Cfg.OutboxRetention)
	s.prune(ctx, "idempotency keys", func(ctx context.Context) (int64, error) {
		return q.ClearExpiredIdempotencyKeys(ctx, now.Add(-s.Cfg.IdempotencyWindow))
	})
	s.prune(ctx, "attempts", func(ctx context.Context) (int64, error) {
		return q.PruneAttempts(ctx, now.Add(-s.Cfg.AttemptRetention))
	})
	s.prune(ctx, "events", func(ctx context.Context) (int64, error) { return q.PruneEvents(ctx, now.Add(-s.Cfg.EventRetention)) })
	s.prune(ctx, "outbox", func(ctx context.Context) (int64, error) { return q.PruneOutbox(ctx, &pubCutoff) })

	if rows, err := q.CountRetryingByTenant(ctx); err == nil {
		telemetry.DeliveriesRetrying.Reset()
		for _, r := range rows {
			telemetry.DeliveriesRetrying.WithLabelValues(domain.FormatID(domain.PrefixTenant, r.TenantID)).Set(float64(r.Retrying))
		}
	}
}

// prune repeats a batched delete until it removes fewer than a batch, capped per pass.
func (s *Scheduler) prune(ctx context.Context, what string, del func(context.Context) (int64, error)) {
	for range 50 {
		n, err := del(ctx)
		if err != nil {
			s.Log.Warn("retention", "what", what, "err", err)
			return
		}
		if n < 1000 {
			return
		}
	}
}

// RunScheduler wires and runs the scheduler service.
func RunScheduler(ctx context.Context, cfg config.Scheduler, log *slog.Logger) error {
	pool, err := OpenPostgres(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	q, err := queue.Connect(cfg.NATSURL, "scheduler")
	if err != nil {
		return err
	}
	if err := ensureStreams(ctx, q, log); err != nil {
		return err
	}
	s := &Scheduler{Pool: pool, Queue: q, Log: log, Cfg: cfg}
	// Safety net for the ingest relay: outbox rows unpublished for over a minute.
	relay := &Relay{Pool: pool, Queue: q, Log: log, MinAge: time.Minute, Every: time.Minute}
	return server.Run(ctx, server.Options{
		Name: "scheduler", Log: log, AdminAddr: cfg.AdminAddr, Pprof: cfg.PprofEnabled,
		DrainDelay: cfg.DrainDelay, StopTimeout: cfg.ShutdownTimeout,
		Ready:   readyAll(pingDB(pool), q.Ready),
		Loops:   []func(context.Context) error{s.DueLoop, s.MaintenanceLoop, relay.Run},
		Closers: []func(){q.Close, pool.Close},
	})
}
