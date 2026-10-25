package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	pb "github.com/GitNimay/hookline/gen/hookline/v1"
	"github.com/GitNimay/hookline/internal/config"
	"github.com/GitNimay/hookline/internal/domain"
	"github.com/GitNimay/hookline/internal/queue"
	"github.com/GitNimay/hookline/internal/server"
	"github.com/GitNimay/hookline/internal/store"
	"github.com/GitNimay/hookline/internal/telemetry"
)

// MatchFunc returns the endpoint IDs subscribed to an event type.
type MatchFunc func(ctx context.Context, tenantID, eventType string) ([]string, error)

// Dispatcher fans events out into deliveries.
type Dispatcher struct {
	Pool        *pgxpool.Pool
	Queue       *queue.Queue
	Match       MatchFunc
	Log         *slog.Logger
	Concurrency int
	cache       *ttlCache[string, []uuid.UUID]
}

// match wraps the control gRPC call with a cache.
func (d *Dispatcher) match(ctx context.Context, tenant, typ string) ([]uuid.UUID, error) {
	k := tenant + "/" + typ
	if ids, ok := d.cache.get(k); ok {
		return ids, nil
	}
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	raw, err := d.Match(cctx, tenant, typ)
	if err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, 0, len(raw))
	for _, s := range raw {
		id, err := domain.ParseID(domain.PrefixEndpoint, s)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	d.cache.put(k, ids)
	return ids, nil
}

// Handle processes one event message. The message is acked only after deliveries are
// committed and their jobs published; any failure naks with a growing delay.
func (d *Dispatcher) Handle(ctx context.Context, msg jetstream.Msg) {
	ctx = telemetry.Extract(ctx, msg.Headers())
	ctx, span := telemetry.Tracer.Start(ctx, "dispatch")
	defer span.End()
	if err := d.handle(ctx, msg); err != nil {
		var n uint64 = 1
		if md, merr := msg.Metadata(); merr == nil {
			n = md.NumDelivered
		}
		delay := min(time.Duration(1<<min(n, 6))*time.Second, time.Minute)
		d.Log.WarnContext(ctx, "dispatch failed; will retry", "err", err, "delay", delay)
		span.RecordError(err)
		_ = msg.NakWithDelay(delay)
		return
	}
	_ = msg.Ack()
}

func (d *Dispatcher) handle(ctx context.Context, msg jetstream.Msg) error {
	var job pb.EventJob
	if err := proto.Unmarshal(msg.Data(), &job); err != nil {
		d.Log.ErrorContext(ctx, "bad event job; dropping", "err", err)
		return nil // poison message: retrying cannot help
	}
	eventID, err1 := domain.ParseID(domain.PrefixEvent, job.EventId)
	tenantID, err2 := domain.ParseID(domain.PrefixTenant, job.TenantId)
	if err1 != nil || err2 != nil {
		d.Log.ErrorContext(ctx, "bad ids in event job; dropping", "event_id", job.EventId)
		return nil
	}
	eps, err := d.match(ctx, job.TenantId, job.EventType)
	if err != nil {
		return err
	}
	if len(eps) == 0 {
		telemetry.EventsUnmatched.Inc()
		return nil
	}
	ids := make([]uuid.UUID, len(eps))
	for i := range eps {
		ids[i] = domain.NewID()
	}
	q := store.New(d.Pool)
	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := q.CreateDeliveries(dctx, store.CreateDeliveriesParams{Ids: ids, TenantID: tenantID, EventID: eventID, EndpointIds: eps}); err != nil {
		return err
	}
	// Read back: on a redelivered event the rows already exist with their original IDs.
	rows, err := q.ListEventDeliveries(dctx, eventID)
	if err != nil {
		return err
	}
	var published float64
	for _, r := range rows {
		if r.Status != domain.StatusPending || r.AttemptCount != 0 || r.ReplayCount != 0 {
			continue // already in progress or replayed; those paths publish their own jobs
		}
		if err := publishJob(ctx, d.Queue, ref(r), "d"); err != nil {
			return err
		}
		published++
	}
	telemetry.DeliveriesCreated.WithLabelValues(job.TenantId).Add(published)
	return nil
}

// Run consumes events with bounded concurrency until ctx ends, then waits for in-flight work.
func (d *Dispatcher) Run(ctx context.Context) error {
	cons, err := d.Queue.EnsureDispatcherConsumer(ctx)
	if err != nil {
		return err
	}
	it, err := cons.Messages(jetstream.PullMaxMessages(d.Concurrency))
	if err != nil {
		return err
	}
	defer context.AfterFunc(ctx, it.Stop)()
	sem := make(chan struct{}, d.Concurrency)
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		msg, err := it.Next()
		if errors.Is(err, jetstream.ErrMsgIteratorClosed) || ctx.Err() != nil {
			return nil
		}
		if err != nil {
			d.Log.Warn("fetch events", "err", err)
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			// Work finishes even during shutdown; the message is acked or naked, never abandoned.
			d.Handle(context.WithoutCancel(ctx), msg)
		}()
	}
}

// RunDispatcher wires and runs the dispatcher service.
func RunDispatcher(ctx context.Context, cfg config.Dispatcher, log *slog.Logger) error {
	pool, err := OpenPostgres(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	q, err := queue.Connect(cfg.NATSURL, "dispatcher")
	if err != nil {
		return err
	}
	if err := ensureStreams(ctx, q, log); err != nil {
		return err
	}
	conn, err := server.DialLocal(cfg.ControlAddr)
	if err != nil {
		return err
	}
	client := pb.NewEndpointServiceClient(conn)
	d := &Dispatcher{
		Pool: pool, Queue: q, Log: log, Concurrency: cfg.Concurrency,
		cache: newCache[string, []uuid.UUID](cfg.MatchCache),
		Match: func(ctx context.Context, tenant, typ string) ([]string, error) {
			res, err := client.ListMatchingEndpoints(ctx, &pb.ListMatchingEndpointsRequest{TenantId: tenant, EventType: typ})
			if err != nil {
				return nil, err
			}
			return res.EndpointIds, nil
		},
	}
	return server.Run(ctx, server.Options{
		Name: "dispatcher", Log: log, AdminAddr: cfg.AdminAddr, Pprof: cfg.PprofEnabled,
		DrainDelay: cfg.DrainDelay, StopTimeout: cfg.ShutdownTimeout,
		Ready:   readyAll(pingDB(pool), q.Ready),
		Loops:   []func(context.Context) error{d.Run},
		Closers: []func(){func() { _ = conn.Close() }, q.Close, pool.Close},
	})
}
