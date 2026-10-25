package app

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/trace"
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

// dispatchBatch is how many events one fetch takes; they share one insert and one publish.
const dispatchBatch = 100

type eventMsg struct {
	msg      jetstream.Msg
	ctx      context.Context
	span     trace.Span
	job      *pb.EventJob
	eventID  uuid.UUID
	tenantID uuid.UUID
}

// HandleBatch fans out a batch of events with one insert, one read-back and one async
// publish. Messages are acked only after their deliveries are committed and their jobs
// published; any failure naks the whole batch with a growing delay (all steps are idempotent).
func (d *Dispatcher) HandleBatch(ctx context.Context, msgs []jetstream.Msg) {
	var evs []eventMsg
	for _, m := range msgs {
		ectx, span := telemetry.Tracer.Start(telemetry.Extract(ctx, m.Headers()), "dispatch")
		defer span.End()
		var job pb.EventJob
		eventID, tenantID, ok := uuid.Nil, uuid.Nil, proto.Unmarshal(m.Data(), &job) == nil
		if ok {
			var err1, err2 error
			eventID, err1 = domain.ParseID(domain.PrefixEvent, job.EventId)
			tenantID, err2 = domain.ParseID(domain.PrefixTenant, job.TenantId)
			ok = err1 == nil && err2 == nil
		}
		if !ok {
			d.Log.ErrorContext(ectx, "bad event job; dropping") // poison: retrying cannot help
			_ = m.Ack()
			continue
		}
		evs = append(evs, eventMsg{msg: m, ctx: ectx, span: span, job: &job, eventID: eventID, tenantID: tenantID})
	}
	if len(evs) == 0 {
		return
	}
	if err := d.dispatch(ctx, evs); err != nil {
		for _, e := range evs {
			var n uint64 = 1
			if md, merr := e.msg.Metadata(); merr == nil {
				n = md.NumDelivered
			}
			e.span.RecordError(err)
			_ = e.msg.NakWithDelay(min(time.Duration(1<<min(n, 6))*time.Second, time.Minute))
		}
		d.Log.Warn("dispatch failed; will retry", "events", len(evs), "err", err)
		return
	}
	for _, e := range evs {
		_ = e.msg.Ack()
	}
}

func (d *Dispatcher) dispatch(ctx context.Context, evs []eventMsg) error {
	var p store.CreateDeliveriesParams
	var eventIDs []uuid.UUID
	byEvent := make(map[uuid.UUID]*eventMsg, len(evs))
	for i := range evs {
		e := &evs[i]
		eps, err := d.match(ctx, e.job.TenantId, e.job.EventType)
		if err != nil {
			return err
		}
		if len(eps) == 0 {
			telemetry.EventsUnmatched.Inc()
			continue
		}
		byEvent[e.eventID] = e
		eventIDs = append(eventIDs, e.eventID)
		for _, ep := range eps {
			p.Ids = append(p.Ids, domain.NewID())
			p.TenantIds = append(p.TenantIds, e.tenantID)
			p.EventIds = append(p.EventIds, e.eventID)
			p.EndpointIds = append(p.EndpointIds, ep)
		}
	}
	if len(eventIDs) == 0 {
		return nil
	}
	q := store.New(d.Pool)
	dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := q.CreateDeliveries(dctx, p); err != nil {
		return err
	}
	// Read back: for redelivered events the rows already exist with their original IDs.
	rows, err := q.ListEventsDeliveries(dctx, eventIDs)
	if err != nil {
		return err
	}
	var msgs []*nats.Msg
	var msgIDs []string
	created := map[string]float64{}
	for _, r := range rows {
		if r.Status != domain.StatusPending || r.AttemptCount != 0 || r.ReplayCount != 0 {
			continue // already in progress or replayed; those paths publish their own jobs
		}
		tenant, dj, id := jobFor(ref(r), "d")
		m, err := queue.Msg(byEvent[r.EventID].ctx, domain.DeliveriesSubject(tenant), dj) //nolint:contextcheck // carries this event's own trace
		if err != nil {
			return err
		}
		msgs, msgIDs = append(msgs, m), append(msgIDs, id)
		created[tenant]++
	}
	if _, err := d.Queue.PublishAll(ctx, msgs, msgIDs); err != nil {
		return err // republishing the acked ones is deduplicated by message ID
	}
	for tenant, n := range created {
		telemetry.DeliveriesCreated.WithLabelValues(tenant).Add(n)
	}
	return nil
}

// Run fetches events in batches on Concurrency/16 loops until ctx ends. A batch in progress
// always finishes: its messages are acked or naked, never abandoned.
func (d *Dispatcher) Run(ctx context.Context) error {
	cons, err := d.Queue.EnsureDispatcherConsumer(ctx)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	for range max(1, d.Concurrency/16) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				batch, err := cons.Fetch(dispatchBatch, jetstream.FetchMaxWait(time.Second))
				if err != nil {
					if !sleep(ctx, time.Second) {
						return
					}
					continue
				}
				var msgs []jetstream.Msg
				for m := range batch.Messages() {
					msgs = append(msgs, m)
				}
				if len(msgs) > 0 {
					d.HandleBatch(context.WithoutCancel(ctx), msgs)
				}
			}
		}()
	}
	return nil
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
