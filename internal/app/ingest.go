package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/GitNimay/hookline/gen/hookline/v1"
	"github.com/GitNimay/hookline/internal/auth"
	"github.com/GitNimay/hookline/internal/config"
	"github.com/GitNimay/hookline/internal/domain"
	"github.com/GitNimay/hookline/internal/queue"
	"github.com/GitNimay/hookline/internal/ratelimit"
	"github.com/GitNimay/hookline/internal/server"
	"github.com/GitNimay/hookline/internal/store"
	"github.com/GitNimay/hookline/internal/telemetry"
)

// Input limits.
const (
	MaxPayloadBytes = 256 << 10
	MaxBatch        = 100
	MaxNameLen      = 255
)

// Ingest implements IngestService and EventService.
type Ingest struct {
	pb.UnimplementedIngestServiceServer
	pb.UnimplementedEventServiceServer

	Pool       *pgxpool.Pool
	Limiter    *ratelimit.Limiter // nil disables ingest rate limiting
	Log        *slog.Logger
	Notify     chan struct{}
	DefaultRPS int

	tenants *ttlCache[uuid.UUID, store.Tenant]
	types   *ttlCache[string, bool]
}

// NewIngest builds the ingest handlers.
func NewIngest(pool *pgxpool.Pool, lim *ratelimit.Limiter, log *slog.Logger, defaultRPS int) *Ingest {
	return &Ingest{
		Pool: pool, Limiter: lim, Log: log, Notify: make(chan struct{}, 1), DefaultRPS: defaultRPS,
		tenants: newCache[uuid.UUID, store.Tenant](30 * time.Second),
		types:   newCache[string, bool](30 * time.Second),
	}
}

// SendEvent stores one event plus its outbox row in one transaction.
func (s *Ingest) SendEvent(ctx context.Context, req *pb.SendEventRequest) (*pb.SendEventResponse, error) {
	res, err := s.ingest(ctx, []*pb.SendEventRequest{req})
	if err != nil {
		return nil, err
	}
	if !res[0].Duplicate {
		server.SetHTTPCode(ctx, http.StatusAccepted)
	}
	return res[0], nil
}

// SendEventBatch stores up to 100 events in one transaction.
func (s *Ingest) SendEventBatch(ctx context.Context, req *pb.SendEventBatchRequest) (*pb.SendEventBatchResponse, error) {
	if len(req.Events) == 0 || len(req.Events) > MaxBatch {
		return nil, domain.Invalid("events", "must contain 1 to %d events", MaxBatch)
	}
	res, err := s.ingest(ctx, req.Events)
	if err != nil {
		return nil, err
	}
	for _, r := range res {
		if !r.Duplicate {
			server.SetHTTPCode(ctx, http.StatusAccepted)
			break
		}
	}
	return &pb.SendEventBatchResponse{Results: res}, nil
}

type validEvent struct {
	typ     string
	payload []byte
	key     *string
	at      time.Time
}

func (s *Ingest) ingest(ctx context.Context, reqs []*pb.SendEventRequest) ([]*pb.SendEventResponse, error) {
	tenantID, err := auth.TenantFrom(ctx)
	if err != nil {
		return nil, err
	}
	tenantLabel := domain.FormatID(domain.PrefixTenant, tenantID)
	if err := s.rateLimit(ctx, tenantID, tenantLabel); err != nil {
		return nil, err
	}
	evs := make([]validEvent, len(reqs))
	for i, r := range reqs {
		if evs[i], err = validate(r); err != nil {
			telemetry.EventsIngested.WithLabelValues(tenantLabel, "rejected").Add(float64(len(reqs)))
			return nil, err
		}
		if err := s.checkType(ctx, tenantID, r.EventType); err != nil {
			telemetry.EventsIngested.WithLabelValues(tenantLabel, "rejected").Add(float64(len(reqs)))
			return nil, err
		}
	}

	traceParent := telemetry.TraceParent(ctx)
	out := make([]*pb.SendEventResponse, len(evs))
	err = pgx.BeginTxFunc(ctx, s.Pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		q := store.New(tx)
		for i, e := range evs {
			id, err := q.InsertEvent(ctx, store.InsertEventParams{
				ID: domain.NewID(), TenantID: tenantID, EventType: e.typ, Payload: e.payload,
				IdempotencyKey: e.key, OccurredAt: e.at, TraceParent: traceParent,
			})
			if errors.Is(err, pgx.ErrNoRows) { // idempotency key seen before
				id, err = q.GetEventIDByIdempotencyKey(ctx, store.GetEventIDByIdempotencyKeyParams{TenantID: tenantID, IdempotencyKey: e.key})
				if err != nil {
					return err
				}
				out[i] = &pb.SendEventResponse{Id: domain.FormatID(domain.PrefixEvent, id), Duplicate: true}
				continue
			}
			if err != nil {
				return err
			}
			if err := q.InsertOutbox(ctx, store.InsertOutboxParams{EventID: id, TenantID: tenantID}); err != nil {
				return err
			}
			out[i] = &pb.SendEventResponse{Id: domain.FormatID(domain.PrefixEvent, id)}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, r := range out {
		result := "accepted"
		if r.Duplicate {
			result = "duplicate"
		}
		telemetry.EventsIngested.WithLabelValues(tenantLabel, result).Inc()
	}
	select { // wake the relay; never block
	case s.Notify <- struct{}{}:
	default:
	}
	return out, nil
}

func validate(r *pb.SendEventRequest) (validEvent, error) {
	var e validEvent
	if r.EventType == "" || len(r.EventType) > MaxNameLen {
		return e, domain.Invalid("event_type", "required, at most %d chars", MaxNameLen)
	}
	if r.Payload == nil {
		return e, domain.Invalid("payload", "required JSON object")
	}
	raw, err := r.Payload.MarshalJSON()
	if err != nil {
		return e, domain.Invalid("payload", "not valid JSON")
	}
	if len(raw) > MaxPayloadBytes {
		return e, domain.Invalid("payload", "larger than %d bytes", MaxPayloadBytes)
	}
	if len(r.IdempotencyKey) > MaxNameLen {
		return e, domain.Invalid("idempotency_key", "at most %d chars", MaxNameLen)
	}
	e = validEvent{typ: r.EventType, payload: raw, at: time.Now().UTC()}
	if r.IdempotencyKey != "" {
		k := r.IdempotencyKey
		e.key = &k
	}
	if r.OccurredAt != nil {
		if err := r.OccurredAt.CheckValid(); err != nil {
			return e, domain.Invalid("occurred_at", "invalid timestamp")
		}
		e.at = r.OccurredAt.AsTime()
	}
	return e, nil
}

func (s *Ingest) checkType(ctx context.Context, tenantID uuid.UUID, typ string) error {
	k := tenantID.String() + "/" + typ
	if _, ok := s.types.get(k); ok {
		return nil
	}
	n, err := store.New(s.Pool).EventTypesExist(ctx, store.EventTypesExistParams{TenantID: tenantID, Names: []string{typ}})
	if err != nil {
		return err
	}
	if n == 0 {
		return domain.Invalid("event_type", "%q is not in the event type catalogue", typ)
	}
	s.types.put(k, true)
	return nil
}

func (s *Ingest) rateLimit(ctx context.Context, tenantID uuid.UUID, label string) error {
	if s.Limiter == nil {
		return nil
	}
	limit := s.DefaultRPS
	if t, ok := s.tenants.get(tenantID); ok {
		limit = int(t.IngestRpsLimit)
	} else if t, err := store.New(s.Pool).GetTenant(ctx, tenantID); err == nil {
		s.tenants.put(tenantID, t)
		limit = int(t.IngestRpsLimit)
	}
	ok, retry, err := s.Limiter.Allow(ctx, "ingest:"+tenantID.String(), limit)
	if err != nil {
		s.Log.WarnContext(ctx, "rate limiter unavailable, failing open", "err", err)
	}
	if ok {
		return nil
	}
	telemetry.RateLimited.WithLabelValues("ingest").Inc()
	telemetry.EventsIngested.WithLabelValues(label, "rejected").Inc()
	server.SetRetryAfter(ctx, retry)
	return status.Error(codes.ResourceExhausted, "ingest rate limit exceeded")
}

// GetEvent returns one of the caller's events.
func (s *Ingest) GetEvent(ctx context.Context, req *pb.GetEventRequest) (*pb.GetEventResponse, error) {
	tenantID, err := auth.TenantFrom(ctx)
	if err != nil {
		return nil, err
	}
	id, err := domain.ParseID(domain.PrefixEvent, req.Id)
	if err != nil {
		return nil, err
	}
	e, err := store.New(s.Pool).GetEvent(ctx, store.GetEventParams{ID: id, TenantID: tenantID})
	if err != nil {
		return nil, notFound(err)
	}
	return &pb.GetEventResponse{Event: eventPB(e)}, nil
}

// ListEvents lists the caller's events, newest first.
func (s *Ingest) ListEvents(ctx context.Context, req *pb.ListEventsRequest) (*pb.ListEventsResponse, error) {
	tenantID, err := auth.TenantFrom(ctx)
	if err != nil {
		return nil, err
	}
	ct, cid, err := decodeCursor(req.PageToken)
	if err != nil {
		return nil, err
	}
	size := pageSize(req.PageSize)
	rows, err := store.New(s.Pool).ListEvents(ctx, store.ListEventsParams{
		TenantID: tenantID, EventType: req.EventType, StartTime: timeOrNil(req.StartTime), EndTime: timeOrNil(req.EndTime),
		CursorTime: ct, CursorID: cid, PageSize: size,
	})
	if err != nil {
		return nil, err
	}
	out := &pb.ListEventsResponse{NextPageToken: nextToken(rows, size, func(e store.Event) (time.Time, uuid.UUID) { return e.CreatedAt, e.ID })}
	for _, e := range rows {
		out.Events = append(out.Events, eventPB(e))
	}
	return out, nil
}

// notFound maps "no rows" to ErrNotFound.
func notFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	return err
}

// RunIngest wires and runs the ingest service.
func RunIngest(ctx context.Context, cfg config.Ingest, log *slog.Logger) error {
	pool, err := OpenPostgres(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	q, err := queue.Connect(cfg.NATSURL, "ingest")
	if err != nil {
		return err
	}
	if err := ensureStreams(ctx, q, log); err != nil {
		return err
	}
	rdb := OpenRedis(cfg.Redis)
	authn := auth.NewAuthenticator(store.New(pool), log, 30*time.Second)
	svc := NewIngest(pool, ratelimit.New(rdb), log, cfg.DefaultIngestRPS)

	gs := server.NewGRPCServer(log, authn.Interceptor())
	pb.RegisterIngestServiceServer(gs, svc)
	pb.RegisterEventServiceServer(gs, svc)

	mux, handler := server.NewGateway()
	conn, err := server.DialLocal(localAddr(cfg.GRPCAddr))
	if err != nil {
		return err
	}
	if err := pb.RegisterIngestServiceHandler(ctx, mux, conn); err != nil {
		return err
	}
	if err := pb.RegisterEventServiceHandler(ctx, mux, conn); err != nil {
		return err
	}
	relay := &Relay{Pool: pool, Queue: q, Log: log, Notify: svc.Notify, Every: 100 * time.Millisecond}
	return server.Run(ctx, server.Options{
		Name: "ingest", Log: log, AdminAddr: cfg.AdminAddr, GRPCAddr: cfg.GRPCAddr, HTTPAddr: cfg.HTTPAddr,
		GRPC: gs, HTTP: handler, Pprof: cfg.PprofEnabled, DrainDelay: cfg.DrainDelay, StopTimeout: cfg.ShutdownTimeout,
		Ready: readyAll(pingDB(pool)),
		Loops: []func(context.Context) error{relay.Run, authn.FlushLoop},
		// Closers run after servers stop: the relay has finished, so the queue can drain.
		Closers: []func(){func() { _ = conn.Close() }, q.Close, func() { _ = rdb.Close() }, pool.Close},
	})
}

// localAddr turns ":9090" into "localhost:9090" for the in-process gateway client.
func localAddr(addr string) string {
	if len(addr) > 0 && addr[0] == ':' {
		return "localhost" + addr
	}
	return addr
}

// ensureStreams retries until NATS answers, so services can start before NATS is ready.
func ensureStreams(ctx context.Context, q *queue.Queue, log *slog.Logger) error {
	for i := 0; ; i++ {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := q.EnsureStreams(cctx)
		cancel()
		if err == nil {
			return nil
		}
		if i == 30 {
			return err
		}
		log.Warn("waiting for NATS", "err", err)
		if !sleep(ctx, time.Second) {
			return ctx.Err()
		}
	}
}
