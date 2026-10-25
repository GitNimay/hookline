package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"

	pb "github.com/GitNimay/hookline/gen/hookline/v1"
	"github.com/GitNimay/hookline/internal/breaker"
	"github.com/GitNimay/hookline/internal/config"
	"github.com/GitNimay/hookline/internal/delivery"
	"github.com/GitNimay/hookline/internal/domain"
	"github.com/GitNimay/hookline/internal/queue"
	"github.com/GitNimay/hookline/internal/ratelimit"
	"github.com/GitNimay/hookline/internal/server"
	"github.com/GitNimay/hookline/internal/signing"
	"github.com/GitNimay/hookline/internal/ssrf"
	"github.com/GitNimay/hookline/internal/store"
	"github.com/GitNimay/hookline/internal/telemetry"
)

// Worker delivers webhooks.
type Worker struct {
	Pool        *pgxpool.Pool
	Queue       *queue.Queue
	Breaker     *breaker.Breaker   // nil disables the circuit breaker
	Limiter     *ratelimit.Limiter // nil disables endpoint rate limits
	Sender      *delivery.Sender
	Box         *signing.Box
	Schedule    delivery.Schedule
	Log         *slog.Logger
	Concurrency int
	// RefreshEvery is how often the tenant consumer list is reloaded (default 10 s).
	RefreshEvery time.Duration
	// LockFor is how long a claimed delivery stays locked: longer than one attempt, shorter than AckWait (default 30 s).
	LockFor time.Duration

	endpoints *ttlCache[uuid.UUID, workerEndpoint]
}

type workerEndpoint struct {
	ep      store.Endpoint
	secrets []string
	headers map[string]string
}

// NewWorker builds a worker.
func NewWorker(w Worker) *Worker {
	w.endpoints = newCache[uuid.UUID, workerEndpoint](5 * time.Second)
	if w.LockFor == 0 {
		w.LockFor = 30 * time.Second
	}
	if w.RefreshEvery == 0 {
		w.RefreshEvery = 10 * time.Second
	}
	return &w
}

// perTenantFetch is how many jobs one tenant may take per round-robin turn.
const perTenantFetch = 10

// Run fetches jobs round-robin across tenant consumers until ctx ends, then waits for in-flight jobs.
func (w *Worker) Run(ctx context.Context) error {
	sem := make(chan struct{}, w.Concurrency)
	var wg sync.WaitGroup
	defer wg.Wait()

	consumers := map[string]jetstream.Consumer{}
	var names []string
	var refreshed time.Time
	start := 0
	for ctx.Err() == nil {
		if time.Since(refreshed) > w.RefreshEvery {
			names = w.refreshConsumers(ctx, consumers)
			refreshed = time.Now()
		}
		got := 0
		// Rotate the starting tenant each round so no tenant is always first.
		for i := range names {
			name := names[(start+i)%len(names)]
			slots := acquire(ctx, sem, perTenantFetch)
			if slots == 0 {
				return nil
			}
			batch, err := consumers[name].FetchNoWait(slots)
			if err != nil {
				release(sem, slots)
				if errors.Is(err, jetstream.ErrConsumerNotFound) {
					refreshed = time.Time{}
				}
				continue
			}
			for msg := range batch.Messages() {
				slots--
				got++
				wg.Add(1)
				go func() {
					defer func() { <-sem; wg.Done() }()
					// In-flight jobs finish during shutdown; their own timeouts bound them.
					w.Handle(context.WithoutCancel(ctx), msg)
				}()
			}
			release(sem, slots)
		}
		start++
		if got == 0 && !sleep(ctx, 50*time.Millisecond) {
			return nil
		}
	}
	return nil
}

// acquire takes between 1 and max slots, blocking only for the first. 0 means ctx ended.
func acquire(ctx context.Context, sem chan struct{}, max int) int {
	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		return 0
	}
	n := 1
	for n < max {
		select {
		case sem <- struct{}{}:
			n++
		default:
			return n
		}
	}
	return n
}

func release(sem chan struct{}, n int) {
	for range n {
		<-sem
	}
}

func (w *Worker) refreshConsumers(ctx context.Context, have map[string]jetstream.Consumer) []string {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := w.Queue.JS.Stream(cctx, queue.DeliveriesStream)
	if err != nil {
		w.Log.Warn("list consumers", "err", err)
		return sortedKeys(have)
	}
	seen := map[string]bool{}
	lister := stream.ConsumerNames(cctx)
	for name := range lister.Name() {
		if !strings.HasPrefix(name, "deliver_") {
			continue
		}
		seen[name] = true
		if _, ok := have[name]; ok {
			continue
		}
		if c, err := stream.Consumer(cctx, name); err == nil {
			have[name] = c
		}
	}
	if lister.Err() != nil {
		w.Log.Warn("list consumers", "err", lister.Err())
		return sortedKeys(have)
	}
	for name := range have {
		if !seen[name] {
			delete(have, name)
		}
	}
	return sortedKeys(have)
}

func sortedKeys(m map[string]jetstream.Consumer) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Handle processes one delivery job. It acks only after the outcome is committed.
func (w *Worker) Handle(ctx context.Context, msg jetstream.Msg) {
	telemetry.WorkerInflight.Inc()
	defer telemetry.WorkerInflight.Dec()
	ctx = telemetry.Extract(ctx, http.Header(msg.Headers()))
	ctx, span := telemetry.Tracer.Start(ctx, "deliver")
	defer span.End()

	var job pb.DeliveryJob
	if err := proto.Unmarshal(msg.Data(), &job); err != nil {
		w.Log.ErrorContext(ctx, "bad delivery job; dropping", "err", err)
		_ = msg.Term()
		return
	}
	span.SetAttributes(attribute.String("delivery_id", job.DeliveryId), attribute.Int("attempt", int(job.Attempt)))
	log := w.Log.With("tenant_id", job.TenantId, "delivery_id", job.DeliveryId, "endpoint_id", job.EndpointId, "event_id", job.EventId, "attempt", job.Attempt)
	act, err := w.handle(ctx, &job, log)
	if err != nil {
		span.RecordError(err)
		log.WarnContext(ctx, "delivery job failed; will retry", "err", err)
		_ = msg.NakWithDelay(5 * time.Second)
		return
	}
	switch {
	case act.nak > 0:
		_ = msg.NakWithDelay(act.nak)
	default:
		_ = msg.Ack()
	}
}

type action struct{ nak time.Duration }

type jobIDs struct {
	delivery, tenant, event, endpoint uuid.UUID
}

func parseJob(j *pb.DeliveryJob) (jobIDs, bool) {
	var ids jobIDs
	var errs [4]error
	ids.delivery, errs[0] = domain.ParseID(domain.PrefixDelivery, j.DeliveryId)
	ids.tenant, errs[1] = domain.ParseID(domain.PrefixTenant, j.TenantId)
	ids.event, errs[2] = domain.ParseID(domain.PrefixEvent, j.EventId)
	ids.endpoint, errs[3] = domain.ParseID(domain.PrefixEndpoint, j.EndpointId)
	return ids, errors.Join(errs[:]...) == nil && j.Attempt >= 1
}

func (w *Worker) handle(ctx context.Context, job *pb.DeliveryJob, log *slog.Logger) (action, error) {
	ids, ok := parseJob(job)
	if !ok {
		log.ErrorContext(ctx, "bad ids in delivery job; dropping")
		return action{}, nil
	}
	expected := job.Attempt - 1
	q := store.New(w.Pool)
	dbctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ep, found, err := w.endpoint(dbctx, ids.tenant, ids.endpoint)
	if err != nil {
		return action{}, err
	}
	if !found || ep.ep.Status != "enabled" {
		n, err := q.KillDelivery(dbctx, store.KillDeliveryParams{ID: ids.delivery, TenantID: ids.tenant, AttemptCount: expected, DeadReason: domain.ReasonEndpointDisable})
		if err == nil && n > 0 {
			telemetry.DeliveriesDead.WithLabelValues(job.TenantId, domain.ReasonEndpointDisable).Inc()
		}
		return action{}, err
	}

	// Circuit breaker: when open, push the delivery to the reopen time without counting an attempt.
	now := time.Now()
	dec := breaker.Decision{Allow: true}
	if w.Breaker != nil {
		if dec, err = w.Breaker.Allow(dbctx, job.EndpointId, now); err != nil {
			log.WarnContext(ctx, "breaker unavailable, failing open", "err", err)
		}
	}
	if !dec.Allow {
		_, err := q.DeferDelivery(dbctx, store.DeferDeliveryParams{ID: ids.delivery, TenantID: ids.tenant, AttemptCount: expected, NextAttemptAt: &dec.RetryAt})
		return action{}, err
	}

	// Rate limit: wait and come back without counting an attempt.
	if w.Limiter != nil {
		ok, retry, err := w.Limiter.Allow(dbctx, "ep:"+job.EndpointId, int(ep.ep.RateLimitPerSec))
		if err != nil {
			log.WarnContext(ctx, "rate limiter unavailable, failing open", "err", err)
		}
		if !ok {
			telemetry.RateLimited.WithLabelValues("endpoint").Inc()
			if dec.NeedProbe {
				w.releaseProbe(ctx, job.EndpointId)
			}
			return action{nak: max(retry, 10*time.Millisecond)}, nil
		}
	}

	d, err := q.ClaimDelivery(dbctx, store.ClaimDeliveryParams{ID: ids.delivery, TenantID: ids.tenant, AttemptCount: expected, LockSecs: w.LockFor.Seconds()})
	if errors.Is(err, pgx.ErrNoRows) {
		return w.notClaimed(dbctx, q, ids, expected)
	}
	if err != nil {
		return action{}, err
	}
	ev, err := q.GetEvent(dbctx, store.GetEventParams{ID: ids.event, TenantID: ids.tenant})
	if err != nil {
		return action{}, notFound(err)
	}
	body, err := delivery.Envelope(ev.EventType, ev.OccurredAt, ev.Payload)
	if err != nil {
		return action{}, err
	}

	sctx, sspan := telemetry.Tracer.Start(ctx, "webhook POST")
	res := w.Sender.Send(sctx, delivery.Request{URL: ep.ep.Url, ID: job.EventId, Body: body, Secrets: ep.secrets, Headers: ep.headers})
	sspan.SetAttributes(attribute.Int("http.status_code", res.StatusCode))
	sspan.End()

	outcome, kind := delivery.Classify(res.StatusCode, res.Err)
	next := delivery.Decide(outcome, int(job.Attempt), w.Schedule, res.StatusCode, delivery.ParseRetryAfter(res.RetryAfter, time.Now()), rand.Float64()) //nolint:gosec // jitter, not security
	if err := w.record(ctx, ids, job, ep, d, res, kind, next); err != nil {
		return action{}, err
	}

	telemetry.DeliveryAttempts.WithLabelValues(job.TenantId, outcomeLabel(outcome), kind).Inc()
	telemetry.AttemptDuration.WithLabelValues(outcomeLabel(outcome)).Observe(res.Duration.Seconds())
	switch next.Status {
	case domain.StatusSucceeded:
		telemetry.EndToEnd.WithLabelValues(job.TenantId).Observe(time.Since(ev.CreatedAt).Seconds())
	case domain.StatusDead:
		telemetry.DeliveriesDead.WithLabelValues(job.TenantId, next.DeadReason).Inc()
		log.WarnContext(ctx, "delivery dead", "reason", next.DeadReason, "status_code", res.StatusCode, "error_kind", kind)
	case domain.StatusRetrying:
		log.DebugContext(ctx, "delivery will retry", "in", next.Delay, "status_code", res.StatusCode, "error_kind", kind)
	}
	if w.Breaker != nil && outcome != delivery.Permanent {
		tr, err := w.Breaker.Record(ctx, job.EndpointId, time.Now(), outcome == delivery.Success, dec.NeedProbe)
		if err != nil {
			log.WarnContext(ctx, "breaker record failed", "err", err)
		}
		if tr != "" {
			telemetry.BreakerTransitions.WithLabelValues(tr).Inc()
			// ponytail: per-pod gauge of transitions; sum across pods approximates open breakers. Exact count needs a Redis scan.
			switch tr {
			case breaker.Open:
				if !dec.NeedProbe {
					telemetry.BreakersOpen.Inc()
				}
			case breaker.Closed:
				telemetry.BreakersOpen.Dec()
			}
			log.WarnContext(ctx, "circuit breaker changed", "to", tr)
		}
	}
	return action{}, nil
}

func (w *Worker) releaseProbe(ctx context.Context, ep string) {
	if err := w.Breaker.ReleaseProbe(ctx, ep); err != nil {
		w.Log.WarnContext(ctx, "release breaker probe", "err", err)
	}
}

func outcomeLabel(o delivery.Outcome) string {
	if o == delivery.Success {
		return "success"
	}
	if o == delivery.Retryable {
		return "retryable"
	}
	return "permanent"
}

// notClaimed decides what to do when the claim matched no row: stale or duplicate jobs are acked;
// a job whose delivery is locked by another attempt waits for the lock instead.
func (w *Worker) notClaimed(ctx context.Context, q *store.Queries, ids jobIDs, expected int32) (action, error) {
	d, err := q.GetDelivery(ctx, store.GetDeliveryParams{ID: ids.delivery, TenantID: ids.tenant})
	if errors.Is(err, pgx.ErrNoRows) {
		return action{}, nil
	}
	if err != nil {
		return action{}, err
	}
	if (d.Status == domain.StatusPending || d.Status == domain.StatusRetrying) && d.AttemptCount == expected && d.LockedUntil != nil {
		if wait := time.Until(*d.LockedUntil); wait > 0 {
			return action{nak: wait + time.Second}, nil
		}
	}
	return action{}, nil
}

// record stores the attempt and the delivery's new state in one transaction.
func (w *Worker) record(ctx context.Context, ids jobIDs, job *pb.DeliveryJob, ep workerEndpoint, d store.Delivery, res delivery.Result, kind string, next delivery.Decision) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var nextAt *time.Time
	if next.Status == domain.StatusRetrying {
		t := time.Now().Add(next.Delay)
		nextAt = &t
	}
	errMsg := ""
	if res.Err != nil {
		errMsg = truncate(res.Err.Error(), 512)
	} else if kind == domain.KindHTTPStatus {
		errMsg = http.StatusText(res.StatusCode)
	}
	return pgx.BeginTxFunc(ctx, w.Pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		q := store.New(tx)
		n, err := q.CompleteAttempt(ctx, store.CompleteAttemptParams{
			ID: ids.delivery, AttemptCount: d.AttemptCount, Status: next.Status, NextAttemptAt: nextAt,
			LastStatusCode: int32(res.StatusCode), LastError: errMsg, DeadReason: next.DeadReason,
		})
		if err != nil {
			return err
		}
		if n == 0 {
			return nil // completed elsewhere meanwhile; keep the first result
		}
		if err := q.InsertAttempt(ctx, store.InsertAttemptParams{
			ID: domain.NewID(), DeliveryID: ids.delivery, TenantID: ids.tenant, AttemptNumber: job.Attempt,
			StartedAt: time.Now().Add(-res.Duration), DurationMs: int32(res.Duration.Milliseconds()),
			StatusCode: int32(res.StatusCode), ErrorKind: kind, ErrorMessage: errMsg, ResponseBodySnippet: res.Body,
		}); err != nil {
			return err
		}
		switch {
		case next.DeadReason == domain.ReasonEndpointGone:
			_, err = q.SetEndpointStatus(ctx, store.SetEndpointStatusParams{ID: ids.endpoint, TenantID: ids.tenant, Status: "disabled", DisabledReason: "gone"})
			if errors.Is(err, pgx.ErrNoRows) {
				err = nil
			}
		case next.Status == domain.StatusSucceeded && ep.ep.FailingSince != nil:
			err = q.ClearEndpointFailing(ctx, ids.endpoint)
		case next.Status != domain.StatusSucceeded && ep.ep.FailingSince == nil:
			err = q.MarkEndpointFailing(ctx, ids.endpoint)
		}
		return err
	})
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func (w *Worker) endpoint(ctx context.Context, tenant, id uuid.UUID) (workerEndpoint, bool, error) {
	if e, ok := w.endpoints.get(id); ok {
		return e, true, nil
	}
	q := store.New(w.Pool)
	ep, err := q.GetEndpoint(ctx, store.GetEndpointParams{ID: id, TenantID: tenant})
	if errors.Is(err, pgx.ErrNoRows) {
		return workerEndpoint{}, false, nil
	}
	if err != nil {
		return workerEndpoint{}, false, err
	}
	rows, err := q.ListActiveSecrets(ctx, id)
	if err != nil {
		return workerEndpoint{}, false, err
	}
	e := workerEndpoint{ep: ep}
	for _, r := range rows {
		s, err := w.Box.Open(r.SecretCiphertext)
		if err != nil {
			return workerEndpoint{}, false, err
		}
		e.secrets = append(e.secrets, s)
	}
	_ = json.Unmarshal(ep.CustomHeaders, &e.headers)
	w.endpoints.put(id, e)
	return e, true, nil
}

// RunWorker wires and runs the worker service.
func RunWorker(ctx context.Context, cfg config.Worker, log *slog.Logger) error {
	sched, err := delivery.ParseSchedule(cfg.RetrySchedule)
	if err != nil {
		return err
	}
	mk, err := cfg.MasterKeyBytes()
	if err != nil {
		return err
	}
	box, err := signing.NewBox(mk)
	if err != nil {
		return err
	}
	pool, err := OpenPostgres(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	q, err := queue.Connect(cfg.NATSURL, "worker")
	if err != nil {
		return err
	}
	if err := ensureStreams(ctx, q, log); err != nil {
		return err
	}
	rdb := OpenRedis(cfg.Redis)
	w := NewWorker(Worker{
		Pool: pool, Queue: q, Breaker: breaker.New(rdb), Limiter: ratelimit.New(rdb),
		Sender: delivery.NewSender(ssrf.NewPolicy(cfg.SSRFAllowlist), telemetry.Version), Box: box,
		Schedule: sched, Log: log, Concurrency: cfg.Concurrency,
	})
	return server.Run(ctx, server.Options{
		Name: "worker", Log: log, AdminAddr: cfg.AdminAddr, Pprof: cfg.PprofEnabled,
		DrainDelay: cfg.DrainDelay, StopTimeout: cfg.ShutdownTimeout,
		Ready:   readyAll(pingDB(pool), q.Ready),
		Loops:   []func(context.Context) error{w.Run},
		Closers: []func(){q.Close, func() { _ = rdb.Close() }, pool.Close},
	})
}
