package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
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
	if w.Breaker != nil {
		wp := &w
		w.Breaker.OnTransition = func(ep, to string) { wp.breakerChanged(ep, to, false) }
		w.Breaker.OnError = func(err error) { wp.Log.Warn("breaker flush failed, failing open", "err", err) }
	}
	if w.LockFor == 0 {
		w.LockFor = 30 * time.Second
	}
	if w.RefreshEvery == 0 {
		w.RefreshEvery = 10 * time.Second
	}
	return &w
}

// perTenantFetch is how many jobs one tenant's puller holds at most before they start.
const perTenantFetch = 10

// Run fetches jobs round-robin across tenant consumers until ctx ends, then waits for in-flight jobs.
func (w *Worker) Run(ctx context.Context) error {
	if w.Breaker != nil {
		// Flushes buffered breaker results; stopped only after in-flight jobs finish
		// (defers run in reverse: wg.Wait, then stop, then wait for the final flush).
		fctx, stopFlush := context.WithCancel(context.WithoutCancel(ctx))
		flushed := make(chan struct{})
		go func() { defer close(flushed); _ = w.Breaker.FlushLoop(fctx) }()
		defer func() { <-flushed }()
		defer stopFlush()
	}
	sem := make(chan struct{}, w.Concurrency)
	var jobs sync.WaitGroup // in-flight jobs
	defer jobs.Wait()

	// One puller per tenant consumer. Pullers fetch without holding slots, then each job
	// waits for a slot. Blocked channel senders are served FIFO, so with one waiting job per
	// tenant at a time the pool is shared round-robin, and idle tenants hold nothing.
	var pullers sync.WaitGroup
	defer pullers.Wait()
	running := map[string]context.CancelFunc{}
	defer func() {
		for _, stop := range running {
			stop()
		}
	}()
	consumers := map[string]jetstream.Consumer{}
	for {
		w.refreshConsumers(ctx, consumers)
		for name, c := range consumers {
			if _, ok := running[name]; ok {
				continue
			}
			pctx, stop := context.WithCancel(ctx)
			running[name] = stop
			pullers.Add(1)
			go func() {
				defer pullers.Done()
				w.pull(pctx, ctx, c, sem, &jobs)
			}()
		}
		for name, stop := range running {
			if _, ok := consumers[name]; !ok {
				stop()
				delete(running, name)
			}
		}
		if !sleep(ctx, w.RefreshEvery) {
			return nil
		}
	}
}

// pull fetches one tenant's jobs and starts each as a slot frees up, until pctx ends.
// Jobs run detached from cancellation (their own timeouts bound them) so shutdown never
// abandons a started delivery; fetched jobs that never started are naked for redelivery.
func (w *Worker) pull(pctx, runCtx context.Context, c jetstream.Consumer, sem chan struct{}, jobs *sync.WaitGroup) {
	for pctx.Err() == nil {
		batch, err := c.Fetch(perTenantFetch, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			// A deleted consumer is stopped by the next refresh; until then just back off.
			if !sleep(pctx, time.Second) {
				return
			}
			continue
		}
		for msg := range batch.Messages() {
			select {
			case sem <- struct{}{}:
			case <-pctx.Done():
				_ = msg.Nak()
				continue
			}
			jobs.Add(1)
			go func() {
				defer func() { <-sem; jobs.Done() }()
				w.Handle(context.WithoutCancel(runCtx), msg)
			}()
		}
	}
}

func (w *Worker) refreshConsumers(ctx context.Context, have map[string]jetstream.Consumer) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := w.Queue.JS.Stream(cctx, queue.DeliveriesStream)
	if err != nil {
		w.Log.Warn("list consumers", "err", err)
		return
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
		return
	}
	for name := range have {
		if !seen[name] {
			delete(have, name)
		}
	}
}

// Handle processes one delivery job. It acks only after the outcome is committed.
func (w *Worker) Handle(ctx context.Context, msg jetstream.Msg) {
	telemetry.WorkerInflight.Inc()
	defer telemetry.WorkerInflight.Dec()
	ctx = telemetry.Extract(ctx, msg.Headers())
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

	d, err := q.ClaimDeliveryWithEvent(dbctx, store.ClaimDeliveryWithEventParams{ID: ids.delivery, TenantID: ids.tenant, AttemptCount: expected, LockSecs: w.LockFor.Seconds()})
	if errors.Is(err, pgx.ErrNoRows) {
		return w.notClaimed(dbctx, q, ids, expected)
	}
	if err != nil {
		return action{}, err
	}
	if job.ScheduledUnixMs > 0 {
		telemetry.ScheduleLag.Observe(max(0, time.Since(time.UnixMilli(job.ScheduledUnixMs)).Seconds()))
	}
	body, err := delivery.Envelope(d.EventType, d.OccurredAt, d.Payload)
	if err != nil {
		return action{}, err
	}

	sctx, sspan := telemetry.Tracer.Start(ctx, "webhook POST")
	res := w.Sender.Send(sctx, delivery.Request{URL: ep.ep.Url, ID: job.EventId, Body: body, Secrets: ep.secrets, Headers: ep.headers})
	sspan.SetAttributes(attribute.Int("http.status_code", res.StatusCode))
	sspan.End()

	outcome, kind := delivery.Classify(res.StatusCode, res.Err)
	next := delivery.Decide(outcome, int(job.Attempt), w.Schedule, res.StatusCode, delivery.ParseRetryAfter(res.RetryAfter, time.Now()), rand.Float64()) //nolint:gosec // jitter, not security
	if err := w.record(ctx, ids, ep, d.AttemptCount, res, kind, next); err != nil {
		return action{}, err
	}

	telemetry.DeliveryAttempts.WithLabelValues(job.TenantId, outcomeLabel(outcome), kind).Inc()
	telemetry.AttemptDuration.WithLabelValues(outcomeLabel(outcome)).Observe(res.Duration.Seconds())
	switch next.Status {
	case domain.StatusSucceeded:
		log.DebugContext(ctx, "delivered", "status_code", res.StatusCode, "duration_ms", res.Duration.Milliseconds())
		telemetry.EndToEnd.WithLabelValues(job.TenantId).Observe(time.Since(d.EventCreatedAt).Seconds())
	case domain.StatusDead:
		telemetry.DeliveriesDead.WithLabelValues(job.TenantId, next.DeadReason).Inc()
		log.WarnContext(ctx, "delivery dead", "reason", next.DeadReason, "status_code", res.StatusCode, "error_kind", kind)
	case domain.StatusRetrying:
		log.WarnContext(ctx, "delivery will retry", "in", next.Delay, "status_code", res.StatusCode, "error_kind", kind)
	}
	if w.Breaker != nil && outcome != delivery.Permanent {
		if dec.NeedProbe {
			tr, err := w.Breaker.Record(ctx, job.EndpointId, time.Now(), outcome == delivery.Success, true)
			if err != nil {
				log.WarnContext(ctx, "breaker record failed", "err", err)
			}
			w.breakerChanged(job.EndpointId, tr, true)
		} else {
			w.Breaker.Observe(job.EndpointId, time.Now(), outcome == delivery.Success)
		}
	}
	return action{}, nil
}

// breakerChanged logs and counts a transition. fromProbe: a failed probe re-opens an
// already-open breaker, which is not a new open breaker for the gauge.
func (w *Worker) breakerChanged(ep, to string, fromProbe bool) {
	if to == "" {
		return
	}
	telemetry.BreakerTransitions.WithLabelValues(to).Inc()
	// ponytail: per-pod gauge of transitions; sum across pods approximates open breakers. Exact count needs a Redis scan.
	switch {
	case to == breaker.Open && !fromProbe:
		telemetry.BreakersOpen.Inc()
	case to == breaker.Closed:
		telemetry.BreakersOpen.Dec()
	}
	w.Log.Warn("circuit breaker changed", "endpoint_id", ep, "to", to)
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

// record stores the attempt and the delivery's new state in one atomic statement,
// then updates the endpoint's health when it changed.
func (w *Worker) record(ctx context.Context, ids jobIDs, ep workerEndpoint, attemptCount int32, res delivery.Result, kind string, next delivery.Decision) error {
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
	q := store.New(w.Pool)
	n, err := q.RecordAttempt(ctx, store.RecordAttemptParams{
		DeliveryID: ids.delivery, AttemptCount: attemptCount, Status: next.Status, NextAttemptAt: nextAt, DeadReason: next.DeadReason,
		AttemptID: domain.NewID(), StartedAt: time.Now().Add(-res.Duration), DurationMs: int32(res.Duration.Milliseconds()),
		StatusCode: int32(res.StatusCode), ErrorKind: kind, ErrorMessage: errMsg, ResponseBodySnippet: res.Body,
	})
	if err != nil || n == 0 {
		return err // n == 0: completed elsewhere meanwhile; keep the first result
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
	if err != nil {
		w.Log.WarnContext(ctx, "update endpoint health", "endpoint_id", domain.FormatID(domain.PrefixEndpoint, ids.endpoint), "err", err)
	}
	return nil
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
