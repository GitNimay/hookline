//go:build integration

package app

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	standardwebhooks "github.com/standard-webhooks/standard-webhooks/libraries/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/GitNimay/hookline/gen/hookline/v1"
	"github.com/GitNimay/hookline/internal/auth"
	"github.com/GitNimay/hookline/internal/breaker"
	"github.com/GitNimay/hookline/internal/config"
	"github.com/GitNimay/hookline/internal/delivery"
	"github.com/GitNimay/hookline/internal/domain"
	"github.com/GitNimay/hookline/internal/queue"
	"github.com/GitNimay/hookline/internal/ratelimit"
	"github.com/GitNimay/hookline/internal/signing"
	"github.com/GitNimay/hookline/internal/ssrf"
	"github.com/GitNimay/hookline/internal/testutil"
)

// testSchedule keeps retry tests fast: 4 attempts, 200 ms apart.
var testSchedule = delivery.Schedule{200 * time.Millisecond, 200 * time.Millisecond, 200 * time.Millisecond}

// harness runs every Hookline component in one process against real Postgres, NATS and Redis.
type harness struct {
	t      *testing.T
	pool   *pgxpool.Pool
	q      *queue.Queue
	rdb    *redis.Client
	ctl    *Control
	ing    *Ingest
	worker *Worker
	log    *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type harnessOpts struct {
	noWorker bool
	lockFor  time.Duration
}

func newHarness(t *testing.T, o harnessOpts) *harness {
	t.Helper()
	dsn, natsURL, redisAddr := testutil.Postgres(t), testutil.NATS(t), testutil.Redis(t)
	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{t: t, ctx: ctx, cancel: cancel, log: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))}
	var err error
	h.pool, err = OpenPostgres(ctx, config.Postgres{DatabaseURL: dsn, DBMaxConns: 20, QueryTimeout: 5 * time.Second})
	require.NoError(t, err)
	h.q, err = queue.Connect(natsURL, "test")
	require.NoError(t, err)
	require.NoError(t, h.q.EnsureStreams(ctx))
	h.rdb = OpenRedis(config.Redis{RedisAddr: redisAddr})
	box, err := signing.NewBox(make([]byte, 32))
	require.NoError(t, err)
	policy := ssrf.NewPolicy([]string{"127.0.0.1"})
	sender := delivery.NewSender(policy, "test")
	h.ctl = &Control{Pool: h.pool, Queue: h.q, Box: box, Breaker: breaker.New(h.rdb), Sender: sender, Policy: policy, Log: h.log}
	h.ing = NewIngest(h.pool, ratelimit.New(h.rdb), h.log, 10_000)
	disp := &Dispatcher{
		Pool: h.pool, Queue: h.q, Log: h.log, Concurrency: 8, cache: newCache[string, []uuid.UUID](0),
		Match: func(ctx context.Context, tenant, typ string) ([]string, error) {
			r, err := h.ctl.ListMatchingEndpoints(ctx, &pb.ListMatchingEndpointsRequest{TenantId: tenant, EventType: typ})
			if err != nil {
				return nil, err
			}
			return r.EndpointIds, nil
		},
	}
	h.worker = NewWorker(Worker{
		Pool: h.pool, Queue: h.q, Breaker: breaker.New(h.rdb), Limiter: ratelimit.New(h.rdb), Sender: sender, Box: box,
		Schedule: testSchedule, Log: h.log, Concurrency: 50, RefreshEvery: 100 * time.Millisecond, LockFor: o.lockFor,
	})
	sched := &Scheduler{Pool: h.pool, Queue: h.q, Log: h.log, Cfg: config.Scheduler{ReconcilePendingAt: 10 * time.Minute, AutoDisableAfter: 72 * time.Hour}}
	relay := &Relay{Pool: h.pool, Queue: h.q, Log: h.log, Notify: h.ing.Notify, Every: 50 * time.Millisecond}

	loops := []func(context.Context) error{relay.Run, disp.Run, sched.DueLoop}
	if !o.noWorker {
		loops = append(loops, h.worker.Run)
	}
	for _, l := range loops {
		h.goRun(l)
	}
	t.Cleanup(h.stop)
	return h
}

func (h *harness) goRun(l func(context.Context) error) {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		if err := l(h.ctx); err != nil {
			h.t.Errorf("loop failed: %v", err)
		}
	}()
}

func (h *harness) stop() {
	h.cancel()
	h.wg.Wait()
	h.q.Close()
	_ = h.rdb.Close()
	h.pool.Close()
}

var admin = auth.WithPrincipal(context.Background(), auth.Principal{Admin: true})

// tenant creates a tenant with the event type "order.created" and returns its context.
func (h *harness) tenant(name string) (context.Context, string) {
	h.t.Helper()
	res, err := h.ctl.CreateTenant(admin, &pb.CreateTenantRequest{Name: name})
	require.NoError(h.t, err)
	id, _ := domain.ParseID(domain.PrefixTenant, res.Tenant.Id)
	ctx := auth.WithPrincipal(context.Background(), auth.Principal{TenantID: id})
	_, err = h.ctl.CreateEventType(ctx, &pb.CreateEventTypeRequest{Name: "order.created"})
	require.NoError(h.t, err)
	return ctx, res.Tenant.Id
}

func (h *harness) endpoint(ctx context.Context, u string) (string, []string) {
	h.t.Helper()
	res, err := h.ctl.CreateEndpoint(ctx, &pb.CreateEndpointRequest{Url: u, EventTypes: []string{"*"}, RateLimitPerSec: 1000})
	require.NoError(h.t, err)
	return res.Endpoint.Id, h.secrets(ctx, res.Endpoint.Id)
}

func (h *harness) secrets(ctx context.Context, ep string) []string {
	s, err := h.ctl.GetEndpointSecret(ctx, &pb.GetEndpointSecretRequest{Id: ep})
	require.NoError(h.t, err)
	return secretStrings(s.Secrets)
}

func (h *harness) send(ctx context.Context, idem string) *pb.SendEventResponse {
	h.t.Helper()
	payload, _ := structpb.NewStruct(map[string]any{"id": uuid.NewString()})
	res, err := h.ing.SendEvent(ctx, &pb.SendEventRequest{EventType: "order.created", Payload: payload, IdempotencyKey: idem})
	require.NoError(h.t, err)
	return res
}

func (h *harness) deliveries(ctx context.Context, f *pb.ListDeliveriesRequest) []*pb.Delivery {
	h.t.Helper()
	res, err := h.ctl.ListDeliveries(ctx, f)
	require.NoError(h.t, err)
	return res.Deliveries
}

// eventually polls cond every 50 ms until it holds or timeout passes.
func eventually(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v: %s", timeout, msg)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// receiver is a webhook endpoint that verifies signatures with the official library.
type receiver struct {
	srv *httptest.Server

	mu      sync.Mutex
	secrets []string
	reply   func(n int) (int, http.Header) // n = request number on this receiver, from 1
	n       int
	ids     map[string]int
	times   []time.Time
	bad     int
}

func newReceiver(t *testing.T) *receiver {
	r := &receiver{ids: map[string]int{}, reply: func(int) (int, http.Header) { return 200, nil }}
	r.srv = httptest.NewServer(http.HandlerFunc(r.serve))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *receiver) url() string { return r.srv.URL + "/hook" }

func (r *receiver) serve(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	r.mu.Lock()
	r.n++
	n := r.n
	r.times = append(r.times, time.Now())
	reply, secrets := r.reply, r.secrets
	r.mu.Unlock()
	code, hdr := reply(n)
	for k, v := range hdr {
		w.Header()[k] = v
	}
	if code/100 == 2 {
		ok := len(secrets) == 0
		for _, s := range secrets {
			wh, err := standardwebhooks.NewWebhook(s)
			if err == nil && wh.Verify(body, req.Header) == nil {
				ok = true
			}
		}
		r.mu.Lock()
		if ok {
			r.ids[req.Header.Get("webhook-id")]++
		} else {
			r.bad++
		}
		r.mu.Unlock()
	}
	w.WriteHeader(code)
}

func (r *receiver) unique() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ids)
}

func (r *receiver) requests() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

// rawID strips a prefixed ID down to its UUID (for direct SQL in tests).
func rawID(prefix, s string) uuid.UUID {
	id, _ := domain.ParseID(prefix, s)
	return id
}
