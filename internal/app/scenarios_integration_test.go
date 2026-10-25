//go:build integration

package app

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	pb "github.com/GitNimay/hookline/gen/hookline/v1"
	"github.com/GitNimay/hookline/internal/breaker"
	"github.com/GitNimay/hookline/internal/config"
	"github.com/GitNimay/hookline/internal/delivery"
	"github.com/GitNimay/hookline/internal/domain"
	"github.com/GitNimay/hookline/internal/server"
	"github.com/GitNimay/hookline/internal/signing"
	"github.com/GitNimay/hookline/internal/ssrf"
	"github.com/GitNimay/hookline/internal/telemetry"
)

// One harness for the whole file keeps container start-up to once.
func TestScenarios(t *testing.T) {
	h := newHarness(t, harnessOpts{})

	t.Run("full path: three endpoints, signed, zero missing", func(t *testing.T) {
		ctx, _ := h.tenant("full-path")
		recvs := []*receiver{newReceiver(t), newReceiver(t), newReceiver(t)}
		for _, r := range recvs {
			_, r.secrets = h.endpoint(ctx, r.url())
		}
		const n = 200
		for range n {
			h.send(ctx, "")
		}
		for _, r := range recvs {
			eventually(t, 30*time.Second, "all events delivered", func() bool { return r.unique() == n })
			require.Zero(t, r.bad, "signature failures")
		}
	})

	t.Run("scenario 6: same idempotency key twice is one event and one delivery per endpoint", func(t *testing.T) {
		ctx, _ := h.tenant("idem")
		r := newReceiver(t)
		ep, secrets := h.endpoint(ctx, r.url())
		r.secrets = secrets
		a := h.send(ctx, "order-42")
		b := h.send(ctx, "order-42")
		require.False(t, a.Duplicate)
		require.True(t, b.Duplicate)
		require.Equal(t, a.Id, b.Id)
		eventually(t, 10*time.Second, "delivered", func() bool { return r.unique() == 1 })
		time.Sleep(500 * time.Millisecond)
		require.Len(t, h.deliveries(ctx, &pb.ListDeliveriesRequest{EndpointId: ep}), 1)
		require.Equal(t, 1, r.requests())
	})

	t.Run("scenario 2: endpoint fails then recovers, every event delivered", func(t *testing.T) {
		ctx, _ := h.tenant("recover")
		r := newReceiver(t)
		failUntil := time.Now().Add(1500 * time.Millisecond)
		r.reply = func(int) (int, http.Header) {
			if time.Now().Before(failUntil) {
				return 500, nil
			}
			return 200, nil
		}
		_, r.secrets = h.endpoint(ctx, r.url())
		for range 5 {
			h.send(ctx, "")
		}
		// 5 events x 2 failed attempts = 10 consecutive failures, which opens the breaker;
		// the backlog must still drain on its own once the half-open probe succeeds.
		eventually(t, 60*time.Second, "delivered after recovery", func() bool { return r.unique() == 5 })
	})

	t.Run("scenario 3: always failing goes dead, replay after fix delivers", func(t *testing.T) {
		ctx, _ := h.tenant("dead")
		r := newReceiver(t)
		failing := true
		r.reply = func(int) (int, http.Header) {
			if failing {
				return 500, nil
			}
			return 200, nil
		}
		ep, secrets := h.endpoint(ctx, r.url())
		r.secrets = secrets
		h.send(ctx, "")
		var dead []*pb.Delivery
		eventually(t, 20*time.Second, "dead after last attempt", func() bool {
			dead = h.deliveries(ctx, &pb.ListDeliveriesRequest{Status: domain.StatusDead})
			return len(dead) == 1
		})
		require.Equal(t, int32(testSchedule.MaxAttempts()), dead[0].AttemptCount)
		require.Equal(t, domain.ReasonExhausted, dead[0].DeadReason)
		attempts, err := h.ctl.ListDeliveryAttempts(ctx, &pb.ListDeliveryAttemptsRequest{Id: dead[0].Id})
		require.NoError(t, err)
		require.Len(t, attempts.Attempts, testSchedule.MaxAttempts())
		require.Equal(t, int32(500), attempts.Attempts[0].StatusCode)

		r.mu.Lock()
		failing = false
		r.mu.Unlock()
		res, err := h.ctl.ReplayEndpoint(ctx, &pb.ReplayEndpointRequest{EndpointId: ep})
		require.NoError(t, err)
		require.Equal(t, int64(1), res.Replayed)
		eventually(t, 10*time.Second, "replayed delivery arrives", func() bool { return r.unique() == 1 })
		eventually(t, 5*time.Second, "succeeded", func() bool {
			return len(h.deliveries(ctx, &pb.ListDeliveriesRequest{Status: domain.StatusSucceeded})) == 1
		})
	})

	t.Run("scenario 8: 429 with Retry-After delays the next attempt", func(t *testing.T) {
		ctx, _ := h.tenant("retry-after")
		r := newReceiver(t)
		r.reply = func(n int) (int, http.Header) {
			if n == 1 {
				return 429, http.Header{"Retry-After": {"3"}}
			}
			return 200, nil
		}
		_, r.secrets = h.endpoint(ctx, r.url())
		h.send(ctx, "")
		eventually(t, 15*time.Second, "delivered", func() bool { return r.unique() == 1 })
		r.mu.Lock()
		gap := r.times[1].Sub(r.times[0])
		r.mu.Unlock()
		require.GreaterOrEqual(t, gap, 3*time.Second, "second attempt came before Retry-After")
	})

	t.Run("scenario 9: 410 Gone disables the endpoint", func(t *testing.T) {
		ctx, _ := h.tenant("gone")
		r := newReceiver(t)
		r.reply = func(int) (int, http.Header) { return 410, nil }
		ep, _ := h.endpoint(ctx, r.url())
		h.send(ctx, "")
		eventually(t, 10*time.Second, "endpoint disabled", func() bool {
			e, err := h.ctl.GetEndpoint(ctx, &pb.GetEndpointRequest{Id: ep})
			return err == nil && e.Endpoint.Status == "disabled" && e.Endpoint.DisabledReason == "gone"
		})
		d := h.deliveries(ctx, &pb.ListDeliveriesRequest{EndpointId: ep})
		require.Equal(t, domain.StatusDead, d[0].Status)
		require.Equal(t, domain.ReasonEndpointGone, d[0].DeadReason)
		require.Equal(t, 1, r.requests())
	})

	t.Run("scenario 10: breaker opens after repeated failures, probes, closes on recovery", func(t *testing.T) {
		ctx, _ := h.tenant("breaker")
		r := newReceiver(t)
		failing := true
		r.reply = func(int) (int, http.Header) {
			r.mu.Lock()
			defer r.mu.Unlock()
			if failing {
				return 503, nil
			}
			return 200, nil
		}
		ep, secrets := h.endpoint(ctx, r.url())
		r.secrets = secrets
		opened := testutil.ToFloat64(telemetry.BreakerTransitions.WithLabelValues(breaker.Open))
		for range 15 {
			h.send(ctx, "")
		}
		eventually(t, 20*time.Second, "breaker open", func() bool {
			e, err := h.ctl.GetEndpoint(ctx, &pb.GetEndpointRequest{Id: ep})
			return err == nil && e.Endpoint.CircuitState == breaker.Open
		})
		require.Greater(t, testutil.ToFloat64(telemetry.BreakerTransitions.WithLabelValues(breaker.Open)), opened)
		// While open, deliveries are deferred, not attempted: requests stop growing.
		before := r.requests()
		time.Sleep(2 * time.Second)
		require.LessOrEqual(t, r.requests()-before, 1)

		// Recover; make the open window end now so the next due job is the half-open probe.
		r.mu.Lock()
		failing = false
		r.mu.Unlock()
		s, err := breaker.New(h.rdb).Get(ctx, ep)
		require.NoError(t, err)
		s.OpenUntil = time.Now().UnixMilli()
		require.NoError(t, h.rdb.Set(ctx, "hookline:breaker:"+ep, mustJSON(s), time.Hour).Err())
		_, err = h.pool.Exec(ctx, "UPDATE deliveries SET next_attempt_at = now() WHERE status = 'retrying' AND endpoint_id = $1", rawID(domain.PrefixEndpoint, ep))
		require.NoError(t, err)
		eventually(t, 30*time.Second, "breaker closed and backlog drained", func() bool {
			e, err := h.ctl.GetEndpoint(ctx, &pb.GetEndpointRequest{Id: ep})
			return err == nil && e.Endpoint.CircuitState == breaker.Closed && r.unique() == 15
		})
		require.Greater(t, testutil.ToFloat64(telemetry.BreakerTransitions.WithLabelValues(breaker.Closed)), 0.0)
	})

	t.Run("scenario 12: during rotation receivers with the old or new secret both verify", func(t *testing.T) {
		ctx, _ := h.tenant("rotation")
		oldR, newR := newReceiver(t), newReceiver(t)
		// Both receivers point at endpoints whose secret is rotated before any delivery.
		epOld, s1 := h.endpoint(ctx, oldR.url())
		epNew, _ := h.endpoint(ctx, newR.url())
		rot, err := h.ctl.RotateEndpointSecret(ctx, &pb.RotateEndpointSecretRequest{Id: epOld})
		require.NoError(t, err)
		require.Len(t, rot.Secrets, 2)
		require.NotNil(t, rot.Secrets[1].ExpireTime, "old secret expires after the overlap")
		oldR.secrets = s1 // receiver still on the old secret
		rot2, err := h.ctl.RotateEndpointSecret(ctx, &pb.RotateEndpointSecretRequest{Id: epNew})
		require.NoError(t, err)
		newR.secrets = []string{rot2.Secrets[0].Secret} // receiver already on the new secret
		h.send(ctx, "")
		for _, r := range []*receiver{oldR, newR} {
			eventually(t, 10*time.Second, "delivered and verified", func() bool { return r.unique() == 1 })
			require.Zero(t, r.bad)
		}
		// A second rotation inside the overlap keeps at most two secrets active.
		rot3, err := h.ctl.RotateEndpointSecret(ctx, &pb.RotateEndpointSecretRequest{Id: epOld})
		require.NoError(t, err)
		require.Len(t, rot3.Secrets, 2)
	})

	t.Run("tenant isolation: other tenants' IDs are NotFound", func(t *testing.T) {
		ctxA, _ := h.tenant("iso-a")
		ctxB, _ := h.tenant("iso-b")
		r := newReceiver(t)
		ep, _ := h.endpoint(ctxA, r.url())
		ev := h.send(ctxA, "")
		var dlv string
		eventually(t, 10*time.Second, "delivery exists", func() bool {
			d := h.deliveries(ctxA, &pb.ListDeliveriesRequest{EventId: ev.Id})
			if len(d) == 1 {
				dlv = d[0].Id
			}
			return dlv != ""
		})
		checks := map[string]error{}
		_, checks["GetEndpoint"] = h.ctl.GetEndpoint(ctxB, &pb.GetEndpointRequest{Id: ep})
		_, checks["UpdateEndpoint"] = h.ctl.UpdateEndpoint(ctxB, &pb.UpdateEndpointRequest{Id: ep, Endpoint: &pb.Endpoint{}, UpdateMask: nil})
		_, checks["DeleteEndpoint"] = h.ctl.DeleteEndpoint(ctxB, &pb.DeleteEndpointRequest{Id: ep})
		_, checks["GetEndpointSecret"] = h.ctl.GetEndpointSecret(ctxB, &pb.GetEndpointSecretRequest{Id: ep})
		_, checks["RotateEndpointSecret"] = h.ctl.RotateEndpointSecret(ctxB, &pb.RotateEndpointSecretRequest{Id: ep})
		_, checks["SendTestEvent"] = h.ctl.SendTestEvent(ctxB, &pb.SendTestEventRequest{Id: ep})
		_, checks["ReplayEndpoint"] = h.ctl.ReplayEndpoint(ctxB, &pb.ReplayEndpointRequest{EndpointId: ep})
		_, checks["GetDelivery"] = h.ctl.GetDelivery(ctxB, &pb.GetDeliveryRequest{Id: dlv})
		_, checks["ListDeliveryAttempts"] = h.ctl.ListDeliveryAttempts(ctxB, &pb.ListDeliveryAttemptsRequest{Id: dlv})
		_, checks["ReplayDelivery"] = h.ctl.ReplayDelivery(ctxB, &pb.ReplayDeliveryRequest{Id: dlv})
		_, checks["GetEvent"] = h.ing.GetEvent(ctxB, &pb.GetEventRequest{Id: ev.Id})
		for name, err := range checks {
			require.Truef(t, errors.Is(err, domain.ErrNotFound), "%s: want NotFound, got %v", name, err)
		}
		require.Empty(t, h.deliveries(ctxB, &pb.ListDeliveriesRequest{EndpointId: ep}))
		evs, err := h.ing.ListEvents(ctxB, &pb.ListEventsRequest{})
		require.NoError(t, err)
		require.Empty(t, evs.Events)
		// And the endpoint is untouched for its owner.
		e, err := h.ctl.GetEndpoint(ctxA, &pb.GetEndpointRequest{Id: ep})
		require.NoError(t, err)
		require.Equal(t, "enabled", e.Endpoint.Status)
	})

	t.Run("scenario 11: private destinations are refused at create time and at dial time", func(t *testing.T) {
		ctx, _ := h.tenant("ssrf")
		strict := *h.ctl
		strict.Policy = ssrf.NewPolicy(nil)
		for _, u := range []string{"http://127.0.0.1:8080/x", "http://169.254.169.254/latest/meta-data", "http://[::1]:9000/", "http://localhost:8080/"} {
			_, err := strict.CreateEndpoint(ctx, &pb.CreateEndpointRequest{Url: u, EventTypes: []string{"*"}})
			require.Truef(t, errors.Is(err, domain.ErrInvalidArgument), "%s: %v", u, err)
		}
		// A URL that passed validation but points to a private IP at dial time (DNS rebinding,
		// decimal notation, ...) is stopped by the dialer, and the attempt is not retryable.
		r := newReceiver(t)
		res := delivery.NewSender(ssrf.NewPolicy(nil), "test").Send(ctx, delivery.Request{URL: r.url(), ID: "evt_x", Body: []byte("{}"), Secrets: []string{signing.NewSecret()}})
		o, kind := delivery.Classify(res.StatusCode, res.Err)
		require.Equal(t, delivery.Permanent, o)
		require.Equal(t, domain.KindSSRF, kind)
		require.Equal(t, domain.ReasonSSRF, delivery.Decide(o, 1, testSchedule, 0, 0, 0).DeadReason)
		require.Zero(t, r.requests(), "no request may reach the private address")
	})

	t.Run("validation errors are InvalidArgument with field details", func(t *testing.T) {
		ctx, _ := h.tenant("validation")
		_, err := h.ing.SendEvent(ctx, &pb.SendEventRequest{EventType: "not.in.catalogue"})
		require.True(t, errors.Is(err, domain.ErrInvalidArgument))
		st := server.ToStatus(err)
		require.Equal(t, codes.InvalidArgument, st.Code())
		require.NotEmpty(t, st.Details())
	})

	t.Run("stuck pending deliveries are republished by reconciliation", func(t *testing.T) {
		ctx, _ := h.tenant("reconcile")
		r := newReceiver(t)
		ep, secrets := h.endpoint(ctx, r.url())
		r.secrets = secrets
		// A delivery whose job was lost: inserted directly, no NATS message.
		ev := h.send(ctx, "")
		eventually(t, 10*time.Second, "first delivery", func() bool { return r.unique() == 1 })
		_, err := h.pool.Exec(ctx, "UPDATE deliveries SET status='pending', attempt_count=0, updated_at = now() - interval '1 hour' WHERE event_id = $1", rawID(domain.PrefixEvent, ev.Id))
		require.NoError(t, err)
		sched := &Scheduler{Pool: h.pool, Queue: h.q, Log: h.log, Cfg: schedulerCfgForTest()}
		sched.Maintain(ctx)
		eventually(t, 10*time.Second, "redelivered", func() bool { return r.requests() >= 2 })
		require.Len(t, h.deliveries(ctx, &pb.ListDeliveriesRequest{EndpointId: ep}), 1)
	})
}

func schedulerCfgForTest() config.Scheduler {
	return config.Scheduler{ReconcilePendingAt: 10 * time.Minute, AutoDisableAfter: 72 * time.Hour, AttemptRetention: 336 * time.Hour, EventRetention: 720 * time.Hour, OutboxRetention: 24 * time.Hour, IdempotencyWindow: 24 * time.Hour}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// Killing a worker that claimed a delivery must lose nothing: the next worker waits for
// the claim lock to expire, then delivers.
func TestWorkerKilledMidDelivery(t *testing.T) {
	h := newHarness(t, harnessOpts{noWorker: true})
	ctx, _ := h.tenant("killed")
	r := newReceiver(t)
	_, r.secrets = h.endpoint(ctx, r.url())
	ev := h.send(ctx, "")
	eventually(t, 10*time.Second, "delivery created", func() bool {
		return len(h.deliveries(ctx, &pb.ListDeliveriesRequest{EventId: ev.Id})) == 1
	})
	// The dead worker's claim: locked for 3 s, never completed, its message never acked.
	_, err := h.pool.Exec(ctx, "UPDATE deliveries SET locked_until = now() + interval '3 seconds' WHERE event_id = $1", rawID(domain.PrefixEvent, ev.Id))
	require.NoError(t, err)
	start := time.Now()
	h.goRun(h.worker.Run)
	eventually(t, 20*time.Second, "delivered after the lock expired", func() bool { return r.unique() == 1 })
	require.GreaterOrEqual(t, time.Since(start), 2*time.Second, "must not deliver while another worker holds the claim")
	time.Sleep(time.Second)
	require.Equal(t, 1, r.requests(), "exactly one HTTP request")
}
