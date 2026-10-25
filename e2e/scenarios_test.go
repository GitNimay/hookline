//go:build e2e

package e2e

import (
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

// The stack must run the demo retry profile (1s, 2s, 5s, 10s, 20s: 6 attempts).
const demoAttempts = 6

func scale(def int) int {
	if v, err := strconv.Atoi(os.Getenv("E2E_EVENTS")); err == nil && v > 0 {
		return v
	}
	return def
}

// Scenario 1: N events to 3 healthy endpoints -> 3N unique deliveries, zero missing, all signed.
func TestHealthyFanOut(t *testing.T) {
	e := loadEnv(t)
	tn := newTenant(t, e)
	var hooks []string
	for _, n := range []string{"fan-a", "fan-b", "fan-c"} {
		_, h := tn.hook(n)
		hooks = append(hooks, h)
	}
	n := scale(10_000)
	tn.send(n, "")
	for _, h := range hooks {
		eventually(t, 10*time.Minute, "all deliveries at "+h, func() bool { return stats(t, e, h).Unique >= int64(n) })
		s := stats(t, e, h)
		if s.Unique != int64(n) || s.SignatureFailures != 0 || s.Unverified != 0 {
			t.Fatalf("%s: %+v", h, s)
		}
		t.Logf("%s: %d unique, %d duplicates, p99 %d ms", h, s.Unique, s.Duplicates, s.P99Ms)
	}
}

// Scenario 2: endpoint fails 100% for a while, then recovers -> every event delivered.
func TestOutageThenRecovery(t *testing.T) {
	e := loadEnv(t)
	tn := newTenant(t, e)
	_, h := tn.hook("outage")
	setBehavior(t, e, h, map[string]any{"outage": true})
	n := 50
	tn.send(n, "")
	outage := 30 * time.Second
	if v, err := time.ParseDuration(os.Getenv("E2E_OUTAGE")); err == nil {
		outage = v
	}
	time.Sleep(outage)
	setBehavior(t, e, h, map[string]any{})
	// Dead letters are possible only if the outage outlasts the whole retry schedule.
	eventually(t, 5*time.Minute, "backlog drained after recovery", func() bool {
		return stats(t, e, h).Unique >= int64(n) || len(tn.deliveries("status=dead")) > 0
	})
	if s := stats(t, e, h); s.Unique != int64(n) {
		t.Fatalf("delivered %d of %d (dead: %d)", s.Unique, n, len(tn.deliveries("status=dead")))
	}
}

// Scenario 3: always failing -> dead after the last attempt; replay after the fix -> delivered.
func TestDeadLetterAndReplay(t *testing.T) {
	e := loadEnv(t)
	tn := newTenant(t, e)
	ep, h := tn.hook("dead")
	setBehavior(t, e, h, map[string]any{"failure_rate": 1, "status_code": 500})
	tn.send(2, "")
	var dead []delivery
	eventually(t, 3*time.Minute, "dead letters", func() bool { dead = tn.deliveries("status=dead"); return len(dead) == 2 })
	if dead[0].AttemptCount != demoAttempts {
		t.Fatalf("attempts %d, want %d", dead[0].AttemptCount, demoAttempts)
	}
	setBehavior(t, e, h, map[string]any{})
	var r struct {
		Replayed string `json:"replayed"`
	}
	must(t, call(t, "POST", e.control+"/v1/endpoints/"+ep+":replay", tn.key, map[string]any{}, &r), 200)
	if r.Replayed != "2" {
		t.Fatalf("replayed %q", r.Replayed)
	}
	eventually(t, time.Minute, "replayed deliveries arrive", func() bool { return stats(t, e, h).Unique == 2 })
}

// Scenario 6: the same idempotency key twice -> one event, one delivery per endpoint.
func TestIdempotency(t *testing.T) {
	e := loadEnv(t)
	tn := newTenant(t, e)
	_, h := tn.hook("idem")
	body := map[string]any{"event_type": "order.created", "payload": map[string]any{"id": 1}, "idempotency_key": "order-1"}
	var a, b struct{ ID string }
	must(t, call(t, "POST", e.ingest+"/v1/events", tn.key, body, &a), 202)
	must(t, call(t, "POST", e.ingest+"/v1/events", tn.key, body, &b), 200)
	if a.ID != b.ID {
		t.Fatalf("ids differ: %s %s", a.ID, b.ID)
	}
	eventually(t, time.Minute, "delivered", func() bool { return stats(t, e, h).Unique == 1 })
	time.Sleep(3 * time.Second)
	if s := stats(t, e, h); s.Received != 1 {
		t.Fatalf("received %d requests, want 1", s.Received)
	}
}

// Scenario 8: 429 with Retry-After: 30 -> the next attempt waits at least 30 s.
func TestRetryAfter(t *testing.T) {
	e := loadEnv(t)
	tn := newTenant(t, e)
	_, h := tn.hook("retry-after")
	setBehavior(t, e, h, map[string]any{"failure_rate": 1, "status_code": 429, "retry_after": "30"})
	tn.send(1, "")
	var d []delivery
	eventually(t, time.Minute, "first attempt", func() bool { d = tn.deliveries(""); return len(d) == 1 && d[0].AttemptCount == 1 })
	first := time.Now()
	setBehavior(t, e, h, map[string]any{})
	eventually(t, 2*time.Minute, "delivered", func() bool { return stats(t, e, h).Unique == 1 })
	if waited := time.Since(first); waited < 25*time.Second {
		t.Fatalf("second attempt after %v; Retry-After was 30 s", waited)
	}
}

// Scenario 9: 410 Gone -> endpoint disabled.
func TestGoneDisablesEndpoint(t *testing.T) {
	e := loadEnv(t)
	tn := newTenant(t, e)
	ep, h := tn.hook("gone")
	setBehavior(t, e, h, map[string]any{"failure_rate": 1, "status_code": 410})
	tn.send(1, "")
	eventually(t, time.Minute, "endpoint disabled", func() bool {
		var r struct {
			Endpoint struct{ Status, DisabledReason string } `json:"endpoint"`
		}
		call(t, "GET", e.control+"/v1/endpoints/"+ep, tn.key, nil, &r)
		return r.Endpoint.Status == "disabled"
	})
}

// Scenario 10: repeated failures open the breaker; recovery closes it and drains the backlog.
func TestCircuitBreaker(t *testing.T) {
	e := loadEnv(t)
	tn := newTenant(t, e)
	ep, h := tn.hook("breaker")
	setBehavior(t, e, h, map[string]any{"failure_rate": 1, "status_code": 503})
	tn.send(20, "")
	state := func() string {
		var r struct {
			Endpoint struct {
				CircuitState string `json:"circuit_state"`
			} `json:"endpoint"`
		}
		call(t, "GET", e.control+"/v1/endpoints/"+ep, tn.key, nil, &r)
		return r.Endpoint.CircuitState
	}
	eventually(t, time.Minute, "breaker open", func() bool { return state() == "open" })
	setBehavior(t, e, h, map[string]any{})
	eventually(t, 3*time.Minute, "breaker closed and backlog delivered", func() bool {
		return state() == "closed" && stats(t, e, h).Unique == 20
	})
}

// Scenario 11: endpoints pointing at private or metadata addresses are refused.
func TestSSRFBlocked(t *testing.T) {
	e := loadEnv(t)
	tn := newTenant(t, e)
	for _, u := range []string{"http://127.0.0.1:8080/x", "http://169.254.169.254/latest/meta-data", "http://[::1]/", "http://10.0.0.5/", "http://localhost/"} {
		code := call(t, "POST", e.control+"/v1/endpoints", tn.key, map[string]any{"url": u, "event_types": []string{"*"}}, nil)
		if code != 400 {
			t.Errorf("%s: HTTP %d, want 400", u, code)
		}
	}
}

// Scenario 12: during rotation, receivers verifying with the old or the new secret both succeed.
func TestSecretRotation(t *testing.T) {
	e := loadEnv(t)
	tn := newTenant(t, e)
	ep, h := tn.hook("rotate")
	old := tn.secrets(ep)
	var rr struct {
		Secrets []struct{ Secret string } `json:"secrets"`
	}
	must(t, call(t, "POST", e.control+"/v1/endpoints/"+ep+"/secret:rotate", tn.key, map[string]any{}, &rr), 200)
	if len(rr.Secrets) != 2 {
		t.Fatalf("%d active secrets after rotation, want 2", len(rr.Secrets))
	}
	// The worker caches endpoints for up to 5 s.
	time.Sleep(6 * time.Second)
	tn.registerSecrets(ep, h, old...) // receiver still on the old secret
	tn.send(1, "")
	eventually(t, time.Minute, "verified with old secret", func() bool { return stats(t, e, h).Unique == 1 })
	tn.registerSecrets(ep, h, rr.Secrets[0].Secret) // receiver moved to the new secret
	tn.send(1, "")
	eventually(t, time.Minute, "verified with new secret", func() bool { return stats(t, e, h).Unique == 2 })
	if s := stats(t, e, h); s.SignatureFailures != 0 {
		t.Fatalf("signature failures: %d", s.SignatureFailures)
	}
}

// Tenant isolation over the public API: another tenant's IDs are 404, never 403.
func TestTenantIsolation(t *testing.T) {
	e := loadEnv(t)
	a, b := newTenant(t, e), newTenant(t, e)
	ep, h := a.hook("iso")
	ids := a.send(1, "")
	eventually(t, time.Minute, "delivered", func() bool { return stats(t, e, h).Unique == 1 })
	dlv := a.deliveries("")[0].ID
	for _, c := range []struct{ method, path string }{
		{"GET", "/v1/endpoints/" + ep},
		{"GET", "/v1/endpoints/" + ep + "/secret"},
		{"POST", "/v1/endpoints/" + ep + "/secret:rotate"},
		{"DELETE", "/v1/endpoints/" + ep},
		{"POST", "/v1/endpoints/" + ep + ":replay"},
		{"GET", "/v1/deliveries/" + dlv},
		{"POST", "/v1/deliveries/" + dlv + ":replay"},
	} {
		if code := call(t, c.method, e.control+c.path, b.key, map[string]any{}, nil); code != 404 {
			t.Errorf("%s %s as other tenant: HTTP %d, want 404", c.method, c.path, code)
		}
	}
	if code := call(t, "GET", e.ingest+"/v1/events/"+ids[0], b.key, nil, nil); code != 404 {
		t.Errorf("GET event as other tenant: HTTP %d", code)
	}
	if code := call(t, "GET", e.control+"/v1/admin/tenants", a.key, nil, nil); code != 403 {
		t.Errorf("tenant key on admin API: HTTP %d, want 403", code)
	}
	if code := call(t, "GET", e.control+"/v1/endpoints", "hk_live_wrong", nil, nil); code != 401 {
		t.Errorf("bad key: HTTP %d, want 401", code)
	}
}

// Scenarios 4 and 5: kill a worker, then restart NATS, while events flow -> zero loss,
// duplicates under 0.1%.
func TestRestartsUnderLoad(t *testing.T) {
	if os.Getenv("E2E_DESTRUCTIVE") == "" {
		t.Skip("set E2E_DESTRUCTIVE=1 to restart services")
	}
	e := loadEnv(t)
	tn := newTenant(t, e)
	_, h := tn.hook("restarts")
	n := scale(5_000)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for sent := 0; sent < n; sent += 100 {
			tn.send(100, "")
			time.Sleep(50 * time.Millisecond)
		}
	}()
	time.Sleep(3 * time.Second)
	restart(t, e, "worker")
	time.Sleep(5 * time.Second)
	restart(t, e, "nats")
	wg.Wait()
	eventually(t, 10*time.Minute, "all delivered after restarts", func() bool { return stats(t, e, h).Unique >= int64(n) })
	s := stats(t, e, h)
	t.Logf("unique=%d duplicates=%d (%.3f%%)", s.Unique, s.Duplicates, 100*float64(s.Duplicates)/float64(s.Unique))
	if s.Unique != int64(n) {
		t.Fatalf("lost %d", int64(n)-s.Unique)
	}
	if float64(s.Duplicates)/float64(s.Unique) > 0.001 {
		t.Fatalf("duplicates %d over 0.1%%", s.Duplicates)
	}
}
