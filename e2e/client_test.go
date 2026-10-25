//go:build e2e

// Package e2e runs the build plan's scenario tests against a running stack
// (Docker Compose or k3d) through the public REST API and the chaos customer.
//
//	make compose-up seed && make test-e2e        # Compose
//	make cluster-up deploy-local && make test-e2e # k3d
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

type env struct {
	control, ingest, chaos, chaosInternal, admin, target string
}

func loadEnv(t *testing.T) env {
	t.Helper()
	e := env{
		control:       getenv("HOOKLINE_CONTROL_URL", "http://localhost:8082"),
		ingest:        getenv("HOOKLINE_INGEST_URL", "http://localhost:8080"),
		chaos:         getenv("CHAOS_URL", "http://localhost:9000"),
		chaosInternal: getenv("CHAOS_INTERNAL_URL", "http://chaos-customer:8080"),
		admin:         os.Getenv("HOOKLINE_ADMIN_KEY"),
		target:        getenv("E2E_TARGET", "compose"),
	}
	if e.admin == "" {
		t.Skip("HOOKLINE_ADMIN_KEY not set; run via make test-e2e")
	}
	return e
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

var client = &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
	MaxIdleConnsPerHost: 64,
	DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		if h, p, err := net.SplitHostPort(addr); err == nil && strings.HasSuffix(h, ".localhost") {
			addr = net.JoinHostPort("127.0.0.1", p)
		}
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, addr)
	},
}}

// call does a JSON request and returns the status code; out may be nil.
func call(t *testing.T, method, url, key string, in, out any) int {
	t.Helper()
	var body io.Reader
	if in != nil {
		b, _ := json.Marshal(in)
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if out != nil && resp.StatusCode/100 == 2 {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("decode %s: %v: %s", url, err, raw)
		}
	}
	return resp.StatusCode
}

func must(t *testing.T, code int, want ...int) {
	t.Helper()
	for _, w := range want {
		if code == w {
			return
		}
	}
	t.Fatalf("HTTP %d, want %v", code, want)
}

// tenant is a fresh tenant with the event type "order.created".
type tenant struct {
	e   env
	t   *testing.T
	id  string
	key string
}

func newTenant(t *testing.T, e env) *tenant {
	t.Helper()
	name := fmt.Sprintf("e2e-%d-%d", time.Now().UnixNano(), rand.IntN(1000))
	var tr struct {
		Tenant struct{ ID string } `json:"tenant"`
	}
	must(t, call(t, "POST", e.control+"/v1/admin/tenants", e.admin, map[string]any{"name": name, "max_concurrency": 100, "ingest_rps_limit": 5000}, &tr), 200)
	var kr struct{ Key string }
	must(t, call(t, "POST", e.control+"/v1/admin/tenants/"+tr.Tenant.ID+"/keys", e.admin, map[string]any{}, &kr), 200)
	tn := &tenant{e: e, t: t, id: tr.Tenant.ID, key: kr.Key}
	must(t, call(t, "POST", e.control+"/v1/event-types", tn.key, map[string]string{"name": "order.created"}, nil), 200)
	return tn
}

// hook creates an endpoint on a unique chaos-customer path and registers its secrets there.
func (tn *tenant) hook(name string) (endpointID, hookName string) {
	tn.t.Helper()
	hookName = fmt.Sprintf("%s-%d", name, time.Now().UnixNano())
	var er struct {
		Endpoint struct{ ID string } `json:"endpoint"`
	}
	must(tn.t, call(tn.t, "POST", tn.e.control+"/v1/endpoints", tn.key, map[string]any{
		"url": tn.e.chaosInternal + "/hooks/" + hookName, "event_types": []string{"*"}, "rate_limit_per_sec": 2000,
	}, &er), 200)
	tn.registerSecrets(er.Endpoint.ID, hookName)
	return er.Endpoint.ID, hookName
}

func (tn *tenant) secrets(ep string) []string {
	var sr struct {
		Secrets []struct{ Secret string } `json:"secrets"`
	}
	must(tn.t, call(tn.t, "GET", tn.e.control+"/v1/endpoints/"+ep+"/secret", tn.key, nil, &sr), 200)
	out := make([]string, len(sr.Secrets))
	for i, s := range sr.Secrets {
		out[i] = s.Secret
	}
	return out
}

func (tn *tenant) registerSecrets(ep, hook string, only ...string) {
	s := only
	if len(s) == 0 {
		s = tn.secrets(ep)
	}
	must(tn.t, call(tn.t, "PUT", tn.e.chaos+"/config", "", map[string]any{"secrets": map[string][]string{hook: s}}, nil), 200)
}

func setBehavior(t *testing.T, e env, hook string, b map[string]any) {
	t.Helper()
	must(t, call(t, "PUT", e.chaos+"/config", "", map[string]any{"hooks": map[string]any{hook: b}}, nil), 200)
}

// send posts n events using the batch API and returns their IDs.
func (tn *tenant) send(n int, idem string) []string {
	tn.t.Helper()
	var ids []string
	for len(ids) < n {
		batch := min(100, n-len(ids))
		evs := make([]map[string]any, batch)
		for i := range evs {
			evs[i] = map[string]any{"event_type": "order.created", "payload": map[string]any{"n": len(ids) + i}}
			if idem != "" {
				evs[i]["idempotency_key"] = idem
			}
		}
		var res struct {
			Results []struct{ ID string } `json:"results"`
		}
		must(tn.t, call(tn.t, "POST", tn.e.ingest+"/v1/events:batch", tn.key, map[string]any{"events": evs}, &res), 200, 202)
		for _, r := range res.Results {
			ids = append(ids, r.ID)
		}
	}
	return ids
}

type hookStats struct {
	Received          int64 `json:"received"`
	Accepted          int64 `json:"accepted"`
	Unique            int64 `json:"unique"`
	Duplicates        int64 `json:"duplicates"`
	SignatureFailures int64 `json:"signature_failures"`
	Unverified        int64 `json:"unverified"`
	P99Ms             int64 `json:"p99_ms"`
}

func stats(t *testing.T, e env, hook string) hookStats {
	t.Helper()
	var s struct {
		Hooks map[string]hookStats `json:"hooks"`
	}
	must(t, call(t, "GET", e.chaos+"/stats", "", nil, &s), 200)
	return s.Hooks[hook]
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for: %s", timeout, what)
		}
		time.Sleep(time.Second)
	}
}

type delivery struct {
	ID           string `json:"id"`
	Status       string `json:"status"`
	AttemptCount int    `json:"attempt_count"`
	DeadReason   string `json:"dead_reason"`
}

func (tn *tenant) deliveries(query string) []delivery {
	var r struct {
		Deliveries []delivery `json:"deliveries"`
	}
	must(tn.t, call(tn.t, "GET", tn.e.control+"/v1/deliveries?"+query, tn.key, nil, &r), 200)
	return r.Deliveries
}

// restart kills a service's process(es) the way the target platform does.
func restart(t *testing.T, e env, service string) {
	t.Helper()
	var cmd *exec.Cmd
	switch {
	case e.target == "k8s" && service == "nats":
		cmd = exec.Command("kubectl", "-n", "infra", "delete", "pod", "-l", "app.kubernetes.io/name=nats", "--wait=false")
	case e.target == "k8s":
		cmd = exec.Command("kubectl", "-n", "hookline", "delete", "pod", "-l", "app.kubernetes.io/component="+service, "--wait=false")
	default:
		cmd = exec.Command("docker", "restart", "-t", "10", "hookline-"+service+"-1")
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("restart %s: %v: %s", service, err, out)
	}
}
