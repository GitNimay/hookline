// Command chaos-customer is a webhook receiver with configurable failure modes.
// It verifies signatures with the official Standard Webhooks library and records every
// webhook-id to measure loss and duplicates.
package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"time"

	standardwebhooks "github.com/standard-webhooks/standard-webhooks/libraries/go"

	"github.com/GitNimay/hookline/internal/app"
	"github.com/GitNimay/hookline/internal/config"
	"github.com/GitNimay/hookline/internal/server"
)

// Behavior is the failure mode of one hook path.
type Behavior struct {
	FailureRate  float64 `json:"failure_rate"`   // 0..1, answer StatusCode
	StatusCode   int     `json:"status_code"`    // default 500
	LatencyMinMs int     `json:"latency_ms_min"` // uniform latency
	LatencyMaxMs int     `json:"latency_ms_max"`
	TimeoutRate  float64 `json:"timeout_rate"` // 0..1, hang for 30 s
	RetryAfter   string  `json:"retry_after"`  // sent with failures
	Outage       bool    `json:"outage"`       // every request fails with 503
}

type hookStats struct {
	Received          int64 `json:"received"`
	Accepted          int64 `json:"accepted"`
	Failed            int64 `json:"failed"`
	Unique            int64 `json:"unique"`
	Duplicates        int64 `json:"duplicates"`
	SignatureFailures int64 `json:"signature_failures"`
	Unverified        int64 `json:"unverified"`
	P50Ms             int64 `json:"p50_ms"`
	P99Ms             int64 `json:"p99_ms"`
	MaxMs             int64 `json:"max_ms"`

	seen      map[string]bool
	latencies []int64
}

type receiver struct {
	mu        sync.Mutex
	behaviors map[string]Behavior
	secrets   map[string][]string
	stats     map[string]*hookStats
	alerts    int64
	log       *slog.Logger
}

func newReceiver(log *slog.Logger) *receiver {
	return &receiver{behaviors: map[string]Behavior{}, secrets: map[string][]string{}, stats: map[string]*hookStats{}, log: log}
}

func (r *receiver) hook(w http.ResponseWriter, req *http.Request) {
	name := req.PathValue("name")
	body, err := io.ReadAll(io.LimitReader(req.Body, 1<<20))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	b := r.behaviors[name]
	secrets := slices.Clone(r.secrets[name])
	st := r.statsFor(name)
	st.Received++
	r.mu.Unlock()

	if b.LatencyMaxMs > 0 {
		d := b.LatencyMinMs + rand.IntN(max(1, b.LatencyMaxMs-b.LatencyMinMs+1)) //nolint:gosec // test traffic
		time.Sleep(time.Duration(d) * time.Millisecond)
	}
	if b.TimeoutRate > 0 && rand.Float64() < b.TimeoutRate { //nolint:gosec // test traffic
		select {
		case <-time.After(30 * time.Second):
		case <-req.Context().Done():
		}
		return
	}
	if b.Outage || (b.FailureRate > 0 && rand.Float64() < b.FailureRate) { //nolint:gosec // test traffic
		code := b.StatusCode
		if code == 0 {
			code = http.StatusInternalServerError
		}
		if b.Outage {
			code = http.StatusServiceUnavailable
		}
		if b.RetryAfter != "" {
			w.Header().Set("Retry-After", b.RetryAfter)
		}
		r.mu.Lock()
		st.Failed++
		r.mu.Unlock()
		w.WriteHeader(code)
		_, _ = w.Write([]byte(`{"error":"chaos"}`))
		return
	}

	verified, sigOK := verify(secrets, body, req.Header)
	id := req.Header.Get("webhook-id")
	r.mu.Lock()
	defer r.mu.Unlock()
	if !sigOK {
		st.SignatureFailures++
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	}
	if !verified {
		st.Unverified++
	}
	st.Accepted++
	if st.seen[id] {
		st.Duplicates++
	} else {
		st.seen[id] = true
		st.Unique++
		var env struct {
			Timestamp time.Time `json:"timestamp"`
		}
		if json.Unmarshal(body, &env) == nil && !env.Timestamp.IsZero() {
			st.latencies = append(st.latencies, time.Since(env.Timestamp).Milliseconds())
		}
	}
	w.WriteHeader(http.StatusOK)
}

// verify returns (checked, ok). Without registered secrets nothing is checked.
func verify(secrets []string, body []byte, h http.Header) (bool, bool) {
	if len(secrets) == 0 {
		return false, true
	}
	for _, s := range secrets {
		wh, err := standardwebhooks.NewWebhook(s)
		if err == nil && wh.Verify(body, h) == nil {
			return true, true
		}
	}
	return true, false
}

func (r *receiver) statsFor(name string) *hookStats {
	st, ok := r.stats[name]
	if !ok {
		st = &hookStats{seen: map[string]bool{}}
		r.stats[name] = st
	}
	return st
}

type configBody struct {
	Hooks   map[string]Behavior `json:"hooks"`
	Secrets map[string][]string `json:"secrets"`
}

func (r *receiver) putConfig(w http.ResponseWriter, req *http.Request) {
	var c configBody
	if err := json.NewDecoder(io.LimitReader(req.Body, 1<<20)).Decode(&c); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.mu.Lock()
	for k, v := range c.Hooks {
		r.behaviors[k] = v
	}
	for k, v := range c.Secrets {
		r.secrets[k] = v
	}
	r.mu.Unlock()
	r.log.Info("config updated", "hooks", len(c.Hooks), "secrets", len(c.Secrets))
	r.getConfig(w, req)
}

func (r *receiver) getConfig(w http.ResponseWriter, _ *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	writeJSON(w, map[string]any{"hooks": r.behaviors, "secret_names": len(r.secrets)})
}

func (r *receiver) getStats(w http.ResponseWriter, _ *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := hookStats{}
	var all []int64
	for _, st := range r.stats {
		st.P50Ms, st.P99Ms, st.MaxMs = percentiles(st.latencies)
		all = append(all, st.latencies...)
		total.Received += st.Received
		total.Accepted += st.Accepted
		total.Failed += st.Failed
		total.Unique += st.Unique
		total.Duplicates += st.Duplicates
		total.SignatureFailures += st.SignatureFailures
		total.Unverified += st.Unverified
	}
	total.P50Ms, total.P99Ms, total.MaxMs = percentiles(all)
	writeJSON(w, map[string]any{"total": total, "hooks": r.stats, "alerts_received": r.alerts})
}

func percentiles(v []int64) (p50, p99, mx int64) {
	if len(v) == 0 {
		return 0, 0, 0
	}
	s := slices.Clone(v)
	slices.Sort(s)
	at := func(p float64) int64 { return s[min(len(s)-1, int(float64(len(s))*p))] }
	return at(0.50), at(0.99), s[len(s)-1]
}

func (r *receiver) reset(w http.ResponseWriter, _ *http.Request) {
	r.mu.Lock()
	r.stats = map[string]*hookStats{}
	r.alerts = 0
	r.mu.Unlock()
	writeJSON(w, map[string]string{"status": "reset"})
}

func (r *receiver) alert(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(req.Body, 1<<20))
	r.mu.Lock()
	r.alerts++
	r.mu.Unlock()
	r.log.Warn("alert received", "body", string(body))
	w.WriteHeader(http.StatusOK)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func run(ctx context.Context, cfg config.Common, log *slog.Logger) error {
	r := newReceiver(log)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /hooks/{name}", r.hook)
	mux.HandleFunc("PUT /config", r.putConfig)
	mux.HandleFunc("GET /config", r.getConfig)
	mux.HandleFunc("GET /stats", r.getStats)
	mux.HandleFunc("POST /reset", r.reset)
	mux.HandleFunc("POST /alerts", r.alert)
	return server.Run(ctx, server.Options{
		Name: "chaos-customer", Log: log, AdminAddr: cfg.AdminAddr, HTTPAddr: cfg.HTTPAddr, HTTP: mux,
		DrainDelay: cfg.DrainDelay, StopTimeout: cfg.ShutdownTimeout,
	})
}

func main() { app.Main("chaos-customer", run) }
