package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// send generates realistic events at a fixed rate per tenant.
func send(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	tenantsFlag := fs.String("tenants", "acme", "comma-separated seeded tenant names")
	rate := fs.Int("rate", 50, "events per second per tenant")
	dur := fs.Duration("duration", 10*time.Second, "how long to send")
	count := fs.Int("count", 0, "stop after this many events per tenant (0 = use duration)")
	batch := fs.Int("batch", 1, "events per request (1 = single endpoint, up to 100 = batch)")
	idem := fs.Bool("idempotent", false, "send every event twice with the same idempotency key")
	workers := fs.Int("workers", 32, "concurrent HTTP requests")
	_ = fs.Parse(args)

	s, err := loadSeed()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()
	if *count == 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *dur)
		defer cancel()
	}

	type req struct{ key, body string }
	jobs := make(chan req, *workers*2)
	var sent, accepted, dup, failed atomic.Int64
	var mu sync.Mutex
	var lat []time.Duration
	tr := localhostTransport()
	tr.MaxIdleConnsPerHost = *workers
	client := &http.Client{Timeout: 10 * time.Second, Transport: tr}
	url := ingestURL() + "/v1/events"
	if *batch > 1 {
		url += ":batch"
	}

	var wg sync.WaitGroup
	for range *workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				start := time.Now()
				rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
				hr, _ := http.NewRequestWithContext(rctx, http.MethodPost, url, strings.NewReader(j.body))
				hr.Header.Set("Authorization", "Bearer "+j.key)
				hr.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(hr)
				d := time.Since(start)
				switch {
				case err != nil:
					failed.Add(1)
				case resp.StatusCode == http.StatusAccepted:
					accepted.Add(1)
				case resp.StatusCode == http.StatusOK:
					dup.Add(1)
				default:
					failed.Add(1)
					if failed.Load() <= 5 {
						b, _ := io.ReadAll(resp.Body)
						fmt.Fprintf(os.Stderr, "HTTP %d: %s\n", resp.StatusCode, bytes.TrimSpace(b))
					}
				}
				if resp != nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
				cancel()
				mu.Lock()
				lat = append(lat, d)
				mu.Unlock()
			}
		}()
	}

	names := strings.Split(*tenantsFlag, ",")
	var pwg sync.WaitGroup
	for _, name := range names {
		t, ok := s.Tenants[strings.TrimSpace(name)]
		if !ok {
			close(jobs)
			return fmt.Errorf("tenant %q not in seed file", name)
		}
		pwg.Add(1)
		go func() {
			defer pwg.Done()
			interval := time.Second * time.Duration(*batch) / time.Duration(max(1, *rate))
			tick := time.NewTicker(max(interval, time.Microsecond))
			defer tick.Stop()
			n := 0
			for *count == 0 || n < *count {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
				evs := make([]map[string]any, 0, *batch)
				for range *batch {
					evs = append(evs, randomEvent(n, *idem))
					n++
				}
				var body []byte
				if *batch > 1 {
					body, _ = json.Marshal(map[string]any{"events": evs})
				} else {
					body, _ = json.Marshal(evs[0])
				}
				copies := 1
				if *idem {
					copies = 2
				}
				for range copies {
					select {
					case jobs <- req{t.Key, string(body)}:
						sent.Add(int64(len(evs)))
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}
	pwg.Wait()
	close(jobs)
	wg.Wait()

	slices.Sort(lat)
	pct := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		return lat[min(len(lat)-1, int(float64(len(lat))*p))]
	}
	fmt.Printf("events_sent=%d requests_accepted=%d requests_duplicate=%d requests_failed=%d p50=%v p99=%v\n",
		sent.Load(), accepted.Load(), dup.Load(), failed.Load(), pct(0.5), pct(0.99))
	if failed.Load() > 0 {
		return fmt.Errorf("%d requests failed", failed.Load())
	}
	return nil
}

var demoNames = []string{"Ada", "Linus", "Grace", "Ken", "Barbara", "Dennis", "Margaret"}

func randomEvent(n int, idem bool) map[string]any {
	typ := demoTypes[rand.IntN(len(demoTypes))] //nolint:gosec // demo data
	ev := map[string]any{
		"event_type": typ,
		"payload": map[string]any{
			"id":       fmt.Sprintf("obj_%d_%d", time.Now().UnixNano(), n),
			"customer": demoNames[rand.IntN(len(demoNames))], //nolint:gosec // demo data
			"amount":   rand.IntN(50000),                     //nolint:gosec // demo data
			"currency": "EUR",
		},
	}
	if idem {
		ev["idempotency_key"] = fmt.Sprintf("idem-%d-%d", time.Now().UnixNano(), n)
	}
	return ev
}
