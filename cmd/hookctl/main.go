// Command hookctl administers Hookline and generates demo traffic.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver for goose
	"github.com/pressly/goose/v3"

	"github.com/GitNimay/hookline/migrations"
)

const usage = `hookctl - Hookline admin and demo CLI

Usage:
  hookctl migrate [up|down|status]        run database migrations (DATABASE_URL)
  hookctl seed                            create demo tenants, event types and endpoints
  hookctl send [flags]                    generate events (see hookctl send -h)
  hookctl tenants list|create NAME
  hookctl keys create TENANT_ID
  hookctl endpoints list --tenant NAME
  hookctl deliveries --tenant NAME [--status dead] [--endpoint ep_...]
  hookctl replay --tenant NAME (--delivery dlv_... | --endpoint ep_... [--since RFC3339])
  hookctl chaos set NAME key=value...     configure a chaos-customer hook (failure_rate=0.4 ...)
  hookctl chaos stats|reset
  hookctl verify --expect N               check chaos-customer received N unique deliveries, no loss

Environment:
  HOOKLINE_CONTROL_URL  control REST URL (default http://localhost:8082)
  HOOKLINE_INGEST_URL   ingest REST URL  (default http://localhost:8080)
  HOOKLINE_ADMIN_KEY    admin API key
  CHAOS_URL             chaos-customer URL as seen by hookctl (default http://localhost:9000)
  CHAOS_INTERNAL_URL    chaos-customer URL as seen by workers (default http://chaos-customer:8080)
  HOOKLINE_SEED_FILE    where seed keys are stored (default .hookline/seed.json)
`

func main() {
	if len(os.Args) < 2 {
		fmt.Print(usage)
		os.Exit(2)
	}
	ctx := context.Background()
	var err error
	args := os.Args[2:]
	switch os.Args[1] {
	case "migrate":
		err = migrate(args)
	case "seed":
		err = seed(ctx)
	case "send":
		err = send(ctx, args)
	case "tenants":
		err = tenants(args)
	case "keys":
		err = keys(args)
	case "endpoints":
		err = endpoints(args)
	case "deliveries":
		err = deliveries(args)
	case "replay":
		err = replay(args)
	case "chaos":
		err = chaos(args)
	case "verify":
		err = verify(args)
	default:
		fmt.Print(usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func controlURL() string {
	return strings.TrimRight(env("HOOKLINE_CONTROL_URL", "http://localhost:8082"), "/")
}
func ingestURL() string {
	return strings.TrimRight(env("HOOKLINE_INGEST_URL", "http://localhost:8080"), "/")
}
func chaosURL() string { return strings.TrimRight(env("CHAOS_URL", "http://localhost:9000"), "/") }
func seedFile() string { return env("HOOKLINE_SEED_FILE", ".hookline/seed.json") }

var httpClient = &http.Client{Timeout: 30 * time.Second}

// apiError is a non-2xx REST response.
type apiError struct {
	Status int
	Body   string
}

func (e *apiError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.Status, e.Body) }

// call does one JSON request. out may be nil.
func call(method, url, key string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if resp.StatusCode/100 != 2 {
		return &apiError{Status: resp.StatusCode, Body: strings.TrimSpace(string(raw))}
	}
	if out != nil && len(raw) > 0 {
		return json.Unmarshal(raw, out)
	}
	return nil
}

func isStatus(err error, code int) bool {
	var ae *apiError
	return errors.As(err, &ae) && ae.Status == code
}

func printJSON(v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}

func migrate(args []string) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}
	cmd := "up"
	if len(args) > 0 {
		cmd = args[0]
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	// Wait for Postgres so the migration container can start alongside it.
	for i := 0; ; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = db.PingContext(ctx)
		cancel()
		if err == nil {
			break
		}
		if i == 30 {
			return fmt.Errorf("postgres not reachable: %w", err)
		}
		time.Sleep(time.Second)
	}
	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	switch cmd {
	case "up":
		return goose.Up(db, ".")
	case "down":
		return goose.Down(db, ".")
	case "status":
		return goose.Status(db, ".")
	}
	return fmt.Errorf("unknown migrate command %q", cmd)
}

// --- seed ---

// Seed is what `hookctl seed` stores for later commands.
type Seed struct {
	Tenants map[string]SeedTenant `json:"tenants"`
}

// SeedTenant holds one demo tenant.
type SeedTenant struct {
	ID        string            `json:"id"`
	Key       string            `json:"key"`
	Endpoints map[string]string `json:"endpoints"` // hook name -> endpoint id
}

var demoTypes = []string{"order.created", "order.paid", "user.signed_up"}

var demoTenants = map[string][]string{
	"acme":    {"acme-a", "acme-b", "acme-c"},
	"globex":  {"globex-a"},
	"initech": {"initech-a"},
}

func seed(_ context.Context) error {
	admin := os.Getenv("HOOKLINE_ADMIN_KEY")
	if admin == "" {
		return errors.New("HOOKLINE_ADMIN_KEY is required")
	}
	internal := strings.TrimRight(env("CHAOS_INTERNAL_URL", "http://chaos-customer:8080"), "/")
	s := Seed{Tenants: map[string]SeedTenant{}}
	secrets := map[string][]string{}
	for name, hooks := range demoTenants {
		id, err := ensureTenant(admin, name)
		if err != nil {
			return err
		}
		var k struct {
			Key string `json:"key"`
		}
		if err := call("POST", controlURL()+"/v1/admin/tenants/"+id+"/keys", admin, map[string]any{}, &k); err != nil {
			return fmt.Errorf("create key: %w", err)
		}
		for _, t := range demoTypes {
			err := call("POST", controlURL()+"/v1/event-types", k.Key, map[string]string{"name": t, "description": "demo " + t}, nil)
			if err != nil && !isStatus(err, 409) {
				return fmt.Errorf("event type: %w", err)
			}
		}
		st := SeedTenant{ID: id, Key: k.Key, Endpoints: map[string]string{}}
		existing, err := listEndpoints(k.Key)
		if err != nil {
			return err
		}
		for _, h := range hooks {
			url := internal + "/hooks/" + h
			epID := existing[url]
			if epID == "" {
				var ep struct {
					Endpoint struct {
						ID string `json:"id"`
					} `json:"endpoint"`
				}
				if err := call("POST", controlURL()+"/v1/endpoints", k.Key, map[string]any{"url": url, "event_types": []string{"*"}, "description": "chaos " + h, "rate_limit_per_sec": 1000}, &ep); err != nil {
					return fmt.Errorf("create endpoint: %w", err)
				}
				epID = ep.Endpoint.ID
			}
			st.Endpoints[h] = epID
			var sec struct {
				Secrets []struct {
					Secret string `json:"secret"`
				} `json:"secrets"`
			}
			if err := call("GET", controlURL()+"/v1/endpoints/"+epID+"/secret", k.Key, nil, &sec); err != nil {
				return fmt.Errorf("get secret: %w", err)
			}
			for _, x := range sec.Secrets {
				secrets[h] = append(secrets[h], x.Secret)
			}
		}
		s.Tenants[name] = st
	}
	if err := call("PUT", chaosURL()+"/config", "", map[string]any{"secrets": secrets}, nil); err != nil {
		fmt.Fprintln(os.Stderr, "warning: could not register secrets with chaos-customer:", err)
	}
	if err := os.MkdirAll(filepath.Dir(seedFile()), 0o700); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(s, "", "  ")
	if err := os.WriteFile(seedFile(), b, 0o600); err != nil {
		return err
	}
	fmt.Println("Seeded tenants (keys saved to " + seedFile() + "):")
	for name, t := range s.Tenants {
		fmt.Printf("  %-8s %s  key=%s\n", name, t.ID, t.Key)
	}
	return nil
}

func ensureTenant(admin, name string) (string, error) {
	var t struct {
		Tenant struct {
			ID string `json:"id"`
		} `json:"tenant"`
	}
	err := call("POST", controlURL()+"/v1/admin/tenants", admin, map[string]any{"name": name}, &t)
	if err == nil {
		return t.Tenant.ID, nil
	}
	if !isStatus(err, 409) {
		return "", fmt.Errorf("create tenant: %w", err)
	}
	var list struct {
		Tenants []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"tenants"`
	}
	if err := call("GET", controlURL()+"/v1/admin/tenants?page_size=100", admin, nil, &list); err != nil {
		return "", err
	}
	for _, x := range list.Tenants {
		if x.Name == name {
			return x.ID, nil
		}
	}
	return "", fmt.Errorf("tenant %s exists but was not listed", name)
}

func listEndpoints(key string) (map[string]string, error) {
	var list struct {
		Endpoints []struct {
			ID  string `json:"id"`
			URL string `json:"url"`
		} `json:"endpoints"`
	}
	if err := call("GET", controlURL()+"/v1/endpoints?page_size=100", key, nil, &list); err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, e := range list.Endpoints {
		out[e.URL] = e.ID
	}
	return out, nil
}

func loadSeed() (Seed, error) {
	var s Seed
	b, err := os.ReadFile(seedFile())
	if err != nil {
		return s, fmt.Errorf("read %s (run hookctl seed first): %w", seedFile(), err)
	}
	return s, json.Unmarshal(b, &s)
}

func tenantKey(name string) (SeedTenant, error) {
	s, err := loadSeed()
	if err != nil {
		return SeedTenant{}, err
	}
	t, ok := s.Tenants[name]
	if !ok {
		return t, fmt.Errorf("tenant %q not in seed file", name)
	}
	return t, nil
}

// --- admin commands ---

func tenants(args []string) error {
	admin := os.Getenv("HOOKLINE_ADMIN_KEY")
	if len(args) == 0 {
		return errors.New("tenants list|create NAME")
	}
	var out any
	switch args[0] {
	case "list":
		if err := call("GET", controlURL()+"/v1/admin/tenants", admin, nil, &out); err != nil {
			return err
		}
	case "create":
		if len(args) < 2 {
			return errors.New("tenants create NAME")
		}
		if err := call("POST", controlURL()+"/v1/admin/tenants", admin, map[string]string{"name": args[1]}, &out); err != nil {
			return err
		}
	default:
		return errors.New("tenants list|create NAME")
	}
	printJSON(out)
	return nil
}

func keys(args []string) error {
	if len(args) < 2 || args[0] != "create" {
		return errors.New("keys create TENANT_ID")
	}
	var out any
	if err := call("POST", controlURL()+"/v1/admin/tenants/"+args[1]+"/keys", os.Getenv("HOOKLINE_ADMIN_KEY"), map[string]any{}, &out); err != nil {
		return err
	}
	printJSON(out)
	return nil
}

func endpoints(args []string) error {
	fs := flag.NewFlagSet("endpoints", flag.ExitOnError)
	tenant := fs.String("tenant", "acme", "seeded tenant name")
	if len(args) == 0 || args[0] != "list" {
		return errors.New("endpoints list --tenant NAME")
	}
	_ = fs.Parse(args[1:])
	t, err := tenantKey(*tenant)
	if err != nil {
		return err
	}
	var out any
	if err := call("GET", controlURL()+"/v1/endpoints", t.Key, nil, &out); err != nil {
		return err
	}
	printJSON(out)
	return nil
}

func deliveries(args []string) error {
	fs := flag.NewFlagSet("deliveries", flag.ExitOnError)
	tenant := fs.String("tenant", "acme", "seeded tenant name")
	st := fs.String("status", "", "pending|retrying|succeeded|dead")
	ep := fs.String("endpoint", "", "endpoint id")
	_ = fs.Parse(args)
	t, err := tenantKey(*tenant)
	if err != nil {
		return err
	}
	var out any
	if err := call("GET", controlURL()+"/v1/deliveries?status="+*st+"&endpoint_id="+*ep, t.Key, nil, &out); err != nil {
		return err
	}
	printJSON(out)
	return nil
}

func replay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	tenant := fs.String("tenant", "acme", "seeded tenant name")
	dlv := fs.String("delivery", "", "delivery id")
	ep := fs.String("endpoint", "", "endpoint id (replays all its dead deliveries)")
	since := fs.String("since", "", "with --endpoint: replay every delivery since this RFC 3339 time")
	_ = fs.Parse(args)
	t, err := tenantKey(*tenant)
	if err != nil {
		return err
	}
	var out any
	switch {
	case *dlv != "":
		err = call("POST", controlURL()+"/v1/deliveries/"+*dlv+":replay", t.Key, map[string]any{}, &out)
	case *ep != "":
		body := map[string]any{}
		if *since != "" {
			body["since"] = *since
		}
		err = call("POST", controlURL()+"/v1/endpoints/"+*ep+":replay", t.Key, body, &out)
	default:
		return errors.New("--delivery or --endpoint is required")
	}
	if err != nil {
		return err
	}
	printJSON(out)
	return nil
}

func chaos(args []string) error {
	if len(args) == 0 {
		return errors.New("chaos set|stats|reset")
	}
	var out any
	switch args[0] {
	case "stats":
		if err := call("GET", chaosURL()+"/stats", "", nil, &out); err != nil {
			return err
		}
	case "reset":
		if err := call("POST", chaosURL()+"/reset", "", nil, &out); err != nil {
			return err
		}
	case "set":
		if len(args) < 2 {
			return errors.New("usage: chaos set NAME key=value")
		}
		b := map[string]any{}
		for _, kv := range args[2:] {
			k, v, _ := strings.Cut(kv, "=")
			var parsed any
			if json.Unmarshal([]byte(v), &parsed) != nil {
				parsed = v
			}
			b[k] = parsed
		}
		if err := call("PUT", chaosURL()+"/config", "", map[string]any{"hooks": map[string]any{args[1]: b}}, &out); err != nil {
			return err
		}
	default:
		return errors.New("chaos set|stats|reset")
	}
	printJSON(out)
	return nil
}

// ChaosStats is the part of /stats that verify reads.
type ChaosStats struct {
	Total struct {
		Unique            int64 `json:"unique"`
		Duplicates        int64 `json:"duplicates"`
		SignatureFailures int64 `json:"signature_failures"`
		P99Ms             int64 `json:"p99_ms"`
	} `json:"total"`
}

func verify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	expect := fs.Int64("expect", 0, "unique deliveries expected")
	wait := fs.Duration("wait", 2*time.Minute, "how long to wait for them")
	_ = fs.Parse(args)
	deadline := time.Now().Add(*wait)
	var st ChaosStats
	for {
		if err := call("GET", chaosURL()+"/stats", "", nil, &st); err != nil {
			return err
		}
		if st.Total.Unique >= *expect || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Second)
	}
	dupPct := 0.0
	if st.Total.Unique > 0 {
		dupPct = 100 * float64(st.Total.Duplicates) / float64(st.Total.Unique)
	}
	fmt.Printf("unique=%d expected=%d missing=%d duplicates=%d (%.3f%%) signature_failures=%d p99=%dms\n",
		st.Total.Unique, *expect, max(0, *expect-st.Total.Unique), st.Total.Duplicates, dupPct, st.Total.SignatureFailures, st.Total.P99Ms)
	if st.Total.Unique < *expect {
		return errors.New("FAIL: deliveries missing")
	}
	if st.Total.SignatureFailures > 0 {
		return errors.New("FAIL: signature failures")
	}
	fmt.Println("PASS")
	return nil
}
