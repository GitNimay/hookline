package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/GitNimay/hookline/internal/config"
	"github.com/GitNimay/hookline/internal/telemetry"
)

// Main is the whole main() of a service: load config, set up logs and tracing, run, exit.
func Main[T any, P interface {
	*T
	Base() *config.Common
}](name string, run func(context.Context, T, *slog.Logger) error) {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	var cfg T
	if err := config.Load(&cfg); err != nil {
		fmt.Fprintln(os.Stderr, name+":", err)
		os.Exit(2)
	}
	base := P(&cfg).Base()
	log := telemetry.NewLogger(os.Stdout, name, base.LogLevel, base.LogFormat)
	slog.SetDefault(log)
	ctx := context.Background()
	flush, err := telemetry.SetupTracing(ctx, name, base.OTLPEndpoint, base.TraceSampling)
	if err != nil {
		log.Error("tracing setup failed", "err", err)
		os.Exit(1)
	}
	err = run(ctx, cfg, log)
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	_ = flush(fctx)
	cancel()
	if err != nil {
		os.Exit(1)
	}
}

// healthcheck probes this container's own /readyz; distroless images have no curl.
func healthcheck() int {
	addr := os.Getenv("ADMIN_ADDR")
	if addr == "" {
		addr = ":8081"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+localAddr(addr)+"/readyz", nil) //nolint:gosec // own admin port
	resp, err := http.DefaultClient.Do(req)                                                             //nolint:gosec // own admin port
	if err != nil {
		return 1
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
