// Package server runs the gRPC, REST gateway and admin servers with graceful shutdown.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/GitNimay/hookline/internal/telemetry"
)

// Options describe one service process.
type Options struct {
	Name         string
	Log          *slog.Logger
	AdminAddr    string
	GRPCAddr     string
	HTTPAddr     string
	GRPC         *grpc.Server // optional
	HTTP         http.Handler // optional REST gateway
	Pprof        bool
	DrainDelay   time.Duration
	StopTimeout  time.Duration
	Ready        func(ctx context.Context) error   // dependency checks for /readyz
	Loops        []func(ctx context.Context) error // background loops; return nil when ctx ends
	Closers      []func()                          // run last, in order
	StartedHooks []func()                          // optional, run once listeners are up
}

// Run starts everything and blocks until SIGINT/SIGTERM or a component fails.
// Shutdown order: fail /readyz, wait DrainDelay, stop loops (they finish in-flight work),
// stop gRPC and HTTP, then run Closers. A component failure returns its error.
func Run(parent context.Context, o Options) error {
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()

	var shuttingDown atomic.Bool
	hs := health.NewServer()
	if o.GRPC != nil {
		healthpb.RegisterHealthServer(o.GRPC, hs)
	}

	g, gctx := errgroup.WithContext(ctx)
	admin := &http.Server{Addr: o.AdminAddr, Handler: adminMux(o, &shuttingDown), ReadHeaderTimeout: 5 * time.Second}
	g.Go(func() error { return serveHTTP(admin) })

	var gw *http.Server
	if o.HTTP != nil {
		gw = &http.Server{Addr: o.HTTPAddr, Handler: o.HTTP, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second}
		g.Go(func() error { return serveHTTP(gw) })
	}
	if o.GRPC != nil {
		lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", o.GRPCAddr)
		if err != nil {
			return err
		}
		g.Go(func() error {
			if err := o.GRPC.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				return err
			}
			return nil
		})
	}

	loopCtx, cancelLoops := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelLoops()
	var loops sync.WaitGroup
	for _, loop := range o.Loops {
		loops.Add(1)
		g.Go(func() error {
			defer loops.Done()
			return loop(loopCtx)
		})
	}
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	o.Log.Info("service started", "grpc", o.GRPCAddr, "http", o.HTTPAddr, "admin", o.AdminAddr, "commit", telemetry.Commit)

	g.Go(func() error {
		<-gctx.Done()
		shuttingDown.Store(true)
		hs.Shutdown()
		if ctx.Err() != nil && parent.Err() == nil { // signal, not a failure
			o.Log.Info("shutting down", "drain_delay", o.DrainDelay)
			time.Sleep(o.DrainDelay)
		}
		cancelLoops()
		loops.Wait()
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.StopTimeout)
		defer cancel()
		if o.GRPC != nil {
			done := make(chan struct{})
			go func() { o.GRPC.GracefulStop(); close(done) }()
			select {
			case <-done:
			case <-sctx.Done():
				o.GRPC.Stop()
			}
		}
		if gw != nil {
			_ = gw.Shutdown(sctx)
		}
		return admin.Shutdown(sctx)
	})

	err := g.Wait()
	for _, c := range o.Closers {
		c()
	}
	if err != nil {
		o.Log.Error("service failed", "err", err)
		return err
	}
	o.Log.Info("service stopped")
	return nil
}

func serveHTTP(s *http.Server) error {
	if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func adminMux(o Options, shuttingDown *atomic.Bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "ok", "service": o.Name, "version": telemetry.Version, "commit": telemetry.Commit, "build_date": telemetry.BuildDate,
		})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if shuttingDown.Load() {
			http.Error(w, "shutting down", http.StatusServiceUnavailable)
			return
		}
		if o.Ready != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if err := o.Ready(ctx); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("GET /metrics", promhttp.Handler())
	if o.Pprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return mux
}
