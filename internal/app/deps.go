// Package app wires each service: constructors, handlers and run loops.
package app

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/GitNimay/hookline/internal/config"
)

// OpenPostgres opens a pgx pool and checks it.
func OpenPostgres(ctx context.Context, c config.Postgres) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(c.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("database url: %w", err)
	}
	pc.MaxConns = c.DBMaxConns
	pc.ConnConfig.ConnectTimeout = 5 * time.Second
	// Statement timeout on the server side backs up the context deadlines in code.
	pc.ConnConfig.RuntimeParams["statement_timeout"] = fmt.Sprint(c.QueryTimeout.Milliseconds())
	if !c.SyncCommit {
		pc.ConnConfig.RuntimeParams["synchronous_commit"] = "off"
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, err
	}
	registerPoolMetrics(pool)
	return pool, nil
}

// OpenRedis returns a Redis client with timeouts on every call.
func OpenRedis(c config.Redis) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         c.RedisAddr,
		Password:     c.RedisPassword,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolTimeout:  2 * time.Second,
	})
}

func pingDB(pool *pgxpool.Pool) func(context.Context) error {
	return func(ctx context.Context) error { return pool.Ping(ctx) }
}

// readyAll combines readiness checks.
func readyAll(checks ...func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		for _, c := range checks {
			if err := c(ctx); err != nil {
				return err
			}
		}
		return nil
	}
}

// registerPoolMetrics exposes pgx pool stats. Re-registering (tests) keeps the first pool.
func registerPoolMetrics(pool *pgxpool.Pool) {
	g := func(name, help string, f func(*pgxpool.Stat) float64) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "hookline_db_pool_" + name, Help: help}, func() float64 { return f(pool.Stat()) })
	}
	for _, c := range []prometheus.Collector{
		g("acquired_connections", "Connections in use.", func(s *pgxpool.Stat) float64 { return float64(s.AcquiredConns()) }),
		g("idle_connections", "Idle connections.", func(s *pgxpool.Stat) float64 { return float64(s.IdleConns()) }),
		g("total_connections", "Open connections.", func(s *pgxpool.Stat) float64 { return float64(s.TotalConns()) }),
		g("max_connections", "Pool size limit.", func(s *pgxpool.Stat) float64 { return float64(s.MaxConns()) }),
		g("acquire_seconds_total", "Total time spent waiting for a connection.", func(s *pgxpool.Stat) float64 { return s.AcquireDuration().Seconds() }),
		g("empty_acquire_total", "Acquires that had to wait.", func(s *pgxpool.Stat) float64 { return float64(s.EmptyAcquireCount()) }),
	} {
		_ = prometheus.Register(c)
	}
}
