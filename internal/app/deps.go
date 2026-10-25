// Package app wires each service: constructors, handlers and run loops.
package app

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, err
	}
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
