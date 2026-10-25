//go:build integration

// Package testutil starts real dependencies in containers for integration tests.
package testutil

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver for goose
	"github.com/pressly/goose/v3"
	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/GitNimay/hookline/migrations"
)

// Images match docker-compose.yml.
const (
	PostgresImage = "postgres:18.6-alpine"
	NATSImage     = "nats:2.12.6-alpine"
	RedisImage    = "redis:8.8.0-alpine"
)

func cleanup(t *testing.T, c testcontainers.Container) {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.Terminate(ctx)
	})
}

// Postgres starts Postgres, runs all migrations and returns its DSN.
func Postgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := tcpg.Run(ctx, PostgresImage, tcpg.WithDatabase("hookline"), tcpg.WithUsername("hookline"), tcpg.WithPassword("hookline"), tcpg.BasicWaitStrategies())
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	cleanup(t, c)
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	goose.SetLogger(goose.NopLogger())
	if err := goose.Up(db, "."); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return dsn
}

// NATS starts a JetStream-enabled NATS server and returns its URL.
func NATS(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := tcnats.Run(ctx, NATSImage)
	if err != nil {
		t.Fatalf("start nats: %v", err)
	}
	cleanup(t, c)
	url, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return url
}

// Redis starts Redis and returns host:port.
func Redis(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := tcredis.Run(ctx, RedisImage)
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	cleanup(t, c)
	addr, err := c.Endpoint(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	return addr
}
