// Package config loads typed, validated service configuration from environment variables.
package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Common is shared by every service.
type Common struct {
	LogLevel        string        `env:"LOG_LEVEL" envDefault:"info"`
	LogFormat       string        `env:"LOG_FORMAT" envDefault:"json"`
	GRPCAddr        string        `env:"GRPC_ADDR" envDefault:":9090"`
	HTTPAddr        string        `env:"HTTP_ADDR" envDefault:":8080"`
	AdminAddr       string        `env:"ADMIN_ADDR" envDefault:":8081"`
	PprofEnabled    bool          `env:"PPROF_ENABLED" envDefault:"false"`
	DrainDelay      time.Duration `env:"DRAIN_DELAY" envDefault:"5s"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"20s"`
	OTLPEndpoint    string        `env:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	TraceSampling   float64       `env:"OTEL_TRACES_SAMPLER_RATIO" envDefault:"1"`
}

// Postgres holds database settings.
type Postgres struct {
	DatabaseURL  string        `env:"DATABASE_URL,notEmpty"`
	DBMaxConns   int32         `env:"DB_MAX_CONNS" envDefault:"20"`
	QueryTimeout time.Duration `env:"DB_QUERY_TIMEOUT" envDefault:"5s"`
	// SyncCommit is Postgres synchronous_commit for this service's sessions. Only the worker
	// may turn it off: its state is re-derivable (ADR-010); ingest and dispatcher must not.
	SyncCommit bool `env:"DB_SYNCHRONOUS_COMMIT" envDefault:"true"`
}

// NATS holds queue settings.
type NATS struct {
	NATSURL string `env:"NATS_URL" envDefault:"nats://localhost:4222"`
}

// Redis holds shared-state settings.
type Redis struct {
	RedisAddr     string `env:"REDIS_ADDR" envDefault:"localhost:6379"`
	RedisPassword string `env:"REDIS_PASSWORD"`
}

// Secrets holds the master key that encrypts endpoint signing secrets.
type Secrets struct {
	MasterKey string `env:"HOOKLINE_MASTER_KEY,notEmpty"`
}

// MasterKeyBytes decodes the base64 master key.
func (s Secrets) MasterKeyBytes() ([]byte, error) {
	k, err := base64.StdEncoding.DecodeString(s.MasterKey)
	if err != nil || len(k) != 32 {
		return nil, errors.New("HOOKLINE_MASTER_KEY must be base64 of 32 bytes")
	}
	return k, nil
}

// Delivery holds HTTP-sending settings shared by worker and control (test-send).
type Delivery struct {
	SSRFAllowlist []string `env:"SSRF_ALLOWLIST" envSeparator:","`
	Version       string   // set from build info, not env
}

// Ingest is the ingest service config.
type Ingest struct {
	Common
	Postgres
	NATS
	Redis
	DefaultIngestRPS int `env:"DEFAULT_INGEST_RPS" envDefault:"500"`
}

// Control is the control service config.
type Control struct {
	Common
	Postgres
	NATS
	Redis
	Secrets
	Delivery
	AdminKey string `env:"HOOKLINE_ADMIN_KEY"`
}

// Dispatcher is the dispatcher service config.
type Dispatcher struct {
	Common
	Postgres
	NATS
	ControlAddr string        `env:"CONTROL_GRPC_ADDR" envDefault:"localhost:9090"`
	MatchCache  time.Duration `env:"MATCH_CACHE_TTL" envDefault:"30s"`
	Concurrency int           `env:"DISPATCHER_CONCURRENCY" envDefault:"32"`
}

// Worker is the worker service config.
type Worker struct {
	Common
	Postgres
	NATS
	Redis
	Secrets
	Delivery
	RetrySchedule string `env:"RETRY_SCHEDULE" envDefault:"default"`
	Concurrency   int    `env:"WORKER_CONCURRENCY" envDefault:"100"`
}

// Scheduler is the scheduler service config.
type Scheduler struct {
	Common
	Postgres
	NATS
	AttemptRetention   time.Duration `env:"ATTEMPT_RETENTION" envDefault:"336h"`
	EventRetention     time.Duration `env:"EVENT_RETENTION" envDefault:"720h"`
	OutboxRetention    time.Duration `env:"OUTBOX_RETENTION" envDefault:"24h"`
	IdempotencyWindow  time.Duration `env:"IDEMPOTENCY_WINDOW" envDefault:"24h"`
	AutoDisableAfter   time.Duration `env:"AUTO_DISABLE_AFTER" envDefault:"72h"`
	ReconcilePendingAt time.Duration `env:"RECONCILE_PENDING_AFTER" envDefault:"10m"`
}

// Load parses the environment into cfg and validates the common fields.
func Load[T any](cfg *T) error {
	if err := env.Parse(cfg); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if c, ok := any(cfg).(interface{ Base() *Common }); ok {
		return c.Base().validate()
	}
	return nil
}

// Base returns the shared settings of any service config.
func (c *Common) Base() *Common { return c }

func (c *Common) validate() error {
	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("config: LOG_LEVEL %q must be debug, info, warn or error", c.LogLevel)
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		return fmt.Errorf("config: LOG_FORMAT %q must be json or text", c.LogFormat)
	}
	if c.TraceSampling < 0 || c.TraceSampling > 1 {
		return errors.New("config: OTEL_TRACES_SAMPLER_RATIO must be between 0 and 1")
	}
	return nil
}
