package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// All Hookline metrics. Labels stay low-cardinality: tenant_id at most, never endpoint/event IDs or URLs.
var (
	EventsIngested = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hookline_events_ingested_total", Help: "Events received by ingest.",
	}, []string{"tenant_id", "result"})
	IngestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "hookline_ingest_request_duration_seconds", Help: "Ingest API latency.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"method", "code"})
	OutboxPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "hookline_outbox_pending", Help: "Unpublished outbox rows.",
	})
	OutboxPublishLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "hookline_outbox_publish_lag_seconds", Help: "Time from event insert to NATS publish.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 5, 30, 60},
	})
	DeliveriesCreated = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hookline_deliveries_created_total", Help: "Deliveries created by fan-out.",
	}, []string{"tenant_id"})
	EventsUnmatched = promauto.NewCounter(prometheus.CounterOpts{
		Name: "hookline_events_unmatched_total", Help: "Events with no matching endpoint.",
	})
	DeliveryAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hookline_delivery_attempts_total", Help: "HTTP delivery attempts.",
	}, []string{"tenant_id", "outcome", "error_kind"})
	AttemptDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "hookline_delivery_attempt_duration_seconds", Help: "HTTP attempt duration.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
	}, []string{"outcome"})
	EndToEnd = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "hookline_delivery_end_to_end_seconds", Help: "Ingest to successful delivery.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2, 5, 10, 30, 60, 300, 3600},
	}, []string{"tenant_id"})
	DeliveriesDead = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hookline_deliveries_dead_total", Help: "Deliveries moved to the dead letter list.",
	}, []string{"tenant_id", "reason"})
	DeliveriesRetrying = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "hookline_deliveries_retrying", Help: "Deliveries waiting for a retry.",
	}, []string{"tenant_id"})
	BreakerTransitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hookline_circuit_breaker_transitions_total", Help: "Circuit breaker state changes.",
	}, []string{"to_state"})
	BreakersOpen = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "hookline_circuit_breakers_open", Help: "Open circuit breakers seen by this pod.",
	})
	RateLimited = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "hookline_rate_limited_total", Help: "Requests or deliveries delayed by rate limits.",
	}, []string{"kind"})
	WorkerInflight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "hookline_worker_inflight", Help: "Delivery jobs in progress.",
	})
	SchedulerDuePublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "hookline_scheduler_due_published_total", Help: "Retry jobs published by the scheduler.",
	})
	EndpointsAutoDisabled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "hookline_endpoints_auto_disabled_total", Help: "Endpoints disabled after failing too long.",
	})
	BuildInfo = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "hookline_build_info", Help: "Build version and commit.",
	}, []string{"version", "commit"})
	GRPCHandled = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "hookline_grpc_server_handling_seconds", Help: "gRPC server call latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"service", "method", "code"})
	GRPCClientHandled = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "hookline_grpc_client_handling_seconds", Help: "gRPC client call latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "code"})
)

func init() { BuildInfo.WithLabelValues(Version, Commit).Set(1) }
