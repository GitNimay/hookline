package telemetry

import (
	"context"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Tracer is the shared Hookline tracer.
var Tracer trace.Tracer = otel.Tracer("hookline")

// SetupTracing installs the global tracer provider and W3C propagator.
// With no OTLP endpoint it only installs the propagator, so trace context still flows.
// The returned func flushes spans and must be called on shutdown.
func SetupTracing(ctx context.Context, service, endpoint string, ratio float64) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpointURL(endpoint), otlptracegrpc.WithTimeout(5*time.Second))
	if err != nil {
		return nil, err
	}
	res := resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(service), semconv.ServiceVersion(Version))
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		// Head sampling by ratio; keeping all error traces is done by tail sampling in Alloy.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
	otel.SetTracerProvider(tp)
	Tracer = tp.Tracer("hookline")
	return tp.Shutdown, nil
}

// Inject writes the context's trace into h (NATS headers share http.Header's shape).
func Inject(ctx context.Context, h http.Header) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(h))
}

// Extract returns ctx carrying the trace found in h.
func Extract(ctx context.Context, h http.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(h))
}

// TraceParent returns the W3C traceparent of ctx, for storing with an event.
func TraceParent(ctx context.Context) string {
	h := http.Header{}
	Inject(ctx, h)
	return h.Get("traceparent")
}

// WithTraceParent returns ctx carrying a stored traceparent.
func WithTraceParent(ctx context.Context, tp string) context.Context {
	if tp == "" {
		return ctx
	}
	return Extract(ctx, http.Header{"Traceparent": []string{tp}})
}
