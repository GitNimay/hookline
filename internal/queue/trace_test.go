package queue

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"

	pb "github.com/GitNimay/hookline/gen/hookline/v1"
	"github.com/GitNimay/hookline/internal/telemetry"
)

func TestMsgCarriesTrace(t *testing.T) {
	if _, err := telemetry.SetupTracing(context.Background(), "t", "", 1); err != nil {
		t.Fatal(err)
	}
	const tp = "00-c892a38ab06bdfe79565f333361028bb-0ac681a0b97e7bad-01"
	ctx := telemetry.WithTraceParent(context.Background(), tp)
	msg, err := Msg(ctx, "events.x", &pb.EventJob{EventId: "e"})
	if err != nil {
		t.Fatal(err)
	}
	// NATS delivers header keys as written; lookups must not depend on case.
	msg.Header = map[string][]string{"traceparent": msg.Header["traceparent"], "Nats-Msg-Id": {"x"}}
	got := trace.SpanContextFromContext(telemetry.Extract(context.Background(), msg.Header))
	if got.TraceID().String() != "c892a38ab06bdfe79565f333361028bb" {
		t.Fatalf("trace lost: headers=%v", msg.Header)
	}
}
