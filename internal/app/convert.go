package app

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/GitNimay/hookline/gen/hookline/v1"
	"github.com/GitNimay/hookline/internal/domain"
	"github.com/GitNimay/hookline/internal/store"
)

func ts(t time.Time) *timestamppb.Timestamp { return timestamppb.New(t) }

func tsp(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func timeOrNil(t *timestamppb.Timestamp) *time.Time {
	if t == nil {
		return nil
	}
	v := t.AsTime()
	return &v
}

func tenantPB(t store.Tenant) *pb.Tenant {
	return &pb.Tenant{
		Id: domain.FormatID(domain.PrefixTenant, t.ID), Name: t.Name, Status: t.Status,
		IngestRpsLimit: t.IngestRpsLimit, MaxConcurrency: t.MaxConcurrency,
		CreateTime: ts(t.CreatedAt), UpdateTime: ts(t.UpdatedAt),
	}
}

func apiKeyPB(k store.ApiKey) *pb.ApiKey {
	out := &pb.ApiKey{Id: domain.FormatID(domain.PrefixKey, k.ID), KeyPrefix: k.KeyPrefix, CreateTime: ts(k.CreatedAt), LastUsedTime: tsp(k.LastUsedAt), RevokeTime: tsp(k.RevokedAt)}
	if k.TenantID != nil {
		out.TenantId = domain.FormatID(domain.PrefixTenant, *k.TenantID)
	}
	return out
}

func endpointPB(e store.Endpoint, circuit string) *pb.Endpoint {
	var headers map[string]string
	_ = json.Unmarshal(e.CustomHeaders, &headers)
	return &pb.Endpoint{
		Id: domain.FormatID(domain.PrefixEndpoint, e.ID), Url: e.Url, Description: e.Description,
		EventTypes: e.EventTypes, CustomHeaders: headers, RateLimitPerSec: e.RateLimitPerSec,
		Status: e.Status, DisabledReason: e.DisabledReason, FailingSince: tsp(e.FailingSince),
		CircuitState: circuit, CreateTime: ts(e.CreatedAt), UpdateTime: ts(e.UpdatedAt),
	}
}

func eventPB(e store.Event) *pb.Event {
	var payload structpb.Struct
	_ = payload.UnmarshalJSON(e.Payload)
	out := &pb.Event{
		Id: domain.FormatID(domain.PrefixEvent, e.ID), EventType: e.EventType, Payload: &payload,
		OccurTime: ts(e.OccurredAt), CreateTime: ts(e.CreatedAt),
	}
	if e.IdempotencyKey != nil {
		out.IdempotencyKey = *e.IdempotencyKey
	}
	return out
}

func deliveryPB(d store.Delivery) *pb.Delivery {
	return &pb.Delivery{
		Id: domain.FormatID(domain.PrefixDelivery, d.ID), EventId: domain.FormatID(domain.PrefixEvent, d.EventID),
		EndpointId: domain.FormatID(domain.PrefixEndpoint, d.EndpointID), Status: d.Status, AttemptCount: d.AttemptCount,
		NextAttemptTime: tsp(d.NextAttemptAt), LastAttemptTime: tsp(d.LastAttemptAt), LastStatusCode: d.LastStatusCode,
		LastError: d.LastError, DeadReason: d.DeadReason, SucceedTime: tsp(d.SucceededAt),
		CreateTime: ts(d.CreatedAt), UpdateTime: ts(d.UpdatedAt),
	}
}

func attemptPB(a store.DeliveryAttempt) *pb.Attempt {
	return &pb.Attempt{
		Id: domain.FormatID(domain.PrefixAttempt, a.ID), DeliveryId: domain.FormatID(domain.PrefixDelivery, a.DeliveryID),
		AttemptNumber: a.AttemptNumber, StartTime: ts(a.StartedAt), DurationMs: a.DurationMs, StatusCode: a.StatusCode,
		ErrorKind: a.ErrorKind, ErrorMessage: a.ErrorMessage, ResponseBodySnippet: a.ResponseBodySnippet,
	}
}

// Cursor pagination: the token is the (created_at, id) of the last row, base64url encoded.

func pageSize(n int32) int32 {
	if n <= 0 || n > 100 {
		return 100
	}
	return n
}

func encodeCursor(t time.Time, id uuid.UUID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(t.UnixNano(), 10) + ":" + id.String()))
}

func decodeCursor(tok string) (*time.Time, uuid.UUID, error) {
	if tok == "" {
		return nil, uuid.Nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return nil, uuid.Nil, domain.Invalid("page_token", "invalid")
	}
	ns, idStr, ok := strings.Cut(string(raw), ":")
	n, err1 := strconv.ParseInt(ns, 10, 64)
	id, err2 := uuid.Parse(idStr)
	if !ok || err1 != nil || err2 != nil {
		return nil, uuid.Nil, domain.Invalid("page_token", "invalid")
	}
	t := time.Unix(0, n)
	return &t, id, nil
}

// nextToken returns the token for the next page when the page is full.
func nextToken[T any](rows []T, size int32, key func(T) (time.Time, uuid.UUID)) string {
	if len(rows) < int(size) || len(rows) == 0 {
		return ""
	}
	t, id := key(rows[len(rows)-1])
	return encodeCursor(t, id)
}
