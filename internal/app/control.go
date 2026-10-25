package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/GitNimay/hookline/gen/hookline/v1"
	"github.com/GitNimay/hookline/internal/auth"
	"github.com/GitNimay/hookline/internal/breaker"
	"github.com/GitNimay/hookline/internal/config"
	"github.com/GitNimay/hookline/internal/delivery"
	"github.com/GitNimay/hookline/internal/domain"
	"github.com/GitNimay/hookline/internal/queue"
	"github.com/GitNimay/hookline/internal/server"
	"github.com/GitNimay/hookline/internal/signing"
	"github.com/GitNimay/hookline/internal/ssrf"
	"github.com/GitNimay/hookline/internal/store"
	"github.com/GitNimay/hookline/internal/telemetry"
)

// Control implements the tenant, event type, endpoint and delivery services.
type Control struct {
	pb.UnimplementedTenantServiceServer
	pb.UnimplementedEventTypeServiceServer
	pb.UnimplementedEndpointServiceServer
	pb.UnimplementedDeliveryServiceServer

	Pool    *pgxpool.Pool
	Queue   *queue.Queue
	Box     *signing.Box
	Breaker *breaker.Breaker // nil: circuit state reported as unknown
	Sender  *delivery.Sender
	Policy  ssrf.Policy
	Log     *slog.Logger
}

const (
	defaultConcurrency  = 50
	defaultEndpointRate = 50
	secretOverlap       = 24 * time.Hour
)

var nameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$`)

// --- tenants (admin) ---

// CreateTenant creates a tenant and its delivery consumer.
func (c *Control) CreateTenant(ctx context.Context, req *pb.CreateTenantRequest) (*pb.CreateTenantResponse, error) {
	if !nameRe.MatchString(req.Name) {
		return nil, domain.Invalid("name", "1-255 chars of letters, digits, . _ : -")
	}
	rps, conc := req.IngestRpsLimit, req.MaxConcurrency
	if rps == 0 {
		rps = 500
	}
	if conc == 0 {
		conc = defaultConcurrency
	}
	if rps < 1 || rps > 100_000 {
		return nil, domain.Invalid("ingest_rps_limit", "between 1 and 100000")
	}
	if conc < 1 || conc > 10_000 {
		return nil, domain.Invalid("max_concurrency", "between 1 and 10000")
	}
	t, err := store.New(c.Pool).CreateTenant(ctx, store.CreateTenantParams{ID: domain.NewID(), Name: req.Name, IngestRpsLimit: rps, MaxConcurrency: conc})
	if isUniqueViolation(err) {
		return nil, fmt.Errorf("tenant %q: %w", req.Name, domain.ErrAlreadyExists)
	}
	if err != nil {
		return nil, err
	}
	if err := c.ensureConsumer(ctx, t); err != nil {
		return nil, err
	}
	return &pb.CreateTenantResponse{Tenant: tenantPB(t)}, nil
}

func (c *Control) ensureConsumer(ctx context.Context, t store.Tenant) error {
	_, err := c.Queue.EnsureTenantConsumer(ctx, domain.FormatID(domain.PrefixTenant, t.ID), int(t.MaxConcurrency))
	if err != nil {
		return fmt.Errorf("ensure consumer: %w: %w", domain.ErrUnavailable, err)
	}
	return nil
}

func (c *Control) getTenant(ctx context.Context, id string) (store.Tenant, error) {
	tid, err := domain.ParseID(domain.PrefixTenant, id)
	if err != nil {
		return store.Tenant{}, err
	}
	t, err := store.New(c.Pool).GetTenant(ctx, tid)
	return t, notFound(err)
}

// GetTenant returns a tenant.
func (c *Control) GetTenant(ctx context.Context, req *pb.GetTenantRequest) (*pb.GetTenantResponse, error) {
	t, err := c.getTenant(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	return &pb.GetTenantResponse{Tenant: tenantPB(t)}, nil
}

// ListTenants lists tenants, newest first.
func (c *Control) ListTenants(ctx context.Context, req *pb.ListTenantsRequest) (*pb.ListTenantsResponse, error) {
	ct, cid, err := decodeCursor(req.PageToken)
	if err != nil {
		return nil, err
	}
	size := pageSize(req.PageSize)
	rows, err := store.New(c.Pool).ListTenants(ctx, store.ListTenantsParams{CursorTime: ct, CursorID: cid, PageSize: size})
	if err != nil {
		return nil, err
	}
	out := &pb.ListTenantsResponse{NextPageToken: nextToken(rows, size, func(t store.Tenant) (time.Time, uuid.UUID) { return t.CreatedAt, t.ID })}
	for _, t := range rows {
		out.Tenants = append(out.Tenants, tenantPB(t))
	}
	return out, nil
}

// UpdateTenant applies the fields named in update_mask.
func (c *Control) UpdateTenant(ctx context.Context, req *pb.UpdateTenantRequest) (*pb.UpdateTenantResponse, error) {
	t, err := c.getTenant(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	if req.Tenant == nil || req.UpdateMask == nil {
		return nil, domain.Invalid("update_mask", "required")
	}
	for _, p := range req.UpdateMask.Paths {
		switch p {
		case "name":
			if !nameRe.MatchString(req.Tenant.Name) {
				return nil, domain.Invalid("name", "1-255 chars of letters, digits, . _ : -")
			}
			t.Name = req.Tenant.Name
		case "status":
			if req.Tenant.Status != "active" && req.Tenant.Status != "disabled" {
				return nil, domain.Invalid("status", "active or disabled")
			}
			t.Status = req.Tenant.Status
		case "ingest_rps_limit":
			if req.Tenant.IngestRpsLimit < 1 || req.Tenant.IngestRpsLimit > 100_000 {
				return nil, domain.Invalid("ingest_rps_limit", "between 1 and 100000")
			}
			t.IngestRpsLimit = req.Tenant.IngestRpsLimit
		case "max_concurrency":
			if req.Tenant.MaxConcurrency < 1 || req.Tenant.MaxConcurrency > 10_000 {
				return nil, domain.Invalid("max_concurrency", "between 1 and 10000")
			}
			t.MaxConcurrency = req.Tenant.MaxConcurrency
		default:
			return nil, domain.Invalid("update_mask", "field %q cannot be updated", p)
		}
	}
	return c.saveTenant(ctx, t)
}

func (c *Control) saveTenant(ctx context.Context, t store.Tenant) (*pb.UpdateTenantResponse, error) {
	t, err := store.New(c.Pool).UpdateTenant(ctx, store.UpdateTenantParams{ID: t.ID, Name: t.Name, Status: t.Status, IngestRpsLimit: t.IngestRpsLimit, MaxConcurrency: t.MaxConcurrency})
	if isUniqueViolation(err) {
		return nil, fmt.Errorf("tenant name: %w", domain.ErrAlreadyExists)
	}
	if err != nil {
		return nil, notFound(err)
	}
	// The consumer stays while disabled so already-accepted deliveries still drain.
	if err := c.ensureConsumer(ctx, t); err != nil {
		return nil, err
	}
	return &pb.UpdateTenantResponse{Tenant: tenantPB(t)}, nil
}

// DisableTenant stops a tenant's keys from working.
func (c *Control) DisableTenant(ctx context.Context, req *pb.DisableTenantRequest) (*pb.DisableTenantResponse, error) {
	t, err := c.getTenant(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	t.Status = "disabled"
	res, err := c.saveTenant(ctx, t)
	if err != nil {
		return nil, err
	}
	return &pb.DisableTenantResponse{Tenant: res.Tenant}, nil
}

// CreateApiKey issues a tenant key; the plaintext is returned only here.
func (c *Control) CreateApiKey(ctx context.Context, req *pb.CreateApiKeyRequest) (*pb.CreateApiKeyResponse, error) {
	t, err := c.getTenant(ctx, req.TenantId)
	if err != nil {
		return nil, err
	}
	key, prefix, hash := auth.NewKey()
	k, err := store.New(c.Pool).CreateAPIKey(ctx, store.CreateAPIKeyParams{ID: domain.NewID(), TenantID: &t.ID, Scope: "tenant", KeyPrefix: prefix, KeyHash: hash})
	if err != nil {
		return nil, err
	}
	return &pb.CreateApiKeyResponse{ApiKey: apiKeyPB(k), Key: key}, nil
}

// ListApiKeys lists a tenant's keys (never the key itself).
func (c *Control) ListApiKeys(ctx context.Context, req *pb.ListApiKeysRequest) (*pb.ListApiKeysResponse, error) {
	t, err := c.getTenant(ctx, req.TenantId)
	if err != nil {
		return nil, err
	}
	keys, err := store.New(c.Pool).ListAPIKeys(ctx, &t.ID)
	if err != nil {
		return nil, err
	}
	out := &pb.ListApiKeysResponse{}
	for _, k := range keys {
		out.ApiKeys = append(out.ApiKeys, apiKeyPB(k))
	}
	return out, nil
}

// RevokeApiKey revokes a key. Cached auth results expire within 30 s.
func (c *Control) RevokeApiKey(ctx context.Context, req *pb.RevokeApiKeyRequest) (*pb.RevokeApiKeyResponse, error) {
	t, err := c.getTenant(ctx, req.TenantId)
	if err != nil {
		return nil, err
	}
	kid, err := domain.ParseID(domain.PrefixKey, req.Id)
	if err != nil {
		return nil, err
	}
	n, err := store.New(c.Pool).RevokeAPIKey(ctx, store.RevokeAPIKeyParams{ID: kid, TenantID: &t.ID})
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, domain.ErrNotFound
	}
	return &pb.RevokeApiKeyResponse{}, nil
}

// --- event types ---

// CreateEventType adds a type to the caller's catalogue.
func (c *Control) CreateEventType(ctx context.Context, req *pb.CreateEventTypeRequest) (*pb.CreateEventTypeResponse, error) {
	tid, err := auth.TenantFrom(ctx)
	if err != nil {
		return nil, err
	}
	if !nameRe.MatchString(req.Name) {
		return nil, domain.Invalid("name", "1-255 chars of letters, digits, . _ : -")
	}
	if len(req.Description) > 1024 {
		return nil, domain.Invalid("description", "at most 1024 chars")
	}
	et, err := store.New(c.Pool).CreateEventType(ctx, store.CreateEventTypeParams{TenantID: tid, Name: req.Name, Description: req.Description})
	if isUniqueViolation(err) {
		return nil, fmt.Errorf("event type %q: %w", req.Name, domain.ErrAlreadyExists)
	}
	if err != nil {
		return nil, err
	}
	return &pb.CreateEventTypeResponse{EventType: &pb.EventType{Name: et.Name, Description: et.Description, CreateTime: ts(et.CreatedAt)}}, nil
}

// ListEventTypes lists the caller's catalogue.
func (c *Control) ListEventTypes(ctx context.Context, _ *pb.ListEventTypesRequest) (*pb.ListEventTypesResponse, error) {
	tid, err := auth.TenantFrom(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := store.New(c.Pool).ListEventTypes(ctx, tid)
	if err != nil {
		return nil, err
	}
	out := &pb.ListEventTypesResponse{}
	for _, et := range rows {
		out.EventTypes = append(out.EventTypes, &pb.EventType{Name: et.Name, Description: et.Description, CreateTime: ts(et.CreatedAt)})
	}
	return out, nil
}

// DeleteEventType removes a type. Existing subscriptions keep their string.
func (c *Control) DeleteEventType(ctx context.Context, req *pb.DeleteEventTypeRequest) (*pb.DeleteEventTypeResponse, error) {
	tid, err := auth.TenantFrom(ctx)
	if err != nil {
		return nil, err
	}
	n, err := store.New(c.Pool).DeleteEventType(ctx, store.DeleteEventTypeParams{TenantID: tid, Name: req.Name})
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, domain.ErrNotFound
	}
	return &pb.DeleteEventTypeResponse{}, nil
}

// --- endpoints ---

type endpointFields struct {
	url, description string
	types            []string
	headers          map[string]string
	rate             int32
}

func (c *Control) validateEndpoint(ctx context.Context, tid uuid.UUID, f endpointFields) error {
	if err := c.Policy.CheckURL(f.url); err != nil {
		return domain.Invalid("url", "%v", err)
	}
	if len(f.description) > 1024 {
		return domain.Invalid("description", "at most 1024 chars")
	}
	if len(f.types) == 0 || len(f.types) > 100 {
		return domain.Invalid("event_types", "1 to 100 event types, or [\"*\"]")
	}
	var names []string
	for _, t := range f.types {
		if t != "*" && !slices.Contains(names, t) {
			names = append(names, t)
		}
	}
	if len(names) > 0 {
		n, err := store.New(c.Pool).EventTypesExist(ctx, store.EventTypesExistParams{TenantID: tid, Names: names})
		if err != nil {
			return err
		}
		if int(n) != len(names) {
			return domain.Invalid("event_types", "every type must exist in the catalogue")
		}
	}
	if err := delivery.ValidateHeaders(f.headers); err != nil {
		return domain.Invalid("custom_headers", "%v", err)
	}
	if f.rate < 1 || f.rate > 10_000 {
		return domain.Invalid("rate_limit_per_sec", "between 1 and 10000")
	}
	return nil
}

// CreateEndpoint creates an endpoint with a fresh signing secret.
func (c *Control) CreateEndpoint(ctx context.Context, req *pb.CreateEndpointRequest) (*pb.CreateEndpointResponse, error) {
	tid, err := auth.TenantFrom(ctx)
	if err != nil {
		return nil, err
	}
	f := endpointFields{url: req.Url, description: req.Description, types: req.EventTypes, headers: req.CustomHeaders, rate: req.RateLimitPerSec}
	if f.rate == 0 {
		f.rate = defaultEndpointRate
	}
	if err := c.validateEndpoint(ctx, tid, f); err != nil {
		return nil, err
	}
	headers, _ := json.Marshal(nonNil(f.headers))
	var ep store.Endpoint
	err = pgx.BeginTxFunc(ctx, c.Pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		q := store.New(tx)
		var err error
		ep, err = q.CreateEndpoint(ctx, store.CreateEndpointParams{
			ID: domain.NewID(), TenantID: tid, Url: f.url, Description: f.description,
			EventTypes: f.types, CustomHeaders: headers, RateLimitPerSec: f.rate,
		})
		if err != nil {
			return err
		}
		return q.InsertEndpointSecret(ctx, store.InsertEndpointSecretParams{ID: domain.NewID(), EndpointID: ep.ID, SecretCiphertext: c.Box.Seal(signing.NewSecret())})
	})
	if err != nil {
		return nil, err
	}
	return &pb.CreateEndpointResponse{Endpoint: endpointPB(ep, breaker.Closed)}, nil
}

func nonNil(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

func (c *Control) getEndpoint(ctx context.Context, id string) (store.Endpoint, error) {
	tid, err := auth.TenantFrom(ctx)
	if err != nil {
		return store.Endpoint{}, err
	}
	eid, err := domain.ParseID(domain.PrefixEndpoint, id)
	if err != nil {
		return store.Endpoint{}, err
	}
	ep, err := store.New(c.Pool).GetEndpoint(ctx, store.GetEndpointParams{ID: eid, TenantID: tid})
	return ep, notFound(err)
}

func (c *Control) circuit(ctx context.Context, id uuid.UUID) string {
	if c.Breaker == nil {
		return "unknown"
	}
	s, err := c.Breaker.Get(ctx, domain.FormatID(domain.PrefixEndpoint, id))
	if err != nil {
		return "unknown"
	}
	return s.View(time.Now())
}

// GetEndpoint returns an endpoint with its circuit breaker state.
func (c *Control) GetEndpoint(ctx context.Context, req *pb.GetEndpointRequest) (*pb.GetEndpointResponse, error) {
	ep, err := c.getEndpoint(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	return &pb.GetEndpointResponse{Endpoint: endpointPB(ep, c.circuit(ctx, ep.ID))}, nil
}

// ListEndpoints lists the caller's endpoints, newest first.
func (c *Control) ListEndpoints(ctx context.Context, req *pb.ListEndpointsRequest) (*pb.ListEndpointsResponse, error) {
	tid, err := auth.TenantFrom(ctx)
	if err != nil {
		return nil, err
	}
	ct, cid, err := decodeCursor(req.PageToken)
	if err != nil {
		return nil, err
	}
	size := pageSize(req.PageSize)
	rows, err := store.New(c.Pool).ListEndpoints(ctx, store.ListEndpointsParams{TenantID: tid, CursorTime: ct, CursorID: cid, PageSize: size})
	if err != nil {
		return nil, err
	}
	out := &pb.ListEndpointsResponse{NextPageToken: nextToken(rows, size, func(e store.Endpoint) (time.Time, uuid.UUID) { return e.CreatedAt, e.ID })}
	for _, e := range rows {
		out.Endpoints = append(out.Endpoints, endpointPB(e, c.circuit(ctx, e.ID)))
	}
	return out, nil
}

// UpdateEndpoint applies the fields named in update_mask.
func (c *Control) UpdateEndpoint(ctx context.Context, req *pb.UpdateEndpointRequest) (*pb.UpdateEndpointResponse, error) {
	ep, err := c.getEndpoint(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	if req.Endpoint == nil || req.UpdateMask == nil {
		return nil, domain.Invalid("update_mask", "required")
	}
	var headers map[string]string
	_ = json.Unmarshal(ep.CustomHeaders, &headers)
	f := endpointFields{url: ep.Url, description: ep.Description, types: ep.EventTypes, headers: headers, rate: ep.RateLimitPerSec}
	for _, p := range req.UpdateMask.Paths {
		switch p {
		case "url":
			f.url = req.Endpoint.Url
		case "description":
			f.description = req.Endpoint.Description
		case "event_types":
			f.types = req.Endpoint.EventTypes
		case "custom_headers":
			f.headers = req.Endpoint.CustomHeaders
		case "rate_limit_per_sec":
			f.rate = req.Endpoint.RateLimitPerSec
		default:
			return nil, domain.Invalid("update_mask", "field %q cannot be updated", p)
		}
	}
	if err := c.validateEndpoint(ctx, ep.TenantID, f); err != nil {
		return nil, err
	}
	hb, _ := json.Marshal(nonNil(f.headers))
	ep, err = store.New(c.Pool).UpdateEndpoint(ctx, store.UpdateEndpointParams{ID: ep.ID, TenantID: ep.TenantID, Url: f.url, Description: f.description, EventTypes: f.types, CustomHeaders: hb, RateLimitPerSec: f.rate})
	if err != nil {
		return nil, notFound(err)
	}
	return &pb.UpdateEndpointResponse{Endpoint: endpointPB(ep, c.circuit(ctx, ep.ID))}, nil
}

// DeleteEndpoint soft-deletes; pending deliveries to it end dead.
func (c *Control) DeleteEndpoint(ctx context.Context, req *pb.DeleteEndpointRequest) (*pb.DeleteEndpointResponse, error) {
	ep, err := c.getEndpoint(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	if _, err := store.New(c.Pool).SoftDeleteEndpoint(ctx, store.SoftDeleteEndpointParams{ID: ep.ID, TenantID: ep.TenantID}); err != nil {
		return nil, err
	}
	return &pb.DeleteEndpointResponse{}, nil
}

func (c *Control) setStatus(ctx context.Context, id, st, reason string) (*pb.Endpoint, error) {
	ep, err := c.getEndpoint(ctx, id)
	if err != nil {
		return nil, err
	}
	ep, err = store.New(c.Pool).SetEndpointStatus(ctx, store.SetEndpointStatusParams{ID: ep.ID, TenantID: ep.TenantID, Status: st, DisabledReason: reason})
	if err != nil {
		return nil, notFound(err)
	}
	return endpointPB(ep, c.circuit(ctx, ep.ID)), nil
}

// EnableEndpoint re-enables an endpoint and clears failing_since.
func (c *Control) EnableEndpoint(ctx context.Context, req *pb.EnableEndpointRequest) (*pb.EnableEndpointResponse, error) {
	ep, err := c.setStatus(ctx, req.Id, "enabled", "")
	if err != nil {
		return nil, err
	}
	return &pb.EnableEndpointResponse{Endpoint: ep}, nil
}

// DisableEndpoint disables an endpoint manually.
func (c *Control) DisableEndpoint(ctx context.Context, req *pb.DisableEndpointRequest) (*pb.DisableEndpointResponse, error) {
	ep, err := c.setStatus(ctx, req.Id, "disabled", "manual")
	if err != nil {
		return nil, err
	}
	return &pb.DisableEndpointResponse{Endpoint: ep}, nil
}

func (c *Control) secrets(ctx context.Context, q *store.Queries, id uuid.UUID) ([]*pb.EndpointSecret, error) {
	rows, err := q.ListActiveSecrets(ctx, id)
	if err != nil {
		return nil, err
	}
	out := make([]*pb.EndpointSecret, 0, len(rows))
	for _, r := range rows {
		plain, err := c.Box.Open(r.SecretCiphertext)
		if err != nil {
			return nil, fmt.Errorf("decrypt secret: %w", err)
		}
		out = append(out, &pb.EndpointSecret{Secret: plain, CreateTime: ts(r.CreatedAt), ExpireTime: tsp(r.ExpiresAt)})
	}
	return out, nil
}

// GetEndpointSecret is the only call that returns signing secrets.
func (c *Control) GetEndpointSecret(ctx context.Context, req *pb.GetEndpointSecretRequest) (*pb.GetEndpointSecretResponse, error) {
	ep, err := c.getEndpoint(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	s, err := c.secrets(ctx, store.New(c.Pool), ep.ID)
	if err != nil {
		return nil, err
	}
	return &pb.GetEndpointSecretResponse{Secrets: s}, nil
}

// RotateEndpointSecret adds a new secret; the previous one keeps signing for 24 h.
func (c *Control) RotateEndpointSecret(ctx context.Context, req *pb.RotateEndpointSecretRequest) (*pb.RotateEndpointSecretResponse, error) {
	ep, err := c.getEndpoint(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	var out []*pb.EndpointSecret
	err = pgx.BeginTxFunc(ctx, c.Pool, pgx.TxOptions{}, func(tx pgx.Tx) error {
		q := store.New(tx)
		if err := q.ExpireOverlappingSecrets(ctx, ep.ID); err != nil {
			return err
		}
		exp := time.Now().Add(secretOverlap)
		if err := q.ScheduleSecretExpiry(ctx, store.ScheduleSecretExpiryParams{EndpointID: ep.ID, ExpiresAt: &exp}); err != nil {
			return err
		}
		if err := q.InsertEndpointSecret(ctx, store.InsertEndpointSecretParams{ID: domain.NewID(), EndpointID: ep.ID, SecretCiphertext: c.Box.Seal(signing.NewSecret())}); err != nil {
			return err
		}
		out, err = c.secrets(ctx, q, ep.ID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &pb.RotateEndpointSecretResponse{Secrets: out}, nil
}

// SendTestEvent sends a sample event through the normal signing path. Nothing is stored.
func (c *Control) SendTestEvent(ctx context.Context, req *pb.SendTestEventRequest) (*pb.SendTestEventResponse, error) {
	ep, err := c.getEndpoint(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	typ := req.EventType
	if typ == "" {
		typ = "hookline.test"
	}
	payload := req.Payload
	if payload == nil {
		payload, _ = structpb.NewStruct(map[string]any{"message": "Test event from Hookline"})
	}
	data, err := payload.MarshalJSON()
	if err != nil {
		return nil, domain.Invalid("payload", "not valid JSON")
	}
	sec, err := c.secrets(ctx, store.New(c.Pool), ep.ID)
	if err != nil {
		return nil, err
	}
	var headers map[string]string
	_ = json.Unmarshal(ep.CustomHeaders, &headers)
	body, err := delivery.Envelope(typ, time.Now(), data)
	if err != nil {
		return nil, err
	}
	res := c.Sender.Send(ctx, delivery.Request{URL: ep.Url, ID: domain.FormatID(domain.PrefixEvent, domain.NewID()), Body: body, Secrets: secretStrings(sec), Headers: headers})
	_, kind := delivery.Classify(res.StatusCode, res.Err)
	return &pb.SendTestEventResponse{StatusCode: int32(res.StatusCode), DurationMs: res.Duration.Milliseconds(), ErrorKind: kind, ResponseBodySnippet: res.Body}, nil
}

func secretStrings(s []*pb.EndpointSecret) []string {
	out := make([]string, len(s))
	for i, x := range s {
		out[i] = x.Secret
	}
	return out
}

// ListMatchingEndpoints is internal (dispatcher): enabled, non-deleted endpoints subscribed to the type or "*".
func (c *Control) ListMatchingEndpoints(ctx context.Context, req *pb.ListMatchingEndpointsRequest) (*pb.ListMatchingEndpointsResponse, error) {
	tid, err := domain.ParseID(domain.PrefixTenant, req.TenantId)
	if err != nil {
		return nil, err
	}
	ids, err := store.New(c.Pool).ListMatchingEndpoints(ctx, store.ListMatchingEndpointsParams{TenantID: tid, EventType: req.EventType})
	if err != nil {
		return nil, err
	}
	out := &pb.ListMatchingEndpointsResponse{}
	for _, id := range ids {
		out.EndpointIds = append(out.EndpointIds, domain.FormatID(domain.PrefixEndpoint, id))
	}
	return out, nil
}

// --- deliveries ---

// ListDeliveries filters by event, endpoint, status and creation time.
func (c *Control) ListDeliveries(ctx context.Context, req *pb.ListDeliveriesRequest) (*pb.ListDeliveriesResponse, error) {
	tid, err := auth.TenantFrom(ctx)
	if err != nil {
		return nil, err
	}
	p := store.ListDeliveriesParams{TenantID: tid, Status: req.Status, StartTime: timeOrNil(req.StartTime), EndTime: timeOrNil(req.EndTime), PageSize: pageSize(req.PageSize)}
	switch req.Status {
	case "", domain.StatusPending, domain.StatusRetrying, domain.StatusSucceeded, domain.StatusDead:
	default:
		return nil, domain.Invalid("status", "pending, retrying, succeeded or dead")
	}
	if req.EventId != "" {
		id, err := domain.ParseID(domain.PrefixEvent, req.EventId)
		if err != nil {
			return &pb.ListDeliveriesResponse{}, nil
		}
		p.EventID = &id
	}
	if req.EndpointId != "" {
		id, err := domain.ParseID(domain.PrefixEndpoint, req.EndpointId)
		if err != nil {
			return &pb.ListDeliveriesResponse{}, nil
		}
		p.EndpointID = &id
	}
	if p.CursorTime, p.CursorID, err = decodeCursor(req.PageToken); err != nil {
		return nil, err
	}
	rows, err := store.New(c.Pool).ListDeliveries(ctx, p)
	if err != nil {
		return nil, err
	}
	out := &pb.ListDeliveriesResponse{NextPageToken: nextToken(rows, p.PageSize, func(d store.Delivery) (time.Time, uuid.UUID) { return d.CreatedAt, d.ID })}
	for _, d := range rows {
		out.Deliveries = append(out.Deliveries, deliveryPB(d))
	}
	return out, nil
}

func (c *Control) deliveryID(ctx context.Context, id string) (uuid.UUID, uuid.UUID, error) {
	tid, err := auth.TenantFrom(ctx)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	did, err := domain.ParseID(domain.PrefixDelivery, id)
	return tid, did, err
}

// GetDelivery returns one delivery.
func (c *Control) GetDelivery(ctx context.Context, req *pb.GetDeliveryRequest) (*pb.GetDeliveryResponse, error) {
	tid, did, err := c.deliveryID(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	d, err := store.New(c.Pool).GetDelivery(ctx, store.GetDeliveryParams{ID: did, TenantID: tid})
	if err != nil {
		return nil, notFound(err)
	}
	return &pb.GetDeliveryResponse{Delivery: deliveryPB(d)}, nil
}

// ListDeliveryAttempts returns every attempt of a delivery.
func (c *Control) ListDeliveryAttempts(ctx context.Context, req *pb.ListDeliveryAttemptsRequest) (*pb.ListDeliveryAttemptsResponse, error) {
	tid, did, err := c.deliveryID(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	q := store.New(c.Pool)
	if _, err := q.GetDelivery(ctx, store.GetDeliveryParams{ID: did, TenantID: tid}); err != nil {
		return nil, notFound(err)
	}
	rows, err := q.ListAttempts(ctx, store.ListAttemptsParams{DeliveryID: did, TenantID: tid})
	if err != nil {
		return nil, err
	}
	out := &pb.ListDeliveryAttemptsResponse{}
	for _, a := range rows {
		out.Attempts = append(out.Attempts, attemptPB(a))
	}
	return out, nil
}

func ref(d store.Delivery) jobRef {
	return jobRef{ID: d.ID, TenantID: d.TenantID, EventID: d.EventID, EndpointID: d.EndpointID, AttemptCount: d.AttemptCount}
}

// ReplayDelivery resets a dead or succeeded delivery and queues it again.
func (c *Control) ReplayDelivery(ctx context.Context, req *pb.ReplayDeliveryRequest) (*pb.ReplayDeliveryResponse, error) {
	tid, did, err := c.deliveryID(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	q := store.New(c.Pool)
	d, err := q.ReplayDelivery(ctx, store.ReplayDeliveryParams{ID: did, TenantID: tid})
	if errors.Is(err, pgx.ErrNoRows) {
		if _, gerr := q.GetDelivery(ctx, store.GetDeliveryParams{ID: did, TenantID: tid}); gerr != nil {
			return nil, notFound(gerr)
		}
		return nil, status.Error(codes.FailedPrecondition, "only dead or succeeded deliveries can be replayed")
	}
	if err != nil {
		return nil, err
	}
	// If this publish is lost, the scheduler's stuck-pending reconciliation republishes it.
	if err := publishJob(ctx, c.Queue, ref(d), "r"+strconv.Itoa(int(d.ReplayCount))); err != nil {
		c.Log.WarnContext(ctx, "replay publish failed; reconciliation will retry", "delivery_id", req.Id, "err", err)
	}
	return &pb.ReplayDeliveryResponse{Delivery: deliveryPB(d)}, nil
}

// ReplayEndpoint replays all dead deliveries of an endpoint, or all since a time, in batches of 500.
func (c *Control) ReplayEndpoint(ctx context.Context, req *pb.ReplayEndpointRequest) (*pb.ReplayEndpointResponse, error) {
	ep, err := c.getEndpoint(ctx, req.EndpointId)
	if err != nil {
		return nil, err
	}
	q := store.New(c.Pool)
	var total int64
	for {
		rows, err := q.ReplayEndpointBatch(ctx, store.ReplayEndpointBatchParams{TenantID: ep.TenantID, EndpointID: ep.ID, Since: timeOrNil(req.Since)})
		if err != nil {
			return nil, err
		}
		for _, d := range rows {
			if err := publishJob(ctx, c.Queue, ref(d), "r"+strconv.Itoa(int(d.ReplayCount))); err != nil {
				c.Log.WarnContext(ctx, "replay publish failed; reconciliation will retry", "err", err)
			}
		}
		total += int64(len(rows))
		if len(rows) < 500 {
			return &pb.ReplayEndpointResponse{Replayed: total}, nil
		}
	}
}

// reconcileConsumers makes sure every active tenant has its delivery consumer.
func (c *Control) reconcileConsumers(ctx context.Context) error {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		tenants, err := store.New(c.Pool).ListActiveTenants(ctx)
		if err == nil {
			for _, tn := range tenants {
				if err := c.ensureConsumer(ctx, tn); err != nil {
					c.Log.Warn("ensure tenant consumer", "tenant_id", domain.FormatID(domain.PrefixTenant, tn.ID), "err", err)
				}
			}
		} else if ctx.Err() == nil {
			c.Log.Warn("list tenants for consumers", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// RunControl wires and runs the control service.
func RunControl(ctx context.Context, cfg config.Control, log *slog.Logger) error {
	pool, err := OpenPostgres(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	mk, err := cfg.MasterKeyBytes()
	if err != nil {
		return err
	}
	box, err := signing.NewBox(mk)
	if err != nil {
		return err
	}
	q, err := queue.Connect(cfg.NATSURL, "control")
	if err != nil {
		return err
	}
	if err := ensureStreams(ctx, q, log); err != nil {
		return err
	}
	if cfg.AdminKey != "" {
		if err := bootstrapAdmin(ctx, pool, cfg.AdminKey); err != nil {
			return err
		}
	}
	rdb := OpenRedis(cfg.Redis)
	policy := ssrf.NewPolicy(cfg.SSRFAllowlist)
	ctl := &Control{Pool: pool, Queue: q, Box: box, Breaker: breaker.New(rdb), Sender: delivery.NewSender(policy, telemetry.Version), Policy: policy, Log: log}
	authn := auth.NewAuthenticator(store.New(pool), log, 30*time.Second)

	gs := server.NewGRPCServer(log, authn.Interceptor())
	pb.RegisterTenantServiceServer(gs, ctl)
	pb.RegisterEventTypeServiceServer(gs, ctl)
	pb.RegisterEndpointServiceServer(gs, ctl)
	pb.RegisterDeliveryServiceServer(gs, ctl)

	mux, handler := server.NewGateway()
	conn, err := server.DialLocal(localAddr(cfg.GRPCAddr))
	if err != nil {
		return err
	}
	for _, reg := range []func(context.Context, *runtime.ServeMux, *grpc.ClientConn) error{
		pb.RegisterTenantServiceHandler, pb.RegisterEventTypeServiceHandler,
		pb.RegisterEndpointServiceHandler, pb.RegisterDeliveryServiceHandler,
	} {
		if err := reg(ctx, mux, conn); err != nil {
			return err
		}
	}
	return server.Run(ctx, server.Options{
		Name: "control", Log: log, AdminAddr: cfg.AdminAddr, GRPCAddr: cfg.GRPCAddr, HTTPAddr: cfg.HTTPAddr,
		GRPC: gs, HTTP: handler, Pprof: cfg.PprofEnabled, DrainDelay: cfg.DrainDelay, StopTimeout: cfg.ShutdownTimeout,
		Ready:   readyAll(pingDB(pool), q.Ready),
		Loops:   []func(context.Context) error{ctl.reconcileConsumers, authn.FlushLoop},
		Closers: []func(){func() { _ = conn.Close() }, q.Close, func() { _ = rdb.Close() }, pool.Close},
	})
}

// bootstrapAdmin makes the configured admin key valid.
func bootstrapAdmin(ctx context.Context, pool *pgxpool.Pool, key string) error {
	if len(key) < len(auth.KeyPrefix)+20 || key[:len(auth.KeyPrefix)] != auth.KeyPrefix {
		return fmt.Errorf("HOOKLINE_ADMIN_KEY must start with %s and be at least 28 chars", auth.KeyPrefix)
	}
	return store.New(pool).UpsertAdminKey(ctx, store.UpsertAdminKeyParams{ID: domain.NewID(), KeyPrefix: key[:len(auth.KeyPrefix)+8], KeyHash: auth.Hash(key)})
}
