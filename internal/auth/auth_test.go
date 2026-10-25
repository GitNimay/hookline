package auth

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/GitNimay/hookline/internal/domain"
	"github.com/GitNimay/hookline/internal/store"
)

type fakeDB struct {
	rows  map[string]store.GetAPIKeyByHashRow
	calls int
}

func (f *fakeDB) GetAPIKeyByHash(_ context.Context, h []byte) (store.GetAPIKeyByHashRow, error) {
	f.calls++
	r, ok := f.rows[string(h)]
	if !ok {
		return r, pgx.ErrNoRows
	}
	return r, nil
}

func (f *fakeDB) TouchAPIKey(context.Context, uuid.UUID) error { return nil }

func TestNewKeyFormat(t *testing.T) {
	k, prefix, hash := NewKey()
	if !strings.HasPrefix(k, KeyPrefix) || len(k) < len(KeyPrefix)+40 {
		t.Fatalf("bad key %q", k)
	}
	if !strings.HasPrefix(k, prefix) || len(prefix) != len(KeyPrefix)+8 {
		t.Fatalf("bad prefix %q", prefix)
	}
	if string(hash) != string(Hash(k)) || len(hash) != 32 {
		t.Fatal("hash mismatch")
	}
	if k2, _, _ := NewKey(); k2 == k {
		t.Fatal("keys repeat")
	}
}

func TestInterceptorScopes(t *testing.T) {
	tenant := uuid.New()
	adminKey, _, ah := NewKey()
	tenantKey, _, th := NewKey()
	revokedKey, _, rh := NewKey()
	disabledKey, _, dh := NewKey()
	now := time.Now()
	db := &fakeDB{rows: map[string]store.GetAPIKeyByHashRow{
		string(ah): {ID: uuid.New(), Scope: "admin", TenantStatus: "active"},
		string(th): {ID: uuid.New(), Scope: "tenant", TenantID: &tenant, TenantStatus: "active"},
		string(rh): {ID: uuid.New(), Scope: "tenant", TenantID: &tenant, TenantStatus: "active", RevokedAt: &now},
		string(dh): {ID: uuid.New(), Scope: "tenant", TenantID: &tenant, TenantStatus: "disabled"},
	}}
	a := NewAuthenticator(db, slog.Default(), time.Minute)
	ic := a.Interceptor()
	call := func(method, key string) (uuid.UUID, error) {
		ctx := context.Background()
		if key != "" {
			ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer "+key))
		}
		var got uuid.UUID
		_, err := ic(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, func(ctx context.Context, _ any) (any, error) {
			got, _ = TenantFrom(ctx)
			return nil, nil
		})
		return got, err
	}
	const tenantMethod = "/hookline.v1.EndpointService/ListEndpoints"
	const adminMethod = "/hookline.v1.TenantService/CreateTenant"

	if id, err := call(tenantMethod, tenantKey); err != nil || id != tenant {
		t.Fatalf("tenant key on tenant method: %v %v", id, err)
	}
	cases := []struct {
		method, key string
		want        error
	}{
		{tenantMethod, "", domain.ErrUnauthenticated},
		{tenantMethod, "hk_live_nope", domain.ErrUnauthenticated},
		{tenantMethod, "Basic abc", domain.ErrUnauthenticated},
		{tenantMethod, revokedKey, domain.ErrUnauthenticated},
		{tenantMethod, disabledKey, domain.ErrUnauthenticated},
		{tenantMethod, adminKey, domain.ErrPermission}, // admin keys cannot act as a tenant
		{adminMethod, tenantKey, domain.ErrPermission}, // tenant keys cannot administer
	}
	for _, c := range cases {
		if _, err := call(c.method, c.key); !errors.Is(err, c.want) {
			t.Errorf("%s with %q: got %v want %v", c.method, c.key, err, c.want)
		}
	}
	if _, err := call(adminMethod, adminKey); err != nil {
		t.Fatalf("admin key on admin method: %v", err)
	}
	if _, err := call("/grpc.health.v1.Health/Check", ""); err != nil {
		t.Fatalf("health must be public: %v", err)
	}

	// Results are cached.
	before := db.calls
	_, _ = call(tenantMethod, tenantKey)
	if db.calls != before {
		t.Fatal("expected cache hit")
	}
}
