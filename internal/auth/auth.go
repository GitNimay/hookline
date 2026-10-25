// Package auth issues and checks API keys and enforces scopes in a gRPC interceptor.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/GitNimay/hookline/internal/domain"
	"github.com/GitNimay/hookline/internal/store"
)

// KeyPrefix starts every API key.
const KeyPrefix = "hk_live_"

// NewKey returns a new key ("hk_live_" + base62 of 32 random bytes), its display prefix and its hash.
func NewKey() (key, prefix string, hash []byte) {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	key = KeyPrefix + new(big.Int).SetBytes(b).Text(62)
	return key, key[:len(KeyPrefix)+8], Hash(key)
}

// Hash is the stored form of a key.
func Hash(key string) []byte {
	h := sha256.Sum256([]byte(key))
	return h[:]
}

// Principal is the authenticated caller.
type Principal struct {
	KeyID    uuid.UUID
	Admin    bool
	TenantID uuid.UUID // zero for admin keys
}

type ctxKey struct{}

// TenantFrom returns the caller's tenant ID, or ErrUnauthenticated.
func TenantFrom(ctx context.Context) (uuid.UUID, error) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	if !ok || p.Admin {
		return uuid.Nil, domain.ErrUnauthenticated
	}
	return p.TenantID, nil
}

// WithPrincipal stores p in ctx (used by tests and the interceptor).
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

// Lookup resolves a key hash to a principal.
type Lookup interface {
	GetAPIKeyByHash(ctx context.Context, hash []byte) (store.GetAPIKeyByHashRow, error)
	TouchAPIKey(ctx context.Context, id uuid.UUID) error
}

// Authenticator checks keys with a short in-process cache so ingest stays fast.
type Authenticator struct {
	db  Lookup
	log *slog.Logger
	ttl time.Duration

	mu      sync.Mutex
	cache   map[string]cached
	touched map[uuid.UUID]time.Time
}

type cached struct {
	p   Principal
	err error
	exp time.Time
}

// NewAuthenticator returns an authenticator caching results for ttl.
func NewAuthenticator(db Lookup, log *slog.Logger, ttl time.Duration) *Authenticator {
	return &Authenticator{db: db, log: log, ttl: ttl, cache: map[string]cached{}, touched: map[uuid.UUID]time.Time{}}
}

// Authenticate resolves a raw key. Revoked keys and keys of disabled tenants fail.
// Keys are found by their SHA-256 hash through a unique index, so no plaintext comparison
// happens and response time does not depend on how many characters match.
func (a *Authenticator) Authenticate(ctx context.Context, key string) (Principal, error) {
	if !strings.HasPrefix(key, KeyPrefix) || len(key) > 128 {
		return Principal{}, domain.ErrUnauthenticated
	}
	hash := Hash(key)
	hk := string(hash)
	a.mu.Lock()
	c, ok := a.cache[hk]
	a.mu.Unlock()
	if ok && time.Now().Before(c.exp) {
		return c.p, c.err
	}

	row, err := a.db.GetAPIKeyByHash(ctx, hash)
	var p Principal
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		err = domain.ErrUnauthenticated
	case err != nil:
		return Principal{}, domain.ErrUnavailable
	case row.RevokedAt != nil || row.TenantStatus != "active":
		err = domain.ErrUnauthenticated
	default:
		p = Principal{KeyID: row.ID, Admin: row.Scope == "admin"}
		if row.TenantID != nil {
			p.TenantID = *row.TenantID
		}
	}
	a.mu.Lock()
	// ponytail: unbounded map keyed by presented key hashes; add an LRU if key-guessing traffic matters.
	if len(a.cache) > 100_000 {
		a.cache = map[string]cached{}
	}
	a.cache[hk] = cached{p: p, err: err, exp: time.Now().Add(a.ttl)}
	a.mu.Unlock()
	return p, err
}

// touch records a key use; FlushLoop writes it later so requests never wait on it.
func (a *Authenticator) touch(id uuid.UUID) {
	a.mu.Lock()
	a.touched[id] = time.Now()
	a.mu.Unlock()
}

// FlushLoop writes last_used_at asynchronously every interval until ctx ends.
func (a *Authenticator) FlushLoop(ctx context.Context) error {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		a.mu.Lock()
		ids := a.touched
		a.touched = map[uuid.UUID]time.Time{}
		a.mu.Unlock()
		for id := range ids {
			tctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			if err := a.db.TouchAPIKey(tctx, id); err != nil {
				a.log.Warn("touch api key", "err", err)
			}
			cancel()
		}
	}
}

// Public methods need no key; internal methods are reachable only inside the cluster network.
var (
	publicMethods = map[string]bool{
		"/grpc.health.v1.Health/Check": true,
		"/grpc.health.v1.Health/Watch": true,
		"/grpc.health.v1.Health/List":  true,
	}
	internalMethods = map[string]bool{
		"/hookline.v1.EndpointService/ListMatchingEndpoints": true,
	}
)

// Interceptor enforces auth: methods under TenantService need an admin key, everything else a tenant key.
// Internal methods must not come through the REST gateway (the gateway never routes them).
func (a *Authenticator) Interceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if publicMethods[info.FullMethod] || internalMethods[info.FullMethod] {
			return h(ctx, req)
		}
		key := bearer(ctx)
		if key == "" {
			return nil, domain.ErrUnauthenticated
		}
		p, err := a.Authenticate(ctx, key)
		if err != nil {
			return nil, err
		}
		adminOnly := strings.HasPrefix(info.FullMethod, "/hookline.v1.TenantService/")
		if adminOnly != p.Admin {
			return nil, domain.ErrPermission
		}
		a.touch(p.KeyID)
		return h(WithPrincipal(ctx, p), req)
	}
}

func bearer(ctx context.Context) string {
	md, _ := metadata.FromIncomingContext(ctx)
	for _, v := range md.Get("authorization") {
		if k, ok := strings.CutPrefix(v, "Bearer "); ok {
			return strings.TrimSpace(k)
		}
	}
	return ""
}
