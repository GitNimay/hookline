package app

import (
	"sync"
	"time"
)

// ttlCache is a tiny concurrent cache with per-entry expiry.
// ponytail: no size bound; keys are tenants/endpoints/event types, which are few. Add LRU if that changes.
type ttlCache[K comparable, V any] struct {
	mu  sync.Mutex
	ttl time.Duration
	m   map[K]ttlEntry[V]
}

type ttlEntry[V any] struct {
	v   V
	exp time.Time
}

func newCache[K comparable, V any](ttl time.Duration) *ttlCache[K, V] {
	return &ttlCache[K, V]{ttl: ttl, m: map[K]ttlEntry[V]{}}
}

func (c *ttlCache[K, V]) get(k K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[k]
	if !ok || time.Now().After(e.exp) {
		var zero V
		return zero, false
	}
	return e.v, true
}

func (c *ttlCache[K, V]) put(k K, v V) {
	c.mu.Lock()
	c.m[k] = ttlEntry[V]{v: v, exp: time.Now().Add(c.ttl)}
	c.mu.Unlock()
}
