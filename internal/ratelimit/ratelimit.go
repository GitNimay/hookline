// Package ratelimit is a GCRA limiter in Redis (atomic Lua via redis_rate).
// Redis errors fail open: the call is allowed and the error returned for logging.
package ratelimit

import (
	"context"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
)

// Limiter limits events per second per key.
type Limiter struct{ l *redis_rate.Limiter }

// New returns a Redis-backed limiter.
func New(rdb *redis.Client) *Limiter { return &Limiter{l: redis_rate.NewLimiter(rdb)} }

// Allow takes one token from key at perSec (burst = perSec).
// When denied, retryAfter says when a token will be free.
func (l *Limiter) Allow(ctx context.Context, key string, perSec int) (ok bool, retryAfter time.Duration, err error) {
	if perSec <= 0 {
		return true, 0, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	res, err := l.l.Allow(ctx, "hookline:rl:"+key, redis_rate.PerSecond(perSec))
	if err != nil {
		return true, 0, err
	}
	return res.Allowed > 0, res.RetryAfter, nil
}
