//go:build integration

package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/GitNimay/hookline/internal/testutil"
)

func TestGCRA(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: testutil.Redis(t)})
	defer func() { _ = rdb.Close() }()
	l := New(rdb)
	ctx := context.Background()

	allowed := 0
	var retry time.Duration
	for range 15 {
		ok, ra, err := l.Allow(ctx, "k", 10)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			allowed++
		} else {
			retry = ra
		}
	}
	if allowed != 10 {
		t.Fatalf("burst of 10/s allowed %d", allowed)
	}
	if retry <= 0 || retry > 200*time.Millisecond {
		t.Fatalf("retry after %v, want ~100ms", retry)
	}
	time.Sleep(250 * time.Millisecond)
	if ok, _, _ := l.Allow(ctx, "k", 10); !ok {
		t.Fatal("token should refill")
	}
	// Keys are independent.
	if ok, _, _ := l.Allow(ctx, "other", 10); !ok {
		t.Fatal("other key limited")
	}
}

func TestFailsOpenWhenRedisIsDown(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond})
	defer func() { _ = rdb.Close() }()
	ok, _, err := New(rdb).Allow(context.Background(), "k", 1)
	if !ok || err == nil {
		t.Fatalf("want allowed with error, got ok=%v err=%v", ok, err)
	}
}
