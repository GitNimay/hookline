//go:build integration

package breaker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/GitNimay/hookline/internal/testutil"
)

func TestRedisBreakerSingleProbe(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: testutil.Redis(t)})
	defer func() { _ = rdb.Close() }()
	b := New(rdb)
	ctx := context.Background()
	now := time.Now()
	for range ConsecutiveToOpen {
		if _, err := b.Record(ctx, "ep", now, false, false); err != nil {
			t.Fatal(err)
		}
	}
	if d, _ := b.Allow(ctx, "ep", now); d.Allow {
		t.Fatal("open breaker allowed a request")
	}
	// After the open window, exactly one of many concurrent callers wins the probe.
	later := now.Add(BaseOpen)
	var winners atomic.Int32
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if d, err := b.Allow(ctx, "ep", later); err == nil && d.Allow {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("%d probes allowed, want 1", winners.Load())
	}
	tr, err := b.Record(ctx, "ep", later, true, true)
	if err != nil || tr != Closed {
		t.Fatalf("probe success: %q %v", tr, err)
	}
	if d, _ := b.Allow(ctx, "ep", later); !d.Allow || d.NeedProbe {
		t.Fatal("closed breaker should allow freely")
	}
}

func TestConcurrentRecordsAreAtomic(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: testutil.Redis(t)})
	defer func() { _ = rdb.Close() }()
	b := New(rdb)
	ctx := context.Background()
	var opens atomic.Int32
	var wg sync.WaitGroup
	for range 9 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if tr, _ := b.Record(ctx, "ep2", time.Now(), false, false); tr == Open {
				opens.Add(1)
			}
		}()
	}
	wg.Wait()
	s, _ := b.Get(ctx, "ep2")
	if s.Consecutive != 9 || opens.Load() != 0 {
		t.Fatalf("lost updates: consecutive=%d opens=%d", s.Consecutive, opens.Load())
	}
}
