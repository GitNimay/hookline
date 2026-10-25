package app

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBatcherResultsAndGrouping(t *testing.T) {
	var calls, biggest atomic.Int32
	b := newBatcher(10, 2, func(_ context.Context, reqs []int) ([]int, error) {
		calls.Add(1)
		if n := int32(len(reqs)); n > biggest.Load() {
			biggest.Store(n)
		}
		time.Sleep(5 * time.Millisecond) // a statement in flight, so callers pile up
		out := make([]int, len(reqs))
		for i, r := range reqs {
			out[i] = r * 2
		}
		return out, nil
	})
	const n = 200
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := b.do(context.Background(), i)
			if err != nil || got != i*2 {
				errs <- errors.New("wrong result")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if c := calls.Load(); c >= n/2 {
		t.Fatalf("%d statements for %d requests: not batching", c, n)
	}
	if biggest.Load() > 10 {
		t.Fatalf("batch of %d exceeds the size limit", biggest.Load())
	}
}

func TestBatcherSingleCallRunsAlone(t *testing.T) {
	b := newBatcher(10, 1, func(_ context.Context, reqs []string) ([]int, error) {
		return []int{len(reqs)}, nil
	})
	got, err := b.do(context.Background(), "x")
	if err != nil || got != 1 {
		t.Fatalf("got %d, %v", got, err)
	}
}

func TestBatcherErrorFailsWholeBatch(t *testing.T) {
	boom := errors.New("boom")
	b := newBatcher(10, 1, func(context.Context, []int) ([]int, error) { return nil, boom })
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := b.do(context.Background(), 1); !errors.Is(err, boom) {
				t.Errorf("err = %v, want boom", err)
			}
		}()
	}
	wg.Wait()
}

func TestBatcherIgnoresFlusherCancel(t *testing.T) {
	b := newBatcher(10, 1, func(ctx context.Context, reqs []int) ([]int, error) {
		return make([]int, len(reqs)), ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.do(ctx, 1); err != nil {
		t.Fatalf("a cancelled caller must not fail the batch: %v", err)
	}
}
