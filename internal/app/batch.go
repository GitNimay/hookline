package app

import (
	"context"
	"sync"
)

// batcher groups concurrent calls into one database statement (group commit). Callers
// queue their request; whoever gets one of the flush slots runs everything queued so far,
// up to size, as one call. When idle, a request runs alone at once; under load, requests
// that arrive while the slots are busy share the next statement.
type batcher[Req, Res any] struct {
	size  int
	slots chan struct{}
	// run handles reqs and returns one result per request, in order; an error fails them all.
	run func(ctx context.Context, reqs []Req) ([]Res, error)

	mu      sync.Mutex
	pending []*batchCall[Req, Res]
}

type batchCall[Req, Res any] struct {
	req  Req
	res  Res
	err  error
	done chan struct{}
}

func newBatcher[Req, Res any](size, flushes int, run func(context.Context, []Req) ([]Res, error)) *batcher[Req, Res] {
	return &batcher[Req, Res]{size: size, slots: make(chan struct{}, flushes), run: run}
}

// do queues req and returns its result once a flush (this caller's or another's) has run it.
func (b *batcher[Req, Res]) do(ctx context.Context, req Req) (Res, error) {
	c := &batchCall[Req, Res]{req: req, done: make(chan struct{})}
	b.mu.Lock()
	b.pending = append(b.pending, c)
	b.mu.Unlock()
	for {
		select {
		case <-c.done:
			return c.res, c.err
		case b.slots <- struct{}{}:
		}
		b.mu.Lock()
		n := min(len(b.pending), b.size)
		calls := b.pending[:n:n]
		b.pending = b.pending[n:]
		if len(b.pending) == 0 {
			b.pending = nil
		}
		b.mu.Unlock()
		if n > 0 {
			b.flush(ctx, calls)
		}
		<-b.slots
		if n == 0 { // another flusher took c and will finish it
			<-c.done
			return c.res, c.err
		}
	}
}

func (b *batcher[Req, Res]) flush(ctx context.Context, calls []*batchCall[Req, Res]) {
	reqs := make([]Req, len(calls))
	for i, c := range calls {
		reqs[i] = c.req
	}
	// Detached from the flusher's own cancellation: the batch belongs to every caller in it.
	res, err := b.run(context.WithoutCancel(ctx), reqs)
	for i, c := range calls {
		if err == nil {
			c.res = res[i]
		}
		c.err = err
		close(c.done)
	}
}
