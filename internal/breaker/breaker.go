// Package breaker is a per-endpoint circuit breaker. The state machine is pure;
// state lives in Redis so every worker pod agrees.
package breaker

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/redis/go-redis/v9"
)

// Phases.
const (
	Closed   = "closed"
	Open     = "open"
	HalfOpen = "half_open"
)

// Tuning (section 9 of the build plan).
const (
	ConsecutiveToOpen = 10
	MinWindowAttempts = 20
	FailRateToOpen    = 0.5
	BaseOpen          = 30 * time.Second
	MaxOpen           = 10 * time.Minute
	ProbeTTL          = 30 * time.Second
	bucketWidth       = 10 * time.Second
	bucketCount       = 6 // 60 s window
)

type bucket struct {
	Start int64 `json:"s"` // unix ms
	Total int   `json:"t"`
	Fail  int   `json:"f"`
}

// State is one endpoint's breaker.
type State struct {
	Phase       string              `json:"p"`
	Consecutive int                 `json:"c"`
	OpenUntil   int64               `json:"u"` // unix ms
	Opens       int                 `json:"o"` // consecutive opens, for doubling
	Buckets     [bucketCount]bucket `json:"b"`
}

// View is the externally visible phase at now: an open breaker past its timer is half-open.
func (s State) View(now time.Time) string {
	if s.Phase == "" {
		return Closed
	}
	if s.Phase == Open && now.UnixMilli() >= s.OpenUntil {
		return HalfOpen
	}
	return s.Phase
}

// Decision says whether an attempt may go out.
type Decision struct {
	Allow     bool
	NeedProbe bool      // allowed only as the single half-open probe
	RetryAt   time.Time // when denied
}

// Check decides for now. It does not change state.
func (s State) Check(now time.Time) Decision {
	switch s.View(now) {
	case Closed:
		return Decision{Allow: true}
	case HalfOpen:
		return Decision{Allow: true, NeedProbe: true, RetryAt: now.Add(5 * time.Second)}
	default:
		return Decision{RetryAt: time.UnixMilli(s.OpenUntil)}
	}
}

// Record applies one result and returns the new phase if it changed ("" otherwise).
// While open, only the probe's result counts.
func (s *State) Record(now time.Time, success, probe bool) string {
	before := s.View(now)
	if before != Closed && !probe {
		return ""
	}
	s.count(now, success)
	if success {
		if before != Closed {
			*s = State{Phase: Closed, Buckets: s.Buckets}
			return Closed
		}
		s.Consecutive = 0
		return ""
	}
	s.Consecutive++
	if before != Closed || s.shouldOpen(now) {
		s.Phase = Open
		d := min(BaseOpen<<min(s.Opens, 10), MaxOpen)
		s.OpenUntil = now.Add(d).UnixMilli()
		s.Opens++
		return Open
	}
	return ""
}

func (s *State) count(now time.Time, success bool) {
	start := now.Truncate(bucketWidth).UnixMilli()
	b := &s.Buckets[(start/bucketWidth.Milliseconds())%bucketCount]
	if b.Start != start {
		*b = bucket{Start: start}
	}
	b.Total++
	if !success {
		b.Fail++
	}
}

func (s *State) shouldOpen(now time.Time) bool {
	if s.Consecutive >= ConsecutiveToOpen {
		return true
	}
	cutoff := now.Add(-bucketWidth * bucketCount).UnixMilli()
	var total, fail int
	for _, b := range s.Buckets {
		if b.Start > cutoff {
			total += b.Total
			fail += b.Fail
		}
	}
	return total >= MinWindowAttempts && float64(fail)/float64(total) > FailRateToOpen
}

// Breaker stores states in Redis.
type Breaker struct{ rdb *redis.Client }

// New returns a Redis-backed breaker.
func New(rdb *redis.Client) *Breaker { return &Breaker{rdb: rdb} }

func key(ep string) string      { return "hookline:breaker:" + ep }
func probeKey(ep string) string { return "hookline:breaker:probe:" + ep }

// Get loads an endpoint's state; missing means closed.
func (b *Breaker) Get(ctx context.Context, ep string) (State, error) {
	raw, err := b.rdb.Get(ctx, key(ep)).Bytes()
	if errors.Is(err, redis.Nil) {
		return State{Phase: Closed}, nil
	}
	if err != nil {
		return State{}, err
	}
	var s State
	err = json.Unmarshal(raw, &s)
	return s, err
}

// Allow decides whether to attempt now. In half-open, exactly one caller wins the
// probe via SET NX with a TTL. Redis errors fail open (allow) so Redis never stops delivery.
func (b *Breaker) Allow(ctx context.Context, ep string, now time.Time) (Decision, error) {
	s, err := b.Get(ctx, ep)
	if err != nil {
		return Decision{Allow: true}, err
	}
	d := s.Check(now)
	if !d.NeedProbe {
		return d, nil
	}
	won, err := b.rdb.SetNX(ctx, probeKey(ep), "1", ProbeTTL).Result()
	if err != nil {
		return Decision{Allow: true}, err
	}
	if !won {
		return Decision{RetryAt: d.RetryAt}, nil
	}
	return d, nil
}

// ReleaseProbe gives up a won probe without a result (the job was delayed), so another job can probe.
func (b *Breaker) ReleaseProbe(ctx context.Context, ep string) error {
	return b.rdb.Del(ctx, probeKey(ep)).Err()
}

// Record stores a result atomically (optimistic WATCH transaction) and returns the transition, if any.
func (b *Breaker) Record(ctx context.Context, ep string, now time.Time, success, probe bool) (string, error) {
	var transition string
	txf := func(tx *redis.Tx) error {
		s, err := b.getTx(ctx, tx, ep)
		if err != nil {
			return err
		}
		transition = s.Record(now, success, probe)
		raw, _ := json.Marshal(s)
		_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, key(ep), raw, 24*time.Hour)
			if probe {
				p.Del(ctx, probeKey(ep))
			}
			return nil
		})
		return err
	}
	var err error
	// Optimistic concurrency: on conflict, back off with jitter and retry.
	for i := range 50 {
		if err = b.rdb.Watch(ctx, txf, key(ep)); !errors.Is(err, redis.TxFailedErr) {
			return transition, err
		}
		time.Sleep(time.Duration(rand.IntN(1000*(min(i, 10)+1))) * time.Microsecond) //nolint:gosec // jitter
	}
	return "", err
}

func (b *Breaker) getTx(ctx context.Context, tx *redis.Tx, ep string) (State, error) {
	raw, err := tx.Get(ctx, key(ep)).Bytes()
	if errors.Is(err, redis.Nil) {
		return State{Phase: Closed}, nil
	}
	if err != nil {
		return State{}, err
	}
	var s State
	err = json.Unmarshal(raw, &s)
	return s, err
}
