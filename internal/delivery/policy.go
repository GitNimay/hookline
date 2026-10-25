// Package delivery sends webhooks and holds the pure retry and classification rules.
package delivery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/GitNimay/hookline/internal/domain"
	"github.com/GitNimay/hookline/internal/ssrf"
)

// Schedule lists the delays before attempts 2, 3, ...; attempt 1 is immediate.
type Schedule []time.Duration

// Named schedules.
var (
	DefaultSchedule = Schedule{5 * time.Second, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour, 5 * time.Hour, 10 * time.Hour, 10 * time.Hour}
	DemoSchedule    = Schedule{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 20 * time.Second}
)

// ParseSchedule accepts "default", "demo" or a comma-separated list of durations.
func ParseSchedule(s string) (Schedule, error) {
	switch s {
	case "", "default":
		return DefaultSchedule, nil
	case "demo":
		return DemoSchedule, nil
	}
	var out Schedule
	for _, p := range strings.Split(s, ",") {
		d, err := time.ParseDuration(strings.TrimSpace(p))
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("RETRY_SCHEDULE: bad duration %q", p)
		}
		out = append(out, d)
	}
	return out, nil
}

// MaxAttempts is the total number of attempts.
func (s Schedule) MaxAttempts() int { return len(s) + 1 }

// Delay returns the wait after attempt n (1-based) with positive jitter of up to 20%.
// jitter is a random number in [0,1). ok is false when n was the last attempt.
func (s Schedule) Delay(n int, jitter float64) (d time.Duration, ok bool) {
	if n < 1 || n > len(s) {
		return 0, false
	}
	base := s[n-1]
	return base + time.Duration(float64(base)*0.2*jitter), true
}

// Outcome classifies one attempt.
type Outcome int

// Outcomes.
const (
	Success   Outcome = iota
	Retryable         // try again later
	Gone              // 410: disable endpoint, dead
	Permanent         // never retry (SSRF blocked)
)

func (o Outcome) String() string {
	return [...]string{"success", "retryable", "gone", "permanent"}[o]
}

// Classify maps an HTTP result to an outcome and error kind.
func Classify(code int, err error) (Outcome, string) {
	if err != nil {
		return classifyErr(err)
	}
	switch {
	case code >= 200 && code < 300:
		return Success, domain.KindNone
	case code == http.StatusGone:
		return Gone, domain.KindHTTPStatus
	default:
		return Retryable, domain.KindHTTPStatus
	}
}

func classifyErr(err error) (Outcome, string) {
	var dnsErr *net.DNSError
	var opErr *net.OpError
	switch {
	case errors.Is(err, ssrf.ErrBlocked):
		return Permanent, domain.KindSSRF
	case errors.Is(err, errBodyTooLarge):
		return Retryable, domain.KindBodyTooLarge
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded):
		return Retryable, domain.KindTimeout
	case errors.As(err, &dnsErr):
		return Retryable, domain.KindDNS
	case errors.Is(err, syscall.ECONNREFUSED):
		return Retryable, domain.KindConnRefused
	case strings.Contains(err.Error(), "tls:") || strings.Contains(err.Error(), "x509:"):
		return Retryable, domain.KindTLS
	case errors.As(err, &opErr) && opErr.Timeout():
		return Retryable, domain.KindTimeout
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return Retryable, domain.KindTimeout
	}
	return Retryable, domain.KindConnectionGeneric
}

// MaxRetryAfter caps a receiver's Retry-After.
const MaxRetryAfter = time.Hour

// ParseRetryAfter reads seconds or an HTTP date; 0 when absent or invalid.
func ParseRetryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return 0
	}
	var d time.Duration
	if secs, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
		d = time.Duration(secs) * time.Second
	} else if t, err := http.ParseTime(v); err == nil {
		d = t.Sub(now)
	}
	return min(max(d, 0), MaxRetryAfter)
}

// Decision is the delivery's next state after an attempt.
type Decision struct {
	Status     string
	Delay      time.Duration // until next attempt, when Status is retrying
	DeadReason string
}

// Decide applies the state machine after attempt number n.
// For 429/503, the next attempt waits at least retryAfter.
func Decide(o Outcome, n int, s Schedule, code int, retryAfter time.Duration, jitter float64) Decision {
	switch o {
	case Success:
		return Decision{Status: domain.StatusSucceeded}
	case Gone:
		return Decision{Status: domain.StatusDead, DeadReason: domain.ReasonEndpointGone}
	case Permanent:
		return Decision{Status: domain.StatusDead, DeadReason: domain.ReasonSSRF}
	}
	d, ok := s.Delay(n, jitter)
	if !ok {
		return Decision{Status: domain.StatusDead, DeadReason: domain.ReasonExhausted}
	}
	if code == http.StatusTooManyRequests || code == http.StatusServiceUnavailable {
		d = max(d, retryAfter)
	}
	return Decision{Status: domain.StatusRetrying, Delay: d}
}
