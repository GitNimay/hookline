package delivery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/GitNimay/hookline/internal/domain"
	"github.com/GitNimay/hookline/internal/signing"
	"github.com/GitNimay/hookline/internal/ssrf"
)

func TestParseSchedule(t *testing.T) {
	s, err := ParseSchedule("default")
	if err != nil || s.MaxAttempts() != 8 {
		t.Fatalf("default: %v %v", s.MaxAttempts(), err)
	}
	var total time.Duration
	for _, d := range s {
		total += d
	}
	if total < 27*time.Hour || total > 29*time.Hour {
		t.Fatalf("default schedule spans %v, want ~28h", total)
	}
	if d, _ := ParseSchedule("demo"); d.MaxAttempts() != 6 {
		t.Fatal("demo schedule")
	}
	c, err := ParseSchedule("1s, 2m")
	if err != nil || len(c) != 2 || c[1] != 2*time.Minute {
		t.Fatalf("custom: %v %v", c, err)
	}
	for _, bad := range []string{"x", "1s,-2s", "1s,,2s"} {
		if _, err := ParseSchedule(bad); err == nil {
			t.Errorf("ParseSchedule(%q) want error", bad)
		}
	}
}

func TestDelayJitterBounds(t *testing.T) {
	s := DefaultSchedule
	for n := 1; n <= len(s); n++ {
		lo, _ := s.Delay(n, 0)
		hi, _ := s.Delay(n, 0.9999)
		if lo != s[n-1] || hi < lo || hi > s[n-1]+s[n-1]/5 {
			t.Fatalf("attempt %d: lo=%v hi=%v base=%v", n, lo, hi, s[n-1])
		}
	}
	if _, ok := s.Delay(8, 0.5); ok {
		t.Fatal("no delay after the last attempt")
	}
	if _, ok := s.Delay(0, 0.5); ok {
		t.Fatal("attempt 0 is invalid")
	}
}

func TestClassify(t *testing.T) {
	dnsErr := &net.DNSError{Err: "no such host", Name: "x"}
	cases := []struct {
		code int
		err  error
		o    Outcome
		kind string
	}{
		{200, nil, Success, domain.KindNone},
		{204, nil, Success, domain.KindNone},
		{301, nil, Retryable, domain.KindHTTPStatus},
		{400, nil, Retryable, domain.KindHTTPStatus},
		{410, nil, Gone, domain.KindHTTPStatus},
		{429, nil, Retryable, domain.KindHTTPStatus},
		{500, nil, Retryable, domain.KindHTTPStatus},
		{0, fmt.Errorf("dial: %w", ssrf.ErrBlocked), Permanent, domain.KindSSRF},
		{0, context.DeadlineExceeded, Retryable, domain.KindTimeout},
		{0, dnsErr, Retryable, domain.KindDNS},
		{0, &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, Retryable, domain.KindConnRefused},
		{0, errors.New("tls: handshake failure"), Retryable, domain.KindTLS},
		{200, errBodyTooLarge, Retryable, domain.KindBodyTooLarge},
		{0, errors.New("weird"), Retryable, domain.KindConnectionGeneric},
	}
	for _, c := range cases {
		o, k := Classify(c.code, c.err)
		if o != c.o || k != c.kind {
			t.Errorf("Classify(%d,%v)=%v,%s want %v,%s", c.code, c.err, o, k, c.o, c.kind)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if d := ParseRetryAfter("30", now); d != 30*time.Second {
		t.Fatal(d)
	}
	if d := ParseRetryAfter("99999", now); d != MaxRetryAfter {
		t.Fatal("cap", d)
	}
	if d := ParseRetryAfter(now.Add(2*time.Minute).Format(http.TimeFormat), now); d != 2*time.Minute {
		t.Fatal("date", d)
	}
	if d := ParseRetryAfter("garbage", now); d != 0 {
		t.Fatal(d)
	}
	if d := ParseRetryAfter("-5", now); d != 0 {
		t.Fatal(d)
	}
}

func TestDecide(t *testing.T) {
	s := DemoSchedule
	if d := Decide(Success, 1, s, 200, 0, 0); d.Status != domain.StatusSucceeded {
		t.Fatal(d)
	}
	if d := Decide(Gone, 1, s, 410, 0, 0); d.Status != domain.StatusDead || d.DeadReason != domain.ReasonEndpointGone {
		t.Fatal(d)
	}
	if d := Decide(Permanent, 1, s, 0, 0, 0); d.Status != domain.StatusDead || d.DeadReason != domain.ReasonSSRF {
		t.Fatal(d)
	}
	if d := Decide(Retryable, 1, s, 500, 0, 0); d.Status != domain.StatusRetrying || d.Delay != time.Second {
		t.Fatal(d)
	}
	// Retry-After wins over a shorter schedule delay for 429 and 503 only.
	if d := Decide(Retryable, 1, s, 429, 30*time.Second, 0); d.Delay != 30*time.Second {
		t.Fatal(d)
	}
	if d := Decide(Retryable, 1, s, 500, 30*time.Second, 0); d.Delay != time.Second {
		t.Fatal(d)
	}
	if d := Decide(Retryable, s.MaxAttempts(), s, 500, 0, 0); d.Status != domain.StatusDead || d.DeadReason != domain.ReasonExhausted {
		t.Fatal(d)
	}
}

func TestSenderSignsAndProtectsHeaders(t *testing.T) {
	secret := signing.NewSecret()
	var got http.Header
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(503)
		_, _ = w.Write([]byte(strings.Repeat("x", 10_000)))
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)

	s := NewSender(ssrf.NewPolicy([]string{u.Hostname()}), "test")
	body, _ := Envelope("order.created", time.Now(), []byte(`{"id":1}`))
	res := s.Send(context.Background(), Request{
		URL: srv.URL, ID: "evt_1", Body: body, Secrets: []string{secret},
		Headers: map[string]string{"X-Custom": "yes", "webhook-id": "evil"},
	})
	if res.Err != nil || res.StatusCode != 503 || res.RetryAfter != "7" {
		t.Fatalf("result %+v", res)
	}
	if len(res.Body) != MaxStoredBody {
		t.Fatalf("stored body %d bytes", len(res.Body))
	}
	if got.Get("webhook-id") != "evt_1" || got.Get("X-Custom") != "yes" || got.Get("User-Agent") != "Hookline/test" {
		t.Fatalf("headers %v", got)
	}
	if err := signing.Verify(secret, "evt_1", got.Get("webhook-timestamp"), got.Get("webhook-signature"), gotBody, time.Now()); err != nil {
		t.Fatal(err)
	}
}

func TestSenderBlocksLoopbackAndRedirects(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://169.254.169.254/", http.StatusFound)
	}))
	defer srv.Close()

	blocked := NewSender(ssrf.NewPolicy(nil), "t").Send(context.Background(), Request{URL: srv.URL, ID: "e", Body: []byte("{}"), Secrets: []string{signing.NewSecret()}})
	if o, k := Classify(blocked.StatusCode, blocked.Err); o != Permanent || k != domain.KindSSRF {
		t.Fatalf("loopback not blocked: %v %s %v", o, k, blocked.Err)
	}

	u, _ := url.Parse(srv.URL)
	res := NewSender(ssrf.NewPolicy([]string{u.Hostname()}), "t").Send(context.Background(), Request{URL: srv.URL, ID: "e", Body: []byte("{}"), Secrets: []string{signing.NewSecret()}})
	if res.StatusCode != http.StatusFound {
		t.Fatalf("redirect was followed: %+v", res)
	}
}

func TestValidateHeaders(t *testing.T) {
	if err := ValidateHeaders(map[string]string{"X-Api-Key": "abc"}); err != nil {
		t.Fatal(err)
	}
	for _, h := range []map[string]string{
		{"webhook-signature": "x"},
		{"Content-Type": "text/plain"},
		{"Bad Header": "x"},
		{"X-Ok": "line\r\nInjected: 1"},
	} {
		if err := ValidateHeaders(h); err == nil {
			t.Errorf("ValidateHeaders(%v) want error", h)
		}
	}
}
