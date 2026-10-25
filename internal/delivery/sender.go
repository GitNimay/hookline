package delivery

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/GitNimay/hookline/internal/signing"
	"github.com/GitNimay/hookline/internal/ssrf"
)

// Limits on what the sender reads and stores.
const (
	TotalTimeout   = 15 * time.Second
	ConnectTimeout = 5 * time.Second
	MaxReadBody    = 64 << 10
	MaxStoredBody  = 4 << 10
)

var errBodyTooLarge = errors.New("response body too large")

// Sender posts signed webhooks through an SSRF-safe, shared transport.
type Sender struct {
	client    *http.Client
	userAgent string
}

// NewSender builds the shared HTTP client: no redirects, TLS 1.2+, connection reuse.
func NewSender(p ssrf.Policy, version string) *Sender {
	tr := &http.Transport{
		DialContext:           p.DialContext(ConnectTimeout),
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   50,
		MaxConnsPerHost:       200,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   ConnectTimeout,
		ResponseHeaderTimeout: TotalTimeout,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
		Proxy:                 nil, // a proxy would bypass the dial-time SSRF check
	}
	return &Sender{
		client: &http.Client{
			Transport:     tr,
			Timeout:       TotalTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		userAgent: "Hookline/" + version,
	}
}

// Request is one webhook to send.
type Request struct {
	URL     string
	ID      string // webhook-id: the event ID, stable across retries and endpoints
	Body    []byte
	Secrets []string // newest first; two during rotation
	Headers map[string]string
}

// Result is what happened.
type Result struct {
	StatusCode int
	Duration   time.Duration
	Err        error
	Body       string // truncated to MaxStoredBody, valid UTF-8
	RetryAfter string
}

// Send signs and posts one webhook. Network failures come back in Result.Err.
func (s *Sender) Send(ctx context.Context, r Request) Result {
	start := time.Now()
	sig, err := signing.Sign(r.ID, start, r.Body, r.Secrets...)
	if err != nil {
		return Result{Err: err}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.URL, bytes.NewReader(r.Body))
	if err != nil {
		return Result{Err: fmt.Errorf("%w: %w", ssrf.ErrBlocked, err)}
	}
	for k, v := range r.Headers {
		req.Header.Set(k, v)
	}
	// Set after custom headers so they can never be overridden.
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", s.userAgent)
	req.Header.Set("webhook-id", r.ID)
	req.Header.Set("webhook-timestamp", fmt.Sprint(start.Unix()))
	req.Header.Set("webhook-signature", sig)

	resp, err := s.client.Do(req)
	if err != nil {
		return Result{Duration: time.Since(start), Err: err}
	}
	defer func() { _ = resp.Body.Close() }()
	body, rerr := io.ReadAll(io.LimitReader(resp.Body, MaxReadBody+1))
	res := Result{StatusCode: resp.StatusCode, Duration: time.Since(start), Body: snippet(body), RetryAfter: resp.Header.Get("Retry-After")}
	if len(body) > MaxReadBody {
		res.Err = errBodyTooLarge
	} else if rerr != nil && resp.StatusCode/100 != 2 {
		res.Err = rerr
	}
	return res
}

func snippet(b []byte) string {
	if len(b) > MaxStoredBody {
		b = b[:MaxStoredBody]
	}
	s := strings.ToValidUTF8(string(b), string(utf8.RuneError))
	return strings.ReplaceAll(s, "\x00", "")
}

// Envelope builds the webhook body {type, timestamp, data}.
func Envelope(eventType string, ts time.Time, data json.RawMessage) ([]byte, error) {
	return json.Marshal(struct {
		Type      string          `json:"type"`
		Timestamp string          `json:"timestamp"`
		Data      json.RawMessage `json:"data"`
	}{eventType, ts.UTC().Format(time.RFC3339Nano), data})
}

var headerName = regexp.MustCompile(`^[A-Za-z0-9!#$%&'*+.^_|~-]{1,64}$`)

var reservedHeaders = map[string]bool{
	"content-type": true, "content-length": true, "user-agent": true, "host": true,
	"webhook-id": true, "webhook-timestamp": true, "webhook-signature": true,
	"connection": true, "transfer-encoding": true, "te": true, "upgrade": true, "trailer": true,
	"keep-alive": true, "proxy-authorization": true, "proxy-connection": true,
}

// ValidateHeaders checks endpoint custom headers: valid names, no reserved ones, sane sizes.
func ValidateHeaders(h map[string]string) error {
	if len(h) > 20 {
		return errors.New("at most 20 custom headers")
	}
	for k, v := range h {
		if !headerName.MatchString(k) {
			return fmt.Errorf("invalid header name %q", k)
		}
		if reservedHeaders[strings.ToLower(k)] {
			return fmt.Errorf("header %q is reserved", k)
		}
		if len(v) > 1024 || strings.ContainsAny(v, "\r\n\x00") {
			return fmt.Errorf("invalid value for header %q", k)
		}
	}
	return nil
}
