package ssrf

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"testing"
	"time"
)

func TestIsBlockedIP(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1":        true,
		"127.1.2.3":        true,
		"10.1.2.3":         true,
		"172.16.0.1":       true,
		"172.31.255.255":   true,
		"192.168.1.1":      true,
		"169.254.169.254":  true,
		"100.64.0.1":       true,
		"0.0.0.0":          true,
		"224.0.0.1":        true,
		"::1":              true,
		"::":               true,
		"fe80::1":          true,
		"fd12:3456::1":     true,
		"::ffff:127.0.0.1": true, // IPv4-mapped loopback
		"::ffff:10.0.0.1":  true,
		"64:ff9b::a00:1":   true, // NAT64 of 10.0.0.1
		"8.8.8.8":          false,
		"1.1.1.1":          false,
		"172.32.0.1":       false,
		"2606:4700::1111":  false,
	}
	for s, want := range cases {
		if got := IsBlockedIP(netip.MustParseAddr(s)); got != want {
			t.Errorf("IsBlockedIP(%s)=%v want %v", s, got, want)
		}
	}
}

func TestCheckURL(t *testing.T) {
	p := NewPolicy([]string{"chaos-customer"})
	cases := []struct {
		url string
		ok  bool
	}{
		{"https://example.com/hook", true},
		{"http://example.com:8080/hook", true},
		{"http://chaos-customer:9000/hooks/a", true}, // allow-listed host and port
		{"ftp://example.com", false},
		{"https://example.com:22/", false},
		{"https://user:pw@example.com/", false},
		{"http://127.0.0.1/", false},
		{"http://169.254.169.254/latest/meta-data", false},
		{"http://[::1]:8080/", false},
		{"http://[::ffff:127.0.0.1]/", false},
		{"http://localhost/", false},
		{"http://metadata.google.internal/", false},
		{"not a url", false},
	}
	for _, c := range cases {
		err := p.CheckURL(c.url)
		if (err == nil) != c.ok {
			t.Errorf("CheckURL(%s) err=%v want ok=%v", c.url, err, c.ok)
		}
	}
}

// Decimal, hex and octal IPv4 forms and names resolving to private IPs are not caught by URL
// parsing, so the dial-time check must stop them on the real IP.
func TestDialBlocksResolvedPrivateAddresses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	_, port, _ := net.SplitHostPort(u.Host)

	dial := NewPolicy(nil).DialContext(time.Second)
	for _, host := range []string{"127.0.0.1", "localhost", "2130706433", "0x7f000001", "0177.0.0.1"} {
		_, err := dial(context.Background(), "tcp", net.JoinHostPort(host, port))
		if err == nil {
			t.Errorf("dial %s succeeded, want blocked", host)
			continue
		}
		// Non-standard notations may fail at DNS instead of the IP check; either way no connection is made.
		var dnsErr *net.DNSError
		if !errors.Is(err, ErrBlocked) && !errors.As(err, &dnsErr) {
			t.Errorf("dial %s: unexpected error %v", host, err)
		}
	}

	// The allow-list lets the in-cluster chaos customer through.
	allowed := NewPolicy([]string{"127.0.0.1"}).DialContext(time.Second)
	c, err := allowed(context.Background(), "tcp", net.JoinHostPort("127.0.0.1", port))
	if err != nil {
		t.Fatalf("allow-listed dial failed: %v", err)
	}
	_ = c.Close()
}
