// Package ssrf blocks webhook deliveries to internal addresses.
// The check runs in the dialer's Control hook on the IP actually being connected to,
// which also defeats DNS rebinding.
package ssrf

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ErrBlocked marks a destination refused by the SSRF policy. Not retryable.
var ErrBlocked = errors.New("ssrf: destination blocked")

var blocked = mustPrefixes(
	"0.0.0.0/8",         // "this" network
	"10.0.0.0/8",        // RFC 1918
	"100.64.0.0/10",     // CGNAT
	"127.0.0.0/8",       // loopback
	"169.254.0.0/16",    // link-local, cloud metadata (169.254.169.254)
	"172.16.0.0/12",     // RFC 1918
	"192.0.0.0/24",      // IETF protocol assignments
	"192.168.0.0/16",    // RFC 1918
	"198.18.0.0/15",     // benchmarking
	"224.0.0.0/4",       // multicast
	"240.0.0.0/4",       // reserved, broadcast
	"::/128",            // unspecified
	"::1/128",           // loopback
	"64:ff9b::/96",      // NAT64 can reach IPv4 internals
	"fc00::/7",          // unique local
	"fe80::/10",         // link-local
	"ff00::/8",          // multicast
	"fd00:ec2::254/128", // AWS IPv6 metadata
)

func mustPrefixes(ss ...string) []netip.Prefix {
	out := make([]netip.Prefix, len(ss))
	for i, s := range ss {
		out[i] = netip.MustParsePrefix(s)
	}
	return out
}

// IsBlockedIP reports whether ip must never be dialled. IPv4-mapped IPv6 is unwrapped first.
func IsBlockedIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() {
		return true
	}
	for _, p := range blocked {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// Policy decides which destinations are allowed.
type Policy struct {
	// Allow lists hostnames (exact, case-insensitive) that skip the IP check,
	// e.g. the in-cluster chaos customer. Never a blanket disable.
	Allow map[string]bool
}

// NewPolicy builds a policy from an allow-list of hostnames.
func NewPolicy(allow []string) Policy {
	p := Policy{Allow: map[string]bool{}}
	for _, h := range allow {
		if h = strings.ToLower(strings.TrimSpace(h)); h != "" {
			p.Allow[h] = true
		}
	}
	return p
}

// CheckURL validates scheme, port and literal IPs. Hostnames are checked again at dial time.
func (p Policy) CheckURL(raw string) error {
	if len(raw) > 2048 {
		return fmt.Errorf("%w: url longer than 2048 chars", ErrBlocked)
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("%w: invalid url", ErrBlocked)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%w: scheme must be http or https", ErrBlocked)
	}
	if u.User != nil {
		return fmt.Errorf("%w: credentials in url are not allowed", ErrBlocked)
	}
	host := strings.ToLower(u.Hostname())
	if p.Allow[host] {
		return nil
	}
	if err := checkPort(u.Port()); err != nil {
		return err
	}
	if ip, err := netip.ParseAddr(host); err == nil && IsBlockedIP(ip) {
		return fmt.Errorf("%w: %s", ErrBlocked, host)
	}
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".internal") {
		return fmt.Errorf("%w: %s", ErrBlocked, host)
	}
	return nil
}

func checkPort(port string) error {
	if port == "" {
		return nil // 80/443 by scheme
	}
	n, err := strconv.Atoi(port)
	if err != nil || (n != 80 && n != 443 && (n < 1024 || n > 65535)) {
		return fmt.Errorf("%w: port %s not allowed", ErrBlocked, port)
	}
	return nil
}

// DialContext returns a dialer that enforces the policy on every connection.
func (p Policy) DialContext(connectTimeout time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	safe := &net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second, Control: control}
	open := &net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		if p.Allow[strings.ToLower(host)] {
			return open.DialContext(ctx, network, addr)
		}
		if err := checkPort(port); err != nil {
			return nil, err
		}
		return safe.DialContext(ctx, network, addr)
	}
}

func control(_, address string, _ syscall.RawConn) error {
	ap, err := netip.ParseAddrPort(address)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrBlocked, address)
	}
	if IsBlockedIP(ap.Addr()) {
		return fmt.Errorf("%w: %s", ErrBlocked, ap.Addr())
	}
	return nil
}
