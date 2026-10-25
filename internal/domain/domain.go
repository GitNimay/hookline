// Package domain holds Hookline's core types, IDs, errors and the delivery state machine.
// It imports nothing internal.
package domain

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Sentinel errors, mapped to gRPC codes in one place (server.ToStatus).
var (
	ErrNotFound        = errors.New("not found")
	ErrAlreadyExists   = errors.New("already exists")
	ErrInvalidArgument = errors.New("invalid argument")
	ErrUnauthenticated = errors.New("unauthenticated")
	ErrPermission      = errors.New("permission denied")
	ErrUnavailable     = errors.New("unavailable")
)

// FieldError is an invalid-argument error on one request field.
type FieldError struct {
	Field  string
	Reason string
}

func (e *FieldError) Error() string { return e.Field + ": " + e.Reason }

// Is makes errors.Is(err, ErrInvalidArgument) true for field errors.
func (e *FieldError) Is(target error) bool { return target == ErrInvalidArgument }

// Invalid builds a FieldError.
func Invalid(field, format string, args ...any) error {
	return &FieldError{Field: field, Reason: fmt.Sprintf(format, args...)}
}

// API ID prefixes.
const (
	PrefixTenant   = "ten_"
	PrefixEndpoint = "ep_"
	PrefixEvent    = "evt_"
	PrefixDelivery = "dlv_"
	PrefixAttempt  = "att_"
	PrefixKey      = "key_"
)

// NewID returns a new UUIDv7.
func NewID() uuid.UUID { return uuid.Must(uuid.NewV7()) }

// FormatID renders a UUID as a prefixed API ID, e.g. "evt_0192...".
func FormatID(prefix string, id uuid.UUID) string {
	return prefix + strings.ReplaceAll(id.String(), "-", "")
}

// ParseID parses a prefixed API ID. A wrong prefix or bad UUID is ErrNotFound,
// so callers never learn whether an ID exists in another form.
func ParseID(prefix, s string) (uuid.UUID, error) {
	raw, ok := strings.CutPrefix(s, prefix)
	if !ok || len(raw) != 32 {
		return uuid.Nil, ErrNotFound
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, ErrNotFound
	}
	return id, nil
}

// Delivery statuses.
const (
	StatusPending   = "pending"
	StatusRetrying  = "retrying"
	StatusSucceeded = "succeeded"
	StatusDead      = "dead"
)

// Dead reasons.
const (
	ReasonExhausted       = "attempts_exhausted"
	ReasonEndpointGone    = "endpoint_gone"
	ReasonEndpointDisable = "endpoint_disabled"
	ReasonSSRF            = "ssrf_blocked"
)

// Error kinds recorded on attempts.
const (
	KindNone              = "none"
	KindTimeout           = "timeout"
	KindConnRefused       = "connection_refused"
	KindDNS               = "dns"
	KindTLS               = "tls"
	KindSSRF              = "ssrf_blocked"
	KindHTTPStatus        = "http_status"
	KindBodyTooLarge      = "body_too_large"
	KindCircuitOpen       = "circuit_open"
	KindRateLimited       = "rate_limited"
	KindEndpointDisabled  = "endpoint_disabled"
	KindConnectionGeneric = "connection_error"
)

// MatchesEventType reports whether an endpoint subscribed to subs wants eventType.
func MatchesEventType(subs []string, eventType string) bool {
	for _, s := range subs {
		if s == "*" || s == eventType {
			return true
		}
	}
	return false
}

// EventsSubject is the NATS subject for a tenant's events.
func EventsSubject(tenantID string) string { return "events." + tenantID }

// DeliveriesSubject is the NATS subject for a tenant's delivery jobs.
func DeliveriesSubject(tenantID string) string { return "deliveries." + tenantID }

// DeliverConsumer is the durable consumer name for a tenant on the DELIVERIES stream.
func DeliverConsumer(tenantID string) string { return "deliver_" + tenantID }

// JobMsgID builds the NATS message ID for a delivery job. The nonce distinguishes
// legitimate republishes (a new schedule or a replay) from duplicate publishes of the same one.
func JobMsgID(deliveryID string, attempt int32, nonce string) string {
	return fmt.Sprintf("%s-%d-%s", deliveryID, attempt, nonce)
}
