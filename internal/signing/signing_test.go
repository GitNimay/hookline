package signing

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	standardwebhooks "github.com/standard-webhooks/standard-webhooks/libraries/go"
)

// Test vector from the Standard Webhooks specification / reference libraries.
const (
	vecSecret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	vecID     = "msg_p5jXN8AQM9LWM0D4loKWxJek"
	vecTS     = 1614265330
	vecBody   = `{"test": 2432232314}`
	vecSig    = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
)

func TestSignMatchesSpecVector(t *testing.T) {
	got, err := Sign(vecID, time.Unix(vecTS, 0), []byte(vecBody), vecSecret)
	if err != nil {
		t.Fatal(err)
	}
	if got != vecSig {
		t.Fatalf("got %s want %s", got, vecSig)
	}
}

func TestVerifiedByReferenceLibrary(t *testing.T) {
	secret := NewSecret()
	old := NewSecret()
	now := time.Now()
	body := []byte(`{"type":"order.created","data":{"id":1}}`)
	sig, err := Sign("evt_1", now, body, secret, old)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(strings.Fields(sig)); n != 2 {
		t.Fatalf("want 2 signatures during rotation, got %d", n)
	}
	h := http.Header{}
	h.Set("webhook-id", "evt_1")
	h.Set("webhook-timestamp", strconv.FormatInt(now.Unix(), 10))
	h.Set("webhook-signature", sig)
	// Receivers holding either the new or the old secret must verify.
	for _, s := range []string{secret, old} {
		wh, err := standardwebhooks.NewWebhook(s)
		if err != nil {
			t.Fatal(err)
		}
		if err := wh.Verify(body, h); err != nil {
			t.Fatalf("reference library rejected signature: %v", err)
		}
		if err := Verify(s, "evt_1", h.Get("webhook-timestamp"), sig, body, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := Verify(NewSecret(), "evt_1", h.Get("webhook-timestamp"), sig, body, now); err == nil {
		t.Fatal("wrong secret verified")
	}
	if err := Verify(secret, "evt_1", h.Get("webhook-timestamp"), sig, []byte("tampered"), now); err == nil {
		t.Fatal("tampered body verified")
	}
	if err := Verify(secret, "evt_1", h.Get("webhook-timestamp"), sig, body, now.Add(10*time.Minute)); err == nil {
		t.Fatal("stale timestamp verified")
	}
}

func TestBox(t *testing.T) {
	b, err := NewBox(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	s := NewSecret()
	sealed := b.Seal(s)
	if strings.Contains(string(sealed), s) {
		t.Fatal("plaintext visible")
	}
	got, err := b.Open(sealed)
	if err != nil || got != s {
		t.Fatalf("open: %v %v", got, err)
	}
	sealed[len(sealed)-1] ^= 1
	if _, err := b.Open(sealed); err == nil {
		t.Fatal("tampered ciphertext opened")
	}
}
