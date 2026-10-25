// Package signing implements Standard Webhooks signatures and encryption of secrets at rest.
package signing

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"
)

// SecretPrefix starts every signing secret.
const SecretPrefix = "whsec_"

// NewSecret returns "whsec_" + base64 of 32 random bytes.
func NewSecret() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return SecretPrefix + base64.StdEncoding.EncodeToString(b)
}

func secretBytes(secret string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, SecretPrefix))
}

// Sign returns the webhook-signature header value: one "v1,<base64>" per secret, space separated.
// It signs exactly "<id>.<timestamp>.<body>".
func Sign(id string, ts time.Time, body []byte, secrets ...string) (string, error) {
	sigs := make([]string, 0, len(secrets))
	for _, s := range secrets {
		key, err := secretBytes(s)
		if err != nil {
			return "", errors.New("invalid signing secret")
		}
		sigs = append(sigs, "v1,"+base64.StdEncoding.EncodeToString(mac(key, id, ts.Unix(), body)))
	}
	return strings.Join(sigs, " "), nil
}

func mac(key []byte, id string, ts int64, body []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(id + "." + strconv.FormatInt(ts, 10) + "."))
	h.Write(body)
	return h.Sum(nil)
}

// Tolerance is how far a webhook timestamp may be from now.
const Tolerance = 5 * time.Minute

// Verify checks a webhook-signature header against secret, in constant time.
func Verify(secret, id, timestamp, signatureHeader string, body []byte, now time.Time) error {
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return errors.New("bad timestamp")
	}
	if d := now.Sub(time.Unix(ts, 0)); d > Tolerance || d < -Tolerance {
		return errors.New("timestamp outside tolerance")
	}
	key, err := secretBytes(secret)
	if err != nil {
		return errors.New("invalid secret")
	}
	want := mac(key, id, ts, body)
	for _, part := range strings.Fields(signatureHeader) {
		v, sig, ok := strings.Cut(part, ",")
		if !ok || v != "v1" {
			continue
		}
		got, err := base64.StdEncoding.DecodeString(sig)
		if err == nil && hmac.Equal(got, want) {
			return nil
		}
	}
	return errors.New("no matching signature")
}

// Box encrypts secrets with AES-256-GCM.
type Box struct{ aead cipher.AEAD }

// NewBox builds a Box from a 32-byte master key.
func NewBox(key []byte) (*Box, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Box{aead: aead}, nil
}

// Seal returns nonce || ciphertext.
func (b *Box) Seal(plain string) []byte {
	nonce := make([]byte, b.aead.NonceSize())
	_, _ = rand.Read(nonce)
	return b.aead.Seal(nonce, nonce, []byte(plain), nil)
}

// Open reverses Seal.
func (b *Box) Open(sealed []byte) (string, error) {
	n := b.aead.NonceSize()
	if len(sealed) < n {
		return "", errors.New("ciphertext too short")
	}
	p, err := b.aead.Open(nil, sealed[:n], sealed[n:], nil)
	return string(p), err
}
