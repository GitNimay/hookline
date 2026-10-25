# ADR-006: Standard Webhooks for headers and signatures

Status: accepted

## Decision
Send `webhook-id` (the event ID, stable across retries and endpoints), `webhook-timestamp` (Unix seconds of this attempt) and `webhook-signature` (`v1,<base64 HMAC-SHA256>` over `<id>.<timestamp>.<body>`). Secrets are `whsec_` + base64 of 32 random bytes. During a rotation both secrets sign, space separated.

## Consequences
Receivers verify with off-the-shelf libraries. Our signer is checked against the specification's test vector and the official Go library in unit tests, and the chaos customer verifies every delivery with the official library.
