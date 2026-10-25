# ADR-007: API keys are checked locally with a short cache, not through control

Status: accepted

## Context
The plan suggested ingest asks `control` to check API keys. Ingest's p99 budget is 50 ms, and an extra network hop adds latency and a failure dependency.

## Decision
`ingest` and `control` share `internal/auth`: keys are hashed with SHA-256 and found through the unique `key_hash` index, with a 30 s in-process cache of results (negative ones too). `last_used_at` is written asynchronously every 10 s.

## Consequences
A revoked key or disabled tenant stops working within 30 s. Ingest never depends on `control` being up.
