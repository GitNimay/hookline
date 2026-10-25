# ADR-002: Transactional outbox for ingest

Status: accepted

## Context
Ingest must return quickly after durable storage, and an event must never be stored without being queued, or queued without being stored.

## Decision
`ingest` writes the `events` row and an `outbox` row in one transaction and returns 202. A relay loop in `ingest` (every 100 ms, or immediately when notified) claims up to 500 unpublished rows with `FOR UPDATE SKIP LOCKED`, publishes each with `Nats-Msg-Id = event_id`, waits for the acks and marks the acked rows published in the same transaction. The `scheduler` runs the same relay for rows older than one minute as a safety net.

## Consequences
- If NATS is down, ingest keeps accepting events; rows wait and the relay backs off.
- A crash between publish and commit re-publishes; JetStream deduplicates by message ID within 2 minutes, and downstream steps are idempotent anyway (ADR-003).
