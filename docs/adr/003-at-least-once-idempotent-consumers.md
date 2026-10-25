# ADR-003: At-least-once delivery with idempotent consumers at every hop

Status: accepted

## Decision
- Ingest dedups by `(tenant_id, idempotency_key)` (partial unique index; the scheduler clears keys after 24 h).
- Publishes carry message IDs; JetStream drops duplicates within 2 minutes.
- The dispatcher inserts deliveries with `ON CONFLICT (event_id, endpoint_id) DO NOTHING`, reads them back and publishes jobs only for untouched `pending` rows.
- Workers claim a delivery with a conditional `UPDATE ... WHERE status IN (pending, retrying) AND attempt_count = $n AND not locked`, which also sets a 30 s lock. No row means the job is stale or a duplicate: ack and skip. If another attempt holds the lock, the job is naked until the lock expires.
- Completion is another conditional update on `attempt_count`, in the same transaction as the attempt row.
- Messages are acked only after the result is committed.

Job message IDs are `<delivery_id>-<attempt>-<nonce>`. The nonce (`d` for dispatch, the lease time for scheduler retries, `p<ms>` for reconciliation, `r<n>` for replays) keeps legitimate republishes of the same attempt from being swallowed by the dedup window, while duplicate publishes of the same schedule still collapse. This extends the plan's `<delivery_id>-<attempt>`.

## Consequences
Duplicates happen only when an HTTP request succeeded but its result could not be committed (crash, Postgres outage). They carry the same `webhook-id` (the event ID), so receivers deduplicate.
