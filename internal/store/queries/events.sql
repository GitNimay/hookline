-- name: InsertEventsWithOutbox :many
-- Events and their outbox rows in one atomic statement (one round trip, any batch size).
-- Events whose idempotency key already exists are skipped and not returned.
WITH input AS (
    SELECT unnest(sqlc.arg(ids)::uuid[]) AS id,
           unnest(sqlc.arg(event_types)::text[]) AS event_type,
           unnest(sqlc.arg(payloads)::text[])::jsonb AS payload,
           unnest(sqlc.arg(idempotency_keys)::text[]) AS idem,
           unnest(sqlc.arg(occurred_ats)::timestamptz[]) AS occurred_at
), e AS (
    INSERT INTO events (id, tenant_id, event_type, payload, idempotency_key, occurred_at, trace_parent)
    SELECT input.id, sqlc.arg(tenant_id)::uuid, input.event_type, input.payload, NULLIF(input.idem, ''), input.occurred_at, sqlc.arg(trace_parent)::text
    FROM input
    ON CONFLICT (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
    RETURNING events.id, events.tenant_id
), o AS (
    INSERT INTO outbox (event_id, tenant_id) SELECT e.id, e.tenant_id FROM e RETURNING outbox.event_id
)
SELECT o.event_id FROM o;

-- name: GetEventIDsByIdempotencyKeys :many
SELECT idempotency_key::text AS idempotency_key, id FROM events
WHERE tenant_id = $1 AND idempotency_key = ANY(sqlc.arg(keys)::text[]);

-- name: GetEvent :one
SELECT * FROM events WHERE id = $1 AND tenant_id = $2;

-- name: ListEvents :many
SELECT * FROM events
WHERE tenant_id = sqlc.arg(tenant_id)
  AND (sqlc.arg(event_type)::text = '' OR event_type = sqlc.arg(event_type)::text)
  AND (sqlc.narg(start_time)::timestamptz IS NULL OR created_at >= sqlc.narg(start_time)::timestamptz)
  AND (sqlc.narg(end_time)::timestamptz IS NULL OR created_at < sqlc.narg(end_time)::timestamptz)
  AND (sqlc.narg(cursor_time)::timestamptz IS NULL OR (created_at, id) < (sqlc.narg(cursor_time)::timestamptz, sqlc.arg(cursor_id)::uuid))
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(page_size);

-- name: ClaimOutbox :many
-- Claims unpublished rows created before older_than. Multiple relays never get the same row.
SELECT o.id, o.event_id, o.tenant_id, o.created_at, e.trace_parent, e.event_type
FROM outbox o
JOIN events e ON e.id = o.event_id
WHERE o.published_at IS NULL AND o.created_at <= sqlc.arg(older_than)::timestamptz
ORDER BY o.id
LIMIT sqlc.arg(batch_size)
FOR UPDATE OF o SKIP LOCKED;

-- name: MarkOutboxPublished :exec
UPDATE outbox SET published_at = now() WHERE id = ANY(sqlc.arg(ids)::bigint[]);

-- name: CountOutboxPending :one
SELECT count(*) FROM outbox WHERE published_at IS NULL;

-- name: ClearExpiredIdempotencyKeys :execrows
UPDATE events SET idempotency_key = NULL
WHERE id IN (
    SELECT e.id FROM events e
    WHERE e.idempotency_key IS NOT NULL AND e.created_at < $1
    LIMIT 1000
);

-- name: PruneEvents :execrows
DELETE FROM events WHERE id IN (SELECT e.id FROM events e WHERE e.created_at < $1 LIMIT 1000);

-- name: PruneOutbox :execrows
DELETE FROM outbox WHERE id IN (SELECT o.id FROM outbox o WHERE o.published_at < $1 LIMIT 1000);

-- name: PruneAttempts :execrows
DELETE FROM delivery_attempts WHERE id IN (SELECT a.id FROM delivery_attempts a WHERE a.created_at < $1 LIMIT 1000);
