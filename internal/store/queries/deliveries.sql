-- name: CreateDeliveries :exec
-- One delivery per (event, endpoint); re-running for the same event is a no-op.
INSERT INTO deliveries (id, tenant_id, event_id, endpoint_id, status)
SELECT unnest(sqlc.arg(ids)::uuid[]), sqlc.arg(tenant_id)::uuid, sqlc.arg(event_id)::uuid, unnest(sqlc.arg(endpoint_ids)::uuid[]), 'pending'
ON CONFLICT (event_id, endpoint_id) DO NOTHING;

-- name: ListEventDeliveries :many
SELECT * FROM deliveries WHERE event_id = $1;

-- name: ClaimDeliveryWithEvent :one
-- ClaimDelivery plus the event payload, in one round trip.
WITH c AS (
    UPDATE deliveries
    SET locked_until = now() + make_interval(secs => sqlc.arg(lock_secs)::float8)
    WHERE deliveries.id = sqlc.arg(id) AND deliveries.tenant_id = sqlc.arg(tenant_id)
      AND deliveries.status IN ('pending', 'retrying') AND deliveries.attempt_count = sqlc.arg(attempt_count)
      AND (deliveries.locked_until IS NULL OR deliveries.locked_until < now())
    RETURNING deliveries.id, deliveries.event_id, deliveries.attempt_count
)
SELECT c.id, c.attempt_count, e.event_type, e.payload, e.occurred_at, e.created_at AS event_created_at
FROM c JOIN events e ON e.id = c.event_id;

-- name: RecordAttempt :execrows
-- Completes an attempt and stores its log row atomically. 0 rows: completed elsewhere.
WITH u AS (
    UPDATE deliveries
    SET status = sqlc.arg(status), attempt_count = attempt_count + 1,
        next_attempt_at = sqlc.narg(next_attempt_at), last_attempt_at = now(),
        last_status_code = sqlc.arg(status_code), last_error = sqlc.arg(error_message),
        dead_reason = sqlc.arg(dead_reason),
        succeeded_at = CASE WHEN sqlc.arg(status) = 'succeeded' THEN now() ELSE NULL END,
        locked_until = NULL, updated_at = now()
    WHERE deliveries.id = sqlc.arg(delivery_id) AND deliveries.attempt_count = sqlc.arg(attempt_count) AND deliveries.status IN ('pending', 'retrying')
    RETURNING deliveries.id, deliveries.tenant_id
)
INSERT INTO delivery_attempts (id, delivery_id, tenant_id, attempt_number, started_at, duration_ms, status_code, error_kind, error_message, response_body_snippet)
SELECT sqlc.arg(attempt_id), u.id, u.tenant_id, sqlc.arg(attempt_count)::int + 1, sqlc.arg(started_at), sqlc.arg(duration_ms),
       sqlc.arg(status_code), sqlc.arg(error_kind), sqlc.arg(error_message), sqlc.arg(response_body_snippet)
FROM u;

-- name: DeferDelivery :execrows
-- Reschedules without counting an attempt (circuit open).
UPDATE deliveries
SET status = 'retrying', next_attempt_at = sqlc.arg(next_attempt_at), locked_until = NULL, updated_at = now()
WHERE id = sqlc.arg(id) AND tenant_id = sqlc.arg(tenant_id) AND attempt_count = sqlc.arg(attempt_count)
  AND status IN ('pending', 'retrying') AND (locked_until IS NULL OR locked_until < now());

-- name: KillDelivery :execrows
UPDATE deliveries
SET status = 'dead', dead_reason = sqlc.arg(dead_reason), next_attempt_at = NULL, locked_until = NULL, updated_at = now()
WHERE id = sqlc.arg(id) AND tenant_id = sqlc.arg(tenant_id) AND attempt_count = sqlc.arg(attempt_count)
  AND status IN ('pending', 'retrying');

-- name: ClaimDueDeliveries :many
-- Takes a lease on due retries by pushing next_attempt_at forward; if the publish is lost, the lease expires and it is claimed again.
WITH due AS (
    SELECT id, next_attempt_at AS due_at FROM deliveries
    WHERE status = 'retrying' AND next_attempt_at <= now()
    ORDER BY next_attempt_at
    LIMIT sqlc.arg(batch_size)
    FOR UPDATE SKIP LOCKED
)
UPDATE deliveries d
SET next_attempt_at = now() + make_interval(secs => sqlc.arg(lease_secs)::float8), updated_at = now()
FROM due WHERE d.id = due.id
RETURNING d.id, d.tenant_id, d.event_id, d.endpoint_id, d.attempt_count, d.next_attempt_at, due.due_at::timestamptz AS due_at;

-- name: ClaimStalePending :many
WITH stale AS (
    SELECT id FROM deliveries
    WHERE status = 'pending' AND updated_at < sqlc.arg(older_than)::timestamptz
      AND (locked_until IS NULL OR locked_until < now())
    LIMIT sqlc.arg(batch_size)
    FOR UPDATE SKIP LOCKED
)
UPDATE deliveries d SET updated_at = now()
FROM stale WHERE d.id = stale.id
RETURNING d.id, d.tenant_id, d.event_id, d.endpoint_id, d.attempt_count, d.updated_at;

-- name: ReplayDelivery :one
UPDATE deliveries
SET status = 'pending', attempt_count = 0, replay_count = replay_count + 1, next_attempt_at = NULL,
    dead_reason = '', locked_until = NULL, updated_at = now()
WHERE id = $1 AND tenant_id = $2 AND status IN ('dead', 'succeeded')
RETURNING *;

-- name: ReplayEndpointBatch :many
WITH batch AS (
    SELECT x.id FROM deliveries x
    WHERE x.tenant_id = sqlc.arg(tenant_id) AND x.endpoint_id = sqlc.arg(endpoint_id)
      AND CASE WHEN sqlc.narg(since)::timestamptz IS NULL THEN x.status = 'dead'
               ELSE x.status IN ('dead', 'succeeded') AND x.created_at >= sqlc.narg(since)::timestamptz END
    LIMIT 500
    FOR UPDATE SKIP LOCKED
)
UPDATE deliveries d
SET status = 'pending', attempt_count = 0, replay_count = replay_count + 1, next_attempt_at = NULL,
    dead_reason = '', locked_until = NULL, updated_at = now()
FROM batch WHERE d.id = batch.id
RETURNING d.*;

-- name: GetDelivery :one
SELECT * FROM deliveries WHERE id = $1 AND tenant_id = $2;

-- name: ListDeliveries :many
SELECT * FROM deliveries
WHERE tenant_id = sqlc.arg(tenant_id)
  AND (sqlc.narg(event_id)::uuid IS NULL OR event_id = sqlc.narg(event_id)::uuid)
  AND (sqlc.narg(endpoint_id)::uuid IS NULL OR endpoint_id = sqlc.narg(endpoint_id)::uuid)
  AND (sqlc.arg(status)::text = '' OR status = sqlc.arg(status)::text)
  AND (sqlc.narg(start_time)::timestamptz IS NULL OR created_at >= sqlc.narg(start_time)::timestamptz)
  AND (sqlc.narg(end_time)::timestamptz IS NULL OR created_at < sqlc.narg(end_time)::timestamptz)
  AND (sqlc.narg(cursor_time)::timestamptz IS NULL OR (created_at, id) < (sqlc.narg(cursor_time)::timestamptz, sqlc.arg(cursor_id)::uuid))
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(page_size);

-- name: ListAttempts :many
SELECT * FROM delivery_attempts WHERE delivery_id = $1 AND tenant_id = $2 ORDER BY attempt_number;

-- name: CountRetryingByTenant :many
SELECT tenant_id, count(*) AS retrying FROM deliveries WHERE status = 'retrying' GROUP BY tenant_id;
