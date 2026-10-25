-- name: CreateEventType :one
INSERT INTO event_types (tenant_id, name, description)
VALUES ($1, $2, $3)
RETURNING *;

-- name: ListEventTypes :many
SELECT * FROM event_types WHERE tenant_id = $1 ORDER BY name;

-- name: DeleteEventType :execrows
DELETE FROM event_types WHERE tenant_id = $1 AND name = $2;

-- name: EventTypesExist :one
SELECT count(*) FROM event_types WHERE tenant_id = $1 AND name = ANY(sqlc.arg(names)::text[]);

-- name: CreateEndpoint :one
INSERT INTO endpoints (id, tenant_id, url, description, event_types, custom_headers, rate_limit_per_sec)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING *;

-- name: GetEndpoint :one
SELECT * FROM endpoints WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL;

-- name: ListEndpoints :many
SELECT * FROM endpoints
WHERE tenant_id = sqlc.arg(tenant_id) AND deleted_at IS NULL
  AND (sqlc.narg(cursor_time)::timestamptz IS NULL OR (created_at, id) < (sqlc.narg(cursor_time)::timestamptz, sqlc.arg(cursor_id)::uuid))
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(page_size);

-- name: UpdateEndpoint :one
UPDATE endpoints
SET url = $3, description = $4, event_types = $5, custom_headers = $6, rate_limit_per_sec = $7, updated_at = now()
WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL
RETURNING *;

-- name: SetEndpointStatus :one
UPDATE endpoints
SET status = $3, disabled_reason = $4,
    failing_since = CASE WHEN $3 = 'enabled' THEN NULL ELSE failing_since END,
    updated_at = now()
WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL
RETURNING *;

-- name: SoftDeleteEndpoint :execrows
UPDATE endpoints SET deleted_at = now(), status = 'disabled', disabled_reason = 'deleted', updated_at = now()
WHERE id = $1 AND tenant_id = $2 AND deleted_at IS NULL;

-- name: ListMatchingEndpoints :many
SELECT id FROM endpoints
WHERE tenant_id = $1 AND status = 'enabled' AND deleted_at IS NULL
  AND event_types && ARRAY[sqlc.arg(event_type)::text, '*'];

-- name: MarkEndpointFailing :exec
UPDATE endpoints SET failing_since = now() WHERE id = $1 AND failing_since IS NULL;

-- name: ClearEndpointFailing :exec
UPDATE endpoints SET failing_since = NULL WHERE id = $1 AND failing_since IS NOT NULL;

-- name: AutoDisableEndpoints :many
UPDATE endpoints
SET status = 'disabled', disabled_reason = 'auto_disabled', updated_at = now()
WHERE status = 'enabled' AND deleted_at IS NULL AND failing_since < $1
RETURNING id, tenant_id, url, failing_since;

-- name: InsertEndpointSecret :exec
INSERT INTO endpoint_secrets (id, endpoint_id, secret_ciphertext) VALUES ($1, $2, $3);

-- name: ListActiveSecrets :many
SELECT * FROM endpoint_secrets
WHERE endpoint_id = $1 AND (expires_at IS NULL OR expires_at > now())
ORDER BY created_at DESC;

-- name: ExpireOverlappingSecrets :exec
-- Secrets already in a rotation overlap expire now, so at most two stay active.
UPDATE endpoint_secrets SET expires_at = now()
WHERE endpoint_id = $1 AND expires_at > now();

-- name: ScheduleSecretExpiry :exec
UPDATE endpoint_secrets SET expires_at = $2
WHERE endpoint_id = $1 AND expires_at IS NULL;
