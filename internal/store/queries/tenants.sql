-- name: CreateTenant :one
INSERT INTO tenants (id, name, ingest_rps_limit, max_concurrency)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: GetTenant :one
SELECT * FROM tenants WHERE id = $1;

-- name: ListTenants :many
SELECT * FROM tenants
WHERE (sqlc.narg(cursor_time)::timestamptz IS NULL OR (created_at, id) < (sqlc.narg(cursor_time)::timestamptz, sqlc.arg(cursor_id)::uuid))
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(page_size);

-- name: ListActiveTenants :many
SELECT * FROM tenants WHERE status = 'active' ORDER BY created_at;

-- name: UpdateTenant :one
UPDATE tenants
SET name = $2, status = $3, ingest_rps_limit = $4, max_concurrency = $5, updated_at = now()
WHERE id = $1
RETURNING *;

-- name: CreateAPIKey :one
INSERT INTO api_keys (id, tenant_id, scope, key_prefix, key_hash)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: UpsertAdminKey :exec
INSERT INTO api_keys (id, tenant_id, scope, key_prefix, key_hash)
VALUES ($1, NULL, 'admin', $2, $3)
ON CONFLICT (key_hash) DO UPDATE SET revoked_at = NULL;

-- name: GetAPIKeyByHash :one
SELECT k.id, k.tenant_id, k.scope, k.revoked_at, COALESCE(t.status, 'active')::text AS tenant_status
FROM api_keys k
LEFT JOIN tenants t ON t.id = k.tenant_id
WHERE k.key_hash = $1;

-- name: TouchAPIKey :exec
UPDATE api_keys SET last_used_at = now() WHERE id = $1;

-- name: ListAPIKeys :many
SELECT * FROM api_keys WHERE tenant_id = $1 ORDER BY created_at DESC;

-- name: RevokeAPIKey :execrows
UPDATE api_keys SET revoked_at = now()
WHERE id = $1 AND tenant_id = $2 AND revoked_at IS NULL;
