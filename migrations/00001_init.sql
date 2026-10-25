-- +goose Up
CREATE TABLE tenants (
    id               uuid PRIMARY KEY,
    name             text NOT NULL UNIQUE,
    status           text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    ingest_rps_limit integer NOT NULL DEFAULT 500 CHECK (ingest_rps_limit > 0),
    max_concurrency  integer NOT NULL DEFAULT 50 CHECK (max_concurrency > 0),
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
    id           uuid PRIMARY KEY,
    tenant_id    uuid REFERENCES tenants (id),
    scope        text NOT NULL CHECK (scope IN ('admin', 'tenant')),
    key_prefix   text NOT NULL,
    key_hash     bytea NOT NULL UNIQUE,
    last_used_at timestamptz,
    revoked_at   timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now(),
    CHECK ((scope = 'admin') = (tenant_id IS NULL))
);

CREATE TABLE event_types (
    tenant_id   uuid NOT NULL REFERENCES tenants (id),
    name        text NOT NULL,
    description text NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, name)
);

CREATE TABLE endpoints (
    id                 uuid PRIMARY KEY,
    tenant_id          uuid NOT NULL REFERENCES tenants (id),
    url                text NOT NULL,
    description        text NOT NULL DEFAULT '',
    event_types        text[] NOT NULL,
    custom_headers     jsonb NOT NULL DEFAULT '{}',
    rate_limit_per_sec integer NOT NULL DEFAULT 50 CHECK (rate_limit_per_sec > 0),
    status             text NOT NULL DEFAULT 'enabled' CHECK (status IN ('enabled', 'disabled')),
    disabled_reason    text NOT NULL DEFAULT '',
    failing_since      timestamptz,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    deleted_at         timestamptz
);
CREATE INDEX endpoints_tenant_status_idx ON endpoints (tenant_id, status);
CREATE INDEX endpoints_event_types_idx ON endpoints USING gin (event_types);
CREATE INDEX endpoints_failing_idx ON endpoints (failing_since) WHERE failing_since IS NOT NULL;

CREATE TABLE endpoint_secrets (
    id                uuid PRIMARY KEY,
    endpoint_id       uuid NOT NULL REFERENCES endpoints (id) ON DELETE CASCADE,
    secret_ciphertext bytea NOT NULL,
    created_at        timestamptz NOT NULL DEFAULT now(),
    expires_at        timestamptz
);
CREATE INDEX endpoint_secrets_endpoint_idx ON endpoint_secrets (endpoint_id, created_at DESC);

CREATE TABLE events (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL REFERENCES tenants (id),
    event_type      text NOT NULL,
    payload         jsonb NOT NULL,
    idempotency_key text,
    occurred_at     timestamptz NOT NULL,
    trace_parent    text NOT NULL DEFAULT '',
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX events_idempotency_idx ON events (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX events_tenant_created_idx ON events (tenant_id, created_at DESC, id DESC);
CREATE INDEX events_created_idx ON events (created_at);

CREATE TABLE outbox (
    id           bigserial PRIMARY KEY,
    event_id     uuid NOT NULL,
    tenant_id    uuid NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
CREATE INDEX outbox_published_idx ON outbox (published_at) WHERE published_at IS NOT NULL;

CREATE TABLE deliveries (
    id               uuid PRIMARY KEY,
    tenant_id        uuid NOT NULL,
    event_id         uuid NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    endpoint_id      uuid NOT NULL REFERENCES endpoints (id),
    status           text NOT NULL CHECK (status IN ('pending', 'retrying', 'succeeded', 'dead')),
    attempt_count    integer NOT NULL DEFAULT 0,
    replay_count     integer NOT NULL DEFAULT 0,
    next_attempt_at  timestamptz,
    last_attempt_at  timestamptz,
    last_status_code integer NOT NULL DEFAULT 0,
    last_error       text NOT NULL DEFAULT '',
    dead_reason      text NOT NULL DEFAULT '',
    succeeded_at     timestamptz,
    locked_until     timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (event_id, endpoint_id)
);
CREATE INDEX deliveries_due_idx ON deliveries (next_attempt_at) WHERE status = 'retrying';
CREATE INDEX deliveries_pending_idx ON deliveries (updated_at) WHERE status = 'pending';
CREATE INDEX deliveries_tenant_endpoint_idx ON deliveries (tenant_id, endpoint_id, status, created_at DESC);
CREATE INDEX deliveries_tenant_created_idx ON deliveries (tenant_id, created_at DESC, id DESC);

CREATE TABLE delivery_attempts (
    id                    uuid PRIMARY KEY,
    delivery_id           uuid NOT NULL REFERENCES deliveries (id) ON DELETE CASCADE,
    tenant_id             uuid NOT NULL,
    attempt_number        integer NOT NULL,
    started_at            timestamptz NOT NULL,
    duration_ms           integer NOT NULL,
    status_code           integer NOT NULL DEFAULT 0,
    error_kind            text NOT NULL,
    error_message         text NOT NULL DEFAULT '',
    response_body_snippet text NOT NULL DEFAULT '',
    created_at            timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX delivery_attempts_delivery_idx ON delivery_attempts (delivery_id, attempt_number);
CREATE INDEX delivery_attempts_created_idx ON delivery_attempts (created_at);

-- +goose Down
DROP TABLE delivery_attempts;
DROP TABLE deliveries;
DROP TABLE outbox;
DROP TABLE events;
DROP TABLE endpoint_secrets;
DROP TABLE endpoints;
DROP TABLE event_types;
DROP TABLE api_keys;
DROP TABLE tenants;
