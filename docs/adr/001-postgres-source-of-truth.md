# ADR-001: Postgres is the source of truth; NATS only carries work

Status: accepted

## Context
Deliveries retry for up to ~28 hours. Queues are good at moving work but poor at long, queryable delays, and a queue reset must never lose a retry.

## Decision
Every event, delivery and attempt lives in Postgres. NATS JetStream carries short-lived job messages that hold IDs only. Long retries are stored as `deliveries.next_attempt_at`; the scheduler republishes jobs when they are due. Workers load payloads from Postgres.

## Consequences
- A NATS stream can be wiped and rebuilt from Postgres (`pending`/`retrying` rows plus the outbox).
- Delivery history and dead letters are plain SQL queries.
- Postgres write throughput bounds the system; it is the component to size first.
