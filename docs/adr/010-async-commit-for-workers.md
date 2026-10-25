# ADR-010: Asynchronous commit for worker sessions only

Status: accepted

## Context
At 1,000 events/s the first benchmark showed workers waiting 62 s per second for a database connection: every delivery commits twice (claim, attempt record) and each commit waits for a WAL fsync. HTTP attempts took 19 ms at p99; the database was the bottleneck, and ingest p99 rose to 193 ms from the shared load.

## Decision
Worker sessions run with `synchronous_commit = off` (`DB_SYNCHRONOUS_COMMIT=false`). Ingest, dispatcher, control and scheduler keep the default `on`.

## Why this keeps the guarantees
- Ingest returns 202 only after a synchronous commit, and the dispatcher acks an event only after its deliveries are durably inserted, so no accepted event can be lost.
- A Postgres *crash* (not a clean restart) can lose a worker's last few hundred milliseconds of commits. The deliveries involved stay `pending` or `retrying`; the scheduler republishes `pending` ones after 10 minutes and `retrying` ones when their 5-minute lease expires. The result is a later retry and possibly a duplicate with the same `webhook-id`, which ADR-003 already allows. Nothing is lost.

## Consequences
After a Postgres crash, a few deliveries can be repeated or delayed by up to 10 minutes. Clean restarts flush the WAL and lose nothing.
