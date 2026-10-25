# ADR-005: NATS JetStream over Kafka

Status: accepted

## Decision
Use NATS JetStream for queues.

## Why
A single NATS server with file storage runs in tens of MB of RAM, starts in about a second, has a native Go client, per-message acks, `NakWithDelay`, message-ID deduplication and work-queue retention. Kafka needs far more memory and has no per-message ack and redelivery, which a 16 GB home server cannot spare.
