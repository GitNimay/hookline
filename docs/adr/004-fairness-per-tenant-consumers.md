# ADR-004: Fairness via one NATS pull consumer per tenant

Status: accepted

## Decision
The `DELIVERIES` work-queue stream has one durable pull consumer per tenant (`deliver_<tenant_id>`, filter `deliveries.<tenant_id>`). Its `MaxAckPending` is the tenant's `max_concurrency` (default 50): a bulkhead on in-flight deliveries. `control` creates and updates consumers on tenant create/update and in a one-minute reconcile loop.

Workers list `deliver_*` consumers every 10 s and fetch round-robin with `FetchNoWait`, taking at most 10 jobs per tenant per turn and rotating the starting tenant each round. A bounded semaphore (`WORKER_CONCURRENCY`) limits jobs per pod.

## Consequences
- A flooding tenant gets at most its turn and its `MaxAckPending` in flight; other tenants are not queued behind it.
- Thousands of tenants would mean thousands of consumers and fetch round trips; then hash tenants into a fixed number of fairness buckets instead.
