# Hookline runbook

Commands assume the k3d cluster (`kubectl` context `k3d-hookline`, namespace `hookline`). For Docker Compose, replace `kubectl ... rollout restart` with `docker compose -f deploy/compose/docker-compose.yml restart <service>`.

## Everyday operations

### Restart a service
```bash
kubectl -n hookline rollout restart deploy/hookline-worker
kubectl -n hookline rollout status deploy/hookline-worker
```
Restarts are safe at any time: in-flight jobs finish or are redelivered (at-least-once), and the ingest outbox keeps every accepted event.

### Scale
Workers and dispatchers scale with KEDA on queue lag; ingest with an HPA on CPU. To change the limits, edit `services.<name>.keda` / `hpa` in the values file and redeploy. For a temporary manual override:
```bash
kubectl -n hookline scale deploy/hookline-scheduler --replicas=2   # scheduler is safe to run twice (SKIP LOCKED)
```

### Replay dead letters
```bash
hookctl deliveries --tenant acme --status dead                 # inspect
hookctl replay --tenant acme --delivery dlv_...                # one delivery
hookctl replay --tenant acme --endpoint ep_...                 # every dead delivery of an endpoint
hookctl replay --tenant acme --endpoint ep_... --since 2026-10-25T10:00:00Z   # everything since a time
```
Or the REST API: `POST /v1/deliveries/{id}:replay`, `POST /v1/endpoints/{id}:replay`. Replays run in batches of 500 and still respect the endpoint's rate limit.

### Re-enable an auto-disabled endpoint
Endpoints that fail continuously for 3 days are disabled with reason `auto_disabled`; a `410 Gone` response disables with reason `gone`.
```bash
curl -X POST $CONTROL/v1/endpoints/ep_...:enable -H "Authorization: Bearer $KEY" -d '{}'
```
Then replay its dead letters if the customer wants them.

### Rotate keys and secrets
- Tenant API key: create a new one, switch the producer, revoke the old one.
  ```bash
  curl -X POST   $CONTROL/v1/admin/tenants/ten_.../keys -H "Authorization: Bearer $ADMIN" -d '{}'
  curl -X DELETE $CONTROL/v1/admin/tenants/ten_.../keys/key_... -H "Authorization: Bearer $ADMIN"
  ```
  Revocation takes effect within 30 s (auth cache).
- Endpoint signing secret: `POST /v1/endpoints/{id}/secret:rotate`. Both secrets sign for 24 h.
- Admin key or master key: edit the `hookline-secrets` Secret and restart `control` (admin key) or all services (master key). **Changing `HOOKLINE_MASTER_KEY` makes existing endpoint secrets unreadable**: rotate every endpoint secret first, or keep the key.

### Back up and restore
The home server runs `hookline-pg-backup` (CronJob, nightly `pg_dump -Fc`, 7 days kept) into `/srv/hookline/backups`.

#### Restore a backup
```bash
kubectl -n hookline scale deploy --all --replicas=0          # stop writers (KEDA will not scale from 0 while paused)
kubectl -n infra exec -i hookline-pg-1 -- pg_restore --clean --if-exists -U postgres -d hookline < /srv/hookline/backups/hookline-<time>.dump
kubectl -n hookline scale deploy --all --replicas=1
```
After a restore, deliveries newer than the backup are gone; producers can resend with the same idempotency keys. The scheduler's reconciliation republishes any `pending` delivery that has no job.

Run a test restore into a scratch database at least once:
```bash
kubectl -n infra exec -i hookline-pg-1 -- createdb -U postgres restore_test
kubectl -n infra exec -i hookline-pg-1 -- pg_restore -U postgres -d restore_test < backup.dump
kubectl -n infra exec -i hookline-pg-1 -- psql -U postgres -d restore_test -c 'select count(*) from events'
```

## Alerts

### HooklineDeliverySuccessLow
Fewer than 95% (warning) or 80% (critical) of attempts succeed.
1. Grafana → Hookline overview → *Attempt errors by kind*. One `error_kind` usually dominates.
2. `http_status` → customers' endpoints are failing; check *Breaker transitions* and which tenants are affected (tenant drill-down). Nothing to fix on our side unless every endpoint fails.
3. `timeout`/`dns`/`connection_refused` across all tenants → outbound network or DNS problem on the node.
4. `ssrf_blocked` → an endpoint URL resolves to a private address; tell the tenant.

### HooklineDeadLettersRising
More than 100 deliveries went dead in an hour. Find the endpoint: `GET /v1/deliveries?status=dead`. Usually one customer endpoint has been down for the whole retry schedule; replay once it is fixed.

### HooklineSLOFastBurn
More than 0.5% × 14.4 of attempts start over 60 s after they were due (fast burn) or 0.5% × 6 (slow burn). Causes, in order of likelihood:
1. Workers saturated: *Worker pods / in-flight* at max and *Queue lag* growing → raise `keda.maxReplicas` or `WORKER_CONCURRENCY`.
2. Postgres slow: *DB pool wait* rising → check Postgres CPU/IO; see HooklinePostgresConnectionsHigh.
3. Scheduler down: due retries are not published → HooklineServiceDown for scheduler.
4. Many endpoints rate limited (`hookline_rate_limited_total{kind="endpoint"}`): expected, the tenant set a low limit.

### HooklineOutboxBacklog
Events are stored but not queued. Check NATS (`kubectl -n infra get pods`, `/healthz?js-enabled-only=true` on port 8222) and the ingest logs for `outbox relay failed`. Accepted events are safe in Postgres; they flow as soon as NATS is back. The scheduler also relays rows older than a minute.

### HooklineDeliveriesLag
More than 10,000 delivery jobs waiting. If one tenant dominates (*Queue lag per tenant*), its bulkhead is doing its job; raise that tenant's `max_concurrency` if it should get more. If all tenants lag, scale workers.

### HooklineServiceDown
Prometheus cannot scrape a service. `kubectl -n hookline get pods`, then `kubectl -n hookline describe pod` and `logs --previous`. Common causes: missing Secret keys (config validation fails fast with a clear message), Postgres or NATS unreachable (readiness fails, liveness does not).

### HooklinePodRestarting
More than 3 restarts in 15 minutes. `kubectl -n hookline logs <pod> --previous`. `OOMKilled` → raise the memory limit (GOMEMLIMIT follows it at 90%).

### HooklinePostgresConnectionsHigh
Over 80% of `max_connections` in use. Each pod opens up to `DB_MAX_CONNS` (default 20; workers 30). With many workers, lower `DB_MAX_CONNS`, raise `max_connections` in `deploy/helm/infra/values.yaml`, or cap `keda.maxReplicas`.
