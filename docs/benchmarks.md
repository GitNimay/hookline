# Benchmarks

## Environment

Developer PC: Windows 11, 18 logical CPUs, 16 GB RAM, Docker Desktop (WSL2 VM with 8 GB). The stack ran in Docker Compose (one pod per service). Postgres sits on Docker Desktop's virtual disk, which lives on a nearly full C: drive. `pg_test_fsync` measures **~5 ms per `fdatasync`** there, 5–50× slower than a normal Linux SSD. That one number explains most of the misses below.

Load came from k6 (`loadtest/steady.js`): 1,000 events/s for 10 minutes in batches of 10, spread over three tenants. acme has 3 endpoints and globex and initech have 1 each, so the system must deliver ~1,670 webhooks/s. Metrics come from Prometheus (`scripts/bench_report.py 10m`) and the chaos customer's `/stats`.

## Results against section-3 targets

| Target | Measured | Status |
| --- | --- | --- |
| 1,000 events/s sustained for 10 min | 600,000 events accepted, 946 events/s average, **0 failed requests** | met |
| Ingest p99 < 50 ms (server side) | p50 37 ms, **p99 608 ms** while deliveries saturate Postgres; p99 ~190 ms at lower delivery load | missed: synchronous commits on a 5 ms-fsync disk shared with ~3,000 worker statements/s |
| Delivery p99 < 2 s at 1,000 events/s | 999,260 / 999,260 delivered; throughput 1,610 deliveries/s average (demand ~1,580/s), but the backlog built during peaks: chaos-customer p50 50 s, p99 74 s | missed: worker Postgres writes saturate the disk (see below) |
| Zero loss on pod/NATS restarts | e2e `TestRestartsUnderLoad`: worker killed + NATS restarted under load, 5,000 / 5,000 delivered | met |
| Duplicates < 0.1% | 0 duplicates in every run, including the restart test | met |
| Fairness (flooding tenant must not raise others' p99 above 5 s) | Not yet measured at full scale; mechanism covered by tests | pending |
| Backlog drains after recovery | e2e outage, breaker and dead-letter replay scenarios all drain without manual steps | met |
| Startup / restart < 10 s | Compose services healthy in ~5 s; on k3d all Hookline pods Running ~10 s after `helm install`; full stack (`make cluster-up`) took ~12 min on first run, dominated by image pulls | services met; first full-stack start missed (image pulls) |
| Hookline services < 4 GB | ingest 55 MiB, control 40 MiB, dispatcher 36 MiB, worker 69 MiB, scheduler 34 MiB RSS; Postgres 1.1 GiB, NATS 240 MiB | met |
| Monitoring < 3 GB | Prometheus 58 MiB, Grafana 152 MiB, Alertmanager 22 MiB (Compose) | met |

## What changed during benchmarking

Each step was found from the metrics in this repo (DB pool wait, CPU, schedule lag) and fixed:

| Change | Effect |
| --- | --- |
| Claim + event load and attempt record + state update as single statements | acme end-to-end p99 8.7 s → 0.75 s at 300 events/s |
| One puller per tenant consumer, FIFO-fair shared slot pool (replacing round-robin `FetchNoWait`) | 30k-delivery burst: p99 36.7 s → 8.6 s; ~600 → ~3,500 deliveries/s |
| Breaker results buffered per pod, flushed every 100 ms | removes per-attempt Redis WATCH contention |
| `synchronous_commit=off` for worker sessions only (ADR-010) | worker DB wait 62 → 2.5 s/s; deliveries 905 → ~1,450/s |
| Batch ingest as one `unnest` insert; async fan-out publish | fewer round trips per request |
| Dispatcher handles up to 100 events per insert/publish | dispatcher DB wait 26 → ~0 s/s; deliveries ~1,290 → 1,610/s |
| Postgres WAL tuning (`max_wal_size=8GB`, timed checkpoints, `wal_compression=lz4`) | removes forced checkpoints; little effect on this disk |

## Why the latency targets are missed here, and what to try next

Per delivery the worker issues two statements (claim, record). At 1,600 deliveries/s that is ~3,200 writes/s on top of ingest and dispatch, all landing on one WAL on a virtual disk with 5 ms flushes. Workers then wait for connections (24 s/s), jobs queue, and ingest's synchronous commits wait behind the same WAL.

On the home server (k3d in WSL2 on a local SSD), re-run `k6 run loadtest/steady.js` and record the numbers here. Code-side options, in order of payoff:
1. Batch worker DB writes: claim and record N jobs per statement, the same way the dispatcher batches.
2. Partition `deliveries` and `delivery_attempts` by time, so writes stay in small, hot indexes.
3. Give Postgres its own fast volume and more `shared_buffers`.

## Fairness (scenario 7)

`k6 run -e FLOOD_EVENTS=100000 loadtest/noisy-neighbour.js`: acme floods 100,000 events (300,000 deliveries) while globex sends 10 events/s. Pass if globex-a's p99 (`hookctl chaos stats`) stays under 5 s. Not yet recorded; run it on the home server. The per-tenant bulkhead (`MaxAckPending`) and the FIFO slot pool are exercised by the integration and e2e suites.

## k3d (stage 1b) run, 2026-10-25

Cluster: 1 server and 2 agents, every third-party chart from `deploy/helmfile.yaml.gotmpl`, ~4.4 GB used by the nodes at idle.

- `make cluster-up deploy-local seed` succeeded. Traffic through the Traefik Ingress (`hookline.localhost`) was delivered and signature-verified at the in-cluster chaos customer.
- Autoscaling: the ingest HPA went from 2 to 4 pods on CPU, KEDA scaled the dispatcher to 2 on `EVENTS` lag, and a 30k-event burst (90k deliveries) scaled workers from 1 to 20 on pending jobs and back to 1 once drained.
- The first burst exhausted Postgres connections (207/200): 20 workers x 30 connections. Pools and limits now follow a budget (sum of maxReplicas x `DB_MAX_CONNS` = 235, below `max_connections` = 300). Workers max out at 10, since more only queue on Postgres.
- Outcome: 112,160 / 112,160 deliveries succeeded, 0 lost, 120 duplicates (0.107%). The duplicates came from statement timeouts during the overload and a Postgres restart to apply the new limit; all carried the same `webhook-id`.
- The NATS chart's exporter prefixes metrics with `nats_` (Compose: `jetstream_`); dashboards, alerts and the KEDA query match both.
