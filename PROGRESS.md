# Progress

| # | Milestone | Status |
| --- | --- | --- |
| 0 | Foundation | done |
| 1 | Walking skeleton | done |
| 2 | Reliability core | done (integration + e2e scenarios 2, 3, 6 pass; worker kill loses nothing) |
| 3 | Security and signing | done (scenarios 11, 12, isolation pass; official Standard Webhooks lib verifies) |
| 4 | Protection and fairness | done (scenarios 8, 9, 10 pass); scenario 7 (noisy neighbour) not measured yet |
| 5 | Observability | done (one event = one trace; dashboards; forced failure fired HooklineServiceDown) |
| 6 | Kubernetes | chart, helmfile, k3d, Tiltfile written and lint/render clean; not yet deployed end to end |
| 7 | Proof | k6 scripts and Chaos Mesh experiments written; benchmarks not run |
| 8 | Home server | values, setup guide, release workflow written; not executed |
| 9 | Polish | pending (README finish, benchmarks.md, demo GIF) |

## Session 1 (2026-10-25) results

- e2e on Compose: 11/12 pass. 10,000 events x 3 endpoints delivered, 0 missing, 0 duplicates.
  Worker kill + NATS restart under load: 5,000/5,000, 0 duplicates.
- Failing: `TestDeadLetterAndReplay` - replayed deliveries wait for the endpoint's open circuit breaker.

## Next steps (in order)

1. Reset the endpoint's breaker on ReplayDelivery / ReplayEndpoint / EnableEndpoint (`Breaker.Reset`: DEL key + probe key).
2. Breaker contention: every attempt runs a WATCH transaction on one Redis key per endpoint, limiting
   fan-out to ~600 deliveries/s. Buffer non-probe results per pod and flush per endpoint every 100 ms
   (`Observe` + `FlushLoop`); keep probes synchronous.
3. k3d: the full stack filled drive C: (Docker Desktop's disk image lives there). Move Docker Desktop's
   disk image to D: (Settings > Resources > Advanced), then `make cluster-up deploy-local seed demo`.
4. Run k6 steady/spike/noisy-neighbour, write `docs/benchmarks.md`, finish README.
