# Progress

Status of each milestone from the build plan. Updated at the end of every work session.

| # | Milestone | Status |
| --- | --- | --- |
| 0 | Foundation | done |
| 1 | Walking skeleton | done |
| 2 | Reliability core | code done; scenario tests in progress |
| 3 | Security and signing | code done; isolation tests in progress |
| 4 | Protection and fairness | code done; scenario tests in progress |
| 5 | Observability | metrics and tracing done; dashboards and alerts pending |
| 6 | Kubernetes | pending |
| 7 | Proof | pending |
| 8 | Home server | pending |
| 9 | Polish | pending |

## Session 1 (2026-10-25)

Done
- Repo layout, Go module, Makefile, golangci-lint config, Dockerfile, `.env.example`, CI workflow.
- Protos for every service (buf lint clean), grpc-gateway REST, OpenAPI in `gen/openapi`.
- Schema migration for all tables; sqlc queries.
- All five services, chaos customer and `hookctl` (migrate, seed, send, replay, chaos, verify).
- Outbox relay, dispatcher fan-out, worker with claim locks, Standard Webhooks signing, SSRF dialer,
  retry schedule with jitter, response classification, circuit breaker, GCRA rate limits,
  per-tenant consumers with round-robin fetching, scheduler (due retries, reconciliation,
  auto-disable, retention).
- Docker Compose stack; `make compose-up seed` then `hookctl send` delivers every event,
  verified by the official Standard Webhooks library in the chaos customer.

Known gaps
- Integration tests (testcontainers) and e2e scenario tests not written yet.
- `-race` runs only in CI/WSL2 (no cgo on native Windows; ADR-009).

Next
- Integration tests for store, relay, worker path; e2e scenario suite.
- Dashboards, alert rules.
