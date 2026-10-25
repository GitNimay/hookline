# Progress

| # | Milestone | Status |
| --- | --- | --- |
| 0 | Foundation | done |
| 1 | Walking skeleton | done |
| 2 | Reliability core | done |
| 3 | Security and signing | done |
| 4 | Protection and fairness | done; full-scale noisy-neighbour number not recorded |
| 5 | Observability | done |
| 6 | Kubernetes | done: `make cluster-up deploy-local seed` works on k3d; HPA and KEDA scale up and down; connection budget fixed |
| 7 | Proof | all 12 e2e scenarios pass on Compose; 10-min 1,000 ev/s benchmark in docs/benchmarks.md |
| 8 | Home server | values and setup guide written; release v0.1.0 published (GHCR images, chart, SBOMs); server setup not executed |
| 9 | Polish | README, architecture, runbook, ADRs 001-010 done; dashboard screenshots and demo GIF pending |

## Next steps

1. Batch worker DB writes (claim/record N jobs per statement) to reach the latency targets.
2. Run `loadtest/noisy-neighbour.js` and the k6 steady test on the home server; record in docs/benchmarks.md.
3. Run the Chaos Mesh experiments (`make chaos`) on k3d.
4. Add dashboard screenshots and the demo GIF.
