# Progress

| # | Milestone | Status |
| --- | --- | --- |
| 0 | Foundation | done |
| 1 | Walking skeleton | done |
| 2 | Reliability core | done |
| 3 | Security and signing | done |
| 4 | Protection and fairness | done; full-scale noisy-neighbour number not recorded |
| 5 | Observability | done |
| 6 | Kubernetes | chart, helmfile, k3d, Tiltfile written and lint/render clean; not deployed end to end (C: drive full) |
| 7 | Proof | all 12 e2e scenarios pass on Compose; 10-min 1,000 ev/s benchmark in docs/benchmarks.md |
| 8 | Home server | values, setup guide, release workflow written; not executed |
| 9 | Polish | README, architecture, runbook, ADRs 001-010 done; dashboard screenshots and demo GIF pending |

## Next steps

1. Move Docker Desktop's disk image to D:, then `make cluster-up deploy-local seed demo` on k3d.
2. Batch worker DB writes (claim/record N jobs per statement) to reach the latency targets.
3. Run `loadtest/noisy-neighbour.js` and the k6 steady test on the home server; record in docs/benchmarks.md.
4. Tag `v0.1.0` to publish images and the chart; add screenshots and the demo GIF.
