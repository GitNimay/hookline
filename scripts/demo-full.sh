#!/usr/bin/env bash
# The ~5-minute demo (make demo-full). Works on Docker Compose and k3d; run `make seed` first.
#  1. steady load across three tenants      2. 40% failures on one endpoint, then recovery
#  3. kill a worker during load             4. one tenant floods, the others stay flat
#  5. a dead letter, its replay and its trace
set -euo pipefail
. scripts/target-env.sh
bin="bin/hookctl$(go env GOEXE)"
go build -o "$bin" ./cmd/hookctl
hookctl() { "./$bin" "$@"; }
say() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
if [ "$E2E_TARGET" = k8s ]; then GRAFANA=http://grafana.localhost:8080; else GRAFANA=http://localhost:3000; fi

hookctl chaos reset >/dev/null

say "1. Steady load: 1,000 events/s across acme, globex, initech. Watch $GRAFANA/d/hookline-overview"
hookctl send --tenants acme,globex,initech --rate 333 --duration 270s --batch 10 > /tmp/hookline-demo-load.txt 2>&1 &
LOAD=$!
sleep 40

say "2. acme-a fails 40% of requests for 60 s: retries rise, the breaker may open, then success recovers"
hookctl chaos set acme-a failure_rate=0.4 status_code=503 >/dev/null
sleep 60
hookctl chaos set acme-a failure_rate=0 >/dev/null

say "3. Kill a worker during load (nothing may be lost)"
if [ "$E2E_TARGET" = k8s ]; then
  kubectl -n hookline delete pod -l app.kubernetes.io/component=worker --wait=false
else
  docker restart -t 0 hookline-worker-1 >/dev/null
fi
sleep 30

say "4. acme floods at 3,000 events/s for 60 s; globex and initech latency stays flat (tenant drill-down)"
hookctl send --tenants acme --rate 3000 --batch 100 --duration 60s > /tmp/hookline-demo-flood.txt 2>&1 || true
wait $LOAD || true
cat /tmp/hookline-demo-load.txt /tmp/hookline-demo-flood.txt

say "Waiting for the backlog to drain, then checking for loss"
sent=$(awk -F'[ =]' '/events_sent/ {s+=$2} END {print s}' /tmp/hookline-demo-load.txt /tmp/hookline-demo-flood.txt)
echo "events sent: $sent"
hookctl chaos stats | head -20

say "5. A dead letter: initech-a always fails; after the last attempt we fix it and replay"
hookctl chaos set initech-a failure_rate=1 status_code=500 >/dev/null
hookctl send --tenants initech --count 1 --rate 1 >/dev/null
echo "demo retry profile: 6 attempts over ~40 s"
sleep 45
hookctl deliveries --tenant initech --status dead | head -30
hookctl chaos set initech-a failure_rate=0 >/dev/null
ep=$(python3 -c "import json;print(json.load(open('.hookline/seed.json'))['tenants']['initech']['endpoints']['initech-a'])" 2>/dev/null || python -c "import json;print(json.load(open('.hookline/seed.json'))['tenants']['initech']['endpoints']['initech-a'])")
hookctl replay --tenant initech --endpoint "$ep"
echo "Open the trace: $GRAFANA/explore (Tempo, search service.name=worker) or follow trace_id from the worker's warn logs in Loki."
