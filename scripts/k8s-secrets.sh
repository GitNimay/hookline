#!/usr/bin/env bash
# Creates the namespaces and secrets Hookline needs, with random values. Idempotent:
# existing secrets are kept, so re-running never rotates live credentials.
set -euo pipefail

rand() { head -c 48 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c "${1:-32}"; }
get() { kubectl -n "$1" get secret "$2" -o "jsonpath={.data.$3}" 2>/dev/null | base64 -d 2>/dev/null || true; }

for ns in infra hookline monitoring; do
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
done

pg_pw=$(get infra hookline-pg-credentials password); pg_pw=${pg_pw:-$(rand)}
redis_pw=$(get infra redis-auth password); redis_pw=${redis_pw:-$(rand)}
master=$(get hookline hookline-secrets HOOKLINE_MASTER_KEY); master=${master:-$(head -c 32 /dev/urandom | base64)}
admin=$(get hookline hookline-secrets HOOKLINE_ADMIN_KEY); admin=${admin:-hk_live_$(rand 40)}

kubectl -n infra create secret generic hookline-pg-credentials --type=kubernetes.io/basic-auth \
  --from-literal=username=hookline --from-literal=password="$pg_pw" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n infra create secret generic redis-auth --from-literal=password="$redis_pw" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl -n hookline create secret generic hookline-secrets \
  --from-literal=DATABASE_URL="postgres://hookline:${pg_pw}@hookline-pg-rw.infra.svc.cluster.local:5432/hookline?sslmode=require" \
  --from-literal=REDIS_PASSWORD="$redis_pw" \
  --from-literal=HOOKLINE_MASTER_KEY="$master" \
  --from-literal=HOOKLINE_ADMIN_KEY="$admin" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null

mkdir -p .hookline
printf 'HOOKLINE_ADMIN_KEY=%s\n' "$admin" > .hookline/k8s.env
chmod 600 .hookline/k8s.env
echo "secrets ready (admin key in .hookline/k8s.env)"
