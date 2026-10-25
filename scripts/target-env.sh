# Sourced by scripts: exports URLs and keys for the current target (k3d after
# `make cluster-up`, the home server when .hookline/target says homeserver, otherwise
# Docker Compose).
target=$(cat .hookline/target 2>/dev/null || true)
if [ "$target" = homeserver ]; then
  set -a; . ./.hookline/k8s.env; set +a
  export E2E_TARGET=k8s
  export HOOKLINE_CONTROL_URL=http://hookline.home HOOKLINE_INGEST_URL=http://hookline.home
  export CHAOS_URL=http://chaos.home CHAOS_INTERNAL_URL=http://hookline-chaos-customer:8080
elif [ "$target" = k8s ]; then
  set -a; . ./.hookline/k8s.env; set +a
  export E2E_TARGET=k8s
  export HOOKLINE_CONTROL_URL=http://hookline.localhost:8080 HOOKLINE_INGEST_URL=http://hookline.localhost:8080
  export CHAOS_URL=http://chaos.localhost:8080 CHAOS_INTERNAL_URL=http://hookline-chaos-customer:8080
else
  set -a; . ./.env; set +a
  export E2E_TARGET=compose
fi
