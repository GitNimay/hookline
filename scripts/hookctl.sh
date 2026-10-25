#!/usr/bin/env bash
# Runs hookctl against the current target (see target-env.sh).
set -euo pipefail
. scripts/target-env.sh
exec go run ./cmd/hookctl "$@"
