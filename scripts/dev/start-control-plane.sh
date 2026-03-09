#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)

mkdir -p "$REPO_ROOT/var/dev"

cd "$REPO_ROOT"
exec cargo run -p galactica-control-plane -- \
  --config "$REPO_ROOT/config/dev/control-plane.toml" \
  "$@"
