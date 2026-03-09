#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)
TOKEN_FILE="$REPO_ROOT/var/dev/enrollment-token.txt"

mkdir -p "$REPO_ROOT/var/dev"

cd "$REPO_ROOT"
TOKEN=$(cargo run -q -p galactica-control-plane -- \
  --config "$REPO_ROOT/config/dev/control-plane.toml" \
  --mint-enrollment-token-only \
  "$@")

printf '%s\n' "$TOKEN" >"$TOKEN_FILE"
printf '%s\n' "$TOKEN"
