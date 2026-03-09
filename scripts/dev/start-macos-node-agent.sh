#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)
TOKEN_FILE="$REPO_ROOT/var/dev/enrollment-token.txt"

TOKEN="${GALACTICA_ENROLLMENT_TOKEN:-${1:-}}"
if [[ -z "$TOKEN" && -f "$TOKEN_FILE" ]]; then
  TOKEN=$(<"$TOKEN_FILE")
fi

if [[ -z "$TOKEN" ]]; then
  echo "missing enrollment token; run scripts/dev/mint-enrollment-token.sh or set GALACTICA_ENROLLMENT_TOKEN" >&2
  exit 1
fi

CONTROL_PLANE_ADDR="${GALACTICA_CONTROL_PLANE_ADDR:-http://127.0.0.1:9090}"
AGENT_ENDPOINT="${GALACTICA_AGENT_ENDPOINT:-http://127.0.0.1:50061}"

cd "$REPO_ROOT"
exec cargo run -p galactica-node-agent -- \
  --config "$REPO_ROOT/config/dev/macos-node-agent.toml" \
  --enrollment-token "$TOKEN" \
  --control-plane-addr "$CONTROL_PLANE_ADDR" \
  --agent-endpoint "$AGENT_ENDPOINT"
