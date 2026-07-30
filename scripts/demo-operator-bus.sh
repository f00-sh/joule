#!/usr/bin/env bash
# Demo: operator key → sign notice → inject into local control.
# Requires a running control with JOULE_OPERATOR_PUBKEY matching the generated key
# (or no key pin for lab). Does not commit secrets.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
API="${JOULE_API:-http://127.0.0.1:7700}"
TMP="${TMPDIR:-/tmp}/joule-op-demo-$$"
mkdir -p "$TMP"
trap 'rm -rf "$TMP"' EXIT

BIN="${JOULE_BIN:-}"
if [[ -z "$BIN" ]]; then
  if [[ -x ./target/release/joule ]]; then
    BIN=./target/release/joule
  elif [[ -x ./target/debug/joule ]]; then
    BIN=./target/debug/joule
  else
    cargo build -p joule
    BIN=./target/debug/joule
  fi
fi

"$BIN" broadcast keygen --secret "$TMP/op.sec" --public "$TMP/op.pub"
PUB=$(grep -v '^#' "$TMP/op.pub" | head -1 | tr -d '[:space:]')
echo "export JOULE_OPERATOR_PUBKEY=$PUB  # pin this on control for verify"
echo "demo public key: $PUB"

BODY="$ROOT/docs/examples/notice.json"
"$BIN" broadcast sign --kind notice --body "$BODY" --secret "$TMP/op.sec" --out "$TMP/notice.env.json"
echo "signed → $TMP/notice.env.json"

if curl -sf "$API/healthz" >/dev/null 2>&1; then
  # Lab inject without pin works; with pin control must have matching env.
  if [[ -n "${JOULE_OPERATOR_PUBKEY:-}" ]]; then
    echo "using already-exported JOULE_OPERATOR_PUBKEY for inject path on CLI only"
  fi
  "$BIN" broadcast inject --api "$API" --envelope "$TMP/notice.env.json" || {
    echo "inject failed — start control with:"
    echo "  JOULE_OPERATOR_PUBKEY=$PUB $BIN control"
    exit 1
  }
  echo "ok — check $API/v1/notices and the dashboard"
else
  echo "control not up at $API — envelope ready at $TMP/notice.env.json"
  echo "start: JOULE_OPERATOR_PUBKEY=$PUB $BIN control"
fi
