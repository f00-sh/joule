#!/usr/bin/env bash
# Demo: operator key → sign notice → inject into local control.
# Prefers official protocol secret (~/.config/f00/joule/protocol.ed25519.sec).
# Lab fallback needs JOULE_ALLOW_UNOFFICIAL_OPERATOR=1 on control. Does not commit secrets.
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

# Prefer official protocol secret if present; else lab key + unofficial flag.
OFF_SEC="${HOME}/.config/f00/joule/protocol.ed25519.sec"
BODY="$ROOT/docs/examples/notice.json"
if [[ -f "$OFF_SEC" ]]; then
  echo "using official protocol secret: $OFF_SEC"
  SEC="$OFF_SEC"
  PUB=$(grep -v '^#' "$ROOT/docs/operator-keys/protocol.ed25519.pub" | head -1 | tr -d '[:space:]')
  echo "official protocol pub: $PUB"
else
  echo "no official secret; generating LAB key (requires JOULE_ALLOW_UNOFFICIAL_OPERATOR=1 on control)"
  "$BIN" broadcast keygen --secret "$TMP/op.sec" --public "$TMP/op.pub"
  SEC="$TMP/op.sec"
  PUB=$(grep -v '^#' "$TMP/op.pub" | head -1 | tr -d '[:space:]')
  echo "export JOULE_ALLOW_UNOFFICIAL_OPERATOR=1"
  echo "export JOULE_OPERATOR_PUBKEY=$PUB"
fi

"$BIN" broadcast sign --kind notice --body "$BODY" --secret "$SEC" --out "$TMP/notice.env.json"
echo "signed → $TMP/notice.env.json"

if curl -sf "$API/healthz" >/dev/null 2>&1; then
  "$BIN" broadcast inject --api "$API" --envelope "$TMP/notice.env.json" || {
    echo "inject failed — stock control verifies the official embed only."
    echo "  official: sign with ~/.config/f00/joule/protocol.ed25519.sec"
    echo "  lab:      JOULE_ALLOW_UNOFFICIAL_OPERATOR=1 JOULE_OPERATOR_PUBKEY=$PUB $BIN control"
    exit 1
  }
  echo "ok — check $API/v1/notices , $API/v1/operator/pins"
else
  echo "control not up at $API — envelope ready at $TMP/notice.env.json"
fi
