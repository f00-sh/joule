#!/usr/bin/env bash
# Seed lab-tiny fixture into the content-addressed blob store (peer swarm).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
BIN="${JOULE_BIN:-}"
if [[ -z "$BIN" ]]; then
  if [[ -x ./target/release/joule ]]; then BIN=./target/release/joule
  elif [[ -x ./target/debug/joule ]]; then BIN=./target/debug/joule
  else cargo build -p joule && BIN=./target/debug/joule
  fi
fi
FILE=models/fixtures/lab-tiny/model.safetensors
[[ -f "$FILE" ]] || { echo "missing $FILE"; exit 1; }
"$BIN" seed-blob --path "$FILE" --kind weight --name lab-tiny/model.safetensors
"$BIN" blobs
echo "start an agent so BlobsHave announces this hash to the control directory"
