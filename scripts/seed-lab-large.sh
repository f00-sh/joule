#!/usr/bin/env bash
# Seed lab-large multi-MiB fixtures into the content-addressed blob store.
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
for FILE in \
  models/fixtures/lab-large/model-00001-of-00002.safetensors \
  models/fixtures/lab-large/model-00002-of-00002.safetensors
do
  [[ -f "$FILE" ]] || { echo "missing $FILE"; exit 1; }
  name="lab-large/$(basename "$FILE")"
  "$BIN" seed-blob --path "$FILE" --kind weight --name "$name"
done
"$BIN" blobs
echo "lab-large digests seeded; agents with mem ≥2048 MiB pick lab-large via pick_quant"
