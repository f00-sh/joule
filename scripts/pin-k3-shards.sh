#!/usr/bin/env bash
# pin-k3-shards.sh — replace MANIFEST kimi-k3-shards digests with real file hashes.
#
# Usage:
#   scripts/pin-k3-shards.sh /path/to/dir/with/model-0000N-of-00016.safetensors
#
# Never hosts on f00. After pin, stage/seed blobs so digests_verified can unlock
# (synthetic a100… placeholders refuse unlock).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MANIFEST="${ROOT}/models/MANIFEST.json"
DIR="${1:-}"

if [[ -z "$DIR" || ! -d "$DIR" ]]; then
  echo "usage: $0 /path/to/k3-shard-dir" >&2
  exit 2
fi

if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
  echo "need sha256sum or shasum" >&2
  exit 1
fi

hash_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

python3 - "$MANIFEST" "$DIR" <<'PY'
import json, os, sys, hashlib, subprocess

manifest_path, shard_dir = sys.argv[1], sys.argv[2]
with open(manifest_path) as f:
    m = json.load(f)

def sha256_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

updated = 0
for model in m.get("models", []):
    for quant in model.get("weights", {}).get("quants", []):
        if quant.get("id") != "kimi-k3-shards":
            continue
        for file in quant.get("files", []):
            path = os.path.join(shard_dir, os.path.basename(file["path"]))
            if not os.path.isfile(path):
                print(f"missing {path}", file=sys.stderr)
                sys.exit(1)
            digest = sha256_file(path)
            size = os.path.getsize(path)
            old = file.get("sha256", "")
            file["sha256"] = digest
            file["size_bytes"] = size
            # Prefer peer:// content-addressed name; keep peer scheme.
            if not file.get("url", "").startswith("peer://"):
                file["url"] = f"peer://kimi-open/k3/{os.path.basename(file['path'])}"
            print(f"{file['path']}: {old[:12]}… → {digest[:12]}… ({size} bytes)")
            updated += 1

if updated == 0:
    print("no kimi-k3-shards quant found", file=sys.stderr)
    sys.exit(1)

with open(manifest_path, "w") as f:
    json.dump(m, f, indent=2)
    f.write("\n")
print(f"updated {updated} files in {manifest_path}")
print("next: seed blobs (joule seed-blob) and re-stage donors")
PY
