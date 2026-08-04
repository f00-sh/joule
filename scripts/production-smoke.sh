#!/usr/bin/env bash
# Production smoke: release binary → control + 2 agents → seed lab-tiny →
# wait for verified capacity (challenges) → chat tensor path → freeloader denied →
# free==total leases.
#
# Exit 0 only when the shipped CLI path works end-to-end (not cargo test harness).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
SCRATCH="${JOULE_PROD_SMOKE_SCRATCH:-/tmp/grok-goal-prod-complete/implementer/production-smoke}"
mkdir -p "$SCRATCH"
rm -rf "$SCRATCH"/data "$SCRATCH"/id-* "$SCRATCH"/policy-* "$SCRATCH"/*.log
mkdir -p "$SCRATCH/data"

echo "=== build release joule ==="
cargo build -p joule --release 2>&1 | tee "$SCRATCH/build.log" | tail -5
BIN="$ROOT/target/release/joule"
test -x "$BIN"

export JOULE_SKIP_OFFICIAL_KEY_FETCH=1
export RUST_LOG=info
# Isolate store so smoke is self-contained and does not pollute developer weights
export JOULE_WEIGHTS_DIR="$SCRATCH/weights"
export JOULE_BLOBS_DIR="$SCRATCH/blobs"
mkdir -p "$JOULE_WEIGHTS_DIR" "$JOULE_BLOBS_DIR"

HTTP=17800
AGENT=17801
for p in 17800 27800 37800; do
  if ! ss -ltn 2>/dev/null | grep -q ":$p "; then HTTP=$p; break; fi
done
for p in 17801 27801 37801; do
  if ! ss -ltn 2>/dev/null | grep -q ":$p "; then AGENT=$p; break; fi
done
export JOULE_HTTP_API="http://127.0.0.1:$HTTP"
echo "control http=127.0.0.1:$HTTP agent=127.0.0.1:$AGENT" | tee "$SCRATCH/ports.txt"

"$BIN" control --http-listen "127.0.0.1:$HTTP" --agent-listen "127.0.0.1:$AGENT" \
  --data-dir "$SCRATCH/data" >"$SCRATCH/control.log" 2>&1 &
CPID=$!
cleanup() {
  kill "$CPID" ${APID1:-} ${APID2:-} 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT
sleep 0.8

# Seed lab-tiny into default blob/weight paths agents use
JOULE_BIN="$BIN" bash scripts/seed-lab-tiny.sh >"$SCRATCH/seed.log" 2>&1 || true

"$BIN" agent --control "127.0.0.1:$AGENT" --account alice --mem-mib 8192 \
  --heartbeat-secs 1 --peer-listen "127.0.0.1:0" \
  --identity "$SCRATCH/id-a.json" --policy "$SCRATCH/policy-a.json" \
  >"$SCRATCH/agent-a.log" 2>&1 &
APID1=$!

"$BIN" agent --control "127.0.0.1:$AGENT" --account bob --mem-mib 8192 \
  --heartbeat-secs 1 --peer-listen "127.0.0.1:0" \
  --identity "$SCRATCH/id-b.json" --policy "$SCRATCH/policy-b.json" \
  >"$SCRATCH/agent-b.log" 2>&1 &
APID2=$!

sleep 2

# Wait for ≥2 healthy backends
BACKENDS=0
for i in $(seq 1 30); do
  CAP=$(curl -fsS "http://127.0.0.1:$HTTP/v1/cluster/capacity" || echo '{}')
  BACKENDS=$(echo "$CAP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("logical_device",{}).get("backends") or d.get("nodes_healthy") or 0))' 2>/dev/null || echo 0)
  echo "wait backends=$BACKENDS (try $i)" | tee -a "$SCRATCH/wait.log"
  [[ "$BACKENDS" -ge 2 ]] && break
  sleep 1
done
echo "capacity_join=$CAP" | tee "$SCRATCH/capacity-join.json"
if [[ "${BACKENDS:-0}" -lt 2 ]]; then
  echo "FAIL: need ≥2 backends"
  tail -50 "$SCRATCH/agent-a.log" "$SCRATCH/control.log" || true
  exit 1
fi

# Wait for verified capacity (challenges unlock stream slots)
SLOTS=0
VER=0
for i in $(seq 1 90); do
  CAP=$(curl -fsS "http://127.0.0.1:$HTTP/v1/cluster/capacity" || echo '{}')
  VER=$(echo "$CAP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("mem_mib_healthy") or 0))' 2>/dev/null || echo 0)
  SLOTS=$(echo "$CAP" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("stream_slots_total") or 0))' 2>/dev/null || echo 0)
  echo "wait verified_mib=$VER stream_slots_total=$SLOTS (try $i)" | tee -a "$SCRATCH/wait.log"
  [[ "$VER" -gt 0 && "$SLOTS" -gt 0 ]] && break
  sleep 1
done
echo "capacity_verified=$CAP" | tee "$SCRATCH/capacity-verified.json"
if [[ "${VER:-0}" -le 0 || "${SLOTS:-0}" -le 0 ]]; then
  echo "FAIL: challenges did not unlock verified capacity / stream slots"
  tail -80 "$SCRATCH/control.log" "$SCRATCH/agent-a.log" || true
  exit 1
fi
echo "OBSERVE production-smoke: nodes_healthy>=2 verified_mib=$VER slots=$SLOTS"

# Load alice key: identity JSON, JOULE-CONNECT.txt, or agent log
KEY=$(python3 - <<PY
import json, re, pathlib
scratch = pathlib.Path("$SCRATCH")
for p in [scratch / "id-a.json", scratch / "identity.json"]:
  try:
    d = json.loads(p.read_text())
    k = d.get("api_key") or d.get("pool_api_key") or ""
    if k:
      print(k); raise SystemExit
  except Exception:
    pass
connect = scratch / "JOULE-CONNECT.txt"
if connect.is_file():
  m = re.search(r"joule_[A-Za-z0-9_]+", connect.read_text())
  if m:
    print(m.group(0)); raise SystemExit
log = scratch / "agent-a.log"
if log.is_file():
  m = re.search(r"joule_[a-f0-9]{20,}", log.read_text())
  if m:
    print(m.group(0)); raise SystemExit
print("")
PY
)
echo "alice_key_prefix=${KEY:0:16}..." | tee -a "$SCRATCH/wait.log"
if [[ -z "$KEY" ]]; then
  echo "FAIL: no alice pool API key (Welcome/identity)"
  tail -40 "$SCRATCH/agent-a.log" || true
  exit 1
fi

# Freeloader key (minted without agent) — must 403
FREE_CODE=$(curl -sS -o "$SCRATCH/freeload.json" -w '%{http_code}' \
  -X POST "http://127.0.0.1:$HTTP/v1/chat/completions" \
  -H 'Authorization: Bearer joule_not_a_real_contributor_key' \
  -H 'Content-Type: application/json' \
  -d '{"model":"kimi-open","messages":[{"role":"user","content":"nope"}],"max_tokens":8}' || echo 000)
echo "freeload_http=$FREE_CODE" | tee -a "$SCRATCH/wait.log"
# 401 or 403 both fail-closed for non-contributor
if [[ "$FREE_CODE" != "403" && "$FREE_CODE" != "401" ]]; then
  echo "FAIL: freeloader must be 401/403, got $FREE_CODE body=$(cat $SCRATCH/freeload.json)"
  exit 1
fi
echo "OBSERVE production-smoke: freeloader denied http=$FREE_CODE"

# Contributor chat
if [[ -n "$KEY" ]]; then
  CHAT_CODE=$(curl -sS -o "$SCRATCH/chat.json" -w '%{http_code}' \
    -X POST "http://127.0.0.1:$HTTP/v1/chat/completions" \
    -H "Authorization: Bearer $KEY" \
    -H 'Content-Type: application/json' \
    -d '{"model":"kimi-open","messages":[{"role":"user","content":"production-smoke-hi"}],"max_tokens":32}' || echo 000)
  echo "chat_http=$CHAT_CODE" | tee -a "$SCRATCH/wait.log"
  cat "$SCRATCH/chat.json" | tee -a "$SCRATCH/wait.log"
  if [[ "$CHAT_CODE" != "200" ]]; then
    echo "FAIL: contributor chat expected 200 got $CHAT_CODE"
    tail -40 "$SCRATCH/control.log" || true
    exit 1
  fi
  python3 -c "
import json
d=json.load(open('$SCRATCH/chat.json'))
content=(d.get('choices') or [{}])[0].get('message',{}).get('content') or ''
print('content=', content[:200])
ok = len(content)>0 and ('joule' in content.lower() or 'production-smoke' in content or len(content)>4)
assert ok, repr(content)
print('OBSERVE production-smoke: chat ok content_len=%d' % len(content))
assert 'joule-tensor' in content or 'joule-decode' in content or 'joule-pipeline' in content, content
"
fi

# Leases free after chat (write pure JSON files — no log prefixes)
SCHED=$(curl -fsS "http://127.0.0.1:$HTTP/v1/cluster/scheduler" || echo '{}')
printf '%s\n' "$SCHED" > "$SCRATCH/scheduler.json"
echo "scheduler=$SCHED" | tee -a "$SCRATCH/wait.log"
python3 -c "
import json
d=json.load(open('$SCRATCH/scheduler.json'))
free=int(d.get('stream_slots_free') or 0)
used=int(d.get('stream_slots_used') or 0)
total=int(d.get('stream_slots_total') or 0)
print(f'OBSERVE production-smoke: free={free} used={used} total={total}')
assert used==0, d
assert free==total and total>0, d
"

CAP2=$(curl -fsS "http://127.0.0.1:$HTTP/v1/cluster/capacity" || echo '{}')
printf '%s\n' "$CAP2" > "$SCRATCH/capacity-after.json"
echo "capacity_after=$CAP2" | tee -a "$SCRATCH/wait.log"
python3 -c "
import json
d=json.load(open('$SCRATCH/capacity-after.json'))
tps=int(d.get('tokens_per_sec') or 0)
samples=int(d.get('tokens_per_sec_samples') or 0)
print(f'OBSERVE production-smoke: tokens_per_sec={tps} samples={samples}')
assert samples >= 1 and tps > 0, d
"

echo "OK production-smoke PASS"
echo "OK production-smoke PASS" > "$SCRATCH/RESULT.txt"
