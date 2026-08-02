#!/usr/bin/env bash
# Multi-agent live pool smoke: control + N≥2 agents + seed lab-mid + pause-only + leave.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
SCRATCH="${JOULE_SMOKE_SCRATCH:-/tmp/joule-live-pool-smoke}"
mkdir -p "$SCRATCH"
rm -rf "$SCRATCH"/data "$SCRATCH"/id-* "$SCRATCH"/policy-*
mkdir -p "$SCRATCH/data"

if [[ -x ./target/debug/joule ]]; then BIN=./target/debug/joule
elif [[ -x ./target/release/joule ]]; then BIN=./target/release/joule
else cargo build -p joule && BIN=./target/debug/joule
fi

export JOULE_SKIP_OFFICIAL_KEY_FETCH=1
export RUST_LOG=info
# Do NOT set JOULE_DONOR_POLICY globally — each agent uses --policy explicitly.

HTTP=17700
AGENT=17701
for p in 17700 27700 37700; do
  if ! ss -ltn 2>/dev/null | grep -q ":$p "; then HTTP=$p; break; fi
done
for p in 17701 27701 37701; do
  if ! ss -ltn 2>/dev/null | grep -q ":$p "; then AGENT=$p; break; fi
done

echo "control http=127.0.0.1:$HTTP agent=127.0.0.1:$AGENT"
"$BIN" control --http-listen "127.0.0.1:$HTTP" --agent-listen "127.0.0.1:$AGENT" \
  --data-dir "$SCRATCH/data" >"$SCRATCH/control.log" 2>&1 &
CPID=$!
cleanup() {
  kill "$CPID" ${APID1:-} ${APID2:-} 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT
sleep 0.6

JOULE_BIN="$BIN" bash scripts/seed-lab-mid.sh >"$SCRATCH/seed.log" 2>&1 || true

# Alice: open policy
"$BIN" agent --control "127.0.0.1:$AGENT" --account alice --mem-mib 8192 \
  --heartbeat-secs 1 --peer-listen "127.0.0.1:0" \
  --identity "$SCRATCH/id-a.json" --policy "$SCRATCH/policy-a.json" \
  >"$SCRATCH/agent-a.log" 2>&1 &
APID1=$!

# Bob: start with mem-cap 4096 (claim clamp) — process stays for pause test
"$BIN" agent --control "127.0.0.1:$AGENT" --account bob --mem-mib 16384 \
  --mem-cap-mib 4096 \
  --heartbeat-secs 1 --peer-listen "127.0.0.1:0" \
  --identity "$SCRATCH/id-b.json" --policy "$SCRATCH/policy-b.json" \
  >"$SCRATCH/agent-b.log" 2>&1 &
APID2=$!

sleep 2.5
CAP1=$(curl -fsS "http://127.0.0.1:$HTTP/v1/cluster/capacity" || echo '{}')
echo "capacity_join=$CAP1" | tee "$SCRATCH/capacity-join.json"
BACKENDS=$(echo "$CAP1" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("logical_device",{}).get("backends") or d.get("nodes_healthy") or 0))' 2>/dev/null || echo 0)
echo "backends_join=$BACKENDS"
if [[ "${BACKENDS:-0}" -lt 2 ]]; then
  echo "FAIL: expected ≥2 backends after agents join (got $BACKENDS)"
  tail -40 "$SCRATCH/agent-a.log" "$SCRATCH/agent-b.log" || true
  exit 1
fi

# --- Pause-only (no kill): write paused policy bob reloads from --policy path ---
"$BIN" donor pause --policy "$SCRATCH/policy-b.json"
# Wait for ≥1 heartbeat (1s) + margin so agent reloads policy-b.json
sleep 2.5
CAP_PAUSE=$(curl -fsS "http://127.0.0.1:$HTTP/v1/cluster/capacity" || echo '{}')
echo "capacity_pause_only=$CAP_PAUSE" | tee "$SCRATCH/capacity-pause.json"
BACKENDS_PAUSE=$(echo "$CAP_PAUSE" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(int(d.get("logical_device",{}).get("backends") or d.get("nodes_healthy") or 0))' 2>/dev/null || echo 0)
echo "backends_pause_only=$BACKENDS_PAUSE"
if [[ "${BACKENDS_PAUSE:-0}" -ge "$BACKENDS" ]]; then
  echo "FAIL: pause-only must reduce healthy backends ($BACKENDS → $BACKENDS_PAUSE)"
  tail -40 "$SCRATCH/agent-b.log" || true
  exit 1
fi
if [[ "${BACKENDS_PAUSE:-0}" -lt 1 ]]; then
  echo "FAIL: alice should still be healthy after bob pause"
  exit 1
fi

# Resume then leave (process kill) for join/leave churn
"$BIN" donor resume --policy "$SCRATCH/policy-b.json"
sleep 2
kill "$APID2" 2>/dev/null || true
unset APID2
sleep 1.5
CAP2=$(curl -fsS "http://127.0.0.1:$HTTP/v1/cluster/capacity" || echo '{}')
echo "capacity_leave=$CAP2" | tee "$SCRATCH/capacity-leave.json"
echo "OK live-pool-smoke backends_join=$BACKENDS backends_pause_only=$BACKENDS_PAUSE"
