#!/usr/bin/env bash
# Multi-agent live pool smoke: control + N≥2 agents + seed lab-mid + capacity under churn.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
SCRATCH="${JOULE_SMOKE_SCRATCH:-/tmp/joule-live-pool-smoke}"
mkdir -p "$SCRATCH"
rm -rf "$SCRATCH"/data "$SCRATCH"/id-*
mkdir -p "$SCRATCH/data"

if [[ -x ./target/debug/joule ]]; then BIN=./target/debug/joule
elif [[ -x ./target/release/joule ]]; then BIN=./target/release/joule
else cargo build -p joule && BIN=./target/debug/joule
fi

export JOULE_SKIP_OFFICIAL_KEY_FETCH=1
export RUST_LOG=info

# Free ports
HTTP=17700
AGENT=17701
# pick free if busy
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

# Seed lab-mid into blob store (shared default path under home; also prepare via agents)
JOULE_BIN="$BIN" bash scripts/seed-lab-mid.sh >"$SCRATCH/seed.log" 2>&1 || true

export JOULE_IDENTITY="$SCRATCH/id-a.json"
export JOULE_DONOR_POLICY="$SCRATCH/policy-a.json"
"$BIN" agent --control "127.0.0.1:$AGENT" --account alice --mem-mib 8192 \
  --heartbeat-secs 1 --peer-listen "127.0.0.1:0" \
  --identity "$SCRATCH/id-a.json" --policy "$SCRATCH/policy-a.json" \
  >"$SCRATCH/agent-a.log" 2>&1 &
APID1=$!

export JOULE_IDENTITY="$SCRATCH/id-b.json"
export JOULE_DONOR_POLICY="$SCRATCH/policy-b.json"
"$BIN" agent --control "127.0.0.1:$AGENT" --account bob --mem-mib 16384 \
  --heartbeat-secs 1 --peer-listen "127.0.0.1:0" \
  --identity "$SCRATCH/id-b.json" --policy "$SCRATCH/policy-b.json" \
  >"$SCRATCH/agent-b.log" 2>&1 &
APID2=$!

sleep 2
CAP1=$(curl -fsS "http://127.0.0.1:$HTTP/v1/cluster/capacity" || echo '{}')
echo "capacity_join=$CAP1" | tee "$SCRATCH/capacity-join.json"
BACKENDS=$(echo "$CAP1" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d.get("logical_device",{}).get("backends") or d.get("nodes_healthy") or 0)' 2>/dev/null || echo 0)
echo "backends=$BACKENDS"
if [[ "${BACKENDS:-0}" -lt 1 ]]; then
  echo "FAIL: expected ≥1 backend after agents join" | tee -a "$SCRATCH/control.log"
  tail -30 "$SCRATCH/agent-a.log" "$SCRATCH/agent-b.log" || true
  exit 1
fi

# Churn: pause bob via local donor policy + kill process
"$BIN" donor pause --policy "$SCRATCH/policy-b.json"
kill "$APID2" 2>/dev/null || true
unset APID2
sleep 1.5
CAP2=$(curl -fsS "http://127.0.0.1:$HTTP/v1/cluster/capacity" || echo '{}')
echo "capacity_churn=$CAP2" | tee "$SCRATCH/capacity-churn.json"
echo "OK live-pool-smoke backends_join=$BACKENDS"
echo "$CAP1" > "$SCRATCH/capacity-join.json"
echo "$CAP2" > "$SCRATCH/capacity-churn.json"
