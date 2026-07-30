# joule — self-governing verifiable economy v0

**Status:** active  
**Goal:** the pool **runs itself**. Users cannot invent millijoules or GPU size. Everything public is **recomputable and hash-chained**.

**No money.** Public pool access is paid only in donated compute, scored as millijoules (mJ).

---

## 1. Product laws (non-negotiable)

1. **No cash path** on the public pool.  
2. **Balances exist only as the replay of a sealed event chain.** There is no “set balance” API.  
3. **Advertised VRAM is a claim.** Mint and placement capacity use **verified** memory only.  
4. **Mint sources are protocol-only:** heartbeat (tiny), verified work, challenge pass. No silent admin mint in production.  
5. **Anyone can audit** by downloading the chain and replaying it.  
6. **Self-governing:** rules are deterministic code (`eco=v0` + this chain). Operators host a control process; they do not get a secret “give myself 1e9 mJ” button without creating a visible, signed, forged-looking event that breaks the public chain.

---

## 2. Threat model

| Attack | Mitigation |
|---|---|
| User edits local balance | Balances not client-side; only chain replay on control |
| User claims 80 GiB VRAM | Claim vs **verified_mem_mib**; mint uses verified |
| User fakes heartbeats | Tiny mint; challenges; ban on fail |
| User double-spends mJ | Single sealed chain per pool; burns checked against replayed balance |
| Operator rewrites history | Hash chain + pool signature; public head hash; notary checkpoints |
| Sybil farm | √VRAM, challenge rate, leecher mult, verified-only mint |

Perfect global trustless consensus is **not** required for v0. **Per-pool sealed ledger + public verify** is.

---

## 3. Sealed ledger (hash chain)

Each event:

```text
height, prev_hash, entry_hash, id, account, delta_mJ, reason, kind, unix_ms,
economy_version, verified_mem_mib?
```

```text
entry_hash = sha256(
  height || prev_hash || account || delta || reason || kind || unix_ms || eco
)
```

`prev_hash` of height 0 is the ASCII string `genesis`.

**Balance(account) = sum(delta)** over chain (reject overdraft at apply time).

**Checkpoint** events every `CHECKPOINT_EVERY` entries record:

- `head_hash`
- `notary_node_ids[]` (random healthy donors — witnesses)
- optional future: notary signatures

Public API:

| Path | Purpose |
|---|---|
| `GET /v1/public/ledger?from=&limit=` | Paginated sealed events |
| `GET /v1/public/ledger/head` | height + head_hash + entry count |
| `GET /v1/public/audit/:account` | balance + last events + chain head |

Persistence stores the **chain**, never orphan balances. On load: replay → balances.

---

## 4. Claimed vs verified capacity

| Field | Meaning |
|---|---|
| `claimed_mem_mib` | What the agent advertised (`NodeCaps.mem_mib`) |
| `verified_mem_mib` | What the protocol currently trusts |

Rules (v0):

- New node: `verified_mem_mib = 0` (mint uses floor 256 MiB participation only).  
- Each **challenge pass**: raise verified toward claim  
  `verified = min(claim, max(verified, step))` with step growth.  
- After **3 consecutive passes** without fail: `verified = claim`.  
- **Challenge fail**: `verified = verified / 2` (floor 0), reputation ban path unchanged.  
- **Fairness mint** uses `verified_mem_mib` (not raw claim).  
- **Pool VRAM for model readiness** uses sum of **verified** healthy mem (anti fake-pool-for-kimi).  

Users cannot “hack GPU size” into economics without passing ongoing challenges.

---

## 5. Self-governing control loop

```
agents hello/heartbeat
    → claimed caps recorded
    → sealed mint (tiny) only if healthy, using verified mem
spot challenges
    → pass/fail adjusts verified + reputation
    → sealed mint on pass
infer shards
    → sealed mint on verified work
chat
    → sealed burn from chain balance only
every N events / T seconds
    → checkpoint with notary set
public site / auditors
    → fetch chain, recompute, compare head
```

No human in the loop for mint/burn once the binary runs.

---

## 6. What “impossible to alter mJ” means in practice

| Who | Can they change mJ? |
|---|---|
| End user / agent | **No** — only work produces sealed mints |
| Attacker with API key | Can only **burn** their own mJ via chat if rules allow |
| Disk editor on control host | Can rewrite chain file — but **public head / mirrors / notaries** detect fork; honest auditors reject |
| Honest majority of notaries (future) | Can refuse to co-sign a forked head |

v0 makes **casual and remote tampering** fail closed. **Physical compromise of the only control host** still requires multi-mirror / multi-operator for full Byzantine safety (Phase B).

---

## 7. Phase B (later, not blocking)

- Notaries **counter-sign** checkpoints with their keys  
- Multiple controls federate chain heads  
- Optional external public log (even a chain) for head hashes  

---

## 8. No money (landing copy)

> joule never takes payment. You put compute in; you get millijoules out. The scoreboard is a public hash chain you can recompute yourself.
