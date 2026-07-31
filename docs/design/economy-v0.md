# joule — millijoule economy v0

**Status:** active (v0 code: √VRAM mint, tenure, **churn**, leecher mults, **donate-to-pool**, dual_verify)  
**Algorithm id:** `eco=v0` (embedded in every ledger reason string)  
**Code:** `crates/joule-ledger/src/economy.rs`, sealed kinds `donate_pool` / `donate_receive`  
**Product:** free public pool — pay only in donated compute

---

## 1. What a millijoule is

| Unit | Meaning |
|---|---|
| **1 millijoule (mJ)** | Integer credit on the ledger |
| **1000 mJ** | 1 joule (display only) |

There is **no cash path** on the public pool. You mint mJ by donating healthy compute; you burn mJ when you use the API. Keys without contribution fail closed.

---

## 2. Fairness laws

1. **Small donors count.** Mint scales with **√VRAM**, not linear VRAM and not “GPU class aristocracy.” A laptop with 4 GiB is not 1/24 of a 24 GiB card — closer to half the factor of a 16 GiB card.
2. **Work over theater.** Heartbeats mint a little; verified shards and challenges mint more.
3. **Tenure boost.** Continuous healthy time in the cluster multiplies earnings (cap 1.5×).
4. **Leecher penalty.** If you **consume more than you contribute** in the rolling window, you **earn less and pay more**. Extreme leechers hit 0.25× mint / 4× usage.
5. **Churn penalty.** Frequent disconnect/reconnect lowers mint (toward 0.40×). Stable presence keeps 1.0×. First two disconnects in the window are free.
6. **Verified VRAM only.** Economic `mem_mib` is **protocol-verified** capacity (challenges), never a raw GPU claim — fake “5070 farms” do not mint as if verified.
7. **Donate.** Optional sealed donation: burn mJ from a rich account; redistribute **equally** among eligible pool participants (deterministic, conserved).
8. **Auditable.** Every mint/burn/donate reason string embeds `eco=v0` and all basis-point factors. Same inputs ⇒ same outputs (pure functions, integer math).

---

## 3. Formulas (basis points: 10 000 = 1.0×)

### Memory factor

```
mem_bp = clamp( 10000 * isqrt(mem_mib) / 32 , 5000, 80000 )
       ≈ 10000 * √(mem_mib / 1024)
```

| VRAM | ≈ mult |
|---|---|
| 1 GiB | 1.0× |
| 4 GiB | 2.0× |
| 16 GiB | 4.0× |
| ≥64 GiB | 8.0× cap |

### Tenure

```
tenure_bp = 10000 + min(5000, ~1200 * log2(1 + whole_days_online))
```

| Continuous healthy | ≈ mult |
|---|---|
| fresh | 1.00× |
| ~1 day | ~1.12× |
| ~7 days | ~1.36× |
| ≥ ~30 days | 1.50× cap |

Offline **resets** the continuous streak (loyalty is presence, not historical sum of random hours).

### Leecher factors

```
if contributed == 0 and consumed > 0:
    mint_bp, usage_bp = 2500, 40000   # 0.25× / 4×
elif consumed <= contributed:
    mint_bp, usage_bp = 10000, 10000  # fair
else:
    L = clamp(consumed/contributed - 1, 0, 3)
    mint_bp  = 10000 / (1 + L)        # floor 0.25×
    usage_bp = 10000 * (1 + L)        # ceil 4×
```

### Mint

```
base =
  heartbeat  → 10 mJ
  work       → 2 mJ × max(1, completion_tokens)
  challenge  → 5 mJ

total = base × mem_bp/10000 × tenure_bp/10000 × leecher_mint_bp/10000 × churn_bp/10000
total = max(1, floor(total))
```

### Churn

```
excess = max(0, disconnects_window - 2)
churn_bp = max(4000, 10000 - excess * 500)
```

### Donate-to-pool

```
donor burns D mJ (kind=donate_pool)
recipients R = eligible online/verified pool accounts excluding donor (sorted)
each gets floor(D/|R|); remainder 1 mJ to first recipients
recipient credits sealed as donate_receive (sum credits = D)
```

API: `POST /v1/account/donate` with Bearer key and `{ "amount": <mJ> }`.

### Burn (API usage)

```
base = prompt_tokens × 1 + completion_tokens × 4
total = max(1, floor(base × leecher_usage_bp / 10000))
```

---

## 4. Ledger reason format

Mint example:

```text
contribute:heartbeat|heartbeat:<node>|eco=v0|base=10|mem_bp=28284|ten_bp=11200|lee_bp=10000|total=31
```

Burn example:

```text
usage:usage|chat:<uuid>|eco=v0|base=50|lee_usage_bp=20000|total=100
```

Anyone can recompute `total` from the published factors.

---

## 5. Rolling window

Control keeps per-account:

- `contributed_mj_window` / `consumed_mj_window`
- `continuous_online_secs` + live `online_since`
- `best_mem_mib` across that account’s healthy nodes

When either window counter exceeds **1 000 000 mJ**, both halves are halved (soft decay so ancient history does not freeze a reformed leecher forever).

Persisted in `state.json` (snapshot v2).

---

## 6. Why this is “fair enough” for v0

| Concern | Mechanism |
|---|---|
| Can’t afford a huge GPU | √VRAM + participation floor |
| Always-on small node vs bursty big node | Tenure boost rewards reliability |
| API-only freeloaders | Must donate to get a live key; leecher mult if consume ≫ contribute |
| Gaming uptime without work | Heartbeat base is small; shards/challenges pay more; spot challenges punish liars |
| Hidden admin knobs | Constants + pure functions in-repo; reasons dump every factor |

Not a blockchain. Settlement is the control-plane ledger. Honesty relies on challenges + open source of the scoring code.

---

## 7. Non-goals (v0)

- Dollar conversion or paid top-ups on the public pool  
- Perfect global anti-sybil (multi-account farms)  
- GPU FLOPs metering (uses VRAM + verified tokens as proxy)  
- Negative balance (burns still fail closed on insufficient funds)

---

## 8. Change control

Bump `ECONOMY_VERSION` (`v1`, …) when formulas change. Old ledger lines remain historically labeled with their `eco=` tag.
