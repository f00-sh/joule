# joule — cluster-v0 design

**Status:** active  
**Version target:** 0.1.0  
**Product:** [joule](https://github.com/f00-sh/joule) · f00-sh  
**Default model family:** open Kimi weights (tag `kimi-open-*` until K3 pin is frozen)

---

## 1. Problem

Frontier open models need more compute than one idle machine often has. Centralized API clouds reintroduce single-operator dependence. joule pools **donated idle compute** into one **distributed cluster on the public internet**: users earn API access only by contributing. Placement can span many nodes. How a node reaches the network (home fiber, coffee-shop Wi‑Fi, 5G, datacenter uplink, carrier pigeon with a USB stick and a prayer) is **not** a product concern — only that the node is reachable, healthy, and contributing.

This is **not** a LAN mesh product. It is a **global volunteer compute cluster**.

## 2. Product laws

1. **Compute is the only currency.** No paid bypass in the public pool.
2. **No contribution ⇒ no API.** Keys fail closed without an active donor window and/or spendable balance.
3. **Credits measure verified useful work** (millijoules), not uptime theater.
4. **Donor machines stay under user control** (caps, schedule, pause, thermal/battery).
5. **Open weights, open agent, open protocol.** Anyone can run a grid; f00 may host a default pool.
6. **Internet-wide cluster:** multi-node placement and failure handling are first-class. No LAN/WAN product tiers.

## 3. Goals / non-goals

### Goals

- Native **donor agent** (not browser compute).
- **Versioned cluster protocol** (hello, heartbeat, plan, infer, challenge, capacity, credit events).
- **Placement planner** for replica / pipeline / later tensor across any healthy nodes that fit.
- **Live cluster capacity** feed powering the **dashboard** (how much distributed compute exists *right now*).
- **OpenAI-compatible gateway** for client apps.
- **Ledger** in millijoules: mint on verified contribution, burn on usage.
- **Stub runtime** for CI; real engine behind a trait.

### Non-goals

- Browser WebGPU as primary capacity.
- Preferring or requiring LAN adjacency for “real” multi-node work.
- Link-class product UX (lan/metro/wan badges, carrier pigeon mode, etc.).
- Trustless global blockchain settlement.
- Training (inference only).
- Shipping weights under a non-redistributable license.

## 4. Architecture

```
                    ┌──────────────────────────┐
   OpenAI clients ──►│ gateway (HTTPS /v1)      │
                    └────────────┬─────────────┘
                                 │ plan + route
                    ┌────────────▼─────────────┐
                    │ cluster control (open)     │
                    │ membership · planner ·     │
                    │ capacity · challenges ·    │
                    │ ledger                     │
                    └─────┬───────────┬─────────┘
                          │           │
              ┌───────────▼──┐   ┌────▼────────────┐
              │ agent node A │   │ agent node B …  │
              │ (any network)│   │ (any network)   │
              └──────────────┘   └─────────────────┘

   Dashboard ──GET /v1/cluster/capacity──► same control plane
              (live aggregate from heartbeats)
```

### Crates

| Crate | Role |
|---|---|
| `joule-proto` | Wire types, `ClusterPlan`, `ClusterCapacity`, messages |
| `joule-cluster` | Membership, **capacity snapshot**, placement |
| `joule-runtime` | `Engine` trait + `StubEngine` |
| `joule-ledger` | Millijoule accounting |
| `joule` | CLI: `lab`, `capacity`, `credits`, `agent` |

### Client model

Users **download a native program** (`joule agent`). Website/dashboard: accounts, keys, **live pool size**, installers — **not** pool FLOPs from the browser.

## 5. Dashboard: live distributed compute

**Requirement:** at any time, the dashboard shows how much distributed compute the cluster has.

### Source of truth

- Agents heartbeat to control with `NodeCaps` + load + health.
- Control maintains the node registry.
- `Cluster::capacity()` aggregates:

| Field | Meaning |
|---|---|
| `nodes_total` / `nodes_healthy` | Registry size vs currently usable |
| `nodes_gpu` / `nodes_metal` / `nodes_cpu` | Device mix |
| `mem_mib_total` / `mem_mib_healthy` | Advertised memory (highlight healthy) |
| `throughput_class_sum` | Relative healthy throughput (verified rates later) |
| `models_available` | Model tags healthy nodes can load |

### API (target)

```http
GET /v1/cluster/capacity
```

Returns `ClusterCapacity` JSON. **Public read** for the marketing/dashboard strip is allowed (no secrets). Optional authenticated richer view (per-node breakdown) for the owner’s account.

### Dashboard UX (minimum)

- Big numbers: **healthy nodes**, **healthy VRAM (GiB)**, **relative throughput**, **models online**
- Subtext: total registered vs healthy (churn visibility)
- Auto-refresh (poll 5–15s or SSE later)
- Empty state: “0 nodes — install the agent to grow the cluster”

### CLI (now)

```text
joule capacity --peers 5
joule capacity --peers 5 --json
```

Same schema as the HTTP endpoint will use.

## 5b. Stream leases + multi-party plan agreement

Distributed admission is **not** "control decides alone." Clients (agents) and control **talk and agree** with hashed, auditable messages:

1. **Stream lease** (`joule_cluster::LeaseBook`): chat/infer only proceeds after a stream slot is taken against verified pool capacity (`try_acquire_stream`). Lifecycle is always **free → used → free** (release on success, error, timeout, or stale deadline). Pool full → fail closed (`503` + `code: pool_full`).
2. **PlanOffer** carries `plan_hash_hex` (domain-separated SHA-256 of the plan body).
3. **PlanAccept** from each required shard carries the same `plan_hash_hex` plus `confirm_hex` = `SHA256(DOMAIN_ACCEPT ‖ plan_id ‖ request_id ‖ node ‖ accepted ‖ plan_hash)`. Missing or mismatched confirmation **fails closed**.
4. Audit trail: grant / plan_agreed / lease_released (and reject events) via `GET /v1/cluster/leases`. Receipt: `lease_receipt_hex`.

Mesh path (`RequestInfer`) and classic control path both take a lease, require multi-shard agreement, fan out with `stream_reserved=false` (lease owns capacity), and **always** release.

## 6. Placement (distributed, medium-agnostic)

### Plan types

1. **Replica** — full quant on one node  
2. **Pipeline** — VRAM-proportional `layer_start`/`layer_end` labels across N nodes (`ShardRole::Pipeline`). Non-tail shards produce a **domain-separated activation commitment** (`activation_hex`) handoff; the tail **verifies** all upstream activations before full `engine.infer`. This is real wire handoff, not an empty ACK — full tensor PP activations remain a later engine upgrade (see `joule_cluster::pipeline`).  
3. **Tensor** — TP ranks (later)  
4. **Prefill/Decode split** — disagg (later)

**Layer count:** placement total is pinned to verified Kimi-K3 meta (`text_config.num_hidden_layers` from sha256-verified `kimi-k3-meta` / `config.json`), not an ungrounded constant alone. MANIFEST `model_layers` must match that pin.

**File weight shards ≠ transformer layers:** `kimi-k3-shards` / `PipelineShard` safetensors files are content-addressed **weight files**. They are **not** 1:1 with `layer_start`/`layer_end` ranges unless a separate design table maps `file_index ↔ layer_range` (none in v0). Do not market “true PP across donors” from geometry alone.

### Policy (v0)

```
if prefer_pipeline && healthy_fit_nodes >= stages && stages > 1:
    assign pipeline shards  # geometry labels only until real PP
else:
    assign single best replica (mem, then load)
```

No path checks the donor’s network type. If a plan is too slow in practice, operators tune stage count / quant / model — they do not get a “LAN-only mode” product surface.

### Failure

- Shard dies → cancel + replan or failover to replica capacity if any.
- Stale heartbeats → node leaves healthy set → capacity drops on the dashboard immediately.

## 7. Runtime strategy

1. **StubEngine** — CI and lab (done).  
2. Pure-Rust engine path preferred under language purity.  
3. FFI only with ADR + AGENTS note.

## 8. Economy (millijoules)

| Action | Effect |
|---|---|
| Verified contribution | **mint** mJ × device multiplier |
| API usage | **burn** mJ |
| Idle with zero useful work | ~0 mint |

**Access:** healthy agent recently seen **and** balance covers estimated burn (or small rolling debt capped by recent mint).

## 9. Security & trust

- Device keys for agents; API keys for clients.
- Challenges + spot recompute — never trust self-reported FLOPs alone for mint.
- Capacity dashboard fields are **aggregates**; do not leak home IPs on the public endpoint.

## 10. Decentralization posture

| Layer | v0 | Later |
|---|---|---|
| Workers | Peer-owned, any internet path | Same |
| Control | Open-source process (f00 default pool) | Multi-coordinator federation |
| Capacity feed | Single control aggregate | Federated sum of pools |
| Ledger | Append log | Gossip + checkpoints |

## 11. Default model

- Open Kimi-family weights; pin in `models/MANIFEST` when engine lands.
- Quant ladder by VRAM class.

## 12. Phased delivery

| Phase | Outcome | Exit | Alpha (0.1.13) |
|---|---|---|---|
| **C0** | Skeleton, capacity type, lab, design | `cargo test`, `joule capacity --json` | **done** |
| **C1** | Agent transport + heartbeats | 2 nodes anywhere form a cluster | **done** |
| **C2** | Real single-node engine | Replica serves real model | **done** (lab tensor ClusterEngine; not full Kimi) |
| **C3** | Multi-node execution | Pipeline/replica across internet nodes | **done** (sequential PP + replan) |
| **C4** | Gateway + keys | OpenAI clients work; freeloader denied | **done** |
| **C5** | Live capacity API + dashboard UI | Public page shows live pool stats | **done** (+ tokens/s) |
| **C6** | Challenges + economy | Mint/burn live; abuse path measured | **done** |
| **C7** | Public alpha installers | External donors move the dashboard numbers | **done** (v0.1.13 assets + channels) |

**Status honesty:** protocol/control/lab track shipped at **0.1.13+**. Production Kimi path: real **96-shard** MANIFEST pins + **ADR 0003** `ProductionEngine` (CUDA driver FFI) + content-proof mesh tail text + **commit-gated** quant upgrades. Donors hold **band slices** (not full multi‑TB on every box). Full multi-TB fleet residency and multi-node service-live still require fleet storage/VRAM (**≥64 GiB**, **≥3** backends) — not a single small card. Permanent product-law non-goals: cash/multi-model, f00 weight CDN.

## 13. Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Name | **joule** | Energy unit; millijoules as credit |
| Topology | **Distributed internet cluster** | Not mesh/LAN product |
| Connectivity | **Irrelevant** | Only reachability + health |
| Dashboard | **Live capacity required** | Show pool strength continuously |
| Client | **Native agent** | Real GPU inference |
| Language | **Rust workspace** | One binary agent |
| API shape | **OpenAI-compatible** | Drop-in tools |
| Capacity API | **Public aggregate JSON** | Dashboard + transparency |

## 14. Open questions

1. Exact Kimi checkpoint + license pin.  
2. Transport (lean QUIC).  
3. Pure-Rust vs ADR’d FFI for production speed.  
4. Contribution window *N* and freeload grace.  
5. ~~Whether public capacity includes rough **tokens/s estimate** once measured (not just throughput_class_sum).~~ **Done (Unreleased):** `ClusterCapacity.tokens_per_sec` measured from completion wall time + samples; dashboard binds `stat-tokens-per-sec`.

## 15. PR Plan

| PR | Title | Scope | Depends |
|---|---|---|---|
| PR1 | chore: cluster rename + capacity | proto/cluster crates, design, CLI capacity | — |
| PR2 | feat(cluster): transport + auth | agent join, heartbeat | PR1 |
| PR3 | feat(runtime): single-node engine | load quant, generate | PR1 |
| PR4 | feat(cluster): multi-node execution | distributed forward | PR2, PR3 |
| PR5 | feat(gateway): OpenAI API | `/v1`, streaming, keys | PR3 |
| PR6 | feat(api): live capacity endpoint | `GET /v1/cluster/capacity` | PR2 |
| PR7 | feat(site): dashboard capacity strip | live numbers UI | PR6 |
| PR8 | feat(ledger): live mint/burn + gates | contribution required | PR5 |
| PR9 | feat(agent): idle policy + installers | thermal/battery, install.sh | PR2 |
| PR10 | feat(security): challenges | anti-cheat sampling | PR4, PR8 |

---

## Appendix — CLI sketch

```text
joule lab --model kimi-open-q4 --peers 3 --stages 2
joule capacity --peers 5 --json
joule credits --account alice
joule agent --model kimi-open-q4
joule version
```
