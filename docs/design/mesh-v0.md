# joule — mesh-v0 design (research mesh-first)

**Status:** active  
**Version target:** 0.1.0 research  
**Product:** [joule](https://github.com/f00-sh/joule) · f00-sh  
**Default model family:** open Kimi weights (tag `kimi-open-*` until K3 pin is frozen)

---

## 1. Problem

Frontier open models need more VRAM and bandwidth than one idle consumer GPU often has. Centralized API clouds reintroduce single-operator dependence. joule pools **donated idle compute** into a **mesh supercomputer**: users earn API access only by contributing, and inference is placed across peers when multi-node plans beat single-node replicas.

## 2. Product laws

1. **Compute is the only currency.** No paid bypass in the public pool.
2. **No contribution ⇒ no API.** Keys fail closed without an active donor window and/or spendable balance.
3. **Credits measure verified useful work** (millijoules), not uptime theater.
4. **Donor machines stay under user control** (caps, schedule, pause, thermal/battery).
5. **Open weights, open agent, open protocol.** Anyone can run a grid; f00 may host a default pool.
6. **Mesh-first research:** multi-node placement and failure handling are first-class, not a post-MVP patch.

## 3. Goals / non-goals

### Goals (v0–v1 research)

- Native **donor agent** (not browser compute).
- **Versioned mesh protocol** (hello, heartbeat, plan, infer, challenge, credit events).
- **Placement planner** that prefers pipeline/tensor plans when peers allow; replica fallback.
- **OpenAI-compatible gateway** surface (after mesh lab works).
- **Ledger** in millijoules: mint on verified contribution, burn on usage.
- **Stub runtime** for CI; real engine behind a trait.

### Non-goals (now)

- Browser WebGPU as primary capacity.
- Trustless global consensus / full blockchain settlement.
- Training (inference only).
- Guaranteeing datacenter-class latency on residential WAN for every plan shape.
- Shipping Kimi under a non-redistributable license without a clear weights policy.

## 4. Architecture

```
                    ┌──────────────────────────┐
   OpenAI clients ──►│ gateway (HTTPS /v1)      │
                    └────────────┬─────────────┘
                                 │ plan + route
                    ┌────────────▼─────────────┐
                    │ mesh control (open)        │
                    │ membership · planner ·    │
                    │ challenges · ledger gossip│
                    └─────┬───────────┬─────────┘
                          │           │
              ┌───────────▼──┐   ┌────▼────────────┐
              │ agent node A │   │ agent node B …  │
              │ runtime shard│   │ runtime shard   │
              └──────────────┘   └─────────────────┘
```

### Crates (workspace)

| Crate | Role |
|---|---|
| `joule-proto` | Wire types, `MeshPlan`, messages |
| `joule-mesh` | Membership + placement policy |
| `joule-runtime` | `Engine` trait + `StubEngine` |
| `joule-ledger` | Millijoule accounting |
| `joule` | CLI: `lab`, `credits`, `agent` (stub) |

### Client model

Users **download a native program** (`joule agent`). The website is for accounts, keys, stats, and installers — **not** for supplying the pool’s FLOPs.

## 5. Mesh-first placement

### Plan types

1. **Replica** — full quant on one node (always available fallback).
2. **Pipeline** — layer ranges across N peers (first multi-node target).
3. **Tensor** — TP ranks (later; needs faster interconnect assumptions).
4. **Prefill/Decode split** — disagg serving (later).

### Policy (v0)

```
if prefer_pipeline && healthy_gpu_peers >= stages && stages > 1:
    assign pipeline shards (layer ranges)
else:
    assign single best replica (by mem, then load)
```

Layer ranges in v0 are **placeholders** (8 layers/stage) for protocol + scheduler tests. Real ranges come from model config + measured memory.

### Failure model

- Any shard dies mid-request → cancel + replan or failover to replica if capacity exists.
- Heartbeats + load advertise soft state; unhealthy peers drop from eligibility.
- Challenges re-run samples on other peers to police bad outputs.

### Network reality

Residential WAN cannot pretend to be NVLink. Mesh-first still means:

- Prefer **same-LAN / high-RTT-class clusters** for TP/pipeline when possible.
- Advertise **link class** (lan / metro / wan) in a later caps revision.
- For wan-only peers, prefer **replica** or **prefill/decode** over fine TP.

Research workstream: measure token/s and TTFT for pipeline depth × RTT matrices; encode thresholds into the planner.

## 6. Runtime strategy

1. **StubEngine** — CI and `joule lab` (done).
2. **Pure-Rust engine path** (preferred under language purity) — e.g. candle / similar for supported arches.
3. **Explicit purity exception** only if required for production Kimi performance (e.g. llama.cpp FFI), recorded in AGENTS.md + ADR.

Do not silently shell out to Python.

## 7. Economy (millijoules)

| Action | Effect |
|---|---|
| Verified contribution (tokens produced under challenge regime) | **mint** mJ × device multiplier |
| API usage (prompt + completion tokens) | **burn** mJ |
| Idle online with zero useful work | ~0 mint |

**Access gate (recommended):**

- At least one healthy agent for the account in the last *N* minutes **and**
- `balance >= estimated_burn` for the request (or small rolling debt capped by recent mint rate).

**Device multipliers (starting point, tune with data):**

| Class | Multiplier |
|---|---|
| Discrete GPU (high VRAM) | 8–16 |
| Discrete GPU (mid) | 4–8 |
| Metal unified | 4–8 |
| CPU | 1 |

Bigger donors get higher rate limits / priority, not infinite free inference without balance.

## 8. Security & trust

- Agent ↔ mesh auth: device keys; rotate on reinstall.
- API keys: bearer tokens bound to account + contribution state.
- **Never** trust client-reported FLOPs alone; use challenges + peer recompute samples.
- Sandbox model weights path; agent must not exfiltrate unrelated user files.
- Clear UX: what binary runs, what ports, what model files, how to stop.

## 9. Decentralization posture

| Layer | v0 | Later |
|---|---|---|
| Inference workers | Peer-owned | Same |
| Planner / gateway | Single open process OK | Multi-coordinator federation |
| Ledger | Single-node append log | Gossip + conflict rules / merkle checkpoints |
| Weights | Content-addressed cache | Multi-mirror |

Honest claim: **inference is multi-node; control plane starts simple and is open-source.** “No single node anywhere” is a multi-phase protocol goal, not a launch blocker.

## 10. Default model

- Serve **open Kimi-family** weights under a redistributable license.
- Pin exact revision in `models/MANIFEST` when first real engine lands.
- Quants ladder by VRAM class (e.g. Q4 for 8–12GB, higher for 24GB+).
- If “Kimi K3” is not yet published under a usable license, ship latest open Kimi and swap the pin.

## 11. Phased delivery (mesh-first)

| Phase | Outcome | Exit criteria |
|---|---|---|
| **M0** — skeleton | Workspace, proto, lab CLI, ledger, design | `cargo test` + `joule lab` green |
| **M1** — transport | QUIC/TCP sessions, hello/heartbeat, auth | 2 processes on LAN form a mesh |
| **M2** — real runtime (single node) | Engine loads quant, streams tokens | Replica plan serves real model |
| **M3** — pipeline mesh | Multi-node forward pass on LAN | 2-node pipeline > toy model correctness |
| **M4** — gateway + keys | OpenAI `/v1/chat/completions`, contribution gate | External client works; freeloader denied |
| **M5** — challenges + economy | Spot recompute, mint/burn live | Abuse path measured; overdraft blocked |
| **M6** — public alpha | Installers, idle policy, docs | External donors join default pool |
| **M7** — federation research | Multi-coordinator, wan classes | Written results + optional code |

## 12. Key decisions

| Decision | Choice | Rationale |
|---|---|---|
| Name | **joule** | Energy unit; millijoules as credit |
| Posture | **Research mesh-first** | Multi-node placement before consumer polish |
| Client | **Native agent** | Real GPU inference; browser is dashboard only |
| Language | **Rust workspace** | One binary agent; purity for protocol |
| Currency | **millijoules** | Integer ledger, physical metaphor |
| API shape | **OpenAI-compatible** (M4) | Drop-in for existing tools |
| Runtime | **Trait + stub first** | Unblock mesh without GPU in CI |

## 13. Open questions

1. Exact Kimi checkpoint + license pin for first real weights.
2. QUIC vs TCP+TLS for agent transport (lean QUIC).
3. Whether pure-Rust inference meets quality/speed for Kimi-class models or ADR for FFI.
4. Public pool operator identity (f00 default) vs bring-your-own coordinator only.
5. Contribution window *N* minutes and freeload grace.

## 14. PR Plan

| PR | Title | Scope | Depends |
|---|---|---|---|
| PR1 | chore: workspace skeleton + design | crates, CI, docs/design/mesh-v0.md, lab CLI | — |
| PR2 | feat(mesh): session transport + auth | connect, hello, heartbeat over network | PR1 |
| PR3 | feat(runtime): single-node engine | load quant, generate tokens | PR1 |
| PR4 | feat(mesh): pipeline execution path | multi-node forward on LAN | PR2, PR3 |
| PR5 | feat(gateway): OpenAI-compatible API | `/v1`, streaming, API keys | PR3 |
| PR6 | feat(ledger): live mint/burn + gates | contribution required for keys | PR5 |
| PR7 | feat(agent): idle policy + installers | thermal/battery, install.sh real | PR2 |
| PR8 | feat(security): challenges + reputation | anti-cheat sampling | PR4, PR6 |
| PR9 | docs: alpha operator SOP + release prep | NASA SOP, Pages, man | PR7 |

---

## Appendix A — CLI sketch

```text
joule lab --model kimi-open-q4 --peers 3 --stages 2
joule credits --account alice
joule agent --model kimi-open-q4
joule version
```

## Appendix B — Related art

- Volunteer compute: BOINC, Folding@home (donation UX; not LLM mesh).
- Distributed inference stacks: vLLM disagg, llm-d (cluster-oriented, not volunteer).
- joule combines **volunteer donation economics** with **mesh placement research** and an **OpenAI-compatible front door**.
