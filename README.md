# joule

**Distributed supercomputer cluster** — pool idle GPUs across the internet into open-weight AI inference (Kimi-class).

Donors run a native agent. Nodes join one shared cluster no matter how they reach the net. Placement can span many machines. Access to the OpenAI-compatible API is earned only by contributing compute — **free on the public pool (no cash)**. Credits are **millijoules** under an auditable fairness algorithm (√VRAM, tenure boost, leecher penalties). The **dashboard always shows live pool capacity**.

| | |
|---|---|
| Status | **0.0.0** research / wip |
| License | [MIT](LICENSE) |
| Site | [joule.f00.sh](https://joule.f00.sh/) |
| Repo | [github.com/f00-sh/joule](https://github.com/f00-sh/joule) |
| Design | [docs/design/cluster-v0.md](docs/design/cluster-v0.md) · [economy-v0](docs/design/economy-v0.md) |

## Why

Central API clouds concentrate power and cost. Consumer GPUs sit idle. joule turns spare cycles into a **community cluster on the public internet**: no cash buy-in for the public pool — only donated compute. Multi-node inference is first-class. We do not care if a node is on LAN, WAN, cellular, or anything else — only that it is online, healthy, and donating.

## How it works (target)

1. Install the **native agent** (`joule agent`) on a machine with a GPU (CPU works for lab only).
2. Agent joins the **distributed cluster**, advertises VRAM/model class, accepts shard or replica plans.
3. Account **mints millijoules** from verified contribution (heartbeats, shards, challenges), scaled fairly:
   - **√VRAM** so small cards are not frozen out
   - **tenure boost** for continuous healthy time (up to 1.5×)
   - **leecher penalty** if you consume ≫ contribute (down to 0.25× earn / up to 4× pay)
4. Point any OpenAI-compatible client at the gateway with your API key.
5. Usage **burns millijoules**. No live contribution / balance ⇒ no access.
6. **Dashboard** shows live aggregate compute (healthy nodes, VRAM, throughput class, models online).

The website ([joule.f00.sh](https://joule.f00.sh/)) is a teaser + capacity strip + docs — **not** browser-side pool compute. Full economy: [docs/design/economy-v0.md](docs/design/economy-v0.md).

## Requirements

- Rust **1.85+** (stable) to build from source
- Linux / macOS / Windows (agent targets; lab works anywhere `cargo test` runs)
- GPU recommended for real inference (CUDA / ROCm / Metal when engines land)

## Install

Every install method installs man page(s). Prefer curl from releases when version ≥ 0.1.0 ships.

### Curl (releases)

```text
curl -fsSL https://github.com/f00-sh/joule/releases/latest/download/install.sh | sh
```

*(Asset download is wired when the first binary release ships.)*

### From source

```text
git clone https://github.com/f00-sh/joule.git
cd joule
cargo build --release -p joule
./target/release/joule version
./target/release/joule capacity --json
./target/release/joule lab
```

## Quick start (local pool)

### Terminal 1 — control plane (localhost)

```text
cargo run -p joule --release -- control
# or: ./target/release/joule control --ephemeral

# agents    → 127.0.0.1:7701
# http      → http://127.0.0.1:7700
# dashboard → http://127.0.0.1:7700/
# healthz   → http://127.0.0.1:7700/healthz
```

Leave this running. Open **http://127.0.0.1:7700/** in a browser.

### Terminal 2+ — donate (one or more machines)

```text
cargo run -p joule --release -- agent --account alice --control 127.0.0.1:7701
# optional second donor:
cargo run -p joule --release -- agent --account bob --control 127.0.0.1:7701 --mem-mib 16384
```

Each agent prints an **API key**. Dashboard should show healthy nodes + VRAM.

### Terminal 3 — use the pool

```text
curl -s http://127.0.0.1:7700/v1/cluster/capacity | jq
joule whoami --key joule_…
joule chat --key joule_… --prompt "hello from the pool"
joule chat --key joule_… --stream --prompt "stream me"
```

**Law:** no active donor agent for that account → chat forbidden. Invalid keys → 401.

**One logical device:** if five machines give 8+16+16+16+16 GiB, joule sees **one GPU with ~72 GiB VRAM** (`capacity.logical_device`). Internally that memory is sharded to place **`kimi-open`**; publicly you are talking to one supercomputer. Concurrent users share stream slots on that device.

**Kimi waits for a big enough pool.** Manifest (`models/MANIFEST.json`) requires ≥**64 GiB** aggregate VRAM and ≥**3** backends before the pool is `model_ready`. Until then (and until weights are published), inference is **stub**. Agents **arm** the weight cache when the pool crosses the gate; they do not download multi‑GB weights yet.

```text
joule ready                              # offline what-if
joule ready --api http://127.0.0.1:7700  # live
curl -s http://127.0.0.1:7700/v1/models/readiness
```

## Usage

```text
joule version
joule control [--agent-listen ADDR] [--http-listen ADDR]
joule agent --account NAME [--control HOST:PORT] [--model TAG] [--mem-mib N]
joule capacity --api http://127.0.0.1:7700 --json
joule chat --key joule_… --prompt "…"
joule whoami --key joule_…
joule lab --peers 3
joule credits --account alice
```

Full option reference: [man/joule.1.md](man/joule.1.md).

## Architecture (crates)

| Crate | Role |
|---|---|
| `joule-proto` | Wire protocol + `ClusterCapacity` |
| `joule-cluster` | Membership, capacity aggregate, placement |
| `joule-runtime` | Inference `Engine` trait + stub |
| `joule-ledger` | Millijoule mint/burn |
| `joule-control` | Control plane: agents + HTTP API |
| `joule` | CLI |

See [docs/design/cluster-v0.md](docs/design/cluster-v0.md) for phases C0–C7 and the PR plan.

## Documentation

| Surface | Location |
|---|---|
| This README | [README.md](README.md) |
| Design | [docs/design/cluster-v0.md](docs/design/cluster-v0.md) · [economy-v0](docs/design/economy-v0.md) |
| Man page | [man/joule.1.md](man/joule.1.md) |
| GitHub Pages | [docs/](docs/) |
| Changelog | [CHANGELOG.md](CHANGELOG.md) |
| Scene card | [file_id.diz](file_id.diz) |
| Operator SOP (PDF) | [docs/sop-joule-ops.pdf](docs/sop-joule-ops.pdf) — generated on release |
| Release memos | [docs/releases/](docs/releases/) |

## Scene card

```text
╔══════════════════════════════════════════════════╗
║▓▓▓▓░░░░  joule  ░░░░▓▓▓▓                         ║
║████████████████████████████████████████████████  ║
║  ▄█▀  DISTRIBUTED CLUSTER  ▀█▄                   ║
║████████████████████████████████████████████████  ║
║  v0.0.0  ·  MIT  ·  2026                         ║
║  idle GPUs → open cluster · pay in compute       ║
║  github:f00-sh/joule  ·  joule.f00.sh             ║
╚══════════════════════════════════════════════════╝
```

## Development

```text
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p joule -- capacity --json
cargo run -p joule -- lab
```

See [CONTRIBUTING.md](CONTRIBUTING.md) and [AGENTS.md](AGENTS.md).

## Versioning

[Semantic Versioning](https://semver.org/). See [CHANGELOG.md](CHANGELOG.md).

## Security

See [SECURITY.md](SECURITY.md). Do not file security issues in public trackers without reading that policy.

## License

[MIT](LICENSE) © William Theesfeld
