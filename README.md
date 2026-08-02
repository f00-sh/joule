# joule

**Distributed supercomputer cluster** — pool idle GPUs across the internet into open-weight AI inference (Kimi-class).

Donors run a native agent. Nodes join one shared cluster no matter how they reach the net. Placement can span many machines. Access to the OpenAI-compatible API is earned only by contributing compute — **free on the public pool (no cash)**. Credits are **millijoules** under an auditable fairness algorithm (√VRAM, tenure boost, leecher penalties). The **dashboard always shows live pool capacity**.

| | |
|---|---|
| Status | **0.1.6** released (GUI dashboard + graphs; donor controls; Kimi gated) |
| License | [MIT](LICENSE) |
| Site | [joule.f00.sh](https://joule.f00.sh/) |
| Repo | [github.com/f00-sh/joule](https://github.com/f00-sh/joule) |
| Design | [cluster](docs/design/cluster-v0.md) · [economy](docs/design/economy-v0.md) · [distribution](docs/design/distribution-v0.md) · [broadcast](docs/design/broadcast-v0.md) · [ceremony](docs/design/operator-ceremony-v0.md) |

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

The website ([joule.f00.sh](https://joule.f00.sh/)) is a f00-themed teaser with **real-time cluster data** (SSE `/api/pool/stream`, poll `/api/pool`) — **not** browser-side pool FLOPs. Control publishes with `JOULE_EDGE_TOKEN`. Full economy: [docs/design/economy-v0.md](docs/design/economy-v0.md).

## Requirements

- Rust **1.85+** (stable) to build from source
- Linux / macOS / Windows (agent targets; lab works anywhere `cargo test` runs)
- GPU recommended for real inference (CUDA / ROCm / Metal when engines land)

## Install

**Dummy easy:** [joule.f00.sh/download.html](https://joule.f00.sh/download.html) autodetects your OS.
Prebuilt binaries are produced by GitHub Actions and published on **GitHub Releases** (the website only links).

### One-liner

```text
# Linux / macOS
curl -fsSL https://github.com/f00-sh/joule/releases/latest/download/install.sh | sh
```

```powershell
# Windows (PowerShell — no admin)
irm https://github.com/f00-sh/joule/releases/latest/download/install.ps1 | iex
```

### Package managers

| Channel | Notes |
|---------|--------|
| **GitHub Releases** | Canonical — [f00-sh/joule releases](https://github.com/f00-sh/joule/releases) |
| **Homebrew** | `brew install f00-sh/tap/joule` ([formula](https://github.com/f00-sh/homebrew-tap/blob/main/Formula/joule.rb)) |
| **Arch PKGBUILD** | [f00-sh/aur-joule-bin](https://github.com/f00-sh/aur-joule-bin) — `makepkg -si` after clone |
| **Windows** | ZIP + `install.ps1` (user-local). Signed MSI/EXE when a cert is available |

### Your joule code (multi-machine, no PII)

```text
# first machine — just run the agent; a UUID code is created for you
joule agent --control HOST:7701
# banner shows:  550e8400-e29b-41d4-a716-446655440000

# other machines — paste the same code
joule identity use 550e8400-e29b-41d4-a716-446655440000
joule agent --control HOST:7701
```

Same code ⇒ same millijoule balance. You never pick a username. See [docs/design/identity-v0.md](docs/design/identity-v0.md).

### From source

```text
git clone https://github.com/f00-sh/joule.git
cd joule
cargo build --release -p joule
./target/release/joule version
./target/release/joule capacity --json
./target/release/joule lab
```

## Quick start (normie / GUI first)

```text
joule            # opens the graphical dashboard (graphs + buttons)
# or:
joule gui
```

In the GUI: **Start control** → **Start agent (donate)** → watch pool graphs.
No separate “open a terminal and guess ports” required for the happy path.

### Advanced: Terminal 1 — control plane (localhost)

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

Each agent prints an **API key** and a CONNECT card. Dashboard should show healthy nodes + VRAM.

### Terminal 3 — use the pool (or Cursor / any OpenAI client)

```text
# After agent Welcome, key is cached — no manual --key needed:
joule connect                         # Base URL + full API key + model
joule connect --copy                  # copy key for Cursor paste
joule whoami
joule chat --prompt "hello from the pool"
joule chat --stream --prompt "stream me"

# Still works with an explicit key:
curl -s http://127.0.0.1:7700/v1/cluster/capacity | jq
joule whoami --key joule_…
```

**Connect other apps:** paste the three fields from `joule connect` (or `~/.config/joule/JOULE-CONNECT.txt`) into Cursor / Continue / Open WebUI: Base URL `http://HOST:7700/v1`, API key `joule_…`, model `kimi-open`.

**Law:** no active donor agent for that account → chat forbidden. Invalid keys → 401.

**One logical device:** if five machines give 8+16+16+16+16 GiB, joule sees **one GPU with ~72 GiB VRAM** (`capacity.logical_device`). Internally that memory is sharded to place **`kimi-open`**; publicly you are talking to one supercomputer. Concurrent users share stream slots on that device.

**Distribution law:** f00 / joule.f00.sh is a **website only** — not a download farm. Weights and software are **sha256 content-addressed** and **peer-seeded** (`docs/design/distribution-v0.md`). Agents fill `~/.local/share/joule/` from local drops, git fixtures (`repo://`), and the swarm; third-party HTTP only if `JOULE_ALLOW_EXTERNAL_FETCH=1`.

**Mesh discovery (decentral):** agents open a **peer listen** port (`--peer-listen`, default ephemeral), gossip `PeerAlive`, and prefer **direct** blob transfer; control is a temporary rendezvous. Phase C starts with `joule-dht` + replaceable `bootstrap.json` (`docs/design/decentral-discovery-v0.md`).

**Kimi waits for a big enough pool.** ≥**64 GiB** verified VRAM and ≥**3** backends for `model_ready`. Manifest lists digests; `lab-tiny` seeds from the git tree; full K3 shards spread once someone on the mesh has them.

```text
joule ready                              # offline what-if
joule ready --api http://127.0.0.1:7700  # live
curl -s http://127.0.0.1:7700/v1/models/readiness
```

## Usage

```text
joule version
joule control [--agent-listen ADDR] [--http-listen ADDR]
joule agent --account NAME [--control HOST:PORT] [--peer-listen HOST:PORT] [--mem-mib N]
joule capacity --api http://127.0.0.1:7700 --json
joule chat --key joule_… --prompt "…"
joule whoami --key joule_…
joule lab --peers 3
joule credits --account alice
joule ready [--api URL]
joule status --api URL --key joule_… [--dash|--json]
joule monitor --api URL --key joule_…          # living dash (all platforms)
joule tray --api URL --key joule_…             # tray/monitor surface
joule service generate --platform linux|macos|windows --kind agent|tray
joule service install-help --platform linux
joule seed-blob --path FILE [--kind software]
joule software status|apply
joule broadcast keygen|sign|inject|plan-chunks
```

**Client / donor UX:** `status` and `monitor` work on **Linux, macOS, and Windows** (same CLI).  
Linux also has **systemd** user units (`joule service generate --platform linux`).  
macOS: LaunchAgents plist; Windows: Task Scheduler XML. Prefer **user-session** auto-start so a tray/monitor can run with the GPU agent (not root-only).

**Operator bus:** stock builds verify the **embedded official** protocol key (OpenPGP master `tj@f00.sh` certifies it). Env override only with `JOULE_ALLOW_UNOFFICIAL_OPERATOR=1` (lab). Trust model: [docs/design/master-key-trust-v0.md](docs/design/master-key-trust-v0.md). Demo: `scripts/demo-operator-bus.sh`. Seed: `scripts/seed-lab-tiny.sh`.

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
| Design | [cluster](docs/design/cluster-v0.md) · [economy](docs/design/economy-v0.md) · [distribution](docs/design/distribution-v0.md) · [broadcast](docs/design/broadcast-v0.md) · [ceremony](docs/design/operator-ceremony-v0.md) |
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
║  v0.1.6  ·  MIT  ·  2026                         ║
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
