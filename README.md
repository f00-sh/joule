# joule

**Distributed supercomputer cluster** — pool idle GPUs across the internet into open-weight AI inference (Kimi-class).

Donors run a native agent. Nodes join one shared cluster no matter how they reach the net. Placement can span many machines. Access to the OpenAI-compatible API is earned only by contributing compute — **free on the public pool (no cash)**. Credits are **millijoules** under an auditable fairness algorithm (√VRAM, tenure boost, leecher penalties). The **dashboard always shows live pool capacity**.

| | |
|---|---|
| Status | **Production path (0.1.13+)** — real Kimi-K3 pins, CUDA `ProductionEngine` (ADR 0003), dumb-user start, mesh content-proof serve |
| License | [MIT](LICENSE) |
| Site | [joule.f00.sh](https://joule.f00.sh/) |
| Repo | [github.com/f00-sh/joule](https://github.com/f00-sh/joule) |
| Design | [cluster](docs/design/cluster-v0.md) · [economy](docs/design/economy-v0.md) · [distribution](docs/design/distribution-v0.md) · [broadcast](docs/design/broadcast-v0.md) · [ceremony](docs/design/operator-ceremony-v0.md) · [K3 map](docs/design/k3-file-layer-map-v0.md) |

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

**Preferred:** permanent links that always resolve to the **newest** release
([/current](https://joule.f00.sh/current/) · [download.html](https://joule.f00.sh/download.html)).

### GUI first (permanent — no version in the URL)

| Platform | Site (forever) | GitHub `latest` stable name |
|----------|----------------|-----------------------------|
| **Windows Setup** | https://joule.f00.sh/current/windows/setup.exe | `joule-windows-x86_64-setup.exe` |
| **macOS arm64 .pkg** | https://joule.f00.sh/current/macos/arm64.pkg | `joule-darwin-aarch64.pkg` |
| **macOS arm64 .dmg** | https://joule.f00.sh/current/macos/arm64.dmg | `joule-darwin-aarch64.dmg` |
| **Linux amd64 .deb** | https://joule.f00.sh/current/linux/amd64.deb | `joule-linux-x86_64.deb` |

GitHub form: `https://github.com/f00-sh/joule/releases/latest/download/<stable-name>`  
Full map (intel mac, arm64 linux, CLI): [joule.f00.sh/current/](https://joule.f00.sh/current/)

### CLI one-liners (also permanent)

```text
# Linux / macOS
curl -fsSL https://joule.f00.sh/current/install.sh | sh
```

```powershell
# Windows (launches Setup.exe when available)
irm https://joule.f00.sh/current/install.ps1 | iex
```

### Package managers

| Channel | Notes |
|---------|--------|
| **GitHub Releases** | Versioned + stable names — [releases](https://github.com/f00-sh/joule/releases) |
| **Homebrew** | `brew install f00-sh/tap/joule` (optional; `.pkg`/`.dmg` also ship) |
| **AUR (`joule-bin`)** | `yay -S joule-bin` · [aur.archlinux.org/packages/joule-bin](https://aur.archlinux.org/packages/joule-bin) (mirror: [f00-sh/aur-joule-bin](https://github.com/f00-sh/aur-joule-bin)) |
| **Windows** | **Setup.exe** via `/current/windows/setup.exe` (unsigned until cert) |

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

## Quick start (stupid-easy — do this)

```text
joule get-started          # print the checklist anytime
joule                      # GUI (default) — click ★ DO EVERYTHING (local pool)
joule service install      # reboot-safe: control + agent + tray autostart
joule connect              # Base URL + joule_… key for Cursor / OpenAI clients
joule chat --prompt "hi"
```

Headless (no GUI):

```text
joule start                # control + agent in background, waits until healthy
joule status --api http://127.0.0.1:7700
```

GUI also auto-starts a local pool on first open if nothing is on `:7700`
(set `JOULE_GUI_NO_AUTO=1` to skip). Graphs / Donor / Chat tabs remain for power users.

**Autostart:** `joule service install` enables **user-session** services (systemd `--user`,
LaunchAgents, Task Scheduler at logon) so control + agent come back after reboot.
Not a silent root system service (tray/GPU need a user session).

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

**Full Kimi baseline (fleet gate):** ≥**64 GiB** healthy/verified aggregate VRAM **and** ≥**3** backends (`full_k3_service_fleet_ok`). One 8 GiB laptop cannot honestly claim full-K3 service-live alone. `lab-*` fixtures are for protocol CI only — **not** production Kimi quality.

**Not every box downloads the full multi‑TB model.** That would defeat the product. Donors prepare **band/shard slices** (preferred weight files for their plan layers). Weights are **sha256 content-addressed and peer-seeded**; fat seeders may hold more files, thin donors only their band. Control service digests SoT is production **`kimi-k3-shards`** (96 real `moonshotai/Kimi-K3` LFS digests) — lab complete does **not** unlock service claims. Agents use **commit-gated** quant upgrade: fleet recommend of K3 never bricks an already-serving lab path before digests verify.

```text
joule ready                              # offline what-if
joule ready --api http://127.0.0.1:7700  # live
curl -s http://127.0.0.1:7700/v1/models/readiness
bash scripts/production-smoke.sh         # release binary: 2 agents → challenges → chat economy
```

## Usage

```text
joule                          # GUI (default)
joule get-started              # dumb-user checklist
joule start                    # local control + agent
joule service install          # control + agent + tray autostart
joule version
joule control [--agent-listen ADDR] [--http-listen ADDR]
joule agent --account NAME [--control HOST:PORT] [--peer-listen HOST:PORT] [--mem-mib N]
joule capacity --api http://127.0.0.1:7700 --json
joule connect [--copy]
joule chat --prompt "…"
joule whoami
joule lab --peers 3
joule credits --account alice
joule ready [--api URL]
joule status --api URL [--dash|--json]
joule monitor --api URL
joule tray --api URL
joule service generate --platform linux|macos|windows --kind control|agent|tray
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
| `joule-runtime` | `Engine` + `ProductionEngine` (ADR 0003) + lab `ClusterEngine` |
| `joule-ledger` | Millijoule mint/burn |
| `joule-control` | Control plane: agents + HTTP API |
| `joule-client` | Shared status + OS service unit generation |
| `joule-mesh` / `joule-dht` / `joule-net` | Peer bus, DHT, public multiaddrs / QUIC path |
| `joule` | CLI + GUI + agent |

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
| Operator SOP (PDF) | [docs/sop-joule-ops.pdf](docs/sop-joule-ops.pdf) · [JSON](docs/sop-joule-ops.json) |
| Latest tagged release memo | [docs/releases/v0.1.13](docs/releases/) when present · see [releases/](docs/releases/) |
| Release memos | [docs/releases/](docs/releases/) |
| ADR 0003 (CUDA production engine) | [docs/adr/0003-cuda-production-engine.md](docs/adr/0003-cuda-production-engine.md) |

## Scene card

```text
╔══════════════════════════════════════════════════╗
║▓▓▓▓░░░░  joule  ░░░░▓▓▓▓                         ║
║████████████████████████████████████████████████  ║
║  ▄█▀  DISTRIBUTED CLUSTER  ▀█▄                   ║
║████████████████████████████████████████████████  ║
║  v0.1.13  ·  MIT  ·  2026                         ║
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
