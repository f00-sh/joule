# joule

**Decentralized mesh supercomputer** — pool idle GPUs into open-weight AI inference (Kimi-class).

joule is an f00 product: donors run a native agent; the mesh places work across peers (pipeline when it helps, replica when it must); access to the OpenAI-compatible API is earned only by contributing compute. Credits are **millijoules**.

| | |
|---|---|
| Status | **0.0.0** research / wip |
| License | [MIT](LICENSE) |
| Site | [joule.f00.sh](https://joule.f00.sh/) |
| Repo | [github.com/f00-sh/joule](https://github.com/f00-sh/joule) |
| Design | [docs/design/mesh-v0.md](docs/design/mesh-v0.md) |

## Why

Central API clouds concentrate power and cost. Consumer GPUs sit idle. joule turns spare cycles into a **community mesh**: no cash buy-in for the public pool — only donated compute. Research focus is **mesh-first** multi-node inference, not a single-box wrapper with marketing.

## How it works (target)

1. Install the **native agent** (`joule agent`) on a machine with a GPU (CPU works for lab only).
2. Agent joins the mesh, advertises VRAM/model class, accepts shard or replica plans.
3. Account **mints millijoules** from verified contribution.
4. Point any OpenAI-compatible client at the gateway with your API key.
5. Usage **burns millijoules**. No live contribution / balance ⇒ no access.

The website is for keys, stats, and installers — **not** for browser-side pool compute.

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
./target/release/joule lab
```

## Usage

```text
# Protocol + build identity
joule version

# Local mesh lab: synthetic peers, placement plan, stub inference, ledger demo
joule lab --model kimi-open-q4 --peers 3 --stages 2

# Millijoule ledger demo
joule credits --account alice

# Donor agent (network transport: not implemented in 0.0.0)
joule agent --model kimi-open-q4
```

Full option reference: [man/joule.1.md](man/joule.1.md).

## Architecture (crates)

| Crate | Role |
|---|---|
| `joule-proto` | Wire protocol types |
| `joule-mesh` | Membership + placement |
| `joule-runtime` | Inference `Engine` trait + stub |
| `joule-ledger` | Millijoule mint/burn |
| `joule` | CLI |

See [docs/design/mesh-v0.md](docs/design/mesh-v0.md) for mesh-first phases M0–M7 and the PR plan.

## Documentation

| Surface | Location |
|---|---|
| This README | [README.md](README.md) |
| Design | [docs/design/mesh-v0.md](docs/design/mesh-v0.md) |
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
║  ▄█▀  MESH SUPERCOMPUTER  ▀█▄                    ║
║████████████████████████████████████████████████  ║
║  v0.0.0  ·  MIT  ·  2026                         ║
║  idle GPUs → open-weight mesh · pay in compute   ║
║  github:f00-sh/joule  ·  joule.f00.sh             ║
╚══════════════════════════════════════════════════╝
```

## Development

```text
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p joule -- lab
```

See [CONTRIBUTING.md](CONTRIBUTING.md) and [AGENTS.md](AGENTS.md).

## Versioning

[Semantic Versioning](https://semver.org/). See [CHANGELOG.md](CHANGELOG.md).

## Security

See [SECURITY.md](SECURITY.md). Do not file security issues in public trackers without reading that policy.

## License

[MIT](LICENSE) © William Theesfeld
