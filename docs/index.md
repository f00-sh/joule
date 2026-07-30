# joule

**Distributed supercomputer cluster for open-weight AI.** Idle GPUs anywhere on the internet join one pool. Access is earned in millijoules — compute in, tokens out. The dashboard shows **live** how much distributed compute is online.

| | |
|---|---|
| Status | 0.0.0 research / wip |
| Repo | [github.com/f00-sh/joule](https://github.com/f00-sh/joule) |
| License | MIT |

## Why this project exists

Frontier models need more than one idle card. Central clouds re-centralize power. joule pools donated machines into an **internet-wide cluster**. Multi-node placement is first-class. Connectivity medium does not matter. Donors keep control of their hardware. Freeloaders without contribution do not get working keys. Everyone can see the pool size on the dashboard.

## Requirements

- Native agent (download a program — not browser compute)
- GPU recommended for production inference
- Rust 1.85+ to build from source

## Install

Every install method installs man page(s).

### Curl (releases)

```text
curl -fsSL https://github.com/f00-sh/joule/releases/latest/download/install.sh | sh
```

### From source

```text
git clone https://github.com/f00-sh/joule.git
cd joule
cargo build --release -p joule
./target/release/joule capacity --json
./target/release/joule lab
```

## Usage

```text
joule control
# open http://127.0.0.1:7700/ for the live dashboard
joule agent --account alice
joule capacity --api http://127.0.0.1:7700 --json
joule chat --key joule_… --prompt "hello"
joule chat --key joule_… --stream --prompt "hello"
```

See the man page for full options. Live capacity is `GET /v1/cluster/capacity`.

## Documents

| Document | Location |
|---|---|
| Design (cluster-v0) | [design/cluster-v0.md](design/cluster-v0.md) |
| Operator SOP (NASA PDF) | [sop-joule-ops.pdf](sop-joule-ops.pdf) — generated on release |
| Release memos | [releases/](releases/) |
| README | [../README.md](../README.md) |
| Man page | [../man/joule.1.md](../man/joule.1.md) |
| Changelog | [../CHANGELOG.md](../CHANGELOG.md) |
| Scene card | [../file_id.diz](../file_id.diz) |

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

See [CONTRIBUTING.md](../CONTRIBUTING.md) and [AGENTS.md](../AGENTS.md).

```text
cargo test --workspace
cargo run -p joule -- capacity --json
```

## License

[MIT](../LICENSE) © William Theesfeld
