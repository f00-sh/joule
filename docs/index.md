# joule

**Mesh supercomputer for open-weight AI.** Idle GPUs join a peer mesh. Access is earned in millijoules — compute in, tokens out. No cash buy-in for the public pool.

| | |
|---|---|
| Status | 0.0.0 research / wip |
| Repo | [github.com/f00-sh/joule](https://github.com/f00-sh/joule) |
| License | MIT |

## Why this project exists

Frontier models need more than one idle card. Central clouds re-centralize power. joule pools donated machines into a **mesh**: multi-node placement first, OpenAI-compatible access when the mesh is real. Donors keep control of their hardware. Freeloaders without contribution do not get keys that work.

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
./target/release/joule lab
```

## Usage

```text
joule version
joule lab --peers 3 --stages 2
joule credits --account alice
```

See the man page for full options.

## Documents

| Document | Location |
|---|---|
| Design (mesh-v0) | [design/mesh-v0.md](design/mesh-v0.md) |
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
║  ▄█▀  MESH SUPERCOMPUTER  ▀█▄                    ║
║████████████████████████████████████████████████  ║
║  v0.0.0  ·  MIT  ·  2026                         ║
║  idle GPUs → open-weight mesh · pay in compute   ║
║  github:f00-sh/joule  ·  joule.f00.sh             ║
╚══════════════════════════════════════════════════╝
```

## Development

See [CONTRIBUTING.md](../CONTRIBUTING.md) and [AGENTS.md](../AGENTS.md).

```text
cargo test --workspace
cargo run -p joule -- lab
```

## License

[MIT](../LICENSE) © William Theesfeld
