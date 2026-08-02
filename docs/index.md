# joule

**Open supercomputer — free, paid in compute.** One logical GPU (sum of donor VRAM). One model (`kimi-open`). Contribute → use the AI. No cash on the public pool.

**Live site:** [joule.f00.sh](https://joule.f00.sh/) ([`index.html`](index.html) teaser + milestones). Point at control with `?api=http://127.0.0.1:7700`.

**Economy:** [design/economy-v0.md](design/economy-v0.md) — millijoules, √VRAM fairness, tenure boost, leecher penalties (`eco=v0`).

| | |
|---|---|
| Status | 0.1.5 released (lab-tiny + lab-mid + lab-large; donor controls; Kimi gated) |
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
# after Welcome: Base URL + full pool-issued joule_… key + model kimi-open
joule connect
joule capacity --api http://127.0.0.1:7700 --json
joule whoami
joule chat --prompt "hello"
joule chat --stream --prompt "hello"
```

`joule connect` is the external-app path (Cursor / Continue / any OpenAI client): Base URL `…/v1`, API key `joule_…`, model `kimi-open`. Chat/whoami use the Welcome-cached key when `--key` is omitted.

See the man page for full options. Live capacity is `GET /v1/cluster/capacity`.

## Documents

| Document | Location |
|---|---|
| Design (cluster-v0) | [design/cluster-v0.md](design/cluster-v0.md) |
| Economy (millijoules v0) | [design/economy-v0.md](design/economy-v0.md) |
| Distribution (website only · peer seed) | [design/distribution-v0.md](design/distribution-v0.md) |
| Broadcast bus | [design/broadcast-v0.md](design/broadcast-v0.md) |
| Operator key ceremony | [design/operator-ceremony-v0.md](design/operator-ceremony-v0.md) |
| **Master key trust (GPG + embed)** | [design/master-key-trust-v0.md](design/master-key-trust-v0.md) · [operator-keys/](operator-keys/) |
| **Decentralized discovery (mesh target)** | [design/decentral-discovery-v0.md](design/decentral-discovery-v0.md) |
| Client status / tray / service install | [design/client-v0.md](design/client-v0.md) |
| Self-govern ledger | [design/self-govern-v0.md](design/self-govern-v0.md) |
| Operator SOP (NASA PDF) | [sop-joule-ops.pdf](sop-joule-ops.pdf) |
| This release memo (PDF) | [releases/v0.1.7-memo.pdf](releases/v0.1.7-memo.pdf) |
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
║  v0.1.7  ·  MIT  ·  2026                         ║
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
