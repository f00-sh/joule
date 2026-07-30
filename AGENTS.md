# joule

## Product

| Fact | Value |
|------|--------|
| Class | **Product** (CLI + distributed cluster protocol) |
| Org | f00-sh |
| Domain | joule.f00.sh |
| License | MIT |
| Language | **Rust** (workspace; strict purity for first-party code) |
| Status | 0.0.0 research / wip |

**One-liner:** Distributed internet-wide supercomputer cluster — pool idle GPUs into open-weight AI inference (Kimi-class).

## Product laws

1. Compute is the only currency (public pool). **No money** accepted or required.
2. No active contribution ⇒ no API access.
3. Credits = sealed hash-chain millijoules only (`docs/design/self-govern-v0.md`). Fairness: √VRAM + tenure + leecher (`eco=v0`). VRAM claims untrusted until challenges verify.
4. Balances cannot be set by users/operators outside the protocol chain.
5. Donor retains control (caps, pause, schedule, thermal/battery).
6. Open weights, open agent, open protocol.
7. **Distributed cluster across the internet** — connectivity medium is irrelevant.
8. **Dashboard always shows live cluster capacity** (nodes, healthy VRAM, throughput aggregate).
9. **Single model:** only `kimi-open` (`CLUSTER_MODEL`).
10. **One logical device:** N physical donors = **one** virtual GPU whose VRAM is the **sum** of healthy donors.
11. **Website only on f00:** `joule.f00.sh` is not a CDN. Weights/software are **content-addressed and peer-seeded** (`docs/design/distribution-v0.md`). No f00 payload hosting.
12. **Operator broadcast:** signed allow-listed orders (update/model/notice/…) verified with public operator key and flooded by the swarm (`docs/design/broadcast-v0.md`).

## Language / purity

- Declared language: **Rust**. Style: `~/.grok/references/coding-standards/rust.md`.
- Protocol, cluster, ledger, CLI: pure Rust + crates.io Rust deps only.
- Inference backends implement `joule_runtime::Engine`. Prefer pure-Rust engines.
- **Any FFI / non-Rust runtime** requires an ADR under `docs/adr/` and an explicit note here before merge.

## Workspace

```text
crates/joule             CLI binary
crates/joule-proto       wire types / ClusterCapacity / plans
crates/joule-cluster     membership + capacity + placement
crates/joule-runtime     Engine trait + StubEngine
crates/joule-ledger      millijoule accounting
crates/joule-control     control plane (agents + HTTP API)
```

Design SoT: [docs/design/cluster-v0.md](docs/design/cluster-v0.md)

## Commands

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace --release
cargo run -p joule --release -- control
cargo run -p joule --release -- agent --account alice
cargo run -p joule --release -- capacity --api http://127.0.0.1:7700 --json
cargo run -p joule --release -- chat --key joule_… --prompt "hi"
cargo run -p joule -- lab
```

## Install channels

- Curl from GitHub Releases: required (`scripts/install.sh`; installs man pages).
- Package managers: **none yet** (do not document fake AUR/brew packages).

## Releases

- Every SemVer release: CHANGELOG.md, root `file_id.diz`, README + docs/ + man sync, attach `file_id.diz` to GitHub Release.
- Docs pack (SOP PDF + release memo) on ship via house release workflow.

## Notes

- Single API model: `kimi-open` (`CLUSTER_MODEL`). Pin exact weights later in `models/MANIFEST`.
- Every donor serves that model; schedulers use the full healthy pool.
- Browser is dashboard only; compute is native `joule agent`.

## f00 membership

- Org: `f00-sh`
- Catalog SSOT: https://f00.sh/catalog.json (`f00` repo `site/catalog.json`)
- Theme: https://f00.sh/theme/f00-theme.css (Heartbox palette — do not redefine brand colors/fonts)
- Card on hub only when catalog `status=released` after a real release
