# joule

## Product

| Fact | Value |
|------|--------|
| Class | **Product** (CLI + mesh protocol) |
| Org | f00-sh |
| Domain | joule.f00.sh |
| License | MIT |
| Language | **Rust** (workspace; strict purity for first-party code) |
| Status | 0.0.0 research / wip |

**One-liner:** Decentralized mesh supercomputer — pool idle GPUs into open-weight AI inference (Kimi-class).

## Product laws

1. Compute is the only currency (public pool).
2. No active contribution ⇒ no API access.
3. Credits = verified useful work in **millijoules**, not idle theater.
4. Donor retains control (caps, pause, schedule, thermal/battery).
5. Open weights, open agent, open protocol.
6. **Mesh-first:** multi-node placement is a primary research track, not a later bolt-on.

## Language / purity

- Declared language: **Rust**. Style: `~/.grok/references/coding-standards/rust.md`.
- Protocol, mesh, ledger, CLI: pure Rust + crates.io Rust deps only.
- Inference backends implement `joule_runtime::Engine`. Prefer pure-Rust engines.
- **Any FFI / non-Rust runtime** (e.g. llama.cpp) requires an ADR under `docs/adr/` and an explicit note here before merge.

## Workspace

```text
crates/joule           CLI binary
crates/joule-proto     wire types / messages
crates/joule-mesh      membership + placement
crates/joule-runtime   Engine trait + StubEngine
crates/joule-ledger    millijoule accounting
```

Design SoT: [docs/design/mesh-v0.md](docs/design/mesh-v0.md)

## Commands

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace --release
cargo run -p joule -- lab
cargo run -p joule -- version
```

## Install channels

- Curl from GitHub Releases: required (`scripts/install.sh`; installs man pages).
- Package managers: **none yet** (do not document fake AUR/brew packages).

## Releases

- Every SemVer release: CHANGELOG.md, root `file_id.diz`, README + docs/ + man sync, attach `file_id.diz` to GitHub Release.
- Docs pack (SOP PDF + release memo) on ship via house release workflow.

## Notes

- Default model tags use `kimi-open-*` until an exact Kimi revision is pinned in `models/MANIFEST`.
- Browser is dashboard only; compute is native `joule agent`.
