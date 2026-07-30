# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **Live dashboard** at `GET /` (capacity + donor table, 3s refresh)
- `GET /v1/cluster/nodes` for dashboard node list
- **SSE streaming** chat (`stream: true` / `joule chat --stream`)
- **Persistence**: accounts, API keys, balances under `--data-dir` / `~/.local/share/joule`
- Integration tests: control + agent + capacity + chat + stream + persist
- **Control plane** (`joule control`): agent TCP + HTTP API
- **Donor agent** (`joule agent`): join, heartbeat, run assigned work, earn millijoules
- Live capacity API: `GET /v1/cluster/capacity`
- OpenAI-shaped chat: `POST /v1/chat/completions` (contribute-to-consume)
- Account view: `GET /v1/account`
- CLI: `chat`, `whoami`, live `capacity --api`
- Crate `joule-control`
- Heartbeat mint + infer mint; burn on API usage

### Changed

- Product language: **distributed compute cluster** (not mesh)
- Crate rename: `joule-mesh` → `joule-cluster`
- Design SoT: `docs/design/cluster-v0.md`

### Prior scaffold

- Workspace crates, lab CLI, stub runtime, ledger, CI

## [0.0.0] - 2026-07-30

### Added

- Initial repository scaffold (house kit)
