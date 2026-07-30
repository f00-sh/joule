# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- **Cross-platform client status**: `joule status` / `monitor` / `tray` (Linux, macOS, Windows CLI) — connection, API/account, millijoule balance, tokens used, pool dash (`joule-client` shared snapshot)
- **Service install helpers**: `joule service generate|install-help` for systemd (Linux), launchd (macOS), Task Scheduler XML (Windows); user-session preferred for tray+GPU
- Account API fields `prompt_tokens_used` / `completion_tokens_used` (lifetime chat usage)
- **Master key trust v0**: OpenPGP master `tj@f00.sh` + embedded protocol ed25519; website HTTPS must match embed; env override only with `JOULE_ALLOW_UNOFFICIAL_OPERATOR=1` — see [docs/design/master-key-trust-v0.md](docs/design/master-key-trust-v0.md)
- Public keys: `docs/operator-keys/master.asc`, `protocol.ed25519.pub` (+ GPG detach-sig); `GET /v1/operator/pins` and `/v1/operator/audit`
- **Peer blob transfer**: `BlobWant` → `BlobProvide` → `BlobChunk` (base64); agents verify sha256 into content-addressed store; `FetchDigests` for assigned model/software digests only
- **Redundant model chunks**: `plan_redundant_chunks` + rebalance when seeders drop; model_update never force-downloads full model
- **Software update path**: peer-seed binary digests, stage under `~/.local/share/joule/software/stage`, `joule software status|apply`, `joule seed-blob`
- **Operator bus actions**: pause/resume/policy allow-list; `GET /v1/notices`, `/v1/operator/status`; dashboard notices + blob strip
- **Operator ceremony** doc: `docs/design/operator-ceremony-v0.md`
- **Persist v4/v5**: operator_paused / service_live / mint knobs + recent operator broadcasts survive restart
- **CLI** `joule blobs` (local or `--api` swarm catalog); `scripts/demo-operator-bus.sh`
- Transfer caps: 256 MiB control-relayed blob max; 64 concurrent xfers; 120s xfer TTL
- **Agents use ClusterEngine**: install lab-tiny tensors into infer/challenge path when peer-seeded/loaded; challenges accept tensor completions
- Operator **revoke** envelope id blacklist; agent broadcast journal (ndjson)
- **dual_verify** wired: every Nth chat runs a free second pool pass and logs mismatch
- Joining agents receive recent operator envelopes + rebalance FetchDigests; agents verify body hash + operator sig locally when key pinned
- Late joiners re-plan model digests (included in redundant placement); e2e coverage
- `scripts/install.sh` installs from local cargo build (or GitHub release when assets exist)
- **Weight download/verify**: HTTP(S) + `repo://` with sha256; `weights.published=true`
- **lab-tiny** fixture + **kimi-k3-meta** (Moonshot HF configs); tensor-backed generate (`decode.rs`)
- **Fair millijoule economy v0** (`docs/design/economy-v0.md`, `joule-ledger::economy`): √VRAM mint, tenure boost, leecher penalties; auditable `eco=v0` ledger reasons
- **Live public pool on joule.f00.sh**: Pages Functions `/api/pool` + SSE `/api/pool/stream` + KV snapshot; control edge publish (`JOULE_EDGE_TOKEN` → `/api/ingest`)
- **Self-govern v0**: sealed hash-chained millijoule ledger; balances only via chain replay; public audit APIs
- **Verified VRAM**: claims untrusted until challenges; mint/placement/readiness use verified mem
- **Decentralized stats v0**: signed `GET /v1/public/snapshot` (ed25519); multi-source site + open directory
- **Auto-announce**: `POST /api/announce` (pool-key signature, no f00 token); `GET /api/sources`; `JOULE_PUBLIC_URL` makes control self-list
- `/api/pool` aggregates announced sources (not just privileged ingest)
- Edge token auto-load from f00 core path `~/.config/f00/joule/edge.token`
- **Teaser site** (f00 theme shell): free/compute pitch, economy explain, real-time cluster stats + donors
- **CI:** GitHub Actions Pages deploy (`pages.yml`) + economy/site sanity in `ci.yml`
- Account fairness windows persisted (snapshot v2); `/v1/account` exposes leecher + tenure fields

### Changed

- **Logical device view:** pool = one virtual GPU (`logical_device.vram_mib` = sum of donors)
- **VRAM-sharded single model** under that device; requests share stream slots
- `/v1/models` is one model; foreign ids rejected
- **Kimi gated:** pool ≥64 GiB + ≥3 backends for model_ready; weights published (lab + HF meta)

### Added

- `models/MANIFEST.json` + readiness API `GET /v1/models/readiness`
- Agent weight **arm** path (`~/.local/share/joule/weights`) when pool ready
- `PoolStatus` / `PrepareOk` protocol; `joule ready` CLI
- `LogicalDevice` readiness fields on capacity/dashboard
- **Anti-cheat challenges**: spot challenges every ~12s + dual-verify every 3rd chat
- **Reputation**: pass/fail scores; ban unhealthy cheaters from scheduling
- **Localhost control hardened**: shared agent routes (fixed dispatch), bind errors, healthz shows agents_connected
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
