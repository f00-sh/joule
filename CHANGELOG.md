# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.7] — 2026-08-02

Interactive GUI graphs and donor/chat tabs; probe-test race fix.

### Added
- **GUI Overview / Graphs / Donor / Chat tabs** — multi-series live plots (backends, agents, mesh, dht, VRAM, balance, tokens, tok/s), zoom/pan/box-zoom, series toggles, sparklines, token bar chart
- In-app **cluster chat** and full **donor policy** editor (pause, VRAM cap, local schedule, thermal/battery)
- Poll interval slider, start/stop control+agent, force poll, clear history, reset plot views

### Fixed
- Serialize `gpu_probe` env override tests so parallel `cargo test` stays green

## [0.1.6] — 2026-08-02

GUI-first shell + skeptic fixes (local TZ, real notary keys, clear agent connect UX).

### Added
- **`joule` / `joule gui`** — interactive egui dashboard: live graphs (backends, VRAM, balance, tokens), Start control / Start agent, donor pause/cap, sensors, open browser dashboard
- Agent connect error explains control is down and points to GUI / `joule control`

### Fixed
- Local schedule TZ uses OS `tm_gmtoff` (not UTC default)
- Notary checkpoints use per-node OS-random keys (not forgeable lab keys)
- `tray --donor-set-cap 0` clears cap

## [0.1.5] — 2026-08-02

Close high-value backlog: notary quorum hot path, attestation tiers on nodes API, donor local-TZ + tray, service_live flip.

### Added
- **Notary multi-sig checkpoints** — production `seal_and_checkpoint` requires cryptographic quorum; `GET /v1/public/ledger/head` exposes last signed checkpoint attestations
- **attestation_tier** on `GET /v1/cluster/nodes` (claim_only / challenge_partial / challenge_full)
- **Donor schedule is local timezone** (`allows_donate_with_offset`); tray `--donor-status|pause|resume|set-cap` + live policy strip
- **service_live auto-flip** when mesh loaded + pool gates; K3 pipeline rejects f00 weight URLs

### Changed
- Workspace **0.1.5**; prior 0.1.4 still published

## [0.1.4] — 2026-08-02

Live multi-agent pool path, lab-large quant, donor local controls, notary/attestation trust slice.

### Added

- **lab-mid quant** (tip since 0.1.3 line) — multi-file, multi-tensor ~360 KiB past lab-tiny; `prepare_and_install` agent path
- **lab-large quant** — multi-MiB multi-layer fixture (~3.1 MiB, ≥5 tensors) past lab-mid; MANIFEST digests + `scripts/seed-lab-large.sh`
- **Live multi-agent pool smoke** — `scripts/live-pool-smoke.sh` + e2e `multi_agent_capacity_under_churn` (N≥2 join, churn)
- **Donor local policy (law 5)** — `joule donor status|pause|resume|set-cap|set-schedule|set-sensors`; agent `--pause` / `--mem-cap-mib` / schedule / thermal / battery; remote cannot raise local caps
- **Notary checkpoint signatures** — `joule_ledger::notary` sign/verify/quorum (fail-closed on bad sigs)
- **Attestation tiers** — `joule_cluster::attestation_tier` claim_only / challenge_partial / challenge_full
- **Dual-pin fail-closed** — website protocol mismatch never overrides embed without lab flag

### Changed

- **pick_quant** prefers largest loadable fixture by size; peer-only K3 skipped when fixtures exist (8192→lab-large, 1024→lab-mid, 256→lab-tiny)
- Workspace package version **0.1.4**; `file_id.diz` aligned

## [0.1.3] — 2026-07-31

Connect path for external OpenAI-compatible apps + ship hygiene after v0.1.2.

### Added

- **`joule connect`** — idiot-proof CONNECT card: Base URL (`…/v1`), full pool-issued `joule_…` API key, and model (`kimi-open`) for Cursor / Continue / any OpenAI client
- **JOULE-CONNECT.txt** next to identity (0600) with paste-ready fields; `--copy` / `--copy-url` / `--open`
- **Tray connect surface**: `tray --connect`, `tray --copy-api-key`
- **Welcome key cache**: agent caches pool API key on identity; `chat` / `whoami` / `status` resolve key without manual `--key` once joined
- E2E/unit: Welcome `joule_` key auth + wrong/missing key fail-closed; connect note + remember/load round-trip

### Fixed

- **cargo fmt** on tray help line (CI red after connect-card)
- Headless clipboard skip + install trap; clipboard tools timeout without session
- Live f00 package docs; soft tray copy-code when clipboard unavailable
- Homebrew template test matches live tap (version may lag Cargo until formula pin)

### Changed

- Workspace **Cargo package version 0.1.3** aligned with this SemVer cut (reduces prior tag/binary lag)
- `file_id.diz` → v0.1.3

## [0.1.0] — 2026-07-31

First public multi-platform release under **f00-sh/joule**.

### Added

- **Signed joule identity** + recovery file + tray product surface (`--copy-code` / `--enter-code` / `--open-recovery` / `--onboard`)
- **Startup GPU probe** clamps advertised claim; mint/placement remain verified-only
- **Product bootstrap** multi-region example + merge/list_urls helpers
- **Multi-platform release CI**, `install.sh` / `install.ps1`, download page, packaging under f00-sh
- **Signed joule identity**: recovery UUID → ed25519 key → `j1` account fingerprint; Hello must be signed so the pool accepts only key holders; multi-device via same code ([docs/design/identity-v0.md](docs/design/identity-v0.md))
- **Multi-platform release CI**: `.github/workflows/release.yml` builds linux/darwin/windows artifacts on `v*` tags (and manual dispatch)
- **Dummy-easy install**: `scripts/install.sh` (Unix) + `scripts/install.ps1` (Windows) from GitHub Releases
- **Download page**: [docs/download.html](docs/download.html) OS autodetect + all-platform links ([joule.f00.sh/download.html](https://joule.f00.sh/download.html))
- **Packaging**: AUR + Homebrew under f00-sh (live digests from Releases)

### Changed

- **Structural capacity invariant**: single verified-only API for anything affecting mJ or placement — `placement_mem_mib` (0 if unverified) vs `economic_mem_mib` (mint floor only); stream slots / plan_sharded_pool / mesh_plan_donors / rank exclude claim-only peers
- **Capacity challenges (1:1 memory-hard + peak)**: `work_bytes(credit) = credit × 1 MiB`; `verified = max(verified, proven)` so serial N×C cannot mint N×C farm; stub / undersized work fail
- **challenge_loop**: capacity oracle on `spawn_blocking` outside control write lock
- **Mesh verified-only (control)**: `mesh_plan_donors` + model_update use cluster verified; exclude unverified
- **Peer gossip anti-farm**: LocalMesh equal-unit placement; agents never self-attest verified on PeerAlive; `MeshDonor::from_untrusted_presence`
- **E2E mesh geometry**: tests assert `mesh_plan_donors()` (cluster verified), not PeerAlive claim

### Added (pre-0.1.0 stack)

- **Fair economy + anti-gaming**: churn mint penalty; sealed **donate-to-pool** with equitable redistribute (`POST /v1/account/donate`); ledger kinds `donate_pool` / `donate_receive`; relational pay-table tests
- **GPU claim integrity**: `capacity_matrix` + farm-claim tests; progressive challenge credit / fail halve; expired challenges → fail
- **Decentral discovery Phase A/B**: agent peer listen + `PeerAlive` gossip; `BlobLocate.multiaddrs`; direct peer BlobWant/BlobChunk (`peer_net`); `GET /v1/mesh/peers`; status `mesh_peers` — [docs/design/decentral-discovery-v0.md](docs/design/decentral-discovery-v0.md)
- **Decentral Phase C**: `joule-dht` (peer/blob keys, XOR distance, bootstrap.json); control DHT mirror; agent **LocalMesh** + P2P PeerAlive/BlobsHave on peer port; bootstrap dial announce; FetchDigests uses local DHT before control; `GET /v1/dht/keys`, `/v1/dht/get/{*key}`, `/v1/bootstrap`; example [docs/examples/bootstrap.json](docs/examples/bootstrap.json)
- **Decentral Phase D start**: `RequestInfer` / `PlanAccept`; PeerAlive `mem_mib`/`throughput_class`; `plan_from_mesh_donors`; agent mesh PlanOffer; `GET /v1/mesh/plan`
- **Decentral Phase D chat path**: `dispatch_mesh_infer` (RequestInfer→PlanOffer→PlanAccept→Infer) when mesh donors advertise mem; control stream fallback; chat returns `joule_coordination`; e2e `mesh_request_infer_chat_multi_donor`
- **Multi-hop DHT**: k-buckets + iterative find/store (`joule-dht` routing); multi-node chain put/get tests
- **Peer-only chat bus**: `joule-mesh` PeerBus (no control relay); coordinator election + re-plan on death
- **Phase E**: `joule-net` quic:// multiaddr + QUIC session, NAT/public advertise; erasure encode/reconstruct + durable placement
- **Product scale**: `kimi-k3-shards` multi-hundred-GB class MANIFEST pins; `k3_pipeline` APIs; public multiaddr announce on agent
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
