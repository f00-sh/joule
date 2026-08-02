# joule(1) — distributed compute cluster CLI

## NAME

joule — donate idle compute to a shared pool; earn millijoules; call open-weight AI

## SYNOPSIS

```text
joule version
joule control [--agent-listen ADDR] [--http-listen ADDR] [--data-dir PATH] [--ephemeral]
joule agent --account NAME [--control HOST:PORT] [--model TAG] [--mem-mib N] [--device gpu|metal|cpu]
joule capacity [--api URL] [--peers N] [--json]
joule chat --key KEY [--api URL] [--model TAG] --prompt TEXT [--stream]
joule whoami --key KEY [--api URL]
joule ready [--api URL] [--pool-vram-gib N] [--backends N]
joule load [--model TAG] [--quant ID] [--mem-mib N]
joule seed-blob --path FILE [--kind KIND] [--name NAME]
joule blobs [--json] [--api URL]
joule software status|apply [--dest PATH]
joule status --api URL [--key KEY] [--json] [--dash]
joule monitor --api URL [--key KEY] [--interval-secs N]
joule tray --api URL [--key KEY] [--interval-secs N]
joule service generate --platform linux|macos|windows --kind agent|tray …
joule service install-help --platform linux|macos|windows
joule broadcast keygen|sign|inject|plan-chunks …
joule lab [options]
joule credits [--account NAME]
```

## DESCRIPTION

**joule** runs a **distributed compute cluster**. Donors install an agent that
joins a control plane and contributes capacity. The control plane exposes live
pool capacity and an OpenAI-shaped chat API. Accounts **earn millijoules by
providing compute** (heartbeats, shards, challenges) under the **economy-v0**
fairness rules (√VRAM, tenure boost, leecher penalties — see
docs/design/economy-v0.md) and **spend millijoules** on API usage. The public
pool is free of cash charges. No contribution ⇒ no API.

How a node reaches the internet is irrelevant. This is not a mesh product.

Version **0.1.7** loads **lab-tiny / lab-mid / lab-large** tensors when seeded and
gates full Kimi on pool VRAM. Donors control contribution locally
(`joule donor pause|set-cap|set-schedule|set-sensors`). After an agent Welcome,
**`joule connect`** shows Base URL + full pool-issued `joule_…` API key + model
`kimi-open` for Cursor / other OpenAI clients. Weights and software are
**peer-seeded by sha256** (f00 is website only). Operator orders are
**ed25519-signed** and flooded by the swarm.

## COMMANDS

### joule gui (default)

Interactive graphical dashboard (tabs: **Overview**, **Graphs**, **Donor**, **Chat**).
Live multi-series plots (backends, agents, mesh, VRAM, balance, tokens, rates) with
zoom/pan/box-zoom and series toggles; one-click Start/Stop control and agent;
full local donor policy (pause, VRAM cap, schedule, thermal/battery); in-app chat.
Running `joule` with no subcommand launches the GUI.

### joule control

Run the control plane: agent TCP registry + HTTP API + live dashboard.

```text
--agent-listen ADDR   default 127.0.0.1:7701
--http-listen ADDR    default 127.0.0.1:7700
--data-dir PATH       persist keys/balances (default ~/.local/share/joule)
--ephemeral           no disk state
```

HTTP routes:

| Method | Path | Purpose |
|---|---|---|
| GET | `/` | Live HTML dashboard |
| GET | `/v1/cluster/capacity` | Live pool aggregate |
| GET | `/v1/cluster/nodes` | Donor node list |
| GET | `/v1/models` | Models offered by healthy donors |
| GET | `/v1/account` | Balance + donating flag (Bearer key) |
| GET | `/v1/public/snapshot` | Signed public pool snapshot (multi-source stats) |
| GET | `/v1/public/pubkey` | Pool ed25519 verifying key |
| GET | `/v1/public/ledger` | Sealed mJ chain (`from`, `limit`) — recompute balances |
| GET | `/v1/public/ledger/head` | Chain head hash + integrity flag |
| GET | `/v1/public/audit/{account}` | Account balance from chain + recent events |
| POST | `/v1/chat/completions` | OpenAI-shaped chat; `stream: true` for SSE |
| GET | `/v1/blobs` | Swarm content directory (who seeds which sha256) |
| GET | `/v1/broadcasts` | Recent operator-signed envelopes |
| POST | `/v1/broadcasts/inject` | Inject pre-signed operator order (flood + act) |
| GET | `/v1/notices` | Notice-kind broadcasts for UI |
| GET | `/v1/operator/status` | service_live, chunk plan, blob count |

### joule agent

Join the pool. Prints an API key on welcome and caches it on identity.
Heartbeats mint millijoules. Handles infer/challenge, **BlobProvide/BlobChunk**,
**FetchDigests**, and operator bus actions (model/software digests, notices).

### joule connect

Show Base URL (`…/v1`) + full pool-issued API key + model for external
OpenAI-compatible apps (Cursor, Continue, etc.). Writes `JOULE-CONNECT.txt`
next to the identity file. Flags: `--copy` (key), `--copy-url`, `--open`.
Tray: `joule tray --connect` / `--copy-api-key`. Key is issued by the pool on
Welcome — not invented client-side. `chat` / `whoami` use the cached key when
`--key` is omitted.

### joule donor

Local contribution policy (product law 5 — remote cannot raise caps):

```text
joule donor status
joule donor pause | resume
joule donor set-cap 4096
joule donor set-schedule 09:00-17:00
joule donor set-sensors --max-temp-c 85 --min-battery-pct 20
```

Agent flags: `--pause`, `--mem-cap-mib`, `--schedule`, `--max-temp-c`,
`--min-battery-pct`, `--policy PATH`.

### joule seed-blob / software

Hash a local file into `blobs/sha256/` for the swarm. After a signed
`software_update`, agents stage the matching digest; `joule software apply`
installs the staged binary (hash-checked).

### joule status / monitor / tray

Cross-platform **client status** (Linux, macOS, Windows CLI): connection,
account/API, millijoule balance, tokens used (prompt+completion), pool
backends/VRAM, service_live. `--dash` prints a compact monitor; `monitor` and
`tray` refresh live. Shared core: `joule-client::ClientStatus`.

### joule service

Generate **OS auto-start** units without root (dry-run to stdout/`--out`):

- Linux: systemd user unit (`systemctl --user enable --now …`)
- macOS: LaunchAgents plist (`launchctl load …`)
- Windows: Task Scheduler XML (`schtasks /Create /XML …`)

Prefer user-session services so tray/monitor can share the GPU session.

### joule broadcast

Operator tools: keygen, sign body JSON, inject into control, demo chunk plan.
Stock builds verify the embedded official protocol key (master OpenPGP
`tj@f00.sh`). See docs/design/master-key-trust-v0.md.

### joule capacity

With `--api`, fetch live capacity from control. Without `--api`, print a
synthetic offline demo.

### joule chat / whoami

Client helpers for the HTTP API. Chat requires a key whose account is
**currently donating** (healthy agent online). Pass `--stream` for SSE chunks.
If `--key` is empty, uses the Welcome-cached key from identity (`joule connect`
to display it).

### joule lab / credits

Offline demos (no network).

## EXIT STATUS

| Code | Meaning |
|---|---|
| 0 | Success |
| non-zero | Failure |

## ENVIRONMENT

| Variable | Purpose |
|---|---|
| `RUST_LOG` | Tracing filter |
| `JOULE_OPERATOR_PUBKEY` | Lab only: override verify key (needs `JOULE_ALLOW_UNOFFICIAL_OPERATOR=1`) |
| `JOULE_ALLOW_UNOFFICIAL_OPERATOR` | `1` to allow non-embedded operator keys (lab/forks) |
| `JOULE_SKIP_OFFICIAL_KEY_FETCH` | `1` skip HTTPS audit of joule.f00.sh key mirror |
| `JOULE_ALLOW_EXTERNAL_FETCH` | `1` to allow third-party weight URL hints (never f00 origin) |
| `JOULE_BLOBS_DIR` | Content-addressed blob store root |
| `JOULE_WEIGHTS_DIR` | Weight cache root |
| `JOULE_SOFTWARE_DIR` | Software stage root |
| `JOULE_PUBLIC_URL` | Public HTTPS base of this control; enables signed **announce** to the open directory (no f00 token) |
| `JOULE_ANNOUNCE_URL` | Override announce endpoint (default `https://joule.f00.sh/api/announce`) |
| `JOULE_EDGE_TOKEN` | Optional bearer to push snapshots into edge KV (`/api/ingest`) |
| `JOULE_EDGE_URL` | Override ingest URL (default `https://joule.f00.sh/api/ingest`) |
| `JOULE_EDGE_DISABLE` | Set to `1` to skip privileged edge ingest |
| `JOULE_POOL_ID` | Pool identity id embedded in signed snapshots |

## SEE ALSO

- [README.md](../README.md)
- [docs/design/cluster-v0.md](../docs/design/cluster-v0.md)
