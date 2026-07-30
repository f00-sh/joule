# joule(1) — distributed compute cluster CLI

## NAME

joule — donate idle compute to a shared pool; earn millijoules; call open-weight AI

## SYNOPSIS

```text
joule version
joule control [--agent-listen ADDR] [--http-listen ADDR] [--data-dir PATH] [--ephemeral]
joule agent --account NAME [--control HOST:PORT] [--model TAG] [--mem-mib N] [--device gpu|metal|cpu]
joule capacity [--api URL] [--peers N] [--json]
joule chat --key KEY [--api URL] [--model TAG] --prompt TEXT
joule whoami --key KEY [--api URL]
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

Version **0.0.0** uses a stub inference engine; real open weights (Kimi-class)
land in a later milestone. Pool membership, capacity, and contribute-to-consume
are implemented.

## COMMANDS

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
| POST | `/v1/chat/completions` | OpenAI-shaped chat; `stream: true` for SSE |

### joule agent

Join the pool. Prints an API key on welcome. Heartbeats mint millijoules.
Assigned jobs run on the local stub engine and mint further credits.

### joule capacity

With `--api`, fetch live capacity from control. Without `--api`, print a
synthetic offline demo.

### joule chat / whoami

Client helpers for the HTTP API. Chat requires a key whose account is
**currently donating** (healthy agent online). Pass `--stream` for SSE chunks.

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
| `JOULE_EDGE_TOKEN` | Bearer token to publish live pool snapshots to `joule.f00.sh/api/ingest` |
| `JOULE_EDGE_URL` | Override ingest URL (default `https://joule.f00.sh/api/ingest`) |
| `JOULE_EDGE_DISABLE` | Set to `1` to skip edge publish |

## SEE ALSO

- [README.md](../README.md)
- [docs/design/cluster-v0.md](../docs/design/cluster-v0.md)
