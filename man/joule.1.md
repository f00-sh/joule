# joule(1) — distributed compute cluster CLI

## NAME

joule — donate idle compute to a shared pool; earn millijoules; call open-weight AI

## SYNOPSIS

```text
joule version
joule control [--agent-listen ADDR] [--http-listen ADDR]
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
providing compute** (heartbeats and completed work) and **spend millijoules** on
API usage. No contribution ⇒ no API.

How a node reaches the internet is irrelevant. This is not a mesh product.

Version **0.0.0** uses a stub inference engine; real open weights (Kimi-class)
land in a later milestone. Pool membership, capacity, and contribute-to-consume
are implemented.

## COMMANDS

### joule control

Run the control plane: agent TCP registry + HTTP API.

```text
--agent-listen ADDR   default 127.0.0.1:7701
--http-listen ADDR    default 127.0.0.1:7700
```

HTTP routes:

| Method | Path | Purpose |
|---|---|---|
| GET | `/v1/cluster/capacity` | Live pool aggregate (dashboard) |
| GET | `/v1/models` | Models offered by healthy donors |
| GET | `/v1/account` | Balance + donating flag (Bearer key) |
| POST | `/v1/chat/completions` | OpenAI-shaped chat (Bearer key) |

### joule agent

Join the pool. Prints an API key on welcome. Heartbeats mint millijoules.
Assigned jobs run on the local stub engine and mint further credits.

### joule capacity

With `--api`, fetch live capacity from control. Without `--api`, print a
synthetic offline demo.

### joule chat / whoami

Client helpers for the HTTP API. Chat requires a key whose account is
**currently donating** (healthy agent online).

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

## SEE ALSO

- [README.md](../README.md)
- [docs/design/cluster-v0.md](../docs/design/cluster-v0.md)
