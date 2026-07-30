# Operator keys (broadcast authority)

The **private** key never goes in git.

## Protocol key (ed25519)

Clients verify operator messages with a **public** ed25519 key:

| Pin location | Notes |
|---|---|
| Env `JOULE_OPERATOR_PUBKEY=<64 hex>` | Preferred in production |
| `~/.config/f00/joule/operator.pub` | f00 core path |
| `docs/operator-keys/operator.ed25519.pub` | Dev tree convenience (public only) |

Generate:

```text
joule broadcast keygen --secret operator.ed25519.sec --public operator.ed25519.pub
```

Copy the public file here as `operator.ed25519.pub` when ready to pin the community key.

## OpenPGP (optional, humans)

Publish `operator.asc` (public) for `gpg --verify` of release notes and of the ed25519 public key file. Agents use ed25519 for speed (pure Rust).

## Ceremony

Full steps: [operator-ceremony-v0](../design/operator-ceremony-v0.md) · design [broadcast-v0](../design/broadcast-v0.md).
