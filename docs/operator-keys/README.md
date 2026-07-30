# Operator keys (broadcast authority)

The **private** key never goes in git.

Clients verify operator messages with a **public** ed25519 key:

- Env: `JOULE_OPERATOR_PUBKEY=<64 hex chars>`
- File: `~/.config/f00/joule/operator.pub` or `docs/operator-keys/operator.ed25519.pub`

Optional: also publish an OpenPGP public key on the website for human `gpg --verify` of release notes. Agents use ed25519 for speed (pure Rust).

See [broadcast-v0](../design/broadcast-v0.md).
