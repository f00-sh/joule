# Operator bus example bodies

JSON bodies for `joule broadcast sign --kind … --body …`.

| File | Kind | Purpose |
|---|---|---|
| [notice.json](notice.json) | `notice` | Dashboard / CLI message |
| [policy_pause.json](policy_pause.json) | `policy` | Pause chat (`operator_paused`) |
| [software_update.json](software_update.json) | `software_update` | Peer-seed binary digests (fill real sha256) |
| [model_update_lab_tiny.json](model_update_lab_tiny.json) | `model_update` | Assign lab-tiny digest with replica plan |
| [revoke.json](revoke.json) | `revoke` | Blacklist envelope UUID(s) |

Bootstrap mesh discovery (Phase C): [bootstrap.json](bootstrap.json) — copy to `~/.local/share/joule/bootstrap.json` or set `JOULE_BOOTSTRAP`. Lists are **replaceable**; f00 is never the only root.

Trust model: [../design/master-key-trust-v0.md](../design/master-key-trust-v0.md) · ceremony: [../design/operator-ceremony-v0.md](../design/operator-ceremony-v0.md) · decentral: [../design/decentral-discovery-v0.md](../design/decentral-discovery-v0.md)

Stock clients verify the **embedded** protocol key. Sign with the official secret (not in git):

```text
joule broadcast sign --kind notice --body docs/examples/notice.json \
  --secret ~/.config/f00/joule/protocol.ed25519.sec --out /tmp/n.env.json
joule broadcast inject --envelope /tmp/n.env.json
```

**Lab only** (forks / ephemeral keys): `JOULE_ALLOW_UNOFFICIAL_OPERATOR=1` plus `JOULE_OPERATOR_PUBKEY=…`.

Or: `scripts/demo-operator-bus.sh` (control must be running; prefers official secret).
