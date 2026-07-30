# Operator bus example bodies

JSON bodies for `joule broadcast sign --kind … --body …`.

| File | Kind | Purpose |
|---|---|---|
| [notice.json](notice.json) | `notice` | Dashboard / CLI message |
| [policy_pause.json](policy_pause.json) | `policy` | Pause chat (`operator_paused`) |
| [software_update.json](software_update.json) | `software_update` | Peer-seed binary digests (fill real sha256) |
| [model_update_lab_tiny.json](model_update_lab_tiny.json) | `model_update` | Assign lab-tiny digest with replica plan |
| [revoke.json](revoke.json) | `revoke` | Blacklist envelope UUID(s) |

Ceremony: [../design/operator-ceremony-v0.md](../design/operator-ceremony-v0.md)

```text
joule broadcast keygen --secret op.sec --public op.pub
export JOULE_OPERATOR_PUBKEY=$(grep -v '^#' op.pub | head -1)
joule broadcast sign --kind notice --body docs/examples/notice.json --secret op.sec --out /tmp/n.env.json
joule broadcast inject --envelope /tmp/n.env.json
```

Or: `scripts/demo-operator-bus.sh` (control must be running).
