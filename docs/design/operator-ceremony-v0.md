# joule — operator key ceremony v0

**Status:** active  
**Companions:** [**master-key-trust-v0**](master-key-trust-v0.md) (read first) · [broadcast-v0](broadcast-v0.md) · [distribution-v0](distribution-v0.md)

---

## 1. Goal

Every official order (model update, software update, pause, notice, policy) must be:

1. **Signed** offline by a key only the operator holds  
2. **Verifiable** by every agent and control with a **public** key  
3. **Flooded by the swarm** — f00 is not a push server  

This document is the human ceremony. Agents verify **ed25519** only (pure Rust). Humans may also use **OpenPGP** for release notes.

---

## 2. Generate the protocol key (ed25519)

```text
joule broadcast keygen \
  --secret operator.ed25519.sec \
  --public operator.ed25519.pub
```

- **Secret** (`operator.ed25519.sec`): 32-byte hex. Mode `0600`. **Never commit. Never put on f00.** Prefer offline / air-gapped storage.  
- **Public** (`operator.ed25519.pub`): safe to commit under `docs/operator-keys/` and publish on the website.

Pin on every control/agent host:

```text
export JOULE_OPERATOR_PUBKEY=<64 hex chars>
# or drop public hex at:
#   ~/.config/f00/joule/operator.pub
#   docs/operator-keys/operator.ed25519.pub  (dev tree)
```

If **no** pubkey is configured, controls accept any envelope (lab only). Production **must** pin.

---

## 3. Optional OpenPGP (humans)

1. Generate or reuse a long-term OpenPGP key for “joule operator” / f00.  
2. Publish the **public** key on [joule.f00.sh](https://joule.f00.sh/) and in git (`docs/operator-keys/operator.asc`).  
3. Sign human-readable release notes and the **ed25519 public key** once:

```text
gpg --armor --detach-sign docs/operator-keys/operator.ed25519.pub
```

4. Agents do **not** require GPG in the hot path. Optional `openpgp_sig` field on `SignedEnvelope` is for humans/tools.

---

## 4. Sign and inject an order

Body JSON files are kind-specific (see broadcast-v0). Example notice:

```text
echo '{"title":"hello mesh","body":"peer seed only"}' > /tmp/notice.json

joule broadcast sign --kind notice --body /tmp/notice.json \
  --secret operator.ed25519.sec --out /tmp/notice.env.json

joule broadcast inject --api http://127.0.0.1:7700 --envelope /tmp/notice.env.json
```

Model update (digests only — not full model force-download):

```text
# body lists chunk sha256 list; control plans redundant placement
joule broadcast sign --kind model_update --body model-body.json \
  --secret operator.ed25519.sec --out model.env.json
joule broadcast inject --envelope model.env.json
```

Software update (peer-seed binary):

```text
# 1) build release binary somewhere you trust
# 2) seed into blob store on one online agent host:
joule seed-blob --path ./target/release/joule --kind software --name joule

# 3) put that sha256 into software_update body targets[] for each os/arch
joule broadcast sign --kind software_update --body sw.json --secret … --out sw.env.json
joule broadcast inject --envelope sw.env.json

# 4) agents fetch digest, stage; operator/user applies:
joule software status
joule software apply
```

Pause / resume public service:

```text
joule broadcast sign --kind pause_service --body '{}' --secret … --out pause.env.json
joule broadcast inject --envelope pause.env.json
# resume_service similarly
```

Policy (allow-listed fields only):

```json
{
  "service_live": false,
  "heartbeat_mint_mj": 10,
  "dual_verify_every": 3
}
```

---

## 5. Rotation

1. Generate new ed25519 keypair.  
2. Publish new public key; optionally GPG-sign the transition notice.  
3. Dual-pin period: controls that know **either** key accept (future: `JOULE_OPERATOR_PUBKEYS` list).  
4. v0: single key only — rotate by updating pinned pubkey everywhere, then re-sign critical policy.

Revoke is **relay-only** in v0 (no automatic key tombstone).

---

## 6. Threat model (v0)

| Threat | Mitigation |
|---|---|
| Forged model_update | ed25519 verify against pinned pubkey |
| Replay | envelope `id` dedupe + optional `expires_at` |
| f00 compromise | website cannot sign; no private key on edge |
| Malicious peer blob | sha256 verify before stage/load |
| Arbitrary code from bus | allow-listed kinds only; software applies **staged** binary after hash check |

---

## 7. Checklist before production

- [ ] Secret offline  
- [ ] Public key in git + site  
- [ ] `JOULE_OPERATOR_PUBKEY` on all controls  
- [ ] Lab inject without key fails in prod config  
- [ ] First software seed done from a trusted builder machine  
- [ ] Dashboard shows notices (`/v1/notices`)  
