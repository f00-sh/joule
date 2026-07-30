# joule — master key trust (GPG + protocol) v0

**Status:** active  
**Who this is for:** you (operator), anyone reviewing how hijacks are blocked  
**Keys:** `docs/operator-keys/` (public only) · secrets never in git

---

## 1. What you asked for

1. **Your / f00 master identity** — OpenPGP key for **`tj@f00.sh`** is the **master** human-facing authority.  
2. **Hardcode the public material in source** so official builds know exactly whom to trust.  
3. **Guardrails** so a random person cannot “change one constant, recompile, and become the official network.”  
4. Optional: **host the public key on the official website** (HTTPS).

This document is the decision and the operational model.

---

## 2. Hard truth (so we design correctly)

**You cannot stop someone from editing source and building a forked binary.**  
Open source means they can change any constant, including a “master” key.

What we *can* do:

| Goal | Mechanism |
|------|-----------|
| Official clients only obey **your** key | Embed **your** public key + fingerprint in the binary; **always** verify against it |
| Env var cannot quietly hijack | Ignore `JOULE_OPERATOR_PUBKEY` unless `JOULE_ALLOW_UNOFFICIAL_OPERATOR=1` (lab only) |
| Website cannot silently replace trust | HTTPS fetch is **only** accepted if it **matches the embed** (or is signed by embed master for rotation) |
| Users of a malicious fork are not “protected” by crypto alone | They chose a different binary; protection is **install provenance** (GitHub release + GPG-signed checksum, curl only from joule.f00.sh / f00-sh) |
| Your private key never ships | Secret stays offline / `~/.config/f00/joule/` only |

So: **embed + website = dual pin of the same key**, not “website is root of trust by itself.”

```
                    ┌─────────────────────────────┐
                    │  Master OpenPGP (tj@f00.sh) │
                    │  private: YOU only          │
                    └─────────────┬───────────────┘
                                  │ certifies once
                                  ▼
                    ┌─────────────────────────────┐
                    │  Protocol ed25519 pubkey    │
                    │  (agents verify this fast)  │
                    └─────────────┬───────────────┘
                                  │
           ┌──────────────────────┼──────────────────────┐
           ▼                      ▼                      ▼
   HARDCODED in binary     HTTPS joule.f00.sh      GitHub + GPG sig
   (fingerprint + ASC)     /operator-keys/*        on release assets
   must match              must match embed
```

---

## 3. Two keys (by design)

### 3.1 Master OpenPGP — `tj@f00.sh`

- **Role:** human ceremony, release signatures, certifying the protocol key, long-term identity.  
- **Public:** `docs/operator-keys/master.asc` (also published at  
  `https://joule.f00.sh/operator-keys/master.asc`).  
- **Fingerprint (pin):** see `MASTER_OPENPGP_FINGERPRINT` in code.  
- **Private:** never git. On the builder machine only  
  (`~/.config/f00/joule/master.gpg.sec.asc` + passphrase file).  
- **Algorithm:** Ed25519 OpenPGP (generated for this project).

If “your” personal GPG key is distinct, use it to **cross-sign / certify** `tj@f00.sh` offline once.  
The **network** still pins `tj@f00.sh` as the product master so you can keep a personal key separate from automation.

### 3.2 Protocol ed25519 — agent hot path

- **Role:** every `SignedEnvelope` on the bus (model_update, software_update, pause, notice, …).  
- **Why not GPG in the hot path:** pure Rust verify, no libgpgme dependency, fast flood.  
- **Public hex:** `docs/operator-keys/protocol.ed25519.pub`  
- **GPG cert:** `protocol.ed25519.pub.asc` (detached signature by master).  
- **Private:** `~/.config/f00/joule/protocol.ed25519.sec` only.

Agents verify **protocol ed25519**. Humans verify **GPG** on release notes and on the protocol pubkey file.

---

## 4. How official clients decide “this order is real”

On every operator envelope:

1. Check body sha256.  
2. Verify ed25519 against **embedded** `PROTOCOL_ED25519_PUBKEY_HEX`  
   (not against whatever is in the environment).  
3. Reject on mismatch.

Lab / forks only:

```text
JOULE_ALLOW_UNOFFICIAL_OPERATOR=1
JOULE_OPERATOR_PUBKEY=<hex>
```

Without the allow flag, env and random `operator.pub` files are **ignored**.

### Website pull (optional refresh / audit)

At control start (and periodically):

1. `GET https://joule.f00.sh/operator-keys/master.asc` over **TLS** (rustls).  
2. Parse fingerprint; must equal **embedded** fingerprint.  
3. `GET …/protocol.ed25519.pub` — hex must equal **embedded** protocol hex.  
4. If mismatch → **hard fail log / refuse service** (do not “upgrade” to the website key).  
5. If website unreachable → continue with embed only (offline / air-gapped OK).

**Why not “website is the only source”?**  
If TLS or DNS were compromised alone, an attacker could push a new master.  
Embed stops that: website can only **confirm** the pin, not replace it.

**Rotation (future):** publish a new protocol key **signed by master OpenPGP**, and a new master only via a release that ships a new binary embed (SemVer bump). v0 does not auto-rotate embed from the network.

---

## 5. What a hijacker would have to do

| Attack | Outcome |
|--------|---------|
| Set `JOULE_OPERATOR_PUBKEY` on a victim machine | **Ignored** (no unofficial flag) |
| Drop `operator.pub` in CWD | **Ignored** for authority |
| Change source, rebuild, give friends the binary | Friends run a **fork** — not joule.f00.sh official; website + release sigs don’t match |
| Compromise joule.f00.sh alone | Clients still require **match to embed**; fake key rejected |
| Compromise embed only (malicious release binary) | Need GitHub release + **GPG signature of the artifact** by master (release process) |
| Steal protocol.ed25519.sec | Can sign bus messages until you revoke/rotate (keep offline; re-issue via new embed) |
| Steal master OpenPGP secret | Can sign releases and new protocol keys — **highest severity**; offline + passphrase |

---

## 6. Where secrets live (this machine)

Generated during setup (not in git):

| Path | Contents |
|------|----------|
| `~/.config/f00/joule/gnupg-joule-master/` | GPG home for tj@f00.sh |
| `~/.config/f00/joule/master.gpg.sec.asc` | Exported secret ASC |
| `~/.config/f00/joule/master.gpg.pass` | Passphrase for that key |
| `~/.config/f00/joule/protocol.ed25519.sec` | Protocol signing secret |

**Back these up offline. Never commit. Never put on f00 edge.**

Public (in git + site):

| Path |
|------|
| `docs/operator-keys/master.asc` |
| `docs/operator-keys/protocol.ed25519.pub` |
| `docs/operator-keys/protocol.ed25519.pub.asc` |

---

## 7. Day-to-day operator flow

```text
# Sign a notice with the official protocol secret (clients verify the embed — no env pin)
joule broadcast sign --kind notice --body docs/examples/notice.json \
  --secret ~/.config/f00/joule/protocol.ed25519.sec --out /tmp/n.env.json
joule broadcast inject --envelope /tmp/n.env.json

# Humans: verify release / protocol pubkey
gpg --import docs/operator-keys/master.asc
gpg --verify docs/operator-keys/protocol.ed25519.pub.asc \
             docs/operator-keys/protocol.ed25519.pub
```

Control/agents with **stock source** always verify against the **embedded** protocol hex.

---

## 8. Code pins (names)

| Constant | Meaning |
|----------|---------|
| `MASTER_OPENPGP_FINGERPRINT` | 40-hex fingerprint of tj@f00.sh |
| `MASTER_OPENPGP_ASC` | Full armored public key (`include_str!`) |
| `PROTOCOL_ED25519_PUBKEY_HEX` | 64-hex protocol verify key |
| `OFFICIAL_MASTER_ASC_URL` | `https://joule.f00.sh/operator-keys/master.asc` |
| `OFFICIAL_PROTOCOL_PUB_URL` | `https://joule.f00.sh/operator-keys/protocol.ed25519.pub` |

Changing these constants **and** publishing a release is how **you** rotate.  
A third party changing them only produces a **different product**.

---

## 9. If you want “your personal key” even more primary

Optional ceremony (not required for the mesh to run):

1. Import personal GPG key.  
2. Certify `tj@f00.sh` with a personal signature:  
   `gpg --sign-key 4B18FA65E246ACC61701B6AFCA4CB80ABF1AF878`  
3. Publish the certification on the site / keyservers.  

Protocol pin remains the ed25519 key certified by `tj@f00.sh`.

---

## 10. Checklist

- [x] Generate OpenPGP master for `tj@f00.sh`  
- [x] Export public ASC into git  
- [x] Generate protocol ed25519; GPG-detach-sign the pubkey file  
- [x] Embed pins in Rust; default verify = official only  
- [x] Publish under `docs/operator-keys/` (Pages deploys to joule.f00.sh)  
- [x] HTTPS match-check helper (optional, non-fatal if offline)  
- [ ] Offline backup of secret + passphrase (you)  
- [ ] Personal-key certify master (optional, you)  
- [ ] First GitHub Release artifact signed with master GPG  

---

## 11. Bottom line

**Master = OpenPGP `tj@f00.sh`.**  
**Wire authority = protocol ed25519 embedded in every official binary.**  
**Website = same public material over TLS, must match embed.**  
**Hijack resistance = ignore env overrides + dual pin + release signatures — not “source is uneditable.”**

That is the strongest honest design available for an open-source mesh.
