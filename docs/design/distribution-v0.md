# joule — distribution law v0 (website only · peer-seeded)

**Status:** active  
**Product stance:** f00 / `joule.f00.sh` is a **project website** (story, stats glass, docs). It is **not** an app store, weight CDN, or update server.

---

## 1. Laws

1. **We do not host payloads.** No model weights, no installers, no release tarballs on f00 infrastructure as a product requirement.  
2. **Install populates itself.** After you have a `joule` binary (built from source, or obtained however *you* choose), the agent fills what it needs **locally** and from **peers**.  
3. **Content is addressed by hash.** Manifest pins `sha256` (+ size). Trust the digest, never the hostname.  
4. **Seeding, not serving.** Whoever already has a blob can seed it into the pool. Version bumps and weight files spread the same way: peer → peer.  
5. **External HTTP is optional and off by default.** Third-party mirrors (HF, IPFS gateways, a friend’s NAS) may be listed in the manifest as *hints*, but agents only hit them if the operator sets `JOULE_ALLOW_EXTERNAL_FETCH=1`. f00 never appears as a required origin.  
6. **Website stays dumb glass.** Live stats multi-source (signed snapshots). No binary download farm.

---

## 2. What “our server” is

| Role | joule.f00.sh / f00 |
|---|---|
| Landing / teaser | Yes |
| Live pool *viewer* | Yes (optional mirrors of **public** signed stats) |
| Distribute `joule` binaries | **No** (not product path) |
| Distribute Kimi weights | **No** |
| Push OTA updates from f00 | **No** — updates are **seeded** as content-addressed blobs |

Chicken-and-egg for the first node: build from **git** (or any path the user trusts). After one node has content, the mesh can seed the rest.

---

## 3. Content-addressed blobs

Everything heavy is a **blob**:

```text
~/.local/share/joule/blobs/sha256/<hex>
~/.local/share/joule/weights/<model>/<quant>/…  # hardlink or copy from blob store after verify
```

Kinds (examples):

- `weight` — safetensors / config pieces listed in MANIFEST  
- `software` — release tarball / binary for a version + target  
- `fixture` — lab-tiny for dev (may ship *inside the git repo* for contributors; not served by f00)

Agents announce:

```json
{ "type": "blobs_have", "blobs": [ { "sha256": "…", "size": 123, "kind": "weight", "name": "…" } ] }
```

Control keeps a **directory only** (who claims which hash) — not the bytes. That directory can also be public (`GET /v1/blobs`) so the website can show “N peers seeding lab-tiny” without hosting files.

---

## 4. How prepare works (order)

For each required `sha256` in the quant:

1. **Local blob store** — already have hash → link into quant dir  
2. **Peer seed** — control says node X has it → fetch from peer (protocol / later direct)  
3. **Operator drop** — user copied files into weights dir; hash match → accept  
4. **External fetch** — only if `JOULE_ALLOW_EXTERNAL_FETCH=1` and a non-f00 URL hint exists  
5. Else **wait / arm** — pool can still grow; inference stays limited until seeded

`repo://` is **developer convenience** (path inside a git checkout), not a CDN.

---

## 5. Software updates “seeded through”

- Manifest or release card lists `version`, `target`, `sha256`, `size`  
- One human builds or obtains `v0.1.0` and runs an agent that **announces** the blob  
- Others `joule update` (future) pull that hash from peers, verify, replace binary  
- f00 site may **link to the git tag / checksum in docs**; it does not have to host the artifact  

Same pipeline as weights. No special f00 update channel.

---

## 6. What we refuse

- Shipping installers that only work by curling `https://f00.sh/.../joule-linux.gz`  
- Making HF or any single host a **hard dependency** of the product  
- Control plane storing multi-GB weight corpora  

---

## 7. Relation to self-govern / economy

Distribution is orthogonal to millijoules. Credits stay on the sealed ledger. Blobs are just **bytes with digests** moving between machines that already donated compute.
