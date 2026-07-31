# joule — distribution law v0 (website only · peer-seeded)

**Status:** active (v0 code: blob store, BlobsHave/Want/Provide/Chunk, seed-blob CLI, peer e2e)  
**Product stance:** f00 / `joule.f00.sh` is a **project website** (story, stats glass, docs). It is **not** an app store, weight CDN, or update server.

---

## 1. Laws

1. **We do not host model weights on f00.** Kimi-class / multi-hundred-GB files never live on Cloudflare as a product CDN. Official third-party origins + **peer seed** + **sha256** only.  
2. **Install binaries live on GitHub Releases** (automated CI). The site may **link / autodetect** (`/download.html`) — it does not re-host multi-GB weight blobs.  
3. **Install populates itself.** After you have a `joule` binary, the agent fills weights **locally** and from **peers** (and optional official HTTP with hash verify).  
4. **Content is addressed by hash.** Manifest pins `sha256` (+ size). Trust the digest, never the hostname.  
5. **Seeding, not serving weights.** Whoever already has a blob can seed it. Software updates can also be peer-seeded digests after first install.  
6. **External HTTP for weights is optional and off by default** (`JOULE_ALLOW_EXTERNAL_FETCH=1`). f00 never appears as a required weight origin.  
7. **Website is glass + download UX.** Live stats multi-source (signed snapshots). Download page points at **GitHub** assets only.

---

## 2. What “our server” is

| Role | joule.f00.sh / f00 | GitHub Releases |
|---|---|---|
| Landing / teaser | Yes | — |
| Live pool *viewer* | Yes | — |
| Download page (autodetect) | Yes (links only) | Canonical **binary** assets |
| `joule` installers / tarballs | Link only | **Yes** (CI) |
| Distribute Kimi weights | **No** | **No** |
| Push weight OTA from f00 | **No** | **No** |

Chicken-and-egg for **weights**: first bytes from official source or git fixtures; then peers.  
Chicken-and-egg for **binary**: `curl \| sh` / `irm \| iex` from GitHub, or package managers (AUR / Homebrew templates in `packaging/`).

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
