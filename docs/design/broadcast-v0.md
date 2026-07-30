# joule — signed operator broadcast bus v0

**Status:** active (design)  
**Companion laws:** [distribution-v0](distribution-v0.md) (website only · peer seed) · [self-govern-v0](self-govern-v0.md) (ledger) · [cluster-v0](cluster-v0.md)

---

## 1. What you are describing (in plain terms)

You want a **single authority channel** that is:

1. **Cryptographically authentic** — only your private key can author “do this”  
2. **Publicly verifiable** — every client has your **public** key (GPG and/or ed25519)  
3. **Flooded by the swarm** — clients **relay** messages; f00 is not a push server  
4. **Actionable** — after verify, clients run a small set of safe operations  
5. **Content-addressed for heavy stuff** — the signed message carries *hashes and instructions*, not multi-GB blobs from f00  

That is **not** “our server downloads the model for everyone.”  
It is: **you publish a signed order; peers copy the order; peers seed the bytes among themselves.**

```
  you (offline private key)
        │  sign(order)
        ▼
  one online seeder (your laptop / any donor)
        │  inject signed envelope into the mesh
        ▼
  agents gossip envelope (verify sig → store → rebroadcast)
        │
        ├─ software_update  → fetch blob(sha256) from peers → install
        ├─ model_update     → fetch needed shard digests from peers → load
        ├─ notice           → show on dashboard / CLI
        └─ policy           → update local eco constants / gates (if allowed)
```

---

## 2. Keys: GPG + protocol key

| Key | Who holds | Purpose |
|---|---|---|
| **Operator public key** | Everyone (git + website) | Verify all official orders |
| **Operator private key** | You only (offline ideally) | Sign orders |
| **Pool / node keys** | Each control/agent | Ledger, snapshots, peer auth (already) |

**GPG for humans, ed25519 for agents (recommended):**

- Publish **OpenPGP public key** on joule.f00.sh / git so people can verify with `gpg --verify`  
- Also publish a **protocol ed25519** public key (same root of trust, or GPG-signs the ed25519 key once)  
- Agents verify ed25519 on every message (pure Rust, no GPG in the hot path)  
- Humans can still GPG-verify release notes and the operator-key ceremony  

“Decrypt” for **broadcast** updates is usually **not** needed: the payload is public.  
- **Sign** = authenticity + integrity (always)  
- **Encrypt** = only if the order is confidential (rare for pool-wide model updates)  
  If you encrypt, encrypt to a **pool shared secret** or hybrid (message key wrapped for subscribers)—not “each user has a different half of the model encrypted.”

---

## 3. Message shape (one envelope for everything)

```text
SignedEnvelope {
  id:          uuid                    // dedupe
  issued_at:   unix_ms
  expires_at:  unix_ms?                // optional
  kind:        software_update | model_update | notice | policy | …
  body:        JSON (kind-specific)
  body_sha256: hex                     // of canonical body
  sig_ed25519: hex                     // sign(body_sha256 || id || issued_at || kind)
  // optional: openpgp_detached_sig for humans
}
```

**Client pipeline (every agent, always):**

1. Receive envelope (from peer, control relay, or CLI inject)  
2. Reject if `id` seen (dedupe store)  
3. Reject if expired  
4. Verify ed25519 against pinned operator pubkey  
5. Rebroadcast to peers (gossip)  
6. Dispatch `kind` handler  
7. Persist in local message log (hash chain optional)

**Important:** handlers are **allow-listed**. Unknown kind → store + relay only, do not execute arbitrary code.

---

## 4. Kind: software update

```json
{
  "version": "0.1.0",
  "targets": [
    { "os": "linux", "arch": "x86_64", "sha256": "…", "size": 12345678, "name": "joule" }
  ],
  "notes": "…"
}
```

Agent:

1. Match own target  
2. If blob `sha256` missing → `BlobWant` / peer fetch (distribution-v0)  
3. Verify hash  
4. Atomic replace binary / stage for restart  
5. Announce `BlobsHave` so others can seed  

**f00 never hosts the binary.** You seed once from a machine that has it; the swarm multiplies it.

---

## 5. Kind: model update — “how does my portion work?”

### 5.0 The problem (why not “everyone downloads the whole model”)

A frontier open model can be **hundreds of GB**. Forcing every donor to store the full set would:

- waste disk and bandwidth  
- make joiners take forever  
- still leave you brittle if only one node finishes  

So: **split into content-addressed chunks**, give each node **only what it needs**, and **replicate each chunk onto several nodes** so nobody is single point of failure.

### 5.1 Redundant overlapping chunks (the real design)

```text
Chunks:   C0  C1  C2  C3  C4  C5     (each has sha256 + layer range)
Nodes:    A   B   C   D

With replica_factor = 2 (ring placement):

  C0 → A (primary), B (replica)
  C1 → B (primary), C (replica)
  C2 → C (primary), D (replica)
  C3 → D (primary), A (replica)
  …

If B dies:
  C0 still on A, C1 still on C  → model intact
  Plan rebalances: ask healthy nodes to pull under-replicated digests
```

Code: `joule_cluster::plan_redundant_chunks` (`DEFAULT_REPLICA_FACTOR = 2`).

| Idea | Meaning |
|---|---|
| **Chunk** | One file / layer band with a **sha256** (not “half a secret model per user”) |
| **Primary** | Preferred holder for serving that band |
| **Replica** | Overlapping copy on another node for survival |
| **Required digests for node X** | Unique sha256 list X must fetch — **subset of the model** |
| **Cannot rely on anyone** | Rely on **R independent holders per chunk** + re-seed when count &lt; R |

**Erasure coding** (k-of-n fragments, less storage than full replicas) is Phase E — same announce/fetch machinery, different encoding.

### 5.2 What a distributed model is on joule (inference view)

The pool is **one logical GPU**. Internally the model is **sharded** for compute *and* stored with **redundancy**:

```text
Logical model (all layers / all chunks)
    ├── Node A: primary C0,C3 + replica C5…   ← A's portion (not full)
    ├── Node B: primary C1 + replica C0…     ← B's portion
    └── …
```

“Portion” = **files this node is assigned to hold (primary and/or replica)**, not a personal encrypted half.

### 5.2 What the signed model_update carries

```json
{
  "model_id": "kimi-open",
  "manifest_version": 4,
  "quants": [{
    "id": "q4_k_m",
    "files": [
      { "path": "layer-000.safetensors", "sha256": "aa…", "size": 1, "layer_start": 0, "layer_end": 9 },
      { "path": "layer-001.safetensors", "sha256": "bb…", "size": 1, "layer_start": 10, "layer_end": 19 }
      // …
    ]
  }],
  "activation": "when_all_required_shards_local | when_pool_ready"
}
```

No multi-GB payload in the message—only **digests + layout**.

### 5.3 What each client does

1. Verify signed `model_update`  
2. Merge into local copy of MANIFEST (or side table “pending model”)  
3. Ask planner / own caps: **which files do I need?**  
   - Conservative v0: download **all** quant files (simplest, wastes disk)  
   - Better: only files intersecting my layer range / mem share  
4. For each needed `sha256`: local blob store → peer seed → (optional external)  
5. When enough of the mesh has loaded → `ModelLoaded` / service-live as today  
6. Seed everything you have (`BlobsHave`) so late joiners catch up  

### 5.4 Who seeds the first copy?

You (or any friend) run one machine that **already has** the full set (USB, HF with `JOULE_ALLOW_EXTERNAL_FETCH=1` on *one* box, datacenter dump).  
That box announces blobs. Everyone else pulls **from the swarm**, not from f00.

```text
You ──seed──► Peer swarm ──► all agents
                ▲
                └── messages also gossip the order to start seeding
```

---

## 6. Kind: notice / policy / everything else

| Kind | Body | Client action |
|---|---|---|
| `notice` | title, body, severity | CLI banner, dashboard strip |
| `policy` | eco constants, min pool VRAM | Apply if version ≥ min_agent |
| `pause_service` | reason | Stop accepting chat |
| `resume_service` | | Resume |
| `revoke_message` | target id | Tombstone prior message |

Same verify → relay → act path.

---

## 7. How messages get to everyone (no f00 push)

| Path | Role |
|---|---|
| **Gossip among agents** | Primary: every peer forwards verified envelopes |
| **Control as relay** | Optional: any control stores last N envelopes and feeds joiners |
| **CLI inject** | `joule broadcast inject signed.json` on one online node |
| **Website** | May **mirror the public key and message IDs** for humans—not required for agents |

Control is still **not** “our cloud app store.” Any operator can run control; the **signature** is the authority, not the hostname.

---

## 8. Threat model (short)

| Attack | Mitigation |
|---|---|
| Fake update from attacker | Signature fail → drop |
| Replay old update | `id` dedupe + `expires_at` + monotonic version |
| Malicious peer edits body | Hash + sig fail |
| Compromise of your private key | Key ceremony + revoke list in a new signed message from a pre-published recovery key |
| Huge traffic from fake blobs | Only fetch digests listed in verified messages |
| RCE via “update” | Updates are **blob replace of known artifacts**, not shell scripts from the message |

---

## 9. How this fits “website only”

- **joule.f00.sh:** public key, human docs, optional message feed mirror for transparency  
- **Bytes:** peer blob store (`distribution-v0`)  
- **Orders:** signed envelopes, gossiped  
- **Money:** still none (`self-govern-v0`)  

---

## 10. Implementation phases

| Phase | Deliverable |
|---|---|
| **A** | Pin operator ed25519 pubkey in repo; `SignedEnvelope` type; verify + local log + gossip |
| **B** | `notice` + `software_update` handlers + blob seed path |
| **C** | `model_update` with per-node required file set from plan |
| **D** | Optional OpenPGP cosign for human verification; recovery key |
| **E** | Direct agent↔agent bulk transfer (BitTorrent-class) for multi-hundred-GB |

---

## 11. One-sentence mental model

**You sign a postcard that says “get these hashes and do X.” Everyone copies the postcard. The hard drives fill from each other. The website just shows the public key and the story.**
