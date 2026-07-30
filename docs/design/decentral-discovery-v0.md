# joule — decentralized discovery & coordination v0

**Status:** design (active) — code today is still **control-centric**; this doc is the target mesh  
**Companions:** [distribution-v0](distribution-v0.md) · [broadcast-v0](broadcast-v0.md) · [self-govern-v0](self-govern-v0.md) · [cluster-v0](cluster-v0.md) · [client-v0](client-v0.md) · [master-key-trust-v0](master-key-trust-v0.md)

---

## 1. Problem

Today:

```text
agent ──must know──► one control TCP
client ──must know──► one control HTTP
```

That is **centralized rendezvous**. Kill the control (or its address), and donors cannot find each other.  
**Goal:** no required central host. f00 stays **website glass only**. Peers find peers; the pool still behaves as **one logical device**.

---

## 2. What “totally decentralized” means here

| Must be decentralized | May stay special |
|----------------------|------------------|
| Finding peers | **Operator signature authority** (your OpenPGP/ed25519 master — crypto root, not a server) |
| Who has which blob (sha256) | Optional **bootstrap hints** (DNS / friends / website *mirrors* of signed peer lists) |
| Transferring bytes peer↔peer | |
| Gossip of membership + sealed ledger events | |
| Flood of operator bus orders | |

**Not** “no cryptography and no trust roots.”  
**Yes** “no single machine you must dial forever.”

Bootstrap is allowed if **any** of several sources work and **none is f00-as-CDN**:

- hard-coded optional community bootstrap multiaddrs (replaceable, not product payload origin)
- DNS TXT / HTTPS JSON on **any** domain a peer trusts (self-hosted)
- website multi-source **signed** peer cards (already the stats pattern — glass + verify)
- manual peer URI paste / QR from a friend

---

## 3. Architecture: three layers

```text
┌─────────────────────────────────────────────────────────┐
│ L3  WORK  — shard plan, infer, mint/burn mJ             │
│     any peer may coordinate a request; plans are signed │
├─────────────────────────────────────────────────────────┤
│ L2  GOSSIP — membership, ledger tips, operator envelopes│
│     flood + anti-entropy; every agent relays            │
├─────────────────────────────────────────────────────────┤
│ L1  FIND  — peer IDs, multiaddrs, blob digests          │
│     Kademlia-style DHT + local cache + bootstrap hints  │
└─────────────────────────────────────────────────────────┘
```

### L1 — Find (discovery)

Each node has:

- **NodeId** (UUID today → migrate to ed25519 pubkey hash)
- **Multiaddrs** (e.g. `quic://1.2.3.4:7702`, `tcp://…`) after NAT mapping if any
- **Capability card** (device class, claimed/verified VRAM, streams, blob index root)

**DHT keys (content-addressed, not hostnames):**

| Key | Value |
|-----|--------|
| `peer/<node_id>` | multiaddrs + caps digest + seq + sig |
| `blob/<sha256>` | list of peer ids that seed (with size, expiry) |
| `pool/<pool_id>/members` | optional soft set of recent peer ids (ring) |

**Lookup:** iterative Kademlia (or simpler v0: full mesh gossip until N≈50, then DHT).  
**No** “query f00 for peers.” Website may host a **signed** bootstrap list that nodes verify with the **embedded operator pin** — same dual-pin rule as keys.

### L2 — Gossip (state)

Messages (all signed by node or operator as appropriate):

| Message | Who signs | Purpose |
|---------|-----------|---------|
| `PeerAlive` | node | caps, load, verified_mem, blob summary hash |
| `BlobsHave` | node | digests this peer seeds (already exists) |
| `LedgerTip` / `LedgerBatch` | node (events already sealed) | sealed-chain segments |
| `OperatorBroadcast` | operator | model/software/policy (already exists) |
| `PlanOffer` / `PlanAccept` | coordinator + shards | infer plan for one request |

Relay rules: verify → dedupe → rebroadcast (TTL / hop limit).  
Control plane **becomes one optional peer** that speaks the same gossip, not a mandatory hub.

### L3 — Work (one logical device without a permanent boss)

For each chat/infer request:

1. Requester (any donor with a valid account key / balance) broadcasts **`RequestInfer`**.  
2. A **coordinator** is chosen without a fixed master:
   - **v0 simple:** first healthy peer that answers `PlanOffer` within RTT budget, or  
   - **v1:** VRF/leader lottery weighted by verified VRAM + tenure (anti-spam).  
3. Coordinator builds **`ClusterPlan`** (same structure as today) from **recent gossip membership**, not from a private registry only it holds.  
4. Shard peers **sign** their acceptance (`PlanAccept`).  
5. Infer runs; millijoule mint/burn are **sealed events** gossiped into the ledger mesh.  
6. If coordinator dies mid-request → timeout → new coordinator from remaining peers.

**One logical device** remains: plan still shards over **sum of verified VRAM** of the selected healthy set. The set is “whoever is in the mesh now,” not “whoever is connected to my laptop’s control.”

---

## 4. Blob transfer (decentralized path)

Today: BlobWant → control → BlobProvide (relay).  

Target:

```text
want digest D
  → DHT lookup blob/D → peer list
  → pick seeder(s) (parallel if multiple)
  → direct stream (QUIC/HTTP) seeder → requester
  → verify sha256 → BlobsHave → re-announce to DHT
```

Control may still **cache** a locator view for web dashboard, but it is not required for seed.

Redundant chunk placement (`plan_redundant_chunks`) still applies: each peer stores **assigned** digests, not full model.

---

## 5. Ledger & millijoules without one notary

Keep **sealed hash chain** semantics:

- Every event links `prev_hash`.  
- Nodes **gossip batches**; on fork, prefer chain with higher **notary weight** (sum of verified VRAM × tenure of signers on checkpoints) — already sketched in self-govern design.  
- Clients recompute balances only from chain (no “admin set balance”).  
- Operator **cannot** forge mJ without breaking the pin story; operator bus is for **policy/model**, not free mint.

---

## 6. How a fresh machine joins (user story)

1. Install binary (git / friend / release — not f00 CDN requirement).  
2. Start `joule agent` (and optional tray).  
3. Bootstrap:
   - try last-known peers from disk  
   - try optional bootstrap multiaddrs  
   - try user-supplied `--peer` / env  
   - try signed bootstrap list from website **if** it matches embed pin  
4. Gossip `PeerAlive` + `BlobsHave`.  
5. DHT announce `peer/<id>` and any local digests.  
6. Status CLI/tray shows **mesh connection** (peer count, DHT ready) not just “control HTTP.”

No step: “must reach William’s VPS.”

---

## 7. Migration from today’s code

| Phase | Behavior |
|-------|----------|
| **Now (v0)** | Control-centric TCP agent + HTTP API; blob directory on control |
| **A** | Agents open **peer listen** port; exchange PeerAlive over gossip; control optional |
| **B** | BlobWant prefers **direct** seeder multiaddr from gossip; control falls back to relay |
| **C** | DHT for blob + peer lookup; bootstrap lists |
| **D** | RequestInfer / PlanOffer without permanent control; multi-control or control-as-peer |
| **E** | Erasure coding, QUIC, NAT traversal (libp2p-class) |

**Product law unchanged:** f00 is glass; operator is crypto; money is forbidden on public pool.

---

## 8. What we deliberately do **not** do

- Require a single global sequencer run by f00  
- Trust unsigned “here’s a peer list” from random HTTP  
- Make the website a DHT super-node  
- Pretend zero bootstrap is possible without **any** out-of-band info (every mesh needs *some* first contact — we just allow **many** contacts, none privileged by brand)

---

## 9. Implementation sketch (Rust crates)

```text
joule-net/          multiaddr, PeerId, PeerAlive, gossip
joule-dht/          kademlia-lite: put/get peer + blob
joule-agent         already: + peer listen, gossip loop, direct blob client
joule-control       becomes optional dashboard/API peer speaking same gossip
joule-client status connection = mesh peers + optional control HTTP
```

Wire protocol: NDJSON envelopes already exist — extend `Message` enum; keep operator verify on embed pin.

---

## 10. Status CLI / tray (decentralized fields)

Add monitor cards when mesh lands:

| Card | Meaning |
|------|---------|
| `mesh_peers` | live gossip neighbors |
| `dht_ready` | can resolve digests |
| `control_optional` | HTTP API peer present or not |
| `balance_mJ` | from local chain replay / gossip tip |
| `tokens_used` | local + sealed usage events |

---

## 11. Decision summary

**How clients find each other:**  
content-addressed **DHT + gossip**, bootstrapped from **replaceable multiaddrs / DNS / signed lists**, not from a single control hostname.

**How work still works:**  
any peer can **coordinate** a sharded plan over the gossip membership view; millijoules stay on a **sealed gossiped chain**.

**How f00 stays pure:**  
website may **mirror signed** bootstrap/stats; never required as payload or as the only discovery root.

---

## 12. Next code steps (when implementing)

1. `PeerAlive` + peer listen on agent (Phase A).  
2. Put seeder multiaddr on `BlobsHave` / BlobLocate (Phase B).  
3. DHT module + bootstrap file format (Phase C).  
4. PlanOffer path without control (Phase D).  

Until then, document honesty: **lab is control-rendezvous; production target is this mesh.**
