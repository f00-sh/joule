# joule — decentralized public stats v0

**Status:** active  
**Goal:** landing page is a **viewer**, not an authority. Pool stats come from **many signed sources**.

---

## Laws

1. Cloudflare / f00 edge is an **optional mirror**, never the only truth.
2. Any control plane may publish a **signed** snapshot at `GET /v1/public/snapshot`.
3. The site loads `sources.json` (and optional `?sources=` / `?api=`), fetches all sources, and paints the best view.
4. Signatures use **ed25519** over `sha256(pool_id || "\n" || updated_unix_ms || "\n" || body_json)`.

---

## Signature scheme

`body_json` is compact JSON of:

```json
{"capacity":…,"readiness":…,"scheduler":…,"nodes":…}
```

(field order fixed by serde structs).

Response also includes:

```json
"signature": {
  "algorithm": "ed25519",
  "verifying_key_hex": "…",
  "signature_hex": "…",
  "body_json": "…"
}
```

Browser verification uses `@noble/ed25519` when available; if verify fails, source is demoted.

---

## Edge token (optional mirror)

Ingest bearer for `POST /api/ingest` resolves from:

1. `JOULE_EDGE_TOKEN`
2. `JOULE_EDGE_TOKEN_FILE`
3. `./.ingest-token`
4. `~/.local/share/joule/edge.token`
5. **`~/.config/f00/joule/edge.token`** (f00 operator core)

---

## Operator checklist

```bash
# f00 core path (preferred)
mkdir -p ~/.config/f00/joule
# token matches Cloudflare Pages INGEST_TOKEN
cp edge.token ~/.config/f00/joule/edge.token

joule control   # signs snapshots; publishes to edge if token present
# public signed feed:
curl -s http://127.0.0.1:7700/v1/public/snapshot | jq .
```

Add your public control URL to `docs/sources.json` so joule.f00.sh multi-sources you without CF ingest.
