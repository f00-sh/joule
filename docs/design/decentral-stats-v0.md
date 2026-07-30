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

## Automatic discovery (no manual sources.json)

Anyone running control can **announce** without an f00 token:

```bash
# Public HTTPS base of YOUR control (required for announce)
export JOULE_PUBLIC_URL=https://my-pool.example.com

joule control
# → signs POST https://joule.f00.sh/api/announce
# → directory GET https://joule.f00.sh/api/sources lists you
# → site multi-fetches your /v1/public/snapshot
```

Announce is authorized only by **holding the pool ed25519 key** (signature), not by Cloudflare secrets.

Optional privileged mirror (push full snapshot into edge KV):

```bash
# f00 core path
mkdir -p ~/.config/f00/joule
# token matches Cloudflare Pages INGEST_TOKEN
cp edge.token ~/.config/f00/joule/edge.token
```

```bash
curl -s https://joule.f00.sh/api/sources | jq .
curl -s http://127.0.0.1:7700/v1/public/snapshot | jq .signature
```
