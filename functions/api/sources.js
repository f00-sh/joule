import { corsPreflight, json, readSources } from "../_shared.js";

/** GET /api/sources — public decentralized source directory (no secrets). */
export async function onRequestGet(context) {
  const list = await readSources(context.env);
  return json({
    ok: true,
    version: 1,
    count: list.length,
    sources: list.map((s) => ({
      id: s.pool_id || s.id,
      url: s.snapshot_url,
      kind: "control",
      pool_id: s.pool_id,
      verifying_key_hex: s.verifying_key_hex,
      announced_unix_ms: s.announced_unix_ms,
      expires_unix_ms: s.expires_unix_ms,
    })),
    note: "Anyone can announce with a signed POST /api/announce — no f00 token required.",
  });
}

export async function onRequestOptions() {
  return corsPreflight();
}
