import {
  announcePreimage,
  corsPreflight,
  ed25519Verify,
  json,
  MAX_SOURCES,
  readSources,
  SOURCE_TTL_MS,
  writeSources,
} from "../_shared.js";

/**
 * POST /api/announce — self-register a public signed snapshot URL.
 * No privileged token. Must hold the pool ed25519 key.
 *
 * Body:
 * {
 *   pool_id, snapshot_url, verifying_key_hex, signature_hex, updated_unix_ms
 * }
 * signature = ed25519( sha256(pool_id\\nsnapshot_url\\nupdated_unix_ms) )
 */
export async function onRequestPost(context) {
  const { request, env } = context;
  let body;
  try {
    body = await request.json();
  } catch {
    return json({ ok: false, error: "invalid_json" }, 400);
  }

  const poolId = String(body.pool_id || "").trim();
  const snapshotUrl = String(body.snapshot_url || "").trim();
  const vk = String(body.verifying_key_hex || "").trim();
  const sig = String(body.signature_hex || "").trim();
  const updated = Number(body.updated_unix_ms || Date.now());

  if (!poolId || !snapshotUrl || !vk || !sig) {
    return json(
      {
        ok: false,
        error: "need pool_id, snapshot_url, verifying_key_hex, signature_hex",
      },
      400,
    );
  }
  if (!/^https:\/\//i.test(snapshotUrl) && !/^http:\/\/127\.0\.0\.1/.test(snapshotUrl)) {
    return json(
      { ok: false, error: "snapshot_url must be https:// (or http://127.0.0.1 for lab)" },
      400,
    );
  }

  const pre = await announcePreimage(poolId, snapshotUrl, updated);
  const good = await ed25519Verify(pre, sig, vk);
  if (!good) {
    return json({ ok: false, error: "bad_signature" }, 401);
  }

  // Soft probe: snapshot should respond (warn-only if down — NAT later)
  let probe_ok = false;
  try {
    const r = await fetch(snapshotUrl, { signal: AbortSignal.timeout(3000) });
    probe_ok = r.ok;
  } catch {
    probe_ok = false;
  }

  const now = Date.now();
  const entry = {
    pool_id: poolId,
    snapshot_url: snapshotUrl,
    verifying_key_hex: vk,
    announced_unix_ms: now,
    expires_unix_ms: now + SOURCE_TTL_MS,
    last_updated_unix_ms: updated,
    probe_ok,
  };

  let list = await readSources(env);
  list = list.filter((s) => s.pool_id !== poolId && s.snapshot_url !== snapshotUrl);
  list.unshift(entry);
  list = list.slice(0, MAX_SOURCES);
  await writeSources(env, list);

  return json({
    ok: true,
    pool_id: poolId,
    snapshot_url: snapshotUrl,
    probe_ok,
    directory_count: list.length,
    expires_unix_ms: entry.expires_unix_ms,
  });
}

export async function onRequestGet() {
  return json({
    ok: true,
    endpoint: "/api/announce",
    method: "POST",
    auth: "none — ed25519 signature of pool key required",
    scheme: "sha256(pool_id\\nsnapshot_url\\nupdated_unix_ms) then ed25519",
  });
}

export async function onRequestOptions() {
  return corsPreflight();
}
