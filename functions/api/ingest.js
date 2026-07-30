import {
  authorized,
  corsPreflight,
  emptySnapshot,
  json,
  writeSnapshot,
} from "../_shared.js";

/**
 * POST /api/ingest — control plane publishes a live snapshot (Bearer INGEST_TOKEN).
 * Body: full snapshot or { capacity, readiness?, scheduler?, nodes? }.
 */
export async function onRequestPost(context) {
  const { request, env } = context;
  if (!authorized(request, env)) {
    return json({ ok: false, error: "unauthorized" }, 401);
  }
  let body;
  try {
    body = await request.json();
  } catch {
    return json({ ok: false, error: "invalid_json" }, 400);
  }
  if (!body || typeof body !== "object") {
    return json({ ok: false, error: "empty_body" }, 400);
  }

  const base = emptySnapshot("ingest");
  const snap = {
    ...base,
    source: "ingest",
    capacity: body.capacity || body.cluster || base.capacity,
    readiness: body.readiness || base.readiness,
    scheduler: body.scheduler || base.scheduler,
    nodes: Array.isArray(body.nodes) ? body.nodes : base.nodes,
    control_origin: body.control_origin || null,
  };

  const saved = await writeSnapshot(env, snap);
  return json({
    ok: true,
    updated_at: saved.updated_at,
    nodes_healthy: saved.capacity?.nodes_healthy ?? 0,
    mem_mib_healthy: saved.capacity?.mem_mib_healthy ?? 0,
  });
}

export async function onRequestOptions() {
  return corsPreflight();
}

export async function onRequestGet() {
  return json({
    ok: true,
    endpoint: "/api/ingest",
    method: "POST",
    auth: "Bearer INGEST_TOKEN or X-Joule-Token",
  });
}
