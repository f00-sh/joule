/** Shared helpers for joule Pages Functions (edge pool feed). */

export const SNAPSHOT_KEY = "pool:live";

export function emptySnapshot(reason = "no_donors") {
  return {
    ok: true,
    source: "empty",
    reason,
    updated_at: new Date().toISOString(),
    updated_unix_ms: Date.now(),
    capacity: {
      nodes_total: 0,
      nodes_healthy: 0,
      nodes_gpu: 0,
      nodes_metal: 0,
      nodes_cpu: 0,
      mem_mib_total: 0,
      mem_mib_healthy: 0,
      throughput_class_sum: 0,
      models_available: [],
      stream_slots_total: 0,
      stream_slots_used: 0,
      logical_device: {
        id: "joule-pool",
        name: "joule pool",
        kind: "aggregate_gpu",
        vram_mib: 0,
        vram_gib: 0,
        backends: 0,
        model: "kimi-open",
        ready: false,
        model_ready: false,
        model_progress_pct: 0,
        inference_mode: "stub_awaiting_pool",
        readiness_message: "Waiting for donors to join the cluster.",
      },
    },
    readiness: {
      model: "kimi-open",
      pool_vram_mib: 0,
      backends: 0,
      pool_ready: false,
      pool_progress_pct: 0,
      countdown_label: "awaiting first donors",
      countdown_secs: null,
      can_load_model: false,
      can_begin_service: false,
      service_live: false,
      milestones: [],
      next_milestone: null,
      inference_mode: "stub_awaiting_pool",
      message: "Pool empty — run joule agent to donate compute.",
    },
    scheduler: {
      stream_slots_total: 0,
      stream_slots_used: 0,
      stream_slots_free: 0,
      can_accept_work: false,
      view: "one_logical_device",
    },
    nodes: [],
  };
}

export function json(data, status = 200, extra = {}) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET, POST, OPTIONS",
      "access-control-allow-headers": "content-type, authorization, x-joule-token",
      ...extra,
    },
  });
}

export function corsPreflight() {
  return new Response(null, {
    status: 204,
    headers: {
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET, POST, OPTIONS",
      "access-control-allow-headers": "content-type, authorization, x-joule-token",
      "access-control-max-age": "86400",
    },
  });
}

export async function readSnapshot(env) {
  if (!env.POOL) {
    return emptySnapshot("kv_unbound");
  }
  const raw = await env.POOL.get(SNAPSHOT_KEY, "json");
  if (!raw || typeof raw !== "object") {
    return emptySnapshot("no_snapshot");
  }
  return raw;
}

export async function writeSnapshot(env, snap) {
  if (!env.POOL) return;
  const body = {
    ...snap,
    ok: true,
    updated_at: new Date().toISOString(),
    updated_unix_ms: Date.now(),
  };
  await env.POOL.put(SNAPSHOT_KEY, JSON.stringify(body));
  return body;
}

/** Pull live cluster JSON from a control origin (optional CONTROL_ORIGIN secret). */
export async function pullFromControl(env) {
  const origin = (env.CONTROL_ORIGIN || "").replace(/\/$/, "");
  if (!origin) return null;
  const timeout = AbortSignal.timeout(4000);
  const [capR, readyR, nodesR, schedR] = await Promise.all([
    fetch(`${origin}/v1/cluster/capacity`, { signal: timeout }),
    fetch(`${origin}/v1/models/readiness`, { signal: timeout }),
    fetch(`${origin}/v1/cluster/nodes`, { signal: timeout }),
    fetch(`${origin}/v1/cluster/scheduler`, { signal: timeout }),
  ]);
  if (!capR.ok) return null;
  const capacity = await capR.json();
  const readiness = readyR.ok ? await readyR.json() : null;
  const nodesBody = nodesR.ok ? await nodesR.json() : { nodes: [] };
  const scheduler = schedR.ok ? await schedR.json() : null;
  return {
    ok: true,
    source: "control",
    control_origin: origin,
    capacity,
    readiness,
    scheduler,
    nodes: nodesBody.nodes || [],
  };
}

export async function liveSnapshot(env) {
  try {
    const pulled = await pullFromControl(env);
    if (pulled) {
      return await writeSnapshot(env, pulled);
    }
  } catch {
    // fall through to KV
  }
  return readSnapshot(env);
}

export function authorized(request, env) {
  const token = env.INGEST_TOKEN || "";
  if (!token) return false;
  const auth = request.headers.get("authorization") || "";
  const bearer = auth.toLowerCase().startsWith("bearer ")
    ? auth.slice(7).trim()
    : "";
  const header = request.headers.get("x-joule-token") || "";
  return bearer === token || header === token;
}
