import {
  aggregateFromSources,
  corsPreflight,
  json,
  liveSnapshot,
} from "../_shared.js";

/**
 * GET /api/pool — best live snapshot from:
 * announced decentralized sources + optional CONTROL_ORIGIN + KV mirror.
 * No privileged token required to read.
 */
export async function onRequestGet(context) {
  try {
    const agg = await aggregateFromSources(context.env);
    if (agg && agg.source !== "empty" && agg.source !== "no_sources") {
      return json(agg);
    }
  } catch {
    /* fall through */
  }
  const snap = await liveSnapshot(context.env);
  return json(snap);
}

export async function onRequestOptions() {
  return corsPreflight();
}
