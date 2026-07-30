import {
  corsPreflight,
  json,
  liveSnapshot,
} from "../_shared.js";

/** GET /api/pool — live (or last-known) cluster snapshot for the public site. */
export async function onRequestGet(context) {
  const snap = await liveSnapshot(context.env);
  return json(snap);
}

export async function onRequestOptions() {
  return corsPreflight();
}
