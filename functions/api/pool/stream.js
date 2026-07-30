import { liveSnapshot } from "../../_shared.js";

/**
 * GET /api/pool/stream — Server-Sent Events of the live pool snapshot.
 * Emits immediately, then on change / heartbeat every ~2s.
 */
export async function onRequestGet(context) {
  const env = context.env;
  const encoder = new TextEncoder();
  let last = "";
  let closed = false;

  const stream = new ReadableStream({
    async start(controller) {
      const send = (event, data) => {
        if (closed) return;
        controller.enqueue(
          encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`),
        );
      };

      const tick = async () => {
        try {
          const snap = await liveSnapshot(env);
          const serialized = JSON.stringify(snap);
          if (serialized !== last) {
            last = serialized;
            send("pool", snap);
          } else {
            send("ping", {
              updated_unix_ms: snap.updated_unix_ms,
              source: snap.source,
            });
          }
        } catch (e) {
          send("error", { message: String(e && e.message ? e.message : e) });
        }
      };

      await tick();
      const id = setInterval(tick, 2000);

      // Keep the stream open until the client disconnects.
      const abort = context.request.signal;
      if (abort) {
        abort.addEventListener("abort", () => {
          closed = true;
          clearInterval(id);
          try {
            controller.close();
          } catch {
            /* already closed */
          }
        });
      }
    },
    cancel() {
      closed = true;
    },
  });

  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream; charset=utf-8",
      "cache-control": "no-store, no-cache",
      connection: "keep-alive",
      "access-control-allow-origin": "*",
    },
  });
}

export async function onRequestOptions() {
  return new Response(null, {
    status: 204,
    headers: {
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET, OPTIONS",
      "access-control-allow-headers": "*",
    },
  });
}
