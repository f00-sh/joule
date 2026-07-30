/* Live cluster feed for joule.f00.sh — SSE + poll fallback. */
(function () {
  const params = new URLSearchParams(location.search);
  // Prefer same-origin edge API; allow override for local control labs.
  const override = params.get("api");
  const POOL_JSON = override
    ? override.replace(/\/$/, "") + "/v1/models/readiness"
    : "/api/pool";
  const POOL_SSE = override ? null : "/api/pool/stream";
  const CAP_JSON = override
    ? override.replace(/\/$/, "") + "/v1/cluster/capacity"
    : null;
  const NODES_JSON = override
    ? override.replace(/\/$/, "") + "/v1/cluster/nodes"
    : null;

  const $ = (id) => document.getElementById(id);

  function fmtGib(mib) {
    if (mib == null || Number.isNaN(Number(mib))) return "—";
    const n = Number(mib);
    if (n >= 1024) return (n / 1024).toFixed(1) + " GiB";
    return n + " MiB";
  }

  function setText(id, v) {
    const el = $(id);
    if (el) el.textContent = v;
  }

  function ageLabel(unixMs) {
    if (!unixMs) return "—";
    const s = Math.max(0, Math.floor((Date.now() - unixMs) / 1000));
    if (s < 3) return "just now";
    if (s < 60) return s + "s ago";
    if (s < 3600) return Math.floor(s / 60) + "m ago";
    return Math.floor(s / 3600) + "h ago";
  }

  function applySnapshot(snap, mode) {
    if (!snap) return;
    const cap = snap.capacity || snap;
    const ready = snap.readiness || {};
    const sched = snap.scheduler || {};
    const nodes = snap.nodes || [];
    const ld = cap.logical_device || {};

    const vram = cap.mem_mib_healthy ?? ready.pool_vram_mib ?? ld.vram_mib ?? 0;
    const backends = cap.nodes_healthy ?? ready.backends ?? ld.backends ?? 0;
    const totalNodes = cap.nodes_total ?? backends;
    const slotsFree =
      sched.stream_slots_free ??
      Math.max(0, (cap.stream_slots_total || 0) - (cap.stream_slots_used || 0));
    const slotsUsed = sched.stream_slots_used ?? cap.stream_slots_used ?? 0;
    const slotsTotal = sched.stream_slots_total ?? cap.stream_slots_total ?? 0;

    setText("vram", fmtGib(vram));
    setText("stat-vram", fmtGib(vram));
    setText("stat-backends", String(backends));
    setText("stat-nodes", totalNodes + " / " + backends + " healthy");
    setText(
      "stat-streams",
      slotsTotal ? slotsUsed + " / " + slotsTotal + " used" : String(slotsFree),
    );
    setText(
      "stat-mode",
      ready.inference_mode || ld.inference_mode || "—",
    );
    setText(
      "stat-throughput",
      String(cap.throughput_class_sum ?? "—"),
    );

    const cd =
      ready.countdown_label ||
      ld.readiness_message ||
      ready.message ||
      "—";
    setText("countdown", "countdown: " + cd);

    const liveBits = [
      backends + " backends",
      (ready.model || ld.model || "kimi-open"),
      ready.can_load_model ? "can load" : null,
      ready.can_begin_service ? "can serve" : null,
      ready.service_live ? "SERVICE LIVE" : null,
      snap.source ? "src:" + snap.source : null,
    ]
      .filter(Boolean)
      .join(" · ");
    setText("pool-meta", liveBits);

    const list = $("milestones");
    if (list) {
      list.innerHTML = "";
      const milestones = ready.milestones || [];
      if (!milestones.length) {
        const li = document.createElement("li");
        li.innerHTML =
          "<div class='muted'>Milestones appear once a control plane publishes readiness.</div>";
        list.appendChild(li);
      }
      milestones.forEach((m) => {
        const li = document.createElement("li");
        if (m.reached) li.classList.add("done");
        if (ready.next_milestone && ready.next_milestone.id === m.id) {
          li.classList.add("next");
        }
        li.innerHTML =
          "<div><strong>" +
          (m.title || m.id) +
          "</strong> " +
          (m.reached ? "<span class='ok'>reached</span>" : "") +
          "</div>" +
          "<div class='muted'>" +
          (m.description || "") +
          "</div>" +
          '<div class="bar"><i style="width:' +
          (m.progress_pct || 0) +
          '%"></i></div>';
        list.appendChild(li);
      });
    }

    const tbody = $("nodes-body");
    if (tbody) {
      tbody.innerHTML = "";
      if (!nodes.length) {
        const tr = document.createElement("tr");
        tr.innerHTML =
          "<td colspan='6' class='muted'>No donors online yet — run <code>joule agent</code>.</td>";
        tbody.appendChild(tr);
      } else {
        nodes.forEach((n) => {
          const tr = document.createElement("tr");
          tr.innerHTML =
            "<td>" +
            (n.account || "—") +
            "</td><td>" +
            (n.device || "—") +
            "</td><td>" +
            fmtGib(n.mem_mib) +
            "</td><td>" +
            (n.compute_state || (n.healthy ? "ok" : "down")) +
            "</td><td>" +
            (n.inflight ?? "—") +
            "/" +
            (n.max_slots ?? "—") +
            "</td><td>" +
            (n.banned ? "banned" : n.healthy ? "healthy" : "unhealthy") +
            "</td>";
          tbody.appendChild(tr);
        });
      }
    }

    const dot = $("live-dot");
    const status = $("live-status");
    const age = $("live-age");
    if (dot) {
      dot.classList.remove("on", "stale");
      if (mode === "live") dot.classList.add("on");
      else if (mode === "stale") dot.classList.add("stale");
    }
    if (status) {
      status.textContent =
        mode === "live"
          ? "LIVE"
          : mode === "stale"
            ? "STALE"
            : mode === "offline"
              ? "OFFLINE"
              : "…";
    }
    if (age) age.textContent = ageLabel(snap.updated_unix_ms);

    const err = $("pool-err");
    if (err) {
      err.hidden = true;
      err.textContent = "";
    }
  }

  function showError(msg) {
    const err = $("pool-err");
    if (err) {
      err.hidden = false;
      err.textContent = msg;
    }
    const dot = $("live-dot");
    if (dot) {
      dot.classList.remove("on");
      dot.classList.add("stale");
    }
    setText("live-status", "OFFLINE");
  }

  async function fetchOverrideBundle() {
    const root = override.replace(/\/$/, "");
    const [ready, cap, nodes] = await Promise.all([
      fetch(root + "/v1/models/readiness").then((r) => r.json()),
      fetch(root + "/v1/cluster/capacity").then((r) => r.json()),
      fetch(root + "/v1/cluster/nodes")
        .then((r) => r.json())
        .catch(() => ({ nodes: [] })),
    ]);
    return {
      ok: true,
      source: "override",
      updated_unix_ms: Date.now(),
      capacity: cap,
      readiness: ready,
      nodes: nodes.nodes || [],
      scheduler: {
        stream_slots_total: cap.stream_slots_total,
        stream_slots_used: cap.stream_slots_used,
        stream_slots_free: Math.max(
          0,
          (cap.stream_slots_total || 0) - (cap.stream_slots_used || 0),
        ),
      },
    };
  }

  async function pollOnce() {
    try {
      let snap;
      if (override) {
        snap = await fetchOverrideBundle();
      } else {
        const r = await fetch(POOL_JSON, { cache: "no-store" });
        if (!r.ok) throw new Error("HTTP " + r.status);
        snap = await r.json();
      }
      applySnapshot(snap, "live");
      return true;
    } catch (e) {
      showError(String(e.message || e));
      return false;
    }
  }

  let es = null;
  function startSse() {
    if (!POOL_SSE || typeof EventSource === "undefined") return false;
    try {
      es = new EventSource(POOL_SSE);
    } catch {
      return false;
    }
    es.addEventListener("pool", (ev) => {
      try {
        applySnapshot(JSON.parse(ev.data), "live");
      } catch (e) {
        showError("bad sse payload");
      }
    });
    es.addEventListener("ping", (ev) => {
      try {
        const p = JSON.parse(ev.data);
        setText("live-age", ageLabel(p.updated_unix_ms));
        const dot = $("live-dot");
        if (dot) {
          dot.classList.add("on");
          dot.classList.remove("stale");
        }
        setText("live-status", "LIVE");
      } catch {
        /* ignore */
      }
    });
    es.onerror = () => {
      // Fall back to polling; browser will also retry EventSource.
      setText("live-status", "RECONNECT");
    };
    return true;
  }

  // Boot
  pollOnce().then(() => {
    const sseOk = startSse();
    // Poll as safety net (SSE pings are enough when healthy).
    setInterval(pollOnce, sseOk ? 8000 : 2500);
  });

  // Age ticker
  setInterval(() => {
    const el = $("live-age");
    if (!el || !el.dataset.ms) return;
  }, 1000);
})();
