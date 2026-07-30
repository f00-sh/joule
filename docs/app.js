/* Multi-source live cluster feed — edge is one mirror, not authority. */
(function () {
  const params = new URLSearchParams(location.search);
  const overrideApi = params.get("api");
  const sourcesParam = params.get("sources"); // optional URL to alternate sources.json

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
    const s = Math.max(0, Math.floor((Date.now() - Number(unixMs)) / 1000));
    if (s < 3) return "just now";
    if (s < 60) return s + "s ago";
    if (s < 3600) return Math.floor(s / 60) + "m ago";
    return Math.floor(s / 3600) + "h ago";
  }

  function applySnapshot(snap, meta) {
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
    setText("stat-mode", ready.inference_mode || ld.inference_mode || "—");
    setText("stat-throughput", String(cap.throughput_class_sum ?? "—"));

    const cd =
      ready.countdown_label || ld.readiness_message || ready.message || "—";
    setText("countdown", "countdown: " + cd);

    const liveBits = [
      backends + " backends",
      ready.model || ld.model || "kimi-open",
      ready.can_load_model ? "can load" : null,
      ready.can_begin_service ? "can serve" : null,
      ready.service_live ? "SERVICE LIVE" : null,
      snap.source ? "src:" + snap.source : null,
      meta && meta.via ? "via:" + meta.via : null,
      meta && meta.signed ? "signed" : null,
      meta && meta.sources_ok != null
        ? meta.sources_ok + "/" + meta.sources_total + " feeds"
        : null,
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
          "<div class='muted'>Milestones appear once a control publishes readiness.</div>";
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
          "<td colspan='6' class='muted'>No donors in this snapshot — run <code>joule agent</code>.</td>";
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
    if (dot) {
      dot.classList.remove("on", "stale");
      if (meta && meta.mode === "live") dot.classList.add("on");
      else if (meta && meta.mode === "stale") dot.classList.add("stale");
    }
    if (status) {
      status.textContent =
        meta && meta.mode === "live"
          ? "LIVE"
          : meta && meta.mode === "stale"
            ? "STALE"
            : "…";
    }
    setText("live-age", ageLabel(snap.updated_unix_ms));

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

  function hexToBytes(hex) {
    const clean = (hex || "").replace(/^0x/, "");
    if (clean.length % 2) return null;
    const out = new Uint8Array(clean.length / 2);
    for (let i = 0; i < out.length; i++) {
      out[i] = parseInt(clean.substr(i * 2, 2), 16);
    }
    return out;
  }

  async function sha256(bytes) {
    const dig = await crypto.subtle.digest("SHA-256", bytes);
    return new Uint8Array(dig);
  }

  async function verifySignature(snap) {
    const sig = snap && snap.signature;
    if (!sig || !sig.signature_hex || !sig.verifying_key_hex || !sig.body_json) {
      return { ok: false, reason: "unsigned" };
    }
    try {
      const poolId = snap.pool_id || "";
      const ms = String(snap.updated_unix_ms || "");
      const enc = new TextEncoder();
      const preRaw = new Uint8Array([
        ...enc.encode(poolId),
        10,
        ...enc.encode(ms),
        10,
        ...enc.encode(sig.body_json),
      ]);
      const pre = await sha256(preRaw);
      // Dynamic import noble when possible
      const noble = await import(
        "https://esm.sh/@noble/ed25519@2.1.0?bundle"
      ).catch(() => null);
      if (!noble || !noble.verifyAsync) {
        return { ok: false, reason: "no_verifier" };
      }
      // noble v2 may need sha512 sync — set if provided
      if (noble.etc && !noble.etc.sha512Sync && window.crypto) {
        // best-effort: leave default
      }
      const sigB = hexToBytes(sig.signature_hex);
      const pubB = hexToBytes(sig.verifying_key_hex);
      if (!sigB || !pubB) return { ok: false, reason: "bad_hex" };
      const good = await noble.verifyAsync(sigB, pre, pubB);
      return { ok: !!good, reason: good ? "valid" : "invalid" };
    } catch (e) {
      return { ok: false, reason: String(e.message || e) };
    }
  }

  async function fetchOne(src) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), 4000);
    try {
      const r = await fetch(src.url, { cache: "no-store", signal: ctrl.signal });
      if (!r.ok) throw new Error("HTTP " + r.status);
      const snap = await r.json();
      // Normalize readiness-only override
      if (!snap.capacity && snap.pool_vram_mib != null) {
        return {
          snap: {
            source: "readiness-only",
            updated_unix_ms: Date.now(),
            capacity: {
              mem_mib_healthy: snap.pool_vram_mib,
              nodes_healthy: snap.backends,
              nodes_total: snap.backends,
            },
            readiness: snap,
            nodes: [],
          },
          src,
          signed: false,
        };
      }
      const v = await verifySignature(snap);
      return {
        snap,
        src,
        signed: v.ok,
        verify: v.reason,
      };
    } catch (e) {
      return { error: String(e.message || e), src };
    } finally {
      clearTimeout(t);
    }
  }

  function score(entry, maxAgeSecs) {
    if (!entry || !entry.snap) return -1e18;
    const age =
      (Date.now() - Number(entry.snap.updated_unix_ms || 0)) / 1000;
    if (maxAgeSecs && age > maxAgeSecs) return -1e12 + -age;
    let s = -age;
    if (entry.signed) s += 1e6;
    const cap = entry.snap.capacity || {};
    s += Number(cap.nodes_healthy || 0) * 10;
    s += Number(cap.mem_mib_healthy || 0) / 1024;
    return s;
  }

  async function loadSourcesConfig() {
    const url = sourcesParam || "sources.json";
    try {
      const r = await fetch(url, { cache: "no-store" });
      if (!r.ok) throw new Error("sources " + r.status);
      return await r.json();
    } catch {
      return {
        sources: [{ id: "f00-edge", url: "/api/pool", kind: "mirror" }],
        trust: { max_age_secs: 180 },
      };
    }
  }

  /** Public directory: anyone announced via signed POST /api/announce. */
  async function loadDirectory() {
    try {
      const r = await fetch("/api/sources", { cache: "no-store" });
      if (!r.ok) return [];
      const d = await r.json();
      return Array.isArray(d.sources) ? d.sources : [];
    } catch {
      return [];
    }
  }

  async function refresh() {
    let sources = [];
    let trust = { max_age_secs: 180 };

    if (overrideApi) {
      const root = overrideApi.replace(/\/$/, "");
      sources = [
        {
          id: "override",
          url: root + "/v1/public/snapshot",
          kind: "control",
        },
        {
          id: "override-ready",
          url: root + "/v1/models/readiness",
          kind: "control",
          optional: true,
        },
      ];
    } else {
      const cfg = await loadSourcesConfig();
      sources = cfg.sources || [];
      trust = cfg.trust || trust;
      // Decentralized directory (auto-announced controls)
      const dir = await loadDirectory();
      dir.forEach((s) => {
        if (!s.url) return;
        if (!sources.some((x) => x.url === s.url)) {
          sources.push({
            id: s.id || s.pool_id || s.url,
            url: s.url,
            kind: "control",
          });
        }
      });
      // Aggregating edge mirror (itself multi-sources the directory)
      if (!sources.some((s) => s.url === "/api/pool" || s.id === "f00-edge")) {
        sources.unshift({ id: "f00-edge", url: "/api/pool", kind: "mirror" });
      }
    }

    const results = await Promise.all(sources.map(fetchOne));
    const ok = results.filter((r) => r.snap);
    const maxAge = trust.max_age_secs || 180;
    ok.sort((a, b) => score(b, maxAge) - score(a, maxAge));

    if (!ok.length) {
      showError(
        "No pool feeds reachable. Run a control (signed /v1/public/snapshot) or wait for a mirror.",
      );
      return false;
    }

    const best = ok[0];
    const age =
      (Date.now() - Number(best.snap.updated_unix_ms || Date.now())) / 1000;
    applySnapshot(best.snap, {
      mode: age <= maxAge ? "live" : "stale",
      via: best.src.id,
      signed: best.signed,
      sources_ok: ok.length,
      sources_total: sources.length,
    });
    setText(
      "live-hint",
      "multi-source · " +
        ok.length +
        "/" +
        sources.length +
        " ok · " +
        (best.signed ? "sig✓" : "unsigned") +
        " · " +
        best.src.id,
    );
    return true;
  }

  // SSE only for same-origin edge (one of the sources); multi-source poll is the SoT.
  let es = null;
  function startSse() {
    if (overrideApi || typeof EventSource === "undefined") return;
    try {
      es = new EventSource("/api/pool/stream");
      es.addEventListener("pool", () => {
        refresh();
      });
      es.addEventListener("ping", () => {
        /* age ticker only */
      });
    } catch {
      /* ignore */
    }
  }

  refresh().then(() => {
    startSse();
    setInterval(refresh, 3000);
  });
})();
