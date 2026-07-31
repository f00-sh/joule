/* joule download page — OS autodetect + latest GitHub release assets */
(function () {
  const REPO = "f00-sh/joule";
  const API = `https://api.github.com/repos/${REPO}/releases/latest`;

  function detect() {
    const ua = navigator.userAgent || "";
    const platform = navigator.platform || "";
    const uaData = navigator.userAgentData;

    let os = "unknown";
    let arch = "x86_64";

    if (uaData && uaData.platform) {
      const p = uaData.platform.toLowerCase();
      if (p.includes("win")) os = "windows";
      else if (p.includes("mac")) os = "darwin";
      else if (p.includes("linux")) os = "linux";
    } else {
      if (/windows|win32|win64/i.test(ua) || /win/i.test(platform)) os = "windows";
      else if (/mac|darwin|iphone|ipad/i.test(ua) || /mac/i.test(platform)) os = "darwin";
      else if (/linux|android/i.test(ua) || /linux/i.test(platform)) os = "linux";
    }

    // Arch: coarse — Apple Silicon often reports MacIntel historically; use UA hints.
    if (/aarch64|arm64|Apple Silicon/i.test(ua) || (os === "darwin" && !/Intel/i.test(ua) && /Mac OS X 1[1-9]|Mac OS X 1[0-9]_/i.test(ua))) {
      // Prefer arm on modern Macs when we cannot prove Intel.
      if (os === "darwin" && !/Intel Mac/i.test(ua)) arch = "aarch64";
    }
    if (/arm64|aarch64/i.test(ua) || /aarch64/i.test(platform)) arch = "aarch64";
    if (/x86_64|Win64|WOW64|Intel/i.test(ua) && os === "windows") arch = "x86_64";

    // Fine-tune Mac: navigator.userAgentData.getHighEntropyValues when available (async later).
    return { os, arch, key: `${os === "windows" ? "windows" : os}-${arch}` };
  }

  function labelFor(d) {
    if (d.os === "windows") return "Windows (x64)";
    if (d.os === "darwin" && d.arch === "aarch64") return "macOS (Apple Silicon)";
    if (d.os === "darwin") return "macOS (Intel)";
    if (d.os === "linux" && d.arch === "aarch64") return "Linux (ARM64)";
    if (d.os === "linux") return "Linux (x86_64)";
    return "Your system";
  }

  function installCommand(d) {
    if (d.os === "windows") {
      return {
        kind: "powershell",
        cmd: "irm https://github.com/f00-sh/joule/releases/latest/download/install.ps1 | iex",
      };
    }
    return {
      kind: "shell",
      cmd: "curl -fsSL https://github.com/f00-sh/joule/releases/latest/download/install.sh | sh",
    };
  }

  function assetMatchKey(name) {
    // joule-0.1.0-linux-x86_64.tar.gz
    const m = name.match(/joule-[^-]+-([a-z]+)-([a-z0-9_]+)\.(tar\.gz|zip)$/i);
    if (!m) return null;
    return `${m[1]}-${m[2]}`;
  }

  function setText(id, text) {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  }

  function highlightCard(key) {
    document.querySelectorAll(".platform-card").forEach((card) => {
      card.classList.toggle("is-detected", card.getAttribute("data-key") === key);
    });
  }

  async function refineMacArch(d) {
    if (d.os !== "darwin" || !navigator.userAgentData || !navigator.userAgentData.getHighEntropyValues) {
      return d;
    }
    try {
      const h = await navigator.userAgentData.getHighEntropyValues(["architecture"]);
      if (h.architecture === "arm") d.arch = "aarch64";
      if (h.architecture === "x86") d.arch = "x86_64";
      d.key = `darwin-${d.arch}`;
    } catch (_) {
      /* keep heuristic */
    }
    return d;
  }

  async function main() {
    let d = detect();
    d = await refineMacArch(d);

    setText("detect-line", "Recommended for this browser / OS");
    setText("detect-label", labelFor(d));
    highlightCard(d.key);

    const ic = installCommand(d);
    const cmdEl = document.getElementById("primary-cmd");
    const cta = document.getElementById("primary-cta");
    const copyBtn = document.getElementById("copy-cmd");

    if (cmdEl) {
      cmdEl.hidden = false;
      const code = cmdEl.querySelector("code");
      if (code) code.textContent = ic.cmd;
    }
    if (copyBtn) {
      copyBtn.hidden = false;
      copyBtn.addEventListener("click", async () => {
        try {
          await navigator.clipboard.writeText(ic.cmd);
          copyBtn.textContent = "Copied";
          setTimeout(() => {
            copyBtn.textContent = "Copy install command";
          }, 1500);
        } catch (_) {
          copyBtn.textContent = "Select & copy the command";
        }
      });
    }

    if (cta) {
      const a = document.createElement("a");
      a.className = "btn";
      a.href = "#primary-cmd";
      a.textContent =
        d.os === "windows" ? "Install with PowerShell" : "Install with one command";
      cta.appendChild(a);

      const all = document.createElement("a");
      all.className = "btn ghost";
      all.href = "#all-platforms";
      all.textContent = "Other platforms";
      cta.appendChild(all);
    }

    // Latest release metadata + wire asset links
    try {
      const res = await fetch(API, {
        headers: { Accept: "application/vnd.github+json" },
      });
      if (!res.ok) throw new Error(`GitHub API ${res.status}`);
      const rel = await res.json();
      const tag = rel.tag_name || "—";
      const when = rel.published_at
        ? new Date(rel.published_at).toLocaleDateString(undefined, {
            year: "numeric",
            month: "short",
            day: "numeric",
          })
        : "";
      setText(
        "release-meta",
        `Latest release: ${tag}${when ? " · " + when : ""} · ${rel.html_url || "https://github.com/" + REPO + "/releases"}`
      );

      const assets = rel.assets || [];
      document.querySelectorAll("a.asset-link").forEach((link) => {
        const want = link.getAttribute("data-match");
        const hit = assets.find((a) => assetMatchKey(a.name) === want);
        if (hit) {
          link.href = hit.browser_download_url;
          link.textContent = `Download ${hit.name}`;
        }
      });

      // Primary binary button when we know the asset
      const primaryKey =
        d.os === "windows"
          ? "windows-x86_64"
          : d.os === "darwin"
            ? `darwin-${d.arch}`
            : `linux-${d.arch}`;
      const primaryAsset = assets.find((a) => assetMatchKey(a.name) === primaryKey);
      if (primaryAsset && cta) {
        const dl = document.createElement("a");
        dl.className = "btn ghost";
        dl.href = primaryAsset.browser_download_url;
        dl.textContent = "Direct binary download";
        dl.rel = "noopener";
        cta.appendChild(dl);
      }
    } catch (e) {
      const err = document.getElementById("release-err");
      if (err) {
        err.hidden = false;
        err.textContent =
          "Could not load latest release from GitHub (network or no release yet). Use the install commands above once a tag is published.";
      }
      setText(
        "release-meta",
        "Latest release: not published yet — tag vX.Y.Z to cut the first automated build."
      );
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", main);
  } else {
    main();
  }
})();
