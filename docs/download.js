/* joule download — OS detect; permanent /current + GitHub latest URLs (no version in path). */
(function () {
  const REPO = "f00-sh/joule";
  const API = `https://api.github.com/repos/${REPO}/releases/latest`;
  /** Site permanent base (Cloudflare redirects → GitHub latest stable names). */
  const CUR = "/current";
  /** GitHub permanent base. */
  const GH = `https://github.com/${REPO}/releases/latest/download`;

  /** Stable asset map: GUI first, then CLI. */
  const STABLE = {
    "windows-x86_64": {
      gui: { href: `${CUR}/windows/setup.exe`, label: "Download Windows Setup (.exe)", gh: `${GH}/joule-windows-x86_64-setup.exe` },
      cli: { href: `${CUR}/windows/portable.zip`, label: "Portable ZIP", gh: `${GH}/joule-windows-x86_64.zip` },
      oneliner: "irm https://joule.f00.sh/current/install.ps1 | iex",
    },
    "darwin-aarch64": {
      gui: { href: `${CUR}/macos/arm64.pkg`, label: "Download macOS Installer (.pkg)", gh: `${GH}/joule-darwin-aarch64.pkg` },
      gui2: { href: `${CUR}/macos/arm64.dmg`, label: "Disk Image (.dmg)", gh: `${GH}/joule-darwin-aarch64.dmg` },
      cli: { href: `${CUR}/macos/arm64.tar.gz`, label: "CLI tarball", gh: `${GH}/joule-darwin-aarch64.tar.gz` },
      oneliner: "curl -fsSL https://joule.f00.sh/current/install.sh | sh",
    },
    "darwin-x86_64": {
      gui: { href: `${CUR}/macos/intel.pkg`, label: "Download macOS Installer (.pkg)", gh: `${GH}/joule-darwin-x86_64.pkg` },
      gui2: { href: `${CUR}/macos/intel.dmg`, label: "Disk Image (.dmg)", gh: `${GH}/joule-darwin-x86_64.dmg` },
      cli: { href: `${CUR}/macos/intel.tar.gz`, label: "CLI tarball", gh: `${GH}/joule-darwin-x86_64.tar.gz` },
      oneliner: "curl -fsSL https://joule.f00.sh/current/install.sh | sh",
    },
    "linux-x86_64": {
      gui: { href: `${CUR}/linux/amd64.deb`, label: "Download Linux package (.deb)", gh: `${GH}/joule-linux-x86_64.deb` },
      cli: { href: `${CUR}/linux/amd64.tar.gz`, label: "CLI tarball", gh: `${GH}/joule-linux-x86_64.tar.gz` },
      oneliner: "curl -fsSL https://joule.f00.sh/current/install.sh | sh",
    },
    "linux-aarch64": {
      gui: { href: `${CUR}/linux/arm64.deb`, label: "Download Linux package (.deb)", gh: `${GH}/joule-linux-aarch64.deb` },
      cli: { href: `${CUR}/linux/arm64.tar.gz`, label: "CLI tarball", gh: `${GH}/joule-linux-aarch64.tar.gz` },
      oneliner: "curl -fsSL https://joule.f00.sh/current/install.sh | sh",
    },
  };

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

    if (
      /aarch64|arm64|Apple Silicon/i.test(ua) ||
      (os === "darwin" && !/Intel/i.test(ua) && /Mac OS X 1[1-9]|Mac OS X 1[0-9]_/i.test(ua))
    ) {
      if (os === "darwin" && !/Intel Mac/i.test(ua)) arch = "aarch64";
    }
    if (/arm64|aarch64/i.test(ua) || /aarch64/i.test(platform)) arch = "aarch64";
    if (/x86_64|Win64|WOW64|Intel/i.test(ua) && os === "windows") arch = "x86_64";

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

  function btn(href, label, primary) {
    const a = document.createElement("a");
    a.className = primary ? "btn" : "btn ghost";
    a.href = href;
    a.rel = "noopener";
    a.textContent = label;
    return a;
  }

  async function main() {
    let d = detect();
    d = await refineMacArch(d);

    setText("detect-line", "Permanent GUI installer for this OS (always newest)");
    setText("detect-label", labelFor(d));
    highlightCard(d.key);

    const pack = STABLE[d.key] || STABLE["linux-x86_64"];
    const cta = document.getElementById("primary-cta");
    const cmdEl = document.getElementById("primary-cmd");
    const copyBtn = document.getElementById("copy-cmd");

    if (cta) {
      cta.innerHTML = "";
      if (pack.gui) cta.appendChild(btn(pack.gui.href, pack.gui.label, true));
      if (pack.gui2) cta.appendChild(btn(pack.gui2.href, pack.gui2.label, false));
      if (pack.cli) cta.appendChild(btn(pack.cli.href, pack.cli.label, false));
      const all = document.createElement("a");
      all.className = "btn ghost";
      all.href = "#all-platforms";
      all.textContent = "All platforms";
      cta.appendChild(all);
      const cur = document.createElement("a");
      cur.className = "btn ghost";
      cur.href = "./current/";
      cur.textContent = "/current map";
      cta.appendChild(cur);
    }

    if (cmdEl && pack.oneliner) {
      cmdEl.hidden = false;
      const code = cmdEl.querySelector("code");
      if (code) code.textContent = pack.oneliner;
    }
    if (copyBtn && pack.oneliner) {
      copyBtn.hidden = false;
      copyBtn.addEventListener("click", async () => {
        try {
          await navigator.clipboard.writeText(pack.oneliner);
          copyBtn.textContent = "Copied";
          setTimeout(() => {
            copyBtn.textContent = "Copy CLI install command";
          }, 1500);
        } catch (_) {
          copyBtn.textContent = "Select & copy the command";
        }
      });
    }

    // Metadata only — links do not depend on API success
    try {
      const res = await fetch(API, { headers: { Accept: "application/vnd.github+json" } });
      if (!res.ok) throw new Error(String(res.status));
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
        `Newest SemVer currently: ${tag}${when ? " · " + when : ""} · permanent paths always track latest · ${rel.html_url || ""}`
      );
    } catch (_) {
      setText(
        "release-meta",
        "Permanent /current and GitHub latest/download links stay valid; could not query tag name (network)."
      );
      const err = document.getElementById("release-err");
      if (err) {
        err.hidden = false;
        err.textContent = "Tag metadata unavailable; download buttons still use permanent URLs.";
      }
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", main);
  } else {
    main();
  }
})();
