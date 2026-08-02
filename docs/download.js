/* joule download page — OS autodetect + native installers from GitHub Releases */
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

  /** Prefer native GUI installers over CLI archives. */
  function pickNativeInstaller(assets, d) {
    const names = assets.map((a) => a.name);
    const find = (re) => assets.find((a) => re.test(a.name));

    if (d.os === "windows") {
      return (
        find(new RegExp(`joule-.*-windows-${d.arch}-setup\\.exe$`, "i")) ||
        find(/joule-.*-windows-.*-setup\.exe$/i)
      );
    }
    if (d.os === "darwin") {
      return (
        find(new RegExp(`joule-.*-darwin-${d.arch}\\.pkg$`, "i")) ||
        find(new RegExp(`joule-.*-darwin-${d.arch}\\.dmg$`, "i")) ||
        find(new RegExp(`joule-.*-darwin-${d.arch}-app\\.zip$`, "i"))
      );
    }
    if (d.os === "linux") {
      return find(new RegExp(`joule-.*-linux-${d.arch}\\.deb$`, "i"));
    }
    return null;
  }

  function pickCliArchive(assets, d) {
    const keyOs = d.os === "windows" ? "windows" : d.os;
    const ext = d.os === "windows" ? "zip" : "tar\\.gz";
    const re = new RegExp(`joule-[^-]+-${keyOs}-${d.arch}\\.${ext}$`, "i");
    return assets.find((a) => re.test(a.name));
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

  function wireAssetLinks(assets) {
    document.querySelectorAll("a.asset-link").forEach((link) => {
      const match = link.getAttribute("data-match");
      if (!match) return;
      // data-match can be exact suffix or regex-ish token
      const hit = assets.find((a) => {
        if (a.name === match) return true;
        if (a.name.includes(match)) return true;
        return false;
      });
      if (hit) {
        link.href = hit.browser_download_url;
        if (!link.dataset.keepLabel) {
          link.textContent = hit.name;
        }
      }
    });
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
            copyBtn.textContent = "Copy CLI install command";
          }, 1500);
        } catch (_) {
          copyBtn.textContent = "Select & copy the command";
        }
      });
    }

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
      wireAssetLinks(assets);

      const native = pickNativeInstaller(assets, d);
      const cli = pickCliArchive(assets, d);

      if (cta) {
        cta.innerHTML = "";
        if (native) {
          const dl = document.createElement("a");
          dl.className = "btn";
          dl.href = native.browser_download_url;
          dl.rel = "noopener";
          if (d.os === "windows") dl.textContent = "Download Windows Setup (.exe)";
          else if (d.os === "darwin") {
            if (/\.pkg$/i.test(native.name)) dl.textContent = "Download macOS Installer (.pkg)";
            else if (/\.dmg$/i.test(native.name)) dl.textContent = "Download macOS Disk Image (.dmg)";
            else dl.textContent = "Download joule.app";
          } else dl.textContent = "Download Linux package (.deb)";
          cta.appendChild(dl);
        }
        if (cli) {
          const a = document.createElement("a");
          a.className = "btn ghost";
          a.href = cli.browser_download_url;
          a.rel = "noopener";
          a.textContent = d.os === "windows" ? "ZIP (portable)" : "CLI tarball";
          cta.appendChild(a);
        }
        const all = document.createElement("a");
        all.className = "btn ghost";
        all.href = "#all-platforms";
        all.textContent = "All platforms";
        cta.appendChild(all);
      }

      if (!native && cmdEl) {
        // Fall back to CLI one-liner as primary when no native asset yet
        setText("detect-line", "CLI install (native installer not in this release yet)");
      } else if (native) {
        setText("detect-line", "Native installer for this OS — real program, double-click to install");
      }
    } catch (e) {
      const err = document.getElementById("release-err");
      if (err) {
        err.hidden = false;
        err.textContent =
          "Could not load latest release from GitHub. Use the install commands once a tag is published.";
      }
      setText(
        "release-meta",
        "Latest release: not published yet — tag vX.Y.Z to cut automated builds."
      );
      if (cta) {
        const a = document.createElement("a");
        a.className = "btn";
        a.href = "#primary-cmd";
        a.textContent =
          d.os === "windows" ? "CLI: PowerShell install" : "CLI: one-command install";
        cta.appendChild(a);
      }
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", main);
  } else {
    main();
  }
})();
