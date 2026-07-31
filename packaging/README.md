# joule packaging

**Canonical binaries:** [GitHub Releases](https://github.com/f00-sh/joule/releases)  
**Easy UI:** [https://joule.f00.sh/download.html](https://joule.f00.sh/download.html) (autodetect + all platforms)  
**Weights:** never on f00 — peers + official sources + sha256 only.

## Dummy-easy install (users)

| Platform | Command |
|----------|---------|
| Linux / macOS | `curl -fsSL https://github.com/f00-sh/joule/releases/latest/download/install.sh \| sh` |
| Windows | `irm https://github.com/f00-sh/joule/releases/latest/download/install.ps1 \| iex` |
| Arch | `yay -S joule-bin` (after AUR publish) |
| macOS brew | `brew install f00-sh/tap/joule` (after tap publish) |

## Maintainer checklist (each SemVer tag)

1. `git tag vX.Y.Z && git push origin vX.Y.Z`
2. GitHub Actions **release** workflow builds all targets and uploads assets.
3. Copy sha256 from `SHA256SUMS` into:
   - `packaging/aur/PKGBUILD` (`pkgver` + `sha256sums_*`)
   - `packaging/homebrew/joule.rb` (`version` + `sha256`)
4. Push AUR / homebrew-tap repos if separate.
5. Site download page reads **latest** from the GitHub API automatically.

## Asset naming

```text
joule-{ver}-linux-x86_64.tar.gz
joule-{ver}-linux-aarch64.tar.gz
joule-{ver}-darwin-x86_64.tar.gz
joule-{ver}-darwin-aarch64.tar.gz
joule-{ver}-windows-x86_64.zip
install.sh
install.ps1
SHA256SUMS
```

## Windows “installer”

- **Primary:** PowerShell one-liner (`install.ps1`) — no admin required, user-local install.
- **Portable:** unzip `*-windows-x86_64.zip` and run `joule.exe`.
- **Future:** signed Inno/MSI when code-signing cert is available; until then PS1 is the supported path.

## AUR / Homebrew

Templates live in this directory. They are not auto-published (AUR/Homebrew need separate repos/maintainers). End users on those ecosystems get one-command install once published.
