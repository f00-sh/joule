# joule packaging (f00-sh)

**Canonical binaries:** [GitHub Releases f00-sh/joule](https://github.com/f00-sh/joule/releases)  
**Easy UI:** [https://joule.f00.sh/download.html](https://joule.f00.sh/download.html)  
**Weights:** never on f00 — peers + official sources + sha256 only.

## Dummy-easy install (users)

| Platform | Command |
|----------|---------|
| Linux / macOS | `curl -fsSL https://github.com/f00-sh/joule/releases/latest/download/install.sh \| sh` |
| Windows | `irm https://github.com/f00-sh/joule/releases/latest/download/install.ps1 \| iex` |
| Arch (f00 PKGBUILD) | `git clone https://github.com/f00-sh/aur-joule-bin.git && cd aur-joule-bin && makepkg -si` |
| Homebrew | `brew install f00-sh/tap/joule` |

## Live package sources (f00 org)

| Channel | URL |
|---------|-----|
| Releases | https://github.com/f00-sh/joule/releases |
| Homebrew | https://github.com/f00-sh/homebrew-tap/blob/main/Formula/joule.rb |
| AUR-style PKGBUILD | https://github.com/f00-sh/aur-joule-bin |

Digests are pinned from each release `SHA256SUMS`.

## Maintainer checklist (each SemVer tag)

1. `git tag vX.Y.Z && git push origin vX.Y.Z`
2. Wait for **release** workflow green
3. Update `f00-sh/homebrew-tap` Formula/joule.rb version + sha256
4. Update `f00-sh/aur-joule-bin` PKGBUILD pkgver + sha256sums
5. Site download page reads **latest** from the GitHub API automatically

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
