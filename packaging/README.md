# joule packaging (f00-sh)

**Canonical binaries + installers:** [GitHub Releases f00-sh/joule](https://github.com/f00-sh/joule/releases)  
**Download UI:** [https://joule.f00.sh/download.html](https://joule.f00.sh/download.html)  
**Weights:** never on f00 — peers + official sources + sha256 only.

## Native GUI OS installers (primary)

| Platform | Artifact | What users get |
|----------|----------|----------------|
| **Windows** | `joule-{ver}-windows-x86_64-setup.exe` | Inno Setup wizard → `joule.exe` under Program Files, Start Menu, optional PATH |
| **macOS** | `joule-{ver}-darwin-{arch}.pkg` | Installer.app puts **joule.app** in `/Applications` + `/usr/local/bin/joule` |
| **macOS** | `joule-{ver}-darwin-{arch}.dmg` | Drag **joule.app** → Applications |
| **macOS** | `joule-{ver}-darwin-{arch}-app.zip` | Standalone `.app` bundle |
| **Linux** | `joule-{ver}-linux-{arch}.deb` | `dpkg -i` → `/usr/bin/joule` + `.desktop` launcher |

Built by `.github/workflows/release.yml` on every `v*` tag from:

- `packaging/windows/joule.iss` + `build-native.ps1`
- `packaging/macos/build-native.sh` + `Info.plist.in`
- `packaging/linux/build-deb.sh`

## CLI installers (still supported)

| Platform | Command |
|----------|---------|
| Linux / macOS | `curl -fsSL …/install.sh \| sh` |
| Windows | `irm …/install.ps1 \| iex` (prefers Setup.exe when present) |
| Arch (f00 PKGBUILD) | `git clone https://github.com/f00-sh/aur-joule-bin.git && makepkg -si` |
| Homebrew | `brew install f00-sh/tap/joule` |

## Signing reality

- **Windows:** Setup.exe is **unsigned** until an Authenticode cert exists (SmartScreen may warn).
- **macOS:** ad-hoc `codesign`; **not notarized** until Apple Developer cert (Gatekeeper: right-click → Open).
- Still real native installers — not “just a zip.”

## Maintainer checklist (each SemVer tag)

1. `git tag vX.Y.Z && git push origin vX.Y.Z`
2. Wait for **release** workflow green (binaries + setup/pkg/dmg/deb)
3. Pin `f00-sh/homebrew-tap` and `f00-sh/aur-joule-bin` digests
4. Download page picks native installers from the GitHub API automatically

## Asset naming

```text
joule-{ver}-windows-x86_64-setup.exe
joule-{ver}-darwin-{arch}.pkg
joule-{ver}-darwin-{arch}.dmg
joule-{ver}-darwin-{arch}-app.zip
joule-{ver}-linux-{arch}.deb
joule-{ver}-linux-{arch}.tar.gz
joule-{ver}-darwin-{arch}.tar.gz
joule-{ver}-windows-{arch}.zip
install.sh  install.ps1  SHA256SUMS  file_id.diz
```
