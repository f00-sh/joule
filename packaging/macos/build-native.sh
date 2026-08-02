#!/usr/bin/env bash
# Build real macOS installers from a release binary:
#   joule.app  +  .pkg  +  .dmg
#
# Usage:
#   packaging/macos/build-native.sh \
#     --bin target/aarch64-apple-darwin/release/joule \
#     --version 0.1.8 \
#     --arch aarch64 \
#     --out dist
#
# Requires: macOS host with pkgbuild + hdiutil (GitHub macos runners).
set -euo pipefail

BIN=""
VERSION=""
ARCH=""
OUT="dist"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PKG_ID="sh.f00.joule"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bin) BIN="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    --arch) ARCH="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

[[ -n "$BIN" && -f "$BIN" ]] || { echo "error: --bin path required" >&2; exit 1; }
[[ -n "$VERSION" ]] || { echo "error: --version required" >&2; exit 1; }
[[ -n "$ARCH" ]] || { echo "error: --arch required (aarch64|x86_64)" >&2; exit 1; }

mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/joule-macos-XXXXXX")"
cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

APP="$WORKDIR/joule.app"
CONTENTS="$APP/Contents"
MACOS="$CONTENTS/MacOS"
RES="$CONTENTS/Resources"
mkdir -p "$MACOS" "$RES"

cp "$BIN" "$MACOS/joule"
chmod 755 "$MACOS/joule"
# Keep a CLI-friendly name symlink inside the app for power users (optional).
# Finder double-click runs MacOS/joule → GUI default.

sed "s/@VERSION@/${VERSION}/g" "$ROOT/packaging/macos/Info.plist.in" > "$CONTENTS/Info.plist"
printf 'APPL????' > "$CONTENTS/PkgInfo"

# Optional icon later — ship without icns if none present.
if [[ -f "$ROOT/packaging/macos/AppIcon.icns" ]]; then
  cp "$ROOT/packaging/macos/AppIcon.icns" "$RES/AppIcon.icns"
  /usr/libexec/PlistBuddy -c 'Add :CFBundleIconFile string AppIcon' "$CONTENTS/Info.plist" 2>/dev/null \
    || /usr/libexec/PlistBuddy -c 'Set :CFBundleIconFile AppIcon' "$CONTENTS/Info.plist" 2>/dev/null \
    || true
fi

# Ad-hoc codesign so Gatekeeper has *something* (not notarized — no Apple cert).
if command -v codesign >/dev/null 2>&1; then
  codesign --force --deep --sign - "$APP" 2>/dev/null || true
fi

# --- .pkg (native installer that puts joule.app in /Applications) ---
PKG_ROOT="$WORKDIR/pkgroot"
mkdir -p "$PKG_ROOT/Applications"
cp -R "$APP" "$PKG_ROOT/Applications/joule.app"
# Also drop CLI wrapper on PATH via /usr/local/bin (common Mac convention).
mkdir -p "$PKG_ROOT/usr/local/bin"
cat > "$PKG_ROOT/usr/local/bin/joule" <<'WRAP'
#!/bin/bash
# Prefer Applications bundle (GUI + CLI subcommands).
if [[ -x "/Applications/joule.app/Contents/MacOS/joule" ]]; then
  exec "/Applications/joule.app/Contents/MacOS/joule" "$@"
fi
if [[ -x "$HOME/Applications/joule.app/Contents/MacOS/joule" ]]; then
  exec "$HOME/Applications/joule.app/Contents/MacOS/joule" "$@"
fi
echo "joule: app not found under /Applications" >&2
exit 127
WRAP
chmod 755 "$PKG_ROOT/usr/local/bin/joule"

PKG_NAME="joule-${VERSION}-darwin-${ARCH}.pkg"
pkgbuild \
  --root "$PKG_ROOT" \
  --identifier "$PKG_ID" \
  --version "$VERSION" \
  --install-location / \
  "$OUT/$PKG_NAME"

# --- .dmg (drag joule.app → Applications) ---
DMG_STAGE="$WORKDIR/dmg"
mkdir -p "$DMG_STAGE"
cp -R "$APP" "$DMG_STAGE/joule.app"
ln -s /Applications "$DMG_STAGE/Applications"
# README for Gatekeeper
cat > "$DMG_STAGE/README-FIRST.txt" <<EOF
joule ${VERSION} for macOS (${ARCH})

Install:
  1. Drag joule.app into Applications
  2. First launch: right-click → Open (unsigned build until Apple notarization cert)
  3. Terminal CLI: open -a joule --args version
     or install the .pkg which also puts /usr/local/bin/joule

Product: https://joule.f00.sh/
EOF

DMG_NAME="joule-${VERSION}-darwin-${ARCH}.dmg"
# UDZO compressed dmg
hdiutil create \
  -volname "joule ${VERSION}" \
  -srcfolder "$DMG_STAGE" \
  -ov -format UDZO \
  "$OUT/$DMG_NAME"

# Also export a zip of the .app for completeness
APP_ZIP="joule-${VERSION}-darwin-${ARCH}-app.zip"
(
  cd "$WORKDIR"
  ditto -c -k --sequesterRsrc --keepParent joule.app "$OUT/$APP_ZIP"
)

# Checksums
(
  cd "$OUT"
  for f in "$PKG_NAME" "$DMG_NAME" "$APP_ZIP"; do
    shasum -a 256 "$f" > "${f}.sha256"
  done
)

echo "macos native packages:"
ls -la "$OUT/$PKG_NAME" "$OUT/$DMG_NAME" "$OUT/$APP_ZIP"
