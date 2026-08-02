#!/usr/bin/env bash
# Build a native .deb package (GUI desktop entry + /usr/bin/joule).
# Usage:
#   packaging/linux/build-deb.sh --bin target/release/joule --version 0.1.8 --arch amd64 --out dist
set -euo pipefail

BIN=""
VERSION=""
ARCH="" # amd64 | arm64
OUT="dist"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bin) BIN="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    --arch) ARCH="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    *) echo "unknown: $1" >&2; exit 2 ;;
  esac
done

[[ -f "$BIN" ]] || { echo "missing --bin" >&2; exit 1; }
[[ -n "$VERSION" && -n "$ARCH" ]] || { echo "need --version and --arch" >&2; exit 1; }

# Map release arch names
case "$ARCH" in
  x86_64|amd64) DEB_ARCH=amd64; ASSET_ARCH=x86_64 ;;
  aarch64|arm64) DEB_ARCH=arm64; ASSET_ARCH=aarch64 ;;
  *) echo "bad arch $ARCH" >&2; exit 1 ;;
esac

mkdir -p "$OUT"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

PKG="$WORKDIR/joule_${VERSION}_${DEB_ARCH}"
mkdir -p "$PKG/DEBIAN" "$PKG/usr/bin" "$PKG/usr/share/applications" \
  "$PKG/usr/share/doc/joule" "$PKG/usr/share/man/man1"

cp "$BIN" "$PKG/usr/bin/joule"
chmod 755 "$PKG/usr/bin/joule"

cat > "$PKG/usr/share/applications/joule.desktop" <<EOF
[Desktop Entry]
Type=Application
Name=joule
GenericName=Compute pool agent
Comment=Donate idle GPUs to the open joule cluster
Exec=joule
Icon=utilities-terminal
Terminal=false
Categories=Network;Science;Utility;
Keywords=gpu;ai;cluster;compute;
StartupNotify=true
EOF

cp "$ROOT/README.md" "$PKG/usr/share/doc/joule/" 2>/dev/null || true
cp "$ROOT/LICENSE" "$PKG/usr/share/doc/joule/copyright" 2>/dev/null || true
cp "$ROOT/CHANGELOG.md" "$PKG/usr/share/doc/joule/" 2>/dev/null || true
if [[ -f "$ROOT/man/joule.1" ]]; then
  gzip -n -c "$ROOT/man/joule.1" > "$PKG/usr/share/man/man1/joule.1.gz"
elif [[ -f "$ROOT/man/joule.1.md" ]]; then
  cp "$ROOT/man/joule.1.md" "$PKG/usr/share/doc/joule/"
fi

INSTALLED_SIZE="$(du -sk "$PKG/usr" | awk '{print $1}')"
cat > "$PKG/DEBIAN/control" <<EOF
Package: joule
Version: ${VERSION}
Section: net
Priority: optional
Architecture: ${DEB_ARCH}
Maintainer: f00-sh <william@theesfeld.net>
Installed-Size: ${INSTALLED_SIZE}
Homepage: https://joule.f00.sh/
Description: distributed idle-GPU compute cluster agent
 joule donates idle compute to an open-weight AI inference pool.
 Ships a native GUI dashboard (default) and full CLI.
 Credits are millijoules — no cash on the public pool.
EOF

DEB_NAME="joule-${VERSION}-linux-${ASSET_ARCH}.deb"
dpkg-deb --build --root-owner-group "$PKG" "$OUT/$DEB_NAME"
(
  cd "$OUT"
  sha256sum "$DEB_NAME" > "${DEB_NAME}.sha256"
)
ls -la "$OUT/$DEB_NAME"
