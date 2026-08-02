#!/usr/bin/env bash
# Create versionless "stable" release asset names next to versioned ones.
#
# Permanent GitHub URLs (never need the SemVer in the path):
#   https://github.com/f00-sh/joule/releases/latest/download/joule-windows-x86_64-setup.exe
#   …/joule-darwin-aarch64.pkg
#   …/joule-linux-x86_64.deb
#   …/install.sh
#
# Usage (from release publish job, after dist/ is filled with versioned assets):
#   VER=0.1.8 scripts/stable-release-aliases.sh dist
#
# Mapping: joule-{VER}-{os}-{arch}{suffix} → joule-{os}-{arch}{suffix}
set -euo pipefail

DIST="${1:-dist}"
VER="${VER:-}"
[[ -d "$DIST" ]] || { echo "error: dist dir missing: $DIST" >&2; exit 1; }

if [[ -z "$VER" ]]; then
  # Infer from any versioned asset name.
  sample="$(find "$DIST" -maxdepth 1 -type f -name 'joule-*-*-*.*' | head -1 || true)"
  if [[ -n "$sample" ]]; then
    base="$(basename "$sample")"
    # joule-0.1.8-windows-x86_64-setup.exe or joule-0.1.8-linux-x86_64.tar.gz
    VER="$(printf '%s' "$base" | sed -n 's/^joule-\([0-9][0-9.]*\)-.*/\1/p')"
  fi
fi
[[ -n "$VER" ]] || { echo "error: set VER= or place versioned joule-* assets in $DIST" >&2; exit 1; }

echo "stable aliases for ver=$VER in $DIST"

# List of (versioned_glob_or_exact → stable_name) patterns we care about.
# Prefer explicit copies so names stay predictable.
copy_if() {
  local src="$1" dest="$2"
  if [[ -f "$DIST/$src" ]]; then
    cp -f "$DIST/$src" "$DIST/$dest"
    echo "  $src  →  $dest"
  else
    echo "  (skip missing) $src"
  fi
}

# --- GUI / native installers first ---
copy_if "joule-${VER}-windows-x86_64-setup.exe" "joule-windows-x86_64-setup.exe"
copy_if "joule-${VER}-darwin-aarch64.pkg" "joule-darwin-aarch64.pkg"
copy_if "joule-${VER}-darwin-aarch64.dmg" "joule-darwin-aarch64.dmg"
copy_if "joule-${VER}-darwin-aarch64-app.zip" "joule-darwin-aarch64-app.zip"
copy_if "joule-${VER}-darwin-x86_64.pkg" "joule-darwin-x86_64.pkg"
copy_if "joule-${VER}-darwin-x86_64.dmg" "joule-darwin-x86_64.dmg"
copy_if "joule-${VER}-darwin-x86_64-app.zip" "joule-darwin-x86_64-app.zip"
copy_if "joule-${VER}-linux-x86_64.deb" "joule-linux-x86_64.deb"
copy_if "joule-${VER}-linux-aarch64.deb" "joule-linux-aarch64.deb"

# --- CLI archives ---
copy_if "joule-${VER}-windows-x86_64.zip" "joule-windows-x86_64.zip"
copy_if "joule-${VER}-darwin-aarch64.tar.gz" "joule-darwin-aarch64.tar.gz"
copy_if "joule-${VER}-darwin-x86_64.tar.gz" "joule-darwin-x86_64.tar.gz"
copy_if "joule-${VER}-linux-x86_64.tar.gz" "joule-linux-x86_64.tar.gz"
copy_if "joule-${VER}-linux-aarch64.tar.gz" "joule-linux-aarch64.tar.gz"

# install.sh / install.ps1 already versionless if present

# Refresh SHA256SUMS including stable names
(
  cd "$DIST"
  : > SHA256SUMS
  for f in *; do
    [[ -f "$f" ]] || continue
    case "$f" in
      *.sha256|SHA256SUMS) continue ;;
    esac
    if command -v sha256sum >/dev/null 2>&1; then
      sha256sum "$f" >> SHA256SUMS
    else
      shasum -a 256 "$f" >> SHA256SUMS
    fi
  done
)

echo "stable alias pass complete"
