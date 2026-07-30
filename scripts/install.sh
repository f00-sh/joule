#!/usr/bin/env bash
# joule — install from GitHub Releases (curl | sh).
# Must install the binary/artifact AND man page(s). Incomplete without man pages.
set -euo pipefail

PROJECT="joule"
REPO="f00-sh/joule"
# Override for testing: INSTALL_BASE=/tmp/foo ./install.sh
INSTALL_BIN_DIR="${INSTALL_BIN_DIR:-${HOME}/.local/bin}"
INSTALL_MAN_DIR="${INSTALL_MAN_DIR:-${HOME}/.local/share/man/man1}"
RELEASE_API="https://api.github.com/repos/${REPO}/releases/latest"
# Prefer a fixed asset name pattern on each release, e.g.:
#   joule-<version>-<os>-<arch>.tar.gz  including man pages

die() {
  printf '%s\n' "error: $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

need_cmd curl
need_cmd uname
need_cmd mkdir

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
case "$arch" in
  x86_64 | amd64) arch="x86_64" ;;
  aarch64 | arm64) arch="aarch64" ;;
  *) die "unsupported architecture: $arch" ;;
esac

# TODO: map os/arch to the release asset name this project publishes.
# Example asset: ${PROJECT}-vX.Y.Z-${os}-${arch}.tar.gz
die "scaffold install.sh: set asset download + extract for ${PROJECT} (${os}/${arch}). Install man pages into ${INSTALL_MAN_DIR}."

# Expected shape after you fill this in:
# 1. Resolve latest tag / asset URL from releases (or use /latest/download/NAME).
# 2. Download to a temp dir; verify checksum when you publish one.
# 3. Install binary to ${INSTALL_BIN_DIR} (create dir; no needless sudo).
# 4. Install man page(s) to ${INSTALL_MAN_DIR} (required).
# 5. Print success and how to run: man ${PROJECT}
