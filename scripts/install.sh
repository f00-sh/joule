#!/usr/bin/env bash
# joule — install binary + man page.
#
# Modes:
#   1) Local tree (default when run from a clone after cargo build):
#        ./scripts/install.sh
#   2) From GitHub Releases (when assets exist):
#        curl -fsSL https://github.com/f00-sh/joule/releases/latest/download/install.sh | sh
#        or: JOULE_FROM_RELEASE=1 ./scripts/install.sh
#
# f00 does **not** host the binary as a CDN product path — releases are on GitHub;
# peer seed is for swarm software_update digests after you already have a binary.
set -euo pipefail

PROJECT="joule"
REPO="f00-sh/joule"
INSTALL_BIN_DIR="${INSTALL_BIN_DIR:-${HOME}/.local/bin}"
INSTALL_MAN_DIR="${INSTALL_MAN_DIR:-${HOME}/.local/share/man/man1}"
ROOT="$(cd "$(dirname "$0")/.." 2>/dev/null && pwd || true)"

die() {
  printf '%s\n' "error: $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

need_cmd mkdir
need_cmd install
need_cmd uname

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
case "$arch" in
  x86_64 | amd64) arch="x86_64" ;;
  aarch64 | arm64) arch="aarch64" ;;
  *) die "unsupported architecture: $arch" ;;
esac

mkdir -p "${INSTALL_BIN_DIR}" "${INSTALL_MAN_DIR}"

install_man_from_repo() {
  local man_src="$1"
  if [[ -f "${man_src}/joule.1" ]]; then
    install -m 0644 "${man_src}/joule.1" "${INSTALL_MAN_DIR}/joule.1"
  elif [[ -f "${man_src}/joule.1.md" ]]; then
    # Ship markdown as man-source until pandoc man is generated on release.
    install -m 0644 "${man_src}/joule.1.md" "${INSTALL_MAN_DIR}/joule.1.md"
    printf 'note: installed joule.1.md (run pandoc on release for roff man)\n' >&2
  else
    die "man page not found under ${man_src}"
  fi
}

install_local() {
  need_cmd cargo
  [[ -n "${ROOT}" && -f "${ROOT}/Cargo.toml" ]] || die "not a joule checkout"
  cd "${ROOT}"
  if [[ ! -x target/release/joule ]]; then
    cargo build --release -p joule
  fi
  install -m 0755 target/release/joule "${INSTALL_BIN_DIR}/joule"
  install_man_from_repo "${ROOT}/man"
  printf 'installed %s/joule\n' "${INSTALL_BIN_DIR}"
  printf 'man sources in %s\n' "${INSTALL_MAN_DIR}"
  printf 'run: joule version\n'
}

install_release() {
  need_cmd curl
  need_cmd tar
  local tag ver asset url tmp
  tag="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -1)"
  [[ -n "${tag}" ]] || die "could not resolve latest release tag"
  ver="${tag#v}"
  asset="${PROJECT}-${ver}-${os}-${arch}.tar.gz"
  url="https://github.com/${REPO}/releases/download/${tag}/${asset}"
  tmp="$(mktemp -d)"
  trap 'rm -rf "${tmp}"' EXIT
  if ! curl -fsSL "${url}" -o "${tmp}/${asset}"; then
    die "release asset missing (${url}). Build from source: git clone + ./scripts/install.sh"
  fi
  tar -C "${tmp}" -xzf "${tmp}/${asset}"
  if [[ -x "${tmp}/joule" ]]; then
    install -m 0755 "${tmp}/joule" "${INSTALL_BIN_DIR}/joule"
  elif [[ -x "${tmp}/bin/joule" ]]; then
    install -m 0755 "${tmp}/bin/joule" "${INSTALL_BIN_DIR}/joule"
  else
    die "tarball has no joule binary"
  fi
  if [[ -f "${tmp}/joule.1" ]]; then
    install -m 0644 "${tmp}/joule.1" "${INSTALL_MAN_DIR}/joule.1"
  elif [[ -f "${tmp}/man/joule.1" ]]; then
    install -m 0644 "${tmp}/man/joule.1" "${INSTALL_MAN_DIR}/joule.1"
  else
    printf 'warning: no man page in tarball\n' >&2
  fi
  printf 'installed %s/joule from %s\n' "${INSTALL_BIN_DIR}" "${tag}"
  printf 'run: joule version && man joule\n'
}

if [[ "${JOULE_FROM_RELEASE:-0}" == "1" ]] || [[ ! -f "${ROOT}/Cargo.toml" ]]; then
  install_release
else
  install_local
fi
