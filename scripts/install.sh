#!/usr/bin/env bash
# joule — install binary + man page (dummy easy).
#
# One-liner (Linux / macOS):
#   curl -fsSL https://github.com/f00-sh/joule/releases/latest/download/install.sh | sh
#
# Modes:
#   1) From GitHub Releases (default for curl | sh):
#        JOULE_FROM_RELEASE=1 ./scripts/install.sh
#   2) Local tree after cargo build:
#        ./scripts/install.sh
#
# Installers / binaries: GitHub Releases only.
# Model weights: never from f00 — peers + official sources + sha256.
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
need_cmd uname

# Normalize OS → asset name (linux | darwin)
raw_os="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "$raw_os" in
  linux*) os="linux" ;;
  darwin*) os="darwin" ;;
  msys* | mingw* | cygwin*)
    die "use install.ps1 on Windows: irm https://github.com/${REPO}/releases/latest/download/install.ps1 | iex"
    ;;
  *) die "unsupported OS: $raw_os" ;;
esac

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
    install -m 0644 "${man_src}/joule.1.md" "${INSTALL_MAN_DIR}/joule.1.md"
    printf 'note: installed joule.1.md (roff man ships in release tarballs when built)\n' >&2
  fi
}

path_hint() {
  case ":${PATH}:" in
    *":${INSTALL_BIN_DIR}:"*) ;;
    *)
      printf '\nAdd to PATH (zsh/bash):\n  export PATH="%s:$PATH"\n' "${INSTALL_BIN_DIR}" >&2
      printf 'Or:  echo '\''export PATH="%s:$PATH"'\'' >> ~/.bashrc\n' "${INSTALL_BIN_DIR}" >&2
      ;;
  esac
}

install_local() {
  need_cmd cargo
  need_cmd install
  [[ -n "${ROOT}" && -f "${ROOT}/Cargo.toml" ]] || die "not a joule checkout"
  cd "${ROOT}"
  if [[ ! -x target/release/joule ]]; then
    cargo build --release -p joule
  fi
  install -m 0755 target/release/joule "${INSTALL_BIN_DIR}/joule"
  install_man_from_repo "${ROOT}/man"
  printf 'installed %s/joule (local build)\n' "${INSTALL_BIN_DIR}"
  path_hint
  printf 'run: joule version\n'
}

install_release() {
  need_cmd curl
  need_cmd tar
  need_cmd install
  local tag ver asset url tmp api
  api="https://api.github.com/repos/${REPO}/releases/latest"
  tag="$(curl -fsSL "${api}" | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' | head -1)"
  [[ -n "${tag}" ]] || die "could not resolve latest release tag (${api})"
  ver="${tag#v}"
  asset="${PROJECT}-${ver}-${os}-${arch}.tar.gz"
  url="https://github.com/${REPO}/releases/download/${tag}/${asset}"
  tmp="$(mktemp -d)"
  # shellcheck disable=SC2064
  trap 'rm -rf "${tmp:-/tmp/joule-install-none}"' EXIT
  printf 'downloading %s\n' "${url}"
  if ! curl -fsSL "${url}" -o "${tmp}/${asset}"; then
    die "release asset missing (${url}). Try another platform on https://joule.f00.sh/download.html or build: git clone + cargo build --release -p joule"
  fi
  tar -C "${tmp}" -xzf "${tmp}/${asset}"
  # Tarball may be flat or a single top-level dir.
  local bin=""
  if [[ -x "${tmp}/joule" ]]; then
    bin="${tmp}/joule"
  elif [[ -x "${tmp}/bin/joule" ]]; then
    bin="${tmp}/bin/joule"
  else
    bin="$(find "${tmp}" -type f -name joule -perm -111 2>/dev/null | head -1 || true)"
  fi
  [[ -n "${bin}" && -x "${bin}" ]] || die "tarball has no joule binary"
  install -m 0755 "${bin}" "${INSTALL_BIN_DIR}/joule"

  local manf=""
  manf="$(find "${tmp}" -type f \( -name 'joule.1' -o -name 'joule.1.md' \) 2>/dev/null | head -1 || true)"
  if [[ -n "${manf}" ]]; then
    install -m 0644 "${manf}" "${INSTALL_MAN_DIR}/$(basename "${manf}")"
  else
    printf 'warning: no man page in tarball\n' >&2
  fi

  printf '\ninstalled %s/joule from %s\n' "${INSTALL_BIN_DIR}" "${tag}"
  path_hint
  printf 'run: joule version\n'
  printf 'then: joule agent   # auto CODE; multi-device: joule identity use <CODE>\n'
  printf 'site: https://joule.f00.sh/download.html\n'
  trap - EXIT
  rm -rf "${tmp}"
}

# curl | sh → no git root → release. Local clone defaults to local build unless forced.
if [[ "${JOULE_FROM_RELEASE:-0}" == "1" ]] || [[ ! -f "${ROOT}/Cargo.toml" ]]; then
  install_release
else
  install_local
fi
