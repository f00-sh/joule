# Maintainer: William Theesfeld <william@theesfeld.net>
pkgname=joule-bin
pkgver=0.1.12
pkgrel=1
pkgdesc="Donate idle compute, earn millijoules, use open-weight AI (prebuilt binary)"
arch=('x86_64' 'aarch64')
url="https://joule.f00.sh/"
license=('MIT')
provides=('joule')
conflicts=('joule')
options=('!strip')
source_x86_64=("https://github.com/f00-sh/joule/releases/download/v${pkgver}/joule-${pkgver}-linux-x86_64.tar.gz")
source_aarch64=("https://github.com/f00-sh/joule/releases/download/v${pkgver}/joule-${pkgver}-linux-aarch64.tar.gz")
sha256sums_x86_64=('91c7862f473a618c7e05181fded7eb2a680446248f058727f5e5e46f444d9c46')
sha256sums_aarch64=('67a1d7a5413a436c9cf0cb9da62b47a9677c3e3ea0de3b84b292db8692ef46ac')
package() {
  cd "${srcdir}"
  root="$(find . -maxdepth 2 -type f -name joule | head -1 | xargs dirname)"
  install -Dm755 "${root}/joule" "${pkgdir}/usr/bin/joule"
}
