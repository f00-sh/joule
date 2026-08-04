# Maintainer: William Theesfeld <william@theesfeld.net>
pkgname=joule-bin
pkgver=0.1.13
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
sha256sums_x86_64=('708c2aa33849bced0180482a376ab26f30662b3220a5bbbdab5da23eff2e6393')
sha256sums_aarch64=('25b1a3290c4ab444ea5441fcc2adc58f0a3ea1a12a7e5012fe4706bafa4de14e')
package() {
  cd "${srcdir}"
  root="$(find . -maxdepth 2 -type f -name joule | head -1 | xargs dirname)"
  install -Dm755 "${root}/joule" "${pkgdir}/usr/bin/joule"
}
