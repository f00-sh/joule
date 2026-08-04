class Joule < Formula
  desc "Donate idle compute, earn millijoules, use open-weight AI"
  homepage "https://joule.f00.sh/"
  version "0.1.13"
  license "MIT"
  on_macos do
    on_arm do
      url "https://github.com/f00-sh/joule/releases/download/v#{version}/joule-#{version}-darwin-aarch64.tar.gz"
      sha256 "14cdd04259833ec585122592cd54345114384224bdf7390c706cb7582a89baff"
    end
    on_intel do
      url "https://github.com/f00-sh/joule/releases/download/v#{version}/joule-#{version}-darwin-x86_64.tar.gz"
      sha256 "99708927092ad341405017b04084a21e5f777ae3bb9539c88a98c5b32f62c390"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/f00-sh/joule/releases/download/v#{version}/joule-#{version}-linux-aarch64.tar.gz"
      sha256 "25b1a3290c4ab444ea5441fcc2adc58f0a3ea1a12a7e5012fe4706bafa4de14e"
    end
    on_intel do
      url "https://github.com/f00-sh/joule/releases/download/v#{version}/joule-#{version}-linux-x86_64.tar.gz"
      sha256 "708c2aa33849bced0180482a376ab26f30662b3220a5bbbdab5da23eff2e6393"
    end
  end
  def install
    bin.install "joule"
  end
  test do
    assert_match(/joule\s+\d+\.\d+\.\d+/, shell_output("#{bin}/joule version"))
  end
end
