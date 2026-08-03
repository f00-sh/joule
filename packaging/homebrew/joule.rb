class Joule < Formula
  desc "Donate idle compute, earn millijoules, use open-weight AI"
  homepage "https://joule.f00.sh/"
  version "0.1.11"
  license "MIT"
  on_macos do
    on_arm do
      url "https://github.com/f00-sh/joule/releases/download/v#{version}/joule-#{version}-darwin-aarch64.tar.gz"
      sha256 "98af90bc443ff4fbac8723a3ec1fc84714076f3cbb1e0f0992e07c99ea22ef5b"
    end
    on_intel do
      url "https://github.com/f00-sh/joule/releases/download/v#{version}/joule-#{version}-darwin-x86_64.tar.gz"
      sha256 "898f913eae145ecb396d3a88d420a1929f9c679e14f68dee50ce4b3d2d9de770"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/f00-sh/joule/releases/download/v#{version}/joule-#{version}-linux-aarch64.tar.gz"
      sha256 "3565e29a474152c90ae7c840ccf77c86365943252cf81a4801393f06bd2ea516"
    end
    on_intel do
      url "https://github.com/f00-sh/joule/releases/download/v#{version}/joule-#{version}-linux-x86_64.tar.gz"
      sha256 "f556eac53b3a29dd6972deddd3e099e10cb21038cff45f83aaadbfffe62e5d2a"
    end
  end
  def install
    bin.install "joule"
  end
  test do
    assert_match(/joule\s+\d+\.\d+\.\d+/, shell_output("#{bin}/joule version"))
  end
end
