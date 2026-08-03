# Homebrew formula for joule (prebuilt bottles from GitHub Releases).
#
# Install (once a tap exists):
#   brew install f00-sh/tap/joule
#
# Or install the formula file directly from this repo:
#   brew install --formula ./packaging/homebrew/joule.rb
#
# Maintainers: bump version + sha256 from the GitHub Release SHA256SUMS
# after each tag. Site download page still points at the curl installer
# for non-Homebrew users.

class Joule < Formula
  desc "Donate idle compute, earn millijoules, use open-weight AI"
  homepage "https://joule.f00.sh/"
  version "0.1.11"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/f00-sh/joule/releases/download/v#{version}/joule-#{version}-darwin-aarch64.tar.gz"
      sha256 "UPDATE_FROM_RELEASE_SHA256SUMS"
    end
    on_intel do
      url "https://github.com/f00-sh/joule/releases/download/v#{version}/joule-#{version}-darwin-x86_64.tar.gz"
      sha256 "UPDATE_FROM_RELEASE_SHA256SUMS"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/f00-sh/joule/releases/download/v#{version}/joule-#{version}-linux-aarch64.tar.gz"
      sha256 "UPDATE_FROM_RELEASE_SHA256SUMS"
    end
    on_intel do
      url "https://github.com/f00-sh/joule/releases/download/v#{version}/joule-#{version}-linux-x86_64.tar.gz"
      sha256 "UPDATE_FROM_RELEASE_SHA256SUMS"
    end
  end

  def install
    bin.install "joule"
    man1.install "man/joule.1" if File.exist?("man/joule.1")
    doc.install "man/joule.1.md" if File.exist?("man/joule.1.md")
  end

  test do
    # Binary Cargo version may lag SemVer tag (e.g. formula 0.1.2, `joule version` 0.1.0).
    out = shell_output("#{bin}/joule version")
    assert_match(/joule\s+\d+\.\d+\.\d+/, out)
    assert_match(/protocol/, out)
  end
end
