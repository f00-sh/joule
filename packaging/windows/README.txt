joule for Windows
=================

Easy install (recommended)
  Open PowerShell and run:

    irm https://github.com/f00-sh/joule/releases/latest/download/install.ps1 | iex

This zip
  1. Unzip anywhere
  2. Put joule.exe on your PATH, or run .\install.ps1 from the zip
  3. joule version
  4. joule agent --account YOURNAME

Service / tray
  joule service generate --platform windows --kind agent
  joule service install-help --platform windows
  joule tray   (when GUI tray is enabled in your build)

Notes
  - Release binaries are on GitHub only (not model weights).
  - Weights come from peers or official sources (sha256 verified).
  - Docs: https://joule.f00.sh/download.html
