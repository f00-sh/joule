# joule — Windows install from GitHub Releases (dummy easy).
#
#   irm https://github.com/f00-sh/joule/releases/latest/download/install.ps1 | iex
#   or:  .\install.ps1
#
# Installs joule.exe to %LOCALAPPDATA%\joule\bin and prepends that dir to User PATH.
# Binaries live on GitHub Releases only — f00 does not host weights.

$ErrorActionPreference = "Stop"
$Repo = "f00-sh/joule"
$Project = "joule"
$InstallRoot = Join-Path $env:LOCALAPPDATA "joule"
$InstallBin = Join-Path $InstallRoot "bin"

function Die([string]$msg) {
    Write-Error "error: $msg"
    exit 1
}

function Get-ArchTag {
    # GH runners / consumer PCs are overwhelmingly x86_64 for Windows joule builds.
    $arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
    switch ($arch) {
        "X64" { return "x86_64" }
        "Arm64" {
            Write-Warning "Windows arm64 release assets are not published yet; trying x86_64 (emulation)."
            return "x86_64"
        }
        default { Die "unsupported architecture: $arch" }
    }
}

Write-Host "joule Windows installer — fetching latest release…"

$api = "https://api.github.com/repos/$Repo/releases/latest"
try {
    $rel = Invoke-RestMethod -Uri $api -Headers @{ "User-Agent" = "joule-install" }
} catch {
    Die "could not resolve latest release ($api): $_"
}

$tag = $rel.tag_name
if (-not $tag) { Die "release has no tag_name" }
$ver = $tag.TrimStart("v")
$arch = Get-ArchTag
$assetName = "$Project-$ver-windows-$arch.zip"
$asset = $rel.assets | Where-Object { $_.name -eq $assetName } | Select-Object -First 1
if (-not $asset) {
    Die "release asset missing: $assetName (tag $tag). See https://github.com/$Repo/releases"
}

$url = $asset.browser_download_url
$tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("joule-install-" + [guid]::NewGuid().ToString("n"))
New-Item -ItemType Directory -Path $tmp | Out-Null
$zipPath = Join-Path $tmp $assetName

try {
    Write-Host "Downloading $url"
    Invoke-WebRequest -Uri $url -OutFile $zipPath -UseBasicParsing
    Expand-Archive -Path $zipPath -DestinationPath $tmp -Force

    $exe = Get-ChildItem -Path $tmp -Recurse -Filter "joule.exe" | Select-Object -First 1
    if (-not $exe) { Die "zip has no joule.exe" }

    New-Item -ItemType Directory -Path $InstallBin -Force | Out-Null
    Copy-Item -Path $exe.FullName -Destination (Join-Path $InstallBin "joule.exe") -Force

    # User PATH
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if (-not $userPath) { $userPath = "" }
    $parts = $userPath -split ";" | Where-Object { $_ -ne "" }
    if ($parts -notcontains $InstallBin) {
        $newPath = ($parts + $InstallBin) -join ";"
        [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
        $env:Path = "$InstallBin;$env:Path"
        Write-Host "Added $InstallBin to User PATH (open a new terminal if needed)."
    }

    Write-Host ""
    Write-Host "installed $($InstallBin)\joule.exe  (from $tag)"
    Write-Host "run:  joule version"
    Write-Host "then: joule agent --account YOU"
    Write-Host "docs: https://joule.f00.sh/download.html"
} finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}
