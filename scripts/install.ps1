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

# Prefer native GUI Setup.exe when present; fall back to portable ZIP.
$setupName = "$Project-$ver-windows-$arch-setup.exe"
$zipName = "$Project-$ver-windows-$arch.zip"
$setup = $rel.assets | Where-Object { $_.name -eq $setupName } | Select-Object -First 1
$zipAsset = $rel.assets | Where-Object { $_.name -eq $zipName } | Select-Object -First 1

$tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("joule-install-" + [guid]::NewGuid().ToString("n"))
New-Item -ItemType Directory -Path $tmp | Out-Null

try {
    if ($setup) {
        $setupPath = Join-Path $tmp $setupName
        Write-Host "Downloading native Setup: $($setup.browser_download_url)"
        Invoke-WebRequest -Uri $setup.browser_download_url -OutFile $setupPath -UseBasicParsing
        Write-Host "Launching GUI installer (Setup wizard)…"
        Start-Process -FilePath $setupPath -Wait
        Write-Host ""
        Write-Host "Setup finished ($tag). Launch joule from the Start Menu, or: joule version"
        Write-Host "docs: https://joule.f00.sh/download.html"
        return
    }

    if (-not $zipAsset) {
        Die "release asset missing: $setupName or $zipName (tag $tag). See https://github.com/$Repo/releases"
    }

    $url = $zipAsset.browser_download_url
    $zipPath = Join-Path $tmp $zipName
    Write-Host "No Setup.exe in release — installing portable ZIP: $url"
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
    Write-Host "run:  joule          # GUI dashboard"
    Write-Host "      joule version"
    Write-Host "docs: https://joule.f00.sh/download.html"
} finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}
