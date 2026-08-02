# Build native Windows GUI installer (Inno Setup .exe)
# Usage (from repo root on windows-latest):
#   powershell -File packaging/windows/build-native.ps1 -Bin path\to\joule.exe -Version 0.1.8 -Out dist
param(
    [Parameter(Mandatory = $true)][string]$Bin,
    [Parameter(Mandatory = $true)][string]$Version,
    [string]$Arch = "x86_64",
    [string]$Out = "dist"
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $Root

if (-not (Test-Path $Bin)) { throw "binary not found: $Bin" }
New-Item -ItemType Directory -Force -Path $Out | Out-Null

# Resolve ISCC (Inno Setup 6)
$iscc = $null
foreach ($c in @(
        "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe",
        "${env:ProgramFiles}\Inno Setup 6\ISCC.exe",
        "C:\Program Files (x86)\Inno Setup 6\ISCC.exe"
    )) {
    if (Test-Path $c) { $iscc = $c; break }
}
if (-not $iscc) {
    Write-Host "Installing Inno Setup 6 via winget/choco…"
    if (Get-Command choco -ErrorAction SilentlyContinue) {
        choco install innosetup -y --no-progress
    } elseif (Get-Command winget -ErrorAction SilentlyContinue) {
        winget install --id JRSoftware.InnoSetup -e --accept-source-agreements --accept-package-agreements
    } else {
        throw "Inno Setup (ISCC.exe) not found and no choco/winget"
    }
    foreach ($c in @(
            "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe",
            "${env:ProgramFiles}\Inno Setup 6\ISCC.exe"
        )) {
        if (Test-Path $c) { $iscc = $c; break }
    }
}
if (-not $iscc) { throw "ISCC.exe still missing after install attempt" }

$iss = Join-Path $Root "packaging\windows\joule.iss"
$absBin = (Resolve-Path $Bin).Path
if (-not [System.IO.Path]::IsPathRooted($Out)) {
    $Out = Join-Path $Root $Out
}
New-Item -ItemType Directory -Force -Path $Out | Out-Null
$absOut = (Resolve-Path $Out).Path

# MyBinPath must be relative to SourceDir (repo root) for Inno, or absolute.
# Prefer absolute path for CI reliability.
Write-Host "ISCC=$iscc"
Write-Host "Bin=$absBin Version=$Version Arch=$Arch Out=$absOut Root=$Root"

& $iscc `
    "/DMyAppVersion=$Version" `
    "/DMyAppArch=$Arch" `
    "/DMyBinPath=$absBin" `
    "/DMyOutDir=$absOut" `
    "/DMySourceRoot=$Root" `
    $iss
if ($LASTEXITCODE -ne 0) { throw "ISCC failed: $LASTEXITCODE" }

$setup = Get-ChildItem -Path $absOut -Filter "joule-$Version-windows-$Arch-setup.exe" | Select-Object -First 1
if (-not $setup) { throw "setup exe not produced" }

# SHA256 sidecar
$hash = (Get-FileHash -Algorithm SHA256 -Path $setup.FullName).Hash.ToLower()
Set-Content -Path ($setup.FullName + ".sha256") -Value "$hash  $($setup.Name)`n" -NoNewline
Write-Host "wrote $($setup.FullName)"
Write-Host "sha256 $hash"
