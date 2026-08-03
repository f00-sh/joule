# Authenticode-sign Windows joule binaries when a code-signing cert is available.
#
# Inputs (env):
#   JOULE_WINDOWS_CERT_PFX_BASE64  - base64 of a .pfx (preferred for CI secrets)
#   JOULE_WINDOWS_CERT_PFX         - path to .pfx on disk
#   JOULE_WINDOWS_CERT_PASSWORD    - PFX password (may be empty)
#   JOULE_WINDOWS_TIMESTAMP_URL    - optional RFC3161 timestamp (default DigiCert)
#
# Usage:
#   packaging/windows/sign.ps1 -Files @("dist\joule.exe","dist\setup.exe")
#
# Exit 0 when no cert is configured (unsigned ship is allowed).
# Exit non-zero if a cert is configured but signing fails.

param(
    [Parameter(Mandatory = $true)][string[]]$Files
)

$ErrorActionPreference = "Stop"

function Write-Info([string]$m) {
    Write-Host ("[sign] " + $m)
}

$pfxPath = $env:JOULE_WINDOWS_CERT_PFX
$pfxB64 = $env:JOULE_WINDOWS_CERT_PFX_BASE64
$pass = $env:JOULE_WINDOWS_CERT_PASSWORD
if (-not $pass) { $pass = "" }
$ts = $env:JOULE_WINDOWS_TIMESTAMP_URL
if (-not $ts) { $ts = "http://timestamp.digicert.com" }

$tmpPfx = $null
try {
    if ($pfxB64 -and $pfxB64.Trim().Length -gt 0) {
        $tmpPfx = Join-Path ([System.IO.Path]::GetTempPath()) ("joule-codesign-" + [guid]::NewGuid().ToString("n") + ".pfx")
        [IO.File]::WriteAllBytes($tmpPfx, [Convert]::FromBase64String($pfxB64.Trim()))
        $pfxPath = $tmpPfx
        Write-Info "decoded PFX from JOULE_WINDOWS_CERT_PFX_BASE64"
    }

    if (-not $pfxPath -or -not (Test-Path $pfxPath)) {
        Write-Info "no cert configured - shipping unsigned"
        exit 0
    }

    $signtool = $null
    $candidates = @(
        "${env:ProgramFiles(x86)}\Windows Kits\10\bin\*\x64\signtool.exe",
        "${env:ProgramFiles}\Windows Kits\10\bin\*\x64\signtool.exe"
    )
    foreach ($pat in $candidates) {
        $hit = Get-Item $pat -ErrorAction SilentlyContinue | Sort-Object FullName -Descending | Select-Object -First 1
        if ($hit) { $signtool = $hit.FullName; break }
    }
    if (-not $signtool) {
        $cmd = Get-Command signtool.exe -ErrorAction SilentlyContinue
        if ($cmd) { $signtool = $cmd.Source }
    }
    if (-not $signtool) {
        Write-Error "signtool.exe not found but cert is configured"
        exit 2
    }

    Write-Info ("using " + $signtool)
    Write-Info ("timestamp " + $ts)

    foreach ($f in $Files) {
        if (-not (Test-Path $f)) {
            Write-Error ("missing file to sign: " + $f)
            exit 3
        }
        Write-Info ("signing " + $f)
        & $signtool sign /f $pfxPath /p $pass /fd sha256 /tr $ts /td sha256 /v $f
        if ($LASTEXITCODE -ne 0) {
            Write-Error ("signtool failed for " + $f + " exit " + $LASTEXITCODE)
            exit $LASTEXITCODE
        }
        & $signtool verify /pa /v $f
        if ($LASTEXITCODE -ne 0) {
            Write-Error ("signtool verify failed for " + $f)
            exit $LASTEXITCODE
        }
        Write-Info ("signed and verified " + $f)
    }
    Write-Info "all files signed"
    exit 0
}
finally {
    if ($tmpPfx -and (Test-Path $tmpPfx)) {
        Remove-Item -Force $tmpPfx -ErrorAction SilentlyContinue
    }
}
