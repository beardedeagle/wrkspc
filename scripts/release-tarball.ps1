<#
Package one Windows platform's release binary in dist/ with a
per-artifact sha256 fragment (just release-tarball windows /
windows-arm). Invoked by the Justfile as a single pwsh process,
because just 1.51 runs each non-shebang recipe line in a fresh shell.

.PARAMETER Target
    Just leg name: 'windows' (x86_64 MSVC) or 'windows-arm' (arm64 MSVC).
.PARAMETER Toolchain
    rustup toolchain override, e.g. '1.99.1' (see rust-toolchain.toml).
#>
param(
    [Parameter(Mandatory = $true)][string]$Target,
    [Parameter(Mandatory = $true)][string]$Toolchain
)

$ErrorActionPreference = 'Stop'

# Map the just leg name to the rustc target triple.
$RustTarget = switch ($Target) {
    'windows'     { 'x86_64-pc-windows-msvc' }
    'windows-arm' { 'aarch64-pc-windows-msvc' }
    default       { Write-Error "release-tarball: unsupported target '$Target' (windows, windows-arm)"; exit 1 }
}

$Root = (Get-Location).Path
$DistDir = Join-Path $Root 'dist'
New-Item -ItemType Directory -Force -Path $DistDir | Out-Null

# Resolve Cargo's effective target directory instead of assuming
# target\: CARGO_TARGET_DIR or [build] target-dir can redirect
# builds, and packaging must follow the build, not guess.
$TargetDir = (cargo +$Toolchain metadata --format-version 1 --no-deps | ConvertFrom-Json).target_directory

# The +toolchain pin keeps ambient shims from winning; the toolchain
# itself syncs from rust-toolchain.toml via rustup.
& cargo +$Toolchain build --locked --release -p wrkspc-cli --bin wrkspc --target $RustTarget
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# LICENSE rides in every artifact (the project's own license requires
# notice retention).
$Stage = Join-Path ([System.IO.Path]::GetTempPath()) ('wrkspc-stage-' + [guid]::NewGuid().ToString('N'))
try {
    New-Item -ItemType Directory -Path $Stage | Out-Null
    Copy-Item (Join-Path $TargetDir "$RustTarget\release\wrkspc.exe") (Join-Path $Stage 'wrkspc.exe')
    Copy-Item (Join-Path $Root 'LICENSE') (Join-Path $Stage 'LICENSE')

    # Compress-Archive is the always-present native zip writer
    # (Git Bash ships none; Microsoft's bsdtar cannot write zip).
    $Zip = Join-Path $DistDir "wrkspc-$RustTarget.zip"
    Compress-Archive -Force -Path (Join-Path $Stage '*') -DestinationPath $Zip

    # Get-FileHash emits the same `hash  path` line the checksum
    # fragments use on the unix legs.
    $Hash = (Get-FileHash -Algorithm SHA256 -Path $Zip).Hash.ToLowerInvariant()
    Add-Content -Path (Join-Path $DistDir "SHA256SUMS-$RustTarget") -Value "$Hash  wrkspc-$RustTarget.zip"
    Write-Output "dist/wrkspc-$RustTarget.zip"
}
finally {
    Remove-Item -Recurse -Force $Stage -ErrorAction SilentlyContinue
}
