#Requires -Version 5.1
<#
.SYNOPSIS
    wrkspc installer + guided setup (Windows x86_64 / arm64).

.DESCRIPTION
    Resolves the release (Version, default: latest) and this machine's
    platform asset, downloads the archive and SHA256SUMS, verifies the
    checksum and the Sigstore attestation (when gh is authenticated),
    installs wrkspc.exe into InstallDir, and walks through first-run
    setup: PKI, bindings.toml, and the assembled hub/node commands.

    Never writes secrets to shell profiles; tokens are handled by the
    wrkspc binary (env override WRKSPC_UI_TOKEN for services, or the
    audited store). PKI generation needs openssl (Git for Windows
    ships one) or certificates you supply yourself.

.PARAMETER Version
    Release tag (default: latest).

.PARAMETER BaseUrl
    Release download base (default: GitHub releases; override for
    testing).

.PARAMETER InstallDir
    Target directory (default: $env:LOCALAPPDATA\wrkspc\bin).

.PARAMETER NoSetup
    Install only; skip the guided setup.

.PARAMETER Yes
    Accept every default without prompting.

.PARAMETER NoAttestation
    Skip Sigstore verification (checksum verification still runs).
#>
[CmdletBinding()]
param(
    [string]$Version = "latest",
    [string]$BaseUrl = "https://github.com/beardedeagle/wrkspc/releases/download",
    [string]$InstallDir = "$env:LOCALAPPDATA\wrkspc\bin",
    [switch]$NoSetup,
    [switch]$Yes,
    [switch]$NoAttestation
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$Repo = "beardedeagle/wrkspc"
$ApiBase = "https://api.github.com/repos/$Repo"

function Write-Step([string]$Message) { Write-Host "==> $Message" }

function Invoke-Ask([string]$Prompt, [string]$Default) {
    if ($Yes) { return $Default }
    $answer = Read-Host "$Prompt [$Default]"
    if ([string]::IsNullOrWhiteSpace($answer)) { return $Default }
    return $answer
}

function Resolve-Target {
    $arch = $env:PROCESSOR_ARCHITECTURE
    switch ($arch) {
        "ARM64" { return "wrkspc-aarch64-pc-windows-msvc" }
        "AMD64" { return "wrkspc-x86_64-pc-windows-msvc" }
        default { throw "unsupported Windows architecture: $arch" }
    }
}

function Resolve-Version {
    if ($Version -ne "latest") { return $Version }
    if ($BaseUrl -notlike "https://github.com/*/releases/download") {
        throw "Version is required when BaseUrl is overridden"
    }
    Write-Step "resolving latest release"
    $release = Invoke-RestMethod -Uri "$ApiBase/releases/latest" -Headers @{ "User-Agent" = "wrkspc-installer" }
    if (-not $release.tag_name) { throw "no published release found for $Repo" }
    return $release.tag_name
}

function Install-Binary {
    $target = Resolve-Target
    $version = Resolve-Version
    $archive = "$target.zip"

    $temp = Join-Path $env:TEMP ("wrkspc-install-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $temp | Out-Null
    try {
        Write-Step "downloading $archive ($version)"
        Invoke-WebRequest -Uri "$BaseUrl/$version/SHA256SUMS" -OutFile (Join-Path $temp "SHA256SUMS")
        Invoke-WebRequest -Uri "$BaseUrl/$version/$archive" -OutFile (Join-Path $temp $archive)

        Write-Step "verifying checksum"
        $expectedLine = Get-Content (Join-Path $temp "SHA256SUMS") |
            Where-Object { $_.TrimEnd() -match "$([regex]::Escape($archive))$" } |
            Select-Object -First 1
        if (-not $expectedLine) { throw "$archive is missing from SHA256SUMS" }
        $expected = ($expectedLine -split "\s+")[0].ToLowerInvariant()
        $actual = (Get-FileHash -Algorithm SHA256 -Path (Join-Path $temp $archive)).Hash.ToLowerInvariant()
        if ($expected -ne $actual) { throw "checksum mismatch for $archive" }

        if (-not $NoAttestation -and (Get-Command gh -ErrorAction SilentlyContinue) -and (gh auth status 2>$null)) {
            Write-Step "verifying Sigstore attestation"
            Push-Location $temp
            try {
                $null = gh attestation verify $archive --repo $Repo 2>&1 | ForEach-Object { "$_" }
                if ($LASTEXITCODE -ne 0) { throw "attestation verification failed -- refusing to install" }
            } finally { Pop-Location }
        } elseif (-not $NoAttestation) {
            Write-Step "gh not authenticated -- attestation skipped (checksum still verified)"
        }

        Write-Step "extracting"
        Expand-Archive -Path (Join-Path $temp $archive) -DestinationPath (Join-Path $temp "out") -Force
        $binary = Join-Path $temp "out\wrkspc.exe"
        if (-not (Test-Path $binary)) { throw "archive does not contain wrkspc.exe" }

        New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
        Copy-Item -Path $binary -Destination (Join-Path $InstallDir "wrkspc.exe") -Force
        Write-Step "installed to $(Join-Path $InstallDir 'wrkspc.exe')"
        # stderr must merge into the pipeline: PS 5.1 turns native stderr
        # into a terminating NativeCommandError under ErrorActionPreference
        # Stop (wrkspc's tracing writes to stderr).
        & (Join-Path $InstallDir "wrkspc.exe") --version 2>&1 | ForEach-Object { Write-Host "$_" }

        $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
        if (-not $userPath) { $userPath = "" }
        if ($userPath.Split(";") -notcontains $InstallDir) {
            [Environment]::SetEnvironmentVariable(
                "Path", ($userPath.TrimEnd(";") + ";" + $InstallDir), "User")
            Write-Host "note: added $InstallDir to your user PATH (new terminals pick it up)"
        }
    } finally {
        Remove-Item -Recurse -Force $temp -ErrorAction SilentlyContinue
    }
}

$WrkspcShare = if ($env:WRKSPC_SHARE) { $env:WRKSPC_SHARE } else { "$env:LOCALAPPDATA\wrkspc" }
$PkiDir = Join-Path $WrkspcShare "pki"

function Find-OpenSsl {
    $candidates = @(
        (Get-Command openssl -ErrorAction SilentlyContinue),
        (Get-Command "C:\Program Files\Git\usr\bin\openssl.exe" -ErrorAction SilentlyContinue)
    )
    foreach ($candidate in $candidates) {
        if ($candidate) { return $candidate.Source }
    }
    return $null
}

function Test-HostName([string]$Value) {
    # Letters, digits, dots, hyphens, underscores: safe for certificate
    # subjects/SANs and host ids, and the ONLY values that may reach
    # the cmd /c openssl string (see Invoke-OpenSsl).
    if ($Value -notmatch '^[A-Za-z0-9._-]+$') {
        throw "invalid name (letters, digits, dots, hyphens, underscores only): $Value"
    }
}

function ConvertTo-TomlString([string]$Value) {
    $Value -replace '\\', '\\\\' -replace '"', '\"'
}

function Invoke-OpenSsl([string[]]$ArgumentList, [string]$Step) {
    # cmd /c: PowerShell 5.1 raises a terminating NativeCommandError
    # for native stderr even when merged through a pipeline (observed
    # live with openssl x509 on the box; genrsa/req happen to pass).
    # SAFE because every user-provided value reaches this string only
    # after Test-HostName -- letters, digits, dots, hyphens,
    # underscores -- none of which cmd re-parses. Keep that invariant:
    # an unvalidated interpolation added here is a command-injection
    # regression.
    $joined = ($ArgumentList -join " ")
    $null = cmd /c "`"$script:OpenSslExe`" $joined 2>NUL"
    if ($LASTEXITCODE -ne 0) { throw "openssl failed ($Step)" }
}

function Invoke-SetupPki {
    if ((Invoke-Ask "Generate a CA and node/hub certificates? (y/n)" "y") -ne "y") { return }
    $script:OpenSslExe = Find-OpenSsl
    if (-not $script:OpenSslExe) {
        Write-Host "openssl is not on this machine. Bring existing certificates or install"
        Write-Host "openssl (Git for Windows ships one: 'C:\Program Files\Git\usr\bin\openssl.exe')."
        return
    }
    New-Item -ItemType Directory -Path $PkiDir -Force | Out-Null
    Push-Location $PkiDir
    try {
        if (-not (Test-Path "ca.pem")) {
            $caCn = Invoke-Ask "CA common name" "wrkspc-ca"
            Test-HostName $caCn
            Invoke-OpenSsl @("genrsa", "-out", "ca.key", "4096") "CA key"
            Invoke-OpenSsl @("req", "-x509", "-new", "-nodes", "-key", "ca.key", "-sha256", "-days", "3650", "-subj", "/CN=$caCn", "-out", "ca.pem") "CA cert"
            Write-Step "CA written to $PkiDir (ca.pem, ca.key)"
        } elseif ((Invoke-Ask "CA already exists at $PkiDir\ca.pem -- reuse it? (y/n)" "y") -ne "y") {
            return
        }

        if (-not (Test-Path "hub.crt")) {
            $hubHost = Invoke-Ask "Hub hostname (the --card-url name; certificate SAN)" "hub.example.com"
            Test-HostName $hubHost
            Invoke-OpenSsl @("genrsa", "-out", "hub.key", "2048") "hub key"
            Invoke-OpenSsl @("req", "-new", "-key", "hub.key", "-subj", "/CN=$hubHost", "-addext", "subjectAltName=DNS:$hubHost", "-out", "hub.csr") "hub CSR"
            Invoke-OpenSsl @("x509", "-req", "-in", "hub.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-days", "825", "-sha256", "-out", "hub.crt") "hub cert"
            Remove-Item hub.csr -ErrorAction SilentlyContinue
            Write-Step "hub certificate written ($PkiDir\hub.crt, hub.key)"
        }

        if (-not (Test-Path "node.crt")) {
            $hostId = Invoke-Ask "Node host id (the client certificate SAN)" "this-machine.example"
            Test-HostName $hostId
            Invoke-OpenSsl @("genrsa", "-out", "node.key", "2048") "node key"
            Invoke-OpenSsl @("req", "-new", "-key", "node.key", "-subj", "/CN=$hostId", "-addext", "subjectAltName=DNS:$hostId", "-out", "node.csr") "node CSR"
            Invoke-OpenSsl @("x509", "-req", "-in", "node.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-days", "825", "-sha256", "-out", "node.crt") "node cert"
            Remove-Item node.csr -ErrorAction SilentlyContinue
            Write-Step "node certificate written ($PkiDir\node.crt, node.key)"
        }
    } finally { Pop-Location }
}

function Invoke-SetupNode {
    if ((Invoke-Ask "Set up a node here? (y/n)" "y") -ne "y") { return }
    $nodeDir = Join-Path $WrkspcShare "node"
    New-Item -ItemType Directory -Path $nodeDir -Force | Out-Null

    $bindings = Join-Path $nodeDir "bindings.toml"
    if (-not (Test-Path $bindings)) {
        $harness = Invoke-Ask "Harness to bind (codex|claude|gemini|pi|qwen|kimi|mcode)" "codex"
        $cwd = Invoke-Ask "Project directory for the binding (cwd)" $env:USERPROFILE
        switch ($harness) {
            "codex" { $adapter = "npx -y @agentclientprotocol/codex-acp@2.1.1" }
            "claude" { $adapter = "npx -y @agentclientprotocol/claude-agent-acp" }
            default { $adapter = Invoke-Ask "Adapter command for $harness" "" }
        }
        $cwdEscaped = ConvertTo-TomlString ($cwd -replace '\\', '/')
        $harnessEscaped = ConvertTo-TomlString $harness
        $adapterEscaped = ConvertTo-TomlString $adapter
        @"
[[binding]]
peer_id = "$harnessEscaped"
harness = "$harnessEscaped"
cwd = "$cwdEscaped"
command = "$adapterEscaped"
"@ | Out-File -FilePath $bindings -Encoding utf8
        Write-Step "bindings.toml written to $bindings"
    }

    $hubUrl = Invoke-Ask "Hub URL (the node dials this over mTLS)" "https://hub.example.com:8443"
    $hostId = Invoke-Ask "Node host id (must match the client certificate SAN)" "this-machine.example"
    Write-Step "the node is started with (once the hub is reachable):"
    Write-Host @"
    wrkspc node run `
        --db $nodeDir\node.db `
        --bindings $bindings `
        --hub $hubUrl `
        --host-id $hostId `
        --tls-cert $PkiDir\node.crt `
        --tls-key $PkiDir\node.key `
        --tls-ca $PkiDir\ca.pem
"@
}

function Invoke-SetupHub {
    if ((Invoke-Ask "Run the hub on this machine too? (y/n)" "n") -ne "y") { return }
    $db = Invoke-Ask "Hub database path" (Join-Path $WrkspcShare "hub\hub.db")
    $cardUrl = Invoke-Ask "External card URL (must match the hub certificate)" "https://hub.example.com:8443"
    $bind = Invoke-Ask "A2A/UI listener bind address" "0.0.0.0:8443"
    Write-Step "the hub is started with:"
    Write-Host @"
    wrkspc hub run `
        --db $db `
        --a2a $bind `
        --card-url $cardUrl `
        --tls-cert $PkiDir\hub.crt `
        --tls-key $PkiDir\hub.key `
        --tls-ca $PkiDir\ca.pem
"@
}

function Invoke-SetupSkills {
    if ((Invoke-Ask "Install the wrkspc skill into a harness now? (y/n)" "y") -ne "y") { return }
    $harness = Invoke-Ask "Harness for the skill install" "codex"
    & (Join-Path $InstallDir "wrkspc.exe") skill install --harness $harness 2>&1 | ForEach-Object { Write-Host "$_" }
}

function Show-TokenPointer {
    Write-Host @"
UI token: the operator UI's bearer token is managed by wrkspc itself --
run `wrkspc ui-token --db <hub db>` to print or copy it. For a
supervised/container hub, put WRKSPC_UI_TOKEN in the service's
environment (never a user profile script) -- the env override is
authoritative while set.
"@
}

Write-Step "wrkspc installer -- $Repo"
Install-Binary
if (-not $NoSetup) {
    Invoke-SetupPki
    Invoke-SetupHub
    Invoke-SetupNode
    if ($env:WRKSPC_NO_SKILLS -ne "1") { Invoke-SetupSkills }
    Show-TokenPointer
}
Write-Step "done -- see https://beardedeagle.github.io/wrkspc/getting-started/quick-start.html"
