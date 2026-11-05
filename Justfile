# =============================================================================
# wrkspc (Rust workspace)
#
# Ported from the owner's house pattern (postmaster). The toolchain is
# pinned to the repo's rust-toolchain.toml channel; every cargo
# invocation names it explicitly because mise's rust shim can resolve a
# different toolchain in background shells (publish-pain #4).
# =============================================================================

toolchain := "1.99.1"

# Pinned OSV Scanner (house battery `osv` gate; platform binary + sha256).
osv_scanner_version := "2.7.0"
osv_scanner_dir := justfile_directory() / "output" / "tools" / ("osv-scanner-" + osv_scanner_version)

# Path hygiene for shipping artifacts: compiler-emitted paths (panic
# locations, error chains, debug sections, build-script OUT_DIRs) are
# remapped to neutral forms so no builder-specific paths reach the
# binaries. cargo does NOT expand env vars in config rustflags and its
# `trim-paths` profile option is still unstable on the pinned toolchain,
# so the expansion happens here (machine-agnostic via just's builtins)
# and the flags ride the RUSTFLAGS env var every just-invoked build
# carries. Enforced by the `no-user-paths` gate recipe.
export RUSTFLAGS := "--remap-path-prefix=" + home_directory() + "/.cargo/registry/src=/registry/src --remap-path-prefix=" + home_directory() + "/.rustup=/rustup --remap-path-prefix=" + justfile_directory() + "=/wrkspc"

default:
    @just --list

# -----------------------------------------------------------------------------
# Gate recipes
# -----------------------------------------------------------------------------

# Formatting (writes)
fmt:
    cargo +{{toolchain}} fmt --all

fmt-check:
    cargo +{{toolchain}} fmt --all --check

# Compile check across the workspace, including tests/benches
check:
    cargo +{{toolchain}} check --locked --workspace --all-targets

# Clippy with warnings denied (dev profile)
clippy:
    cargo +{{toolchain}} clippy --locked --workspace --all-targets -- -D warnings

# Release-profile clippy (house battery gate)
clippy-release:
    cargo +{{toolchain}} clippy --locked --workspace --all-targets --release -- -D warnings

# Linux-target clippy (house battery gate). The target's std comes from
# the `targets` entry in rust-toolchain.toml; the bundled sqlite builds
# through scripts/zig-cc-shim.sh (plain cargo has no zigbuild's internal
# cc shim). Clippy only type-checks, so no cross linker is needed.
clippy-linux:
    CC_x86_64_unknown_linux_gnu="{{justfile_directory()}}/scripts/zig-cc-shim.sh" cargo +{{toolchain}} clippy --locked --workspace --all-targets --target x86_64-unknown-linux-gnu -- -D warnings

build:
    cargo +{{toolchain}} build --locked --workspace

test:
    cargo +{{toolchain}} test --locked --workspace --all-targets

# Dependency policy
deny-check:
    cargo deny check

audit:
    cargo audit --deny warnings

# OSV gate: scans the workspace Cargo.lock with the pinned osv-scanner
# and enforces live, dated exceptions (.github/security-exceptions.json).
# Ported from the tech-radar house pattern, Rust-specific.
osv: _osv-scanner
    sh .github/scripts/audit.sh "{{ osv_scanner_dir }}/osv-scanner"

# Exercise the OSV gate's scanner failures and exception policy without
# network access.
test-osv:
    sh .github/scripts/audit-test.sh

# Container image (hub role, from source)
docker-build:
    docker build -t wrkspc-hub:local .

# Full quality gate (house battery): formatting, compile check, three
# clippy profiles, tests, dependency policy, and the no-user-paths
# binary audit.
quality: fmt-check check clippy clippy-release clippy-linux test no-user-paths deny-check audit osv

# CI battery (house naming): the gate GitHub's ci.yml runs on every
# push and PR. Kept deliberately lighter than `quality`; the full
# battery gates releases and the security workflow.
ci-rust: fmt-check clippy clippy-release clippy-linux build test

# Markdown lint (house recipe): every .md in the repo, pinned CLI.
lint-md:
    npx --yes markdownlint-cli2@0.23.2 "**/*.md"

# -----------------------------------------------------------------------------
# Docs (mdBook) — docs/README.md conventions
# -----------------------------------------------------------------------------

# Build the book. CHANGELOG.md and deploy/README.md are copied into
# docs/src for the build (single source of truth stays at the repo root).
docs:
    #!/usr/bin/env bash
    set -euo pipefail
    cp CHANGELOG.md docs/src/changelog.md
    cp deploy/README.md docs/src/hub-container-deployment.md
    mdbook build docs
    rm -f docs/src/changelog.md docs/src/hub-container-deployment.md
    echo "Documentation built: docs/book/"

# Serve the book locally with hot reload.
docs-serve:
    #!/usr/bin/env bash
    set -euo pipefail
    cp CHANGELOG.md docs/src/changelog.md
    cp deploy/README.md docs/src/hub-container-deployment.md
    trap 'rm -f docs/src/changelog.md docs/src/hub-container-deployment.md' EXIT
    (cd docs && mdbook serve --open)

# Shipping-artifact audit: the release binary must not contain builder
# home paths (/Users/... or /home/...) from panic locations, error
# chains, or debug sections. Pinned by the RUSTFLAGS remaps above plus
# [profile.release] strip; this fails the gate if either regresses.
no-user-paths:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo +{{toolchain}} build --locked --release -p wrkspc-cli --bin wrkspc
    hits=$(strings target/release/wrkspc | grep -cE '/(Users|home)/' || true)
    if [ "$hits" -ne 0 ]; then
        echo "no-user-paths FAILED: $hits builder-path strings in target/release/wrkspc"
        strings target/release/wrkspc | grep -E '/(Users|home)/' | sort -u | head -5
        exit 1
    fi
    echo "no-user-paths: clean"

# =============================================================================
# Build Matrix — cross-platform builds with varying optimization levels
# =============================================================================
#
# Optimization Levels:
#   debug     - Fast compile, no optimizations, debug symbols (for development)
#   release   - Cargo defaults (opt-level=3, no LTO)
#   optimized - Thin LTO + single codegen unit (good balance of size/speed)
#   max       - Fat LTO + strip + CPU targeting (smallest, fastest binary)
#
# Targets:
#   native     - Current platform
#   linux      - x86_64-unknown-linux-gnu (docker-lab), via cargo-zigbuild
#   linux-arm  - aarch64-unknown-linux-gnu (ARM64 Linux), via cargo-zigbuild
#
# The Linux targets build with cargo-zigbuild (zig 0.16) — the proven
# wrkspc cross-build path; no rustup std for the build itself is needed.
#
# Usage:
#   just build-matrix optimized linux     # Optimized build for docker-lab
#   just build-matrix max native          # Max optimization for this host
#   just build-matrix release             # Release build for current platform
# =============================================================================

# Build with specific optimization level, optional target
build-matrix level target="native":
    #!/usr/bin/env bash
    set -euo pipefail

    LEVEL="{{level}}"
    TARGET="{{target}}"

    # Validate optimization level
    case "$LEVEL" in
        debug|release|optimized|max) ;;
        *)
            echo "Error: Invalid optimization level '$LEVEL'"
            echo "Valid levels: debug, release, optimized, max"
            exit 1
            ;;
    esac

    # Map target alias to Rust target triple
    case "$TARGET" in
        native)     RUST_TARGET="" ;;
        linux)      RUST_TARGET="x86_64-unknown-linux-gnu" ;;
        linux-arm)  RUST_TARGET="aarch64-unknown-linux-gnu" ;;
        *)
            echo "Error: Invalid target '$TARGET'"
            echo "Valid targets: native, linux, linux-arm"
            exit 1
            ;;
    esac

    # Set optimization flags based on level
    case "$LEVEL" in
        debug)
            CARGO_ARGS=""
            export CARGO_PROFILE_DEV_OPT_LEVEL=0
            echo "Building: debug (fast compile, no optimizations)"
            ;;
        release)
            CARGO_ARGS="--release"
            echo "Building: release (Cargo defaults)"
            ;;
        optimized)
            CARGO_ARGS="--release"
            export CARGO_PROFILE_RELEASE_LTO=thin
            export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
            echo "Building: optimized (LTO=thin, codegen-units=1)"
            ;;
        max)
            CARGO_ARGS="--release"
            export CARGO_PROFILE_RELEASE_LTO=fat
            export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
            export CARGO_PROFILE_RELEASE_OPT_LEVEL=3
            export CARGO_PROFILE_RELEASE_STRIP=symbols
            # CPU targeting: `native` describes the build HOST, so only
            # apply it when the target IS the host. Cross targets get no
            # target-cpu override (toolchain baseline).
            if [[ "$TARGET" == "native" ]]; then
                case "$(uname -m)" in
                    arm64|aarch64) export RUSTFLAGS="-C target-cpu=native" ;;
                    *)             export RUSTFLAGS="-C target-cpu=x86-64-v3" ;;
                esac
            fi
            echo "Building: max (LTO=fat, strip, target-cpu optimized)"
            ;;
    esac

    # Build command: Linux targets cross-build via cargo-zigbuild; the
    # mise rust shim needs the explicit RUSTUP_TOOLCHAIN pin.
    if [[ -n "$RUST_TARGET" ]]; then
        echo "Target: $RUST_TARGET (cargo-zigbuild)"
        RUSTUP_TOOLCHAIN="{{toolchain}}" cargo zigbuild --locked -p wrkspc-cli --bin wrkspc $CARGO_ARGS --target "$RUST_TARGET"
        echo "Output: target/$RUST_TARGET/$([ "$LEVEL" = "debug" ] && echo "debug" || echo "release")/wrkspc"
    else
        echo "Target: native ($(rustc +{{toolchain}} -vV | grep host | cut -d' ' -f2))"
        cargo +{{toolchain}} build --locked $CARGO_ARGS -p wrkspc-cli --bin wrkspc
        echo "Output: target/$([ "$LEVEL" = "debug" ] && echo "debug" || echo "release")/wrkspc"
    fi

# =============================================================================
# Node deploy (macOS) — see deploy/node.md
# =============================================================================

# Deploy target/release/wrkspc to a supervised macOS node (wrkspc-node.app
# launcher or launchd job) and wait for its UI; needs `just quality` first.
# Usage: just deploy-node [ssh-host]  (no host = this Mac)
deploy-node $host="":
    #!/usr/bin/env bash
    set -euo pipefail
    binary=target/release/wrkspc
    # Keychain grants follow the signature; changing this re-prompts.
    sign_id=io.beardedeagle.wrkspc
    # The launchd job label, on nodes that launchd supervises.
    node_job=io.beardedeagle.wrkspc-node
    if [[ ! -f "$binary" ]]; then
        echo "deploy-node: $binary is missing; run 'just quality' first" >&2
        exit 1
    fi
    if [[ -n "$host" && ! "$host" =~ ^[A-Za-z0-9][A-Za-z0-9._@-]*$ ]]; then
        echo "deploy-node: '$host' is not a plain ssh host" >&2
        exit 1
    fi

    # Runs on the node (here, or fed to `bash -s` over ssh). Refuses when
    # nothing would restart the node (no wrkspc-node.app launcher, no
    # launchd job); otherwise signs the staged binary, keeps a rollback
    # copy, swaps it in, restarts the node through its supervisor (the
    # launcher wins when both exist), and waits for the node's UI.
    install_node() {
        set -euo pipefail
        local sign_id=$1 job
        job="gui/$(id -u)/$2"
        local bin="$HOME/.local/bin/wrkspc" staged="$HOME/.local/bin/wrkspc.new"
        local where supervisor old pid alive _
        where=$(hostname -s)
        if pgrep -f '[w]rkspc-node.app/Contents/MacOS/launcher' >/dev/null; then
            supervisor=launcher
        elif launchctl print "$job" >/dev/null 2>&1; then
            supervisor=launchd
        else
            rm -f "$staged"
            echo "deploy-node: no wrkspc-node.app launcher and no launchd job $job on $where; nothing would restart the node, refusing" >&2
            return 1
        fi
        codesign -f -s - -i "$sign_id" "$staged"
        codesign -v "$staged"
        # An identical redeploy keeps the last real rollback point.
        if [[ -f "$bin" ]] && ! cmp -s "$bin" "$staged"; then
            cp -f "$bin" "$bin.prev"
        fi
        mv -f "$staged" "$bin"
        old=$(pgrep -f '[w]rkspc node run' || true)
        if [[ $supervisor == launchd ]]; then
            launchctl kickstart -k "$job"
        elif [[ -n "$old" ]]; then
            pkill -f '[w]rkspc node run' || true
        fi
        for _ in $(seq 75); do
            alive=0
            for pid in $old; do
                if kill -0 "$pid" 2>/dev/null; then alive=1; fi
            done
            if (( ! alive )) && curl -fsS -o /dev/null --max-time 2 http://127.0.0.1:8083/ 2>/dev/null; then
                echo "deploy-node: $where is serving the new binary"
                return 0
            fi
            sleep 2
        done
        echo "deploy-node: $where is not serving after 150 s; a Keychain prompt may be waiting there (click Allow; rerun this deploy if the node stopped). Roll back: mv -f $bin.prev $bin" >&2
        return 1
    }

    if [[ -z "$host" ]]; then
        cp "$binary" "$HOME/.local/bin/wrkspc.new"
        install_node "$sign_id" "$node_job"
    else
        scp -q "$binary" "$host:.local/bin/wrkspc.new"
        ssh -o BatchMode=yes "$host" bash -s -- "$sign_id" "$node_job" <<< "$(declare -f install_node); install_node \"\$1\" \"\$2\""
    fi

# =============================================================================
# Release packaging — GitHub release tarballs (.github/workflows/release.yml)
# =============================================================================

# Package one platform's release binary as a tarball in dist/ with a
# per-artifact sha256. Native targets build with plain cargo; Linux
# cross-targets build via cargo-zigbuild, matching `build-matrix`.
release-tarball target="native":
    #!/usr/bin/env bash
    set -euo pipefail
    TARGET="{{target}}"
    mkdir -p dist
    case "$TARGET" in
        native)
            cargo +{{toolchain}} build --locked --release -p wrkspc-cli --bin wrkspc
            bin="target/release/wrkspc"
            name="$(rustc +{{toolchain}} -vV | grep host | cut -d' ' -f2)"
            ;;
        *linux*)
            RUSTUP_TOOLCHAIN="{{toolchain}}" cargo zigbuild --locked --release -p wrkspc-cli --bin wrkspc --target "$TARGET"
            bin="target/$TARGET/release/wrkspc"
            name="$TARGET"
            ;;
        *)
            echo "release-tarball: unsupported target '$TARGET' (use 'native' or a linux triple)" >&2
            exit 1
            ;;
    esac
    tar -C "$(dirname "$bin")" -czf "dist/wrkspc-${name}.tar.gz" wrkspc
    (cd dist && shasum -a 256 "wrkspc-${name}.tar.gz" >> "SHA256SUMS-${name}")
    echo "dist/wrkspc-${name}.tar.gz"

# Consolidate the per-artifact checksums into the release SHA256SUMS.
release-checksum:
    #!/usr/bin/env bash
    set -euo pipefail
    (cd dist && cat SHA256SUMS-* 2>/dev/null | sort -k2 > SHA256SUMS && rm -f SHA256SUMS-*)
    cat dist/SHA256SUMS

# Sign the release tarballs with minisign. MINISIGN_KEY_FILE names the
# secret key; without one the artifacts stay unsigned (warned, exit 0).
release-sign:
    #!/usr/bin/env bash
    set -euo pipefail
    key="${MINISIGN_KEY_FILE:-$HOME/.local/share/wrkspc/release/minisign.key}"
    if [[ ! -f "$key" ]]; then
        echo "release-sign: no minisign key at $key (set MINISIGN_KEY_FILE); artifacts are unsigned" >&2
        exit 0
    fi
    count=0
    for f in dist/wrkspc-*.tar.gz; do
        minisign -S -s "$key" -m "$f" -x "$f.minisig"
        count=$((count + 1))
    done
    echo "release-sign: signed $count tarballs"

# Create (or attach to) the GitHub release for a version. The version
# comes from a maintainer-pushed tag or a workflow_dispatch input, both
# write-access-only; it is still shape-checked before touching the shell.
release-publish version:
    #!/usr/bin/env bash
    set -euo pipefail
    VERSION="{{version}}"
    [[ "$VERSION" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
        echo "release-publish: invalid version '$VERSION' (want X.Y.Z, optional v prefix)" >&2
        exit 1
    }
    gh release create "$VERSION" --title "wrkspc $VERSION" --generate-notes dist/*

# Publish the wrkspc-hub image to GHCR (the release pipeline's
# container leg). VERSION is a maintainer-pushed tag or a
# workflow_dispatch input, both write-access-only; the script
# shape-checks it before touching any shell construct. Expects the
# linux tarballs in dist/ and the Actions GITHUB_* env vars.
release-image version:
    #!/usr/bin/env bash
    set -euo pipefail
    .github/scripts/publish-image.sh "{{version}}"

# =============================================================================
# OSV Scanner (pinned) — used by the `osv` gate
# =============================================================================

# Installs the pinned osv-scanner release binary (platform + sha256
# verified) under output/tools; a no-op when it is already present.
[private]
_osv-scanner:
    #!/usr/bin/env bash
    set -euo pipefail
    dir="{{ osv_scanner_dir }}"
    if [[ -x "$dir/osv-scanner" ]]; then exit 0; fi
    case "$(uname -s)" in
        Darwin)
            case "$(uname -m)" in
                arm64) platform=darwin_arm64; checksum=d4926ef8da134332543be276dff157e4c170f99ad91fc48e89448a8816bdf70b ;;
                *)     platform=darwin_amd64; checksum=e82c5df7ab0a42dff29a1cd0660711447b56b4c0483ff66ad8634df10fd81a45 ;;
            esac ;;
        Linux)
            case "$(uname -m)" in
                x86_64) platform=linux_amd64; checksum=160c6253e2922e9e0a6a9771c5c4c40a4b3c6090b34a3cdd5e80c96cc2e4218b ;;
                *)      echo 'osv-scanner: unsupported Linux arch' >&2; exit 1 ;;
            esac ;;
        *) echo 'osv-scanner: unsupported platform' >&2; exit 1 ;;
    esac
    temporary="$(mktemp -d)"
    trap 'rm -rf "$temporary"' EXIT
    curl --fail --silent --show-error --location --retry 3 \
        "https://github.com/google/osv-scanner/releases/download/v{{ osv_scanner_version }}/osv-scanner_$platform" -o "$temporary/osv-scanner"
    if command -v sha256sum >/dev/null; then
        actual="$(sha256sum "$temporary/osv-scanner" | cut -d ' ' -f 1)"
    else
        actual="$(shasum -a 256 "$temporary/osv-scanner" | cut -d ' ' -f 1)"
    fi
    [[ "$actual" == "$checksum" ]] || { echo 'osv-scanner checksum mismatch.' >&2; exit 1; }
    mkdir -p "$dir"
    mv "$temporary/osv-scanner" "$dir/osv-scanner"
    chmod +x "$dir/osv-scanner"
    "$dir/osv-scanner" --version
