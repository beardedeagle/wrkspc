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
export RUSTFLAGS := "--remap-path-prefix=" + home_directory() + "/.cargo/registry/src=/registry/src --remap-path-prefix=" + home_directory() + "/.cargo/git=/git --remap-path-prefix=" + home_directory() + "/.rustup=/rustup --remap-path-prefix=" + justfile_directory() + "=/wrkspc"

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
# On a native x86_64-linux host (the CI runners) target == host: skip
# the zig shim and use the system cc. Zig-compiled C objects can carry
# UBSan runtime references that the host's gcc dylib links (sqlx's proc
# macros) cannot resolve, so the shim is for genuine cross-compiles only.
clippy-linux:
    #!/usr/bin/env bash
    set -euo pipefail
    host="$(rustc +{{toolchain}} -vV | grep '^host' | cut -d' ' -f2)"
    if [[ "$host" == "x86_64-unknown-linux-gnu" ]]; then
        cargo +{{toolchain}} clippy --locked --workspace --all-targets --target x86_64-unknown-linux-gnu -- -D warnings
    else
        CC_x86_64_unknown_linux_gnu="{{justfile_directory()}}/scripts/zig-cc-shim.sh" \
            cargo +{{toolchain}} clippy --locked --workspace --all-targets --target x86_64-unknown-linux-gnu -- -D warnings
    fi

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
# network access. The suite's assertions run under jq, so the tool is
# provisioned explicitly (T14) instead of depending on an ambient copy.
test-osv: _jq
    sh .github/scripts/audit-test.sh

# A pinned jq for the OSV policy suite. Installed into .tools like the
# other house tools so the gate never trusts a preinstalled version.
_jq:
    #!/usr/bin/env bash
    set -euo pipefail
    if command -v jq >/dev/null 2>&1; then
        exit 0
    fi
    mkdir -p .tools
    os=$(uname -s | tr '[:upper:]' '[:lower:]')
    case "$os" in
        darwin) archive="jq-1.7.1/jq-osx-arm64" ;;
        linux) archive="jq-1.7.1/jq-linux64" ;;
        *)
            echo "test-osv: no pinned jq for $os — install jq and retry" >&2
            exit 1
            ;;
    esac
    curl -fsSL "https://github.com/jqlang/jq/releases/download/${archive}" -o .tools/jq
    chmod +x .tools/jq

# Container image (hub role, from source)
docker-build:
    docker build -t wrkspc-hub:local .

# Full quality gate (house battery): formatting, compile check, three
# clippy profiles, tests, dependency policy, the OSV policy regression
# suite (fail-closed scanner/exception paths), and the no-user-paths
# binary audit.
quality: fmt-check check clippy clippy-release clippy-linux test no-user-paths deny-check audit test-osv osv

# CI battery (house naming): the gate GitHub's ci.yml runs on every
# push and PR. Kept deliberately lighter than `quality`; the full
# battery gates releases and the security workflow. test-osv rides
# here too (T14): scanner-failure and exception-expiry policy must
# fail closed on every PR, not only during releases.
ci-rust: fmt-check clippy clippy-release clippy-linux build test test-osv

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
    # The copied pages are cleaned up on EVERY exit — a failed build
    # must not leave untracked copies behind.
    trap 'rm -f docs/src/changelog.md docs/src/hub-container-deployment.md' EXIT
    cp CHANGELOG.md docs/src/changelog.md
    cp deploy/README.md docs/src/hub-container-deployment.md
    mdbook build docs
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
    # Build the release profile with the gate's own remapped RUSTFLAGS
    # (see the header export): the audit examines the artifact these
    # flags produce, never a stale binary left by an ad-hoc build.
    cargo +{{toolchain}} build --locked --release -p wrkspc-cli --bin wrkspc
    # The binary's real artifact path: a user-supplied CARGO_TARGET_DIR
    # must not leave this recipe scanning a stale default location.
    profile_dir="release"
    if [ -n "${CARGO_TARGET_DIR:-}" ]; then
        binary="$CARGO_TARGET_DIR/$profile_dir/wrkspc"
    else
        binary="target/$profile_dir/wrkspc"
    fi
    if [ ! -f "$binary" ]; then
        echo "no-user-paths FAILED: artifact not found at $binary (is the profile set correctly?)"
        exit 1
    fi
    # Extraction is a separately checked command (T10): a failing
    # `strings` must FAIL the gate, never certify an unscanned binary.
    if ! extracted=$(mktemp); then
        echo "no-user-paths FAILED: cannot create the extraction temp file"
        exit 1
    fi
    trap 'rm -f "$extracted"' EXIT
    if ! strings "$binary" > "$extracted"; then
        echo "no-user-paths FAILED: strings could not extract from $binary"
        exit 1
    fi
    # grep's documented no-match status (1) is the only acceptable
    # "clean" outcome; any other failure must surface.
    set +e
    grep -nE '/(Users|home)/' "$extracted" > "$extracted.hits"
    grep_status=$?
    set -e
    if [ "$grep_status" -eq 1 ]; then
        echo "no-user-paths: clean"
    elif [ "$grep_status" -eq 0 ]; then
        echo "no-user-paths FAILED: builder-path strings in $binary"
        head -5 "$extracted.hits"
        exit 1
    else
        echo "no-user-paths FAILED: path scan errored (status $grep_status)"
        exit 1
    fi

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
                    # APPEND, never replace: the justfile-level
                    # RUSTFLAGS carry the path-remapping policy, and a
                    # plain export would silently drop it (T09).
                    arm64|aarch64) export RUSTFLAGS="${RUSTFLAGS:-} -C target-cpu=native" ;;
                    *)             export RUSTFLAGS="${RUSTFLAGS:-} -C target-cpu=x86-64-v3" ;;
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
        # Readiness is a NODE-specific handshake (T13): the old pid must
        # be gone AND a NEW process must OWN the UI listener — an
        # unrelated process (a hub, another node) serving the port must
        # never certify this deploy. lsof names the listener owner; the
        # node's default UI port is 8081 and this pilot's nodes bind
        # 8083, so both are checked.
        serving_pid() {
            lsof -nP -iTCP:"$1" -sTCP:LISTEN -t 2>/dev/null | head -1
        }
        for _ in $(seq 75); do
            alive=0
            for pid in $old; do
                if kill -0 "$pid" 2>/dev/null; then alive=1; fi
            done
            (( ! alive )) || { sleep 2; continue; }
            for port in 8081 8083; do
                sp=$(serving_pid "$port")
                if [[ -n "$sp" ]] && kill -0 "$sp" 2>/dev/null; then
                    if [[ " $old " == *" $sp "* ]]; then
                        echo "deploy-node: port $port is still held by the OLD process $sp; waiting" >&2
                    else
                        echo "deploy-node: $where is serving the new binary (pid $sp owns the UI listener on $port)"
                        return 0
                    fi
                fi
            done
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
    # LICENSE rides in every artifact (the project's own license
    # requires notice retention).
    stage="$(mktemp -d)"
    trap 'rm -rf "$stage"' EXIT
    cp "$bin" "$stage/wrkspc"
    cp LICENSE "$stage/LICENSE"
    tar -C "$stage" -czf "dist/wrkspc-${name}.tar.gz" wrkspc LICENSE
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
# write-access-only. Both values arrive as POSITIONAL arguments and are
# shape-checked before any use — nothing is interpolated into the
# script text ahead of validation. The SHA is the exact commit the
# packaged binaries were built from — the release is anchored to IT,
# never to the default branch's head (T11): a dispatch that raced new
# commits must not tag them.
release-publish version sha:
    #!/usr/bin/env bash
    set -euo pipefail
    # just interpolates declared parameters into the recipe text; it does
    # not forward positionals to shebang scripts (verified on just 1.57).
    VERSION="{{ version }}"
    SHA="{{ sha }}"
    [[ "$VERSION" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
        echo "release-publish: invalid version '$VERSION' (want X.Y.Z, optional v prefix)" >&2
        exit 1
    }
    [[ "$SHA" =~ ^[0-9a-f]{40}$ ]] || {
        echo "release-publish: invalid sha '$SHA'" >&2
        exit 1
    }
    # The release version must equal the workspace's declared version:
    # a tag naming one version must never wrap another's binaries.
    declared=$(awk -F'"' '/^version = /{print $2; exit}' Cargo.toml)
    [[ "$VERSION" == "v$declared" || "$VERSION" == "$declared" ]] || {
        echo "release-publish: version '$VERSION' does not match the workspace's declared version '$declared'" >&2
        exit 1
    }
    if gh release view "$VERSION" >/dev/null 2>&1; then
        # Attach to the existing release — but only when its tag points
        # at the very commit these binaries were built from.
        existing=$(git rev-parse "refs/tags/$VERSION^{}" 2>/dev/null || echo "")
        if [[ -z "$existing" ]]; then
            echo "release-publish: tag $VERSION exists but does not resolve to a commit; refusing" >&2
            exit 1
        fi
        if [[ "$existing" != "$SHA" ]]; then
            echo "release-publish: tag $VERSION points at $existing, the build was $SHA; refusing to attach" >&2
            exit 1
        fi
        gh release upload "$VERSION" dist/* --clobber
    else
        gh release create "$VERSION" --title "wrkspc $VERSION" --generate-notes --target "$SHA" dist/*
    fi

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
