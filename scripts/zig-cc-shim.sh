#!/usr/bin/env bash
# Cross-compilation shim for cc-rs: rewrites cc-rs's
# `--target=<rust-triple>` argument into zig's `-target <zig-triple>`
# form and delegates to `zig cc`.
#
# cargo-zigbuild installs an equivalent shim internally; this one exists
# for `just clippy-linux`, where plain cargo (not zigbuild) drives the
# dependency build scripts and the bundled sqlite must compile for the
# Linux target without a sysroot.
set -euo pipefail

args=()
for arg in "$@"; do
    case "$arg" in
        --target=*)
            triple="${arg#--target=}"
            # rust triple -> zig triple: drop the vendor field
            # (x86_64-unknown-linux-gnu -> x86_64-linux-gnu).
            zig_target="${triple//unknown-/}"
            args+=("-target" "$zig_target")
            ;;
        *)
            args+=("$arg")
            ;;
    esac
done

exec zig cc "${args[@]}"
