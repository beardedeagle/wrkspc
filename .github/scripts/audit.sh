#!/bin/sh
# Scan the workspace Cargo.lock. Exit 0: accepted, 1: refused, 2: scan/policy error.
# Args: the pinned osv-scanner executable, supplied by `just osv`.
set -eu

fail() {
    printf 'audit: %s\n' "$*" >&2
    exit 2
}

[ "$#" -eq 1 ] || fail 'usage: audit.sh /path/to/osv-scanner'
command -v jq >/dev/null 2>&1 || fail 'jq is required; install jq 1.6 or newer'
scripts=$(CDPATH='' cd -P "$(dirname "$0")" && pwd -P)
root=$(pwd -P)
cd "$root"
if command -v cygpath >/dev/null 2>&1; then root=$(cygpath -m "$root"); fi
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' 0
trap 'exit 2' HUP INT TERM
mkdir -p output/audit
# A scan that fails must not leave a previous passing verdict behind.
rm -f output/audit/report.json output/audit/osv.json

# This gate's dated exceptions are the only allowed suppressions.
: > "$temporary/osv.toml"
status=0
"$1" scan source --config "$temporary/osv.toml" --all-packages \
    --no-call-analysis=all --format json --lockfile Cargo.lock \
    > "$temporary/osv.json" || status=$?
cp "$temporary/osv.json" output/audit/osv.json
case "$status" in
    0|1) ;;
    *) fail "OSV Scanner failed with exit $status" ;;
esac

if ! jq -s --arg root "$root" --arg today "$(date -u +%Y-%m-%d)" \
    --argjson scanner_status "$status" \
    --slurpfile policy .github/security-exceptions.json \
    -f "$scripts/audit.jq" "$temporary/osv.json" > "$temporary/report.json"; then
    fail 'invalid scanner output or exception policy'
fi
cp "$temporary/report.json" output/audit/report.json
jq -r '
    .findings[] | "\(.status): \(.package)@\(.version) [\(.ids | join(", "))]",
    (if .exception then "  \(.exception.reason) (accepted by \(.exception.accepted_by), expires \(.exception.expires_on))" else empty end)
' output/audit/report.json
jq -r '.stale[] | "STALE: \(.package) [\(.id)]: remove the unused exception"' output/audit/report.json
if ! jq -e 'all(.findings[]; .status == "ACCEPTED")' output/audit/report.json >/dev/null; then
    printf 'audit: refused; findings require live, dated exceptions\n' >&2
    exit 1
fi
printf 'audit: passed; workspace lockfile scanned (output/audit/report.json)\n'
