#!/bin/sh
# Exercise the gate with scanner responses, without network access.
set -eu

scripts=$(CDPATH='' cd -P "$(dirname "$0")" && pwd -P)
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' 0
trap 'exit 2' HUP INT TERM
fixture="$temporary/repo with spaces"
mkdir -p "$fixture/.github"
printf 'version = 4\n\n[[package]]\nname = "example-lib"\nversion = "1.0.0"\n' > "$fixture/Cargo.lock"
export AUDIT_TEST_OUTPUT="$temporary/scanner.json" AUDIT_TEST_EXIT=0
export AUDIT_TEST_ARGS="$temporary/args.json"
cat > "$temporary/scanner" <<'SH'
#!/bin/sh
printf '%s\n' "$@" | jq -Rs 'split("\n")[:-1]' > "$AUDIT_TEST_ARGS"
cat "$AUDIT_TEST_OUTPUT"
exit "$AUDIT_TEST_EXIT"
SH
chmod +x "$temporary/scanner"
root=$(cd "$fixture" && pwd -P)
if command -v cygpath >/dev/null 2>&1; then root=$(cygpath -m "$root"); fi
jq -n --arg root "$root" '{results: ["/Cargo.lock"] | map({
    source: {path: ($root + .), type: "lockfile"},
    packages: [{package: {name: "example-lib", version: "1.0.0", ecosystem: "crates.io"}}]
})}' > "$temporary/clean.json"
jq '.results[0].packages[0] += {
    vulnerabilities: [{id: "RUSTSEC-TEST-1", aliases: ["CVE-TEST-1"]}],
    groups: [{ids: ["RUSTSEC-TEST-1"]}]
}' "$temporary/clean.json" > "$temporary/vulnerable.json"

reset() {
    cp "$temporary/clean.json" "$AUDIT_TEST_OUTPUT"
    printf '{"exceptions": []}\n' > "$fixture/.github/security-exceptions.json"
    AUDIT_TEST_EXIT=0
}

exception() {
    jq -n --arg expires "$1" '{exceptions: [{
        id: "CVE-TEST-1", package: "example-lib", reason: "Test fixture",
        accepted_by: "Maintainer", expires_on: $expires
    }]}' > "$fixture/.github/security-exceptions.json"
}

run() {
    status=0
    (cd "$fixture" && sh "$scripts/audit.sh" "$temporary/scanner") > "$temporary/log" 2>&1 || status=$?
    if [ "$status" -ne "$2" ]; then
        printf 'FAIL: %s: expected exit %s, got %s\n' "$1" "$2" "$status" >&2
        cat "$temporary/log" >&2
        exit 1
    fi
    printf 'ok: %s\n' "$1"
}

reset
run 'clean scan covers the workspace lockfile' 0
jq -e 'index("--all-packages") != null and index("--no-call-analysis=all") != null
    and index("Cargo.lock") != null
    and index("--config") != null' "$AUDIT_TEST_ARGS" >/dev/null

cp "$temporary/vulnerable.json" "$AUDIT_TEST_OUTPUT"
AUDIT_TEST_EXIT=1
run 'workspace dependency blocks the gate' 1
jq -e '.findings[0].status == "REFUSED" and .findings[0].package == "example-lib"' \
    "$fixture/output/audit/report.json" >/dev/null

exception 9999-12-31
run 'advisory alias matches a live exception' 0
jq -e '.findings[0].status == "ACCEPTED" and .findings[0].exception.accepted_by == "Maintainer"' \
    "$fixture/output/audit/report.json" >/dev/null

jq '.results[0].packages[0] |= (
    .vulnerabilities += [{id: "GHSA-TEST-1", aliases: ["CVE-TEST-1"]}]
    | .groups[0].ids += ["GHSA-TEST-1"]
)' "$temporary/vulnerable.json" > "$AUDIT_TEST_OUTPUT"
run 'aliases from multiple databases produce one verdict' 0
jq -e '.findings | length == 1 and .[0].ids == ["CVE-TEST-1", "GHSA-TEST-1", "RUSTSEC-TEST-1"]' \
    "$fixture/output/audit/report.json" >/dev/null
cp "$temporary/vulnerable.json" "$AUDIT_TEST_OUTPUT"

exception "$(date -u +%Y-%m-%d)"
run 'exception is valid through its UTC expiration date' 0
exception 2000-02-29
run 'expired exception blocks a finding' 1

exception 9999-12-31
jq '.exceptions[0].package = "example-other"' "$fixture/.github/security-exceptions.json" > "$temporary/changed.json"
cp "$temporary/changed.json" "$fixture/.github/security-exceptions.json"
run 'exception cannot suppress another dependency' 1

for expiry in 2025-02-29 2026-04-31 2026-13-01 2026-1-01; do
    exception "$expiry"
    run "invalid date $expiry is rejected" 2
done
exception 9999-12-31
jq '.exceptions += .exceptions' "$fixture/.github/security-exceptions.json" > "$temporary/changed.json"
cp "$temporary/changed.json" "$fixture/.github/security-exceptions.json"
run 'duplicate exceptions are rejected' 2
exception 9999-12-31
jq '.exceptions += [.exceptions[0] | .id = "RUSTSEC-TEST-1"]' "$fixture/.github/security-exceptions.json" > "$temporary/changed.json"
cp "$temporary/changed.json" "$fixture/.github/security-exceptions.json"
run 'overlapping alias exceptions are rejected' 2

reset
exception 9999-12-31
run 'unused exceptions are reported' 0
jq -e '.stale | length == 1' "$fixture/output/audit/report.json" >/dev/null

reset
for status in 127 128 130; do
    AUDIT_TEST_EXIT=$status
    run "scanner failure $status cannot pass with clean JSON" 2
    [ ! -e "$fixture/output/audit/report.json" ]
done
reset
AUDIT_TEST_EXIT=1
run 'scanner finding exit requires actual findings' 2
reset
cp "$temporary/vulnerable.json" "$AUDIT_TEST_OUTPUT"
run 'scanner clean exit cannot hide findings' 2
reset
for invalid in '' '{}' '{"results":null}' '{"results":[]}' '{"results":['; do
    printf '%s' "$invalid" > "$AUDIT_TEST_OUTPUT"
    run 'empty, incomplete or malformed scanner output is rejected' 2
done
reset
jq 'del(.results[0])' "$temporary/clean.json" > "$AUDIT_TEST_OUTPUT"
run 'missing lockfile scan is rejected' 2
jq '.results[0].packages = []' "$temporary/clean.json" > "$AUDIT_TEST_OUTPUT"
run 'empty lockfile inventory is rejected' 2
jq '.results[0].packages[0].vulnerabilities = null' "$temporary/clean.json" > "$AUDIT_TEST_OUTPUT"
run 'null vulnerabilities cannot be mistaken for no findings' 2
jq '.results[0].packages[0].groups = []' "$temporary/vulnerable.json" > "$AUDIT_TEST_OUTPUT"
run 'ungrouped advisories cannot disappear' 2

reset
printf '{"exceptions": []}\n{}\n' > "$fixture/.github/security-exceptions.json"
run 'multiple exception documents are rejected' 2
printf '{"exceptions": [{"id":"RUSTSEC-TEST-1"}]}\n' > "$fixture/.github/security-exceptions.json"
run 'incomplete exceptions are rejected' 2

printf 'audit-test: all scenarios passed\n'
