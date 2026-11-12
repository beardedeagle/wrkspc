# Installation

wrkspc ships as a single binary per platform, plus a container image
for the hub role. Release binaries are published for macOS (arm64 and
x86_64), Linux (x86_64 glibc and musl, and arm64), and Windows (x86_64
and arm64 — Windows artifacts ship as `.zip`). There is no crates.io
package in the pre-release tree, so `cargo install wrkspc` does not
work anywhere yet.

## Release artifacts

Each release publishes per-platform archives (`.tar.gz`, `.zip` on
Windows) with a `SHA256SUMS` file, and every archive carries a Sigstore
attestation over its digest, issued to the release workflow's ambient
OIDC identity — keyless: no long-lived signing key exists. Verify
before you run:

```console
$ curl -fsSLO https://github.com/<owner>/wrkspc/releases/download/vX.Y.Z/SHA256SUMS
$ curl -fsSLO https://github.com/<owner>/wrkspc/releases/download/vX.Y.Z/wrkspc-<target>.tar.gz
$ gh attestation verify wrkspc-<target>.tar.gz --repo <owner>/wrkspc
$ gh attestation verify SHA256SUMS --repo <owner>/wrkspc
$ shasum -a 256 -c SHA256SUMS          # compares, do not just print
```

A successful attestation verification prints the workflow identity and
the artifact digest; do not run an archive that fails any of these
checks. The full instructions live in `DISTRIBUTION.md` at the repo
root.

macOS note: binaries fetched with `curl` or built locally carry no
quarantine attribute, so Gatekeeper leaves them alone. A tarball
downloaded through a browser gets quarantined; remove it per-artifact
(`xattr -d com.apple.quarantine wrkspc`) or use "Open Anyway" in
System Settings → Privacy & Security. Never disable Gatekeeper
system-wide (`spctl --master-disable`) — that is a security downgrade,
not a workaround.

## Hub: container image

The hub role ships as a minimal, distroless container image built
from source — non-root, no shell — and the example Compose file
selects the runtime hardening (`cap_drop: [ALL]`, `no-new-privileges`,
read-only rootfs with only `/data` writable); the image itself does
not enforce those, so bring them with you in any other runtime. See
[Hub container deployment](../hub-container-deployment.md)
for the build, mounts, ports, PKI, and path conventions.

## Node: macOS

Nodes are supervised by launchd (or the optional app launcher) so they
come back after a reboot and log to the system log:

```console
$ launchctl print gui/$(id -u)/io.beardedeagle.wrkspc-node
```

`just deploy-node [ssh-host]` deploys the already-built release binary
(run `just quality` first — the recipe deploys, it does not compile),
signs it, stages it with a rollback copy, and restarts a supervised
node — on this machine, or over ssh when a host is given. Readiness
waits for the OLD process to exit and a NEW process to own the node's
UI listener, so an unrelated process on the port never certifies the
deploy.

## Nodes elsewhere

Any machine that can reach the hub over TLS can run a node: install
the binary, enroll with hub-issued certificate material, and bind its
harnesses. The node dials the hub; no inbound port on the node is
required for conversation flow.
