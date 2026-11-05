# Installation

wrkspc ships as a single static-ish binary per platform, plus a
container image for the hub role. macOS and Linux are first-class
today; Windows rides `cargo install` with no extra work.

## Release tarballs

Each release publishes per-platform tarballs with a `SHA256SUMS` file
and minisign signatures. Verify before you run:

```console
$ shasum -a 256 wrkspc-<target>.tar.gz
$ minisign -Vm wrkspc-<target>.tar.gz
```

macOS note: binaries fetched with `curl` or built locally carry no
quarantine attribute, so Gatekeeper leaves them alone. A tarball
downloaded through a browser gets quarantined; remove it per-artifact
(`xattr -d com.apple.quarantine wrkspc`) or use "Open Anyway" in
System Settings → Privacy & Security. Never disable Gatekeeper
system-wide (`spctl --master-disable`) — that is a security downgrade,
not a workaround.

## Hub: container image

The hub role ships as a hardened, distroless container image built
from source. It runs read-only, as a non-root user, with no
capabilities, and mounts only what it needs: its database directory,
its TLS material, and its CA. See
[Hub container deployment](../hub-container-deployment.md)
for the build, mounts, ports, PKI, and path conventions.

## Node: macOS

Nodes are supervised by launchd (or the optional app launcher) so they
come back after a reboot and log to the system log:

```console
$ launchctl print gui/$(id -u)/io.beardedeagle.wrkspc-node
```

`just deploy-node [ssh-host]` builds, signs, stages with a rollback
copy, and restarts a supervised node — on this machine, or over ssh
when a host is given.

## Nodes elsewhere

Any machine that can reach the hub over TLS can run a node: install
the binary, enroll with hub-issued certificate material, and bind its
harnesses. The node dials the hub; no inbound port on the node is
required for conversation flow.
