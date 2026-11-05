# Distribution

## Platforms

macOS and Linux first; Windows rides `cargo install` with no extra
work and gets signed artifacts later.

## Channels

- **`cargo install wrkspc`** — builds from source locally, so
  Gatekeeper friction is zero on macOS.
- **GitHub release tarballs** — per-platform, with `SHA256SUMS` and
  minisign signatures. Built in CI from the full `just quality` gate
  and the release profile; Linux targets cross-build with
  cargo-zigbuild.
- **Container image** — the hub role ships as a hardened distroless
  image, published to GHCR as
  `ghcr.io/<owner>/wrkspc-hub` (`vX.Y.Z` + `latest`, multi-arch
  `linux/amd64`/`linux/arm64`, cosign keyless-signed). The image is
  assembled from the same release binaries as the tarballs — no
  recompilation in CI — and the package follows the repository across
  an org transfer. See
  [Hub container deployment](../hub-container-deployment.md).

The release pipeline (`release.yml` plus the `release-tarball`,
`release-checksum`, `release-sign`, and `release-publish` recipes) is
in place; signing keys and the first published release land with the
first tagged version.

## macOS signing

Tarballs ship unsigned until Apple Developer Program enrollment is
complete. The practical impact is small and specific: binaries
fetched with `curl` or built locally carry no quarantine attribute,
so Gatekeeper leaves them alone. Browser downloads get quarantined;
the documented, per-artifact removal is the supported workaround.
Enrollment buys a Developer ID and notarization — one durable
identity per app — which replaces ad-hoc signing and retires the
per-binary prompt class for good.
