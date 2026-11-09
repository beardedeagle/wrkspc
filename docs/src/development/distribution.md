# Distribution

## Platforms

macOS and Linux first; Windows builds from a checkout the same way
(no crates.io package in the pre-release tree) and gets signed
artifacts later.

## Channels

- **Checkout build** — `cargo build --release --locked` from a clone
  gives zero Gatekeeper friction on macOS. There is no crates.io
  package yet: the installable package is `wrkspc-cli`, and it is not
  published to the registry until the versioned-release work lands.
- **GitHub release tarballs** — per-platform, with `SHA256SUMS` and
  minisign signatures. Built in CI from the full `just quality` gate
  and the release profile; Linux targets cross-build with
  cargo-zigbuild. Signatures exist from the first tagged release
  onward (the signing key is generated for that release; earlier
  artifacts are checksum-only).
- **Container image** — the hub role ships as a hardened distroless
  image, published to GHCR as
  `ghcr.io/<owner>/wrkspc-hub` (`vX.Y.Z` + `latest`, multi-arch
  `linux/amd64`/`linux/arm64`, cosign keyless-signed). The image is
  assembled from the same release binaries as the tarballs — no
  recompilation in CI. Verify an image signature with:

  ```console
  $ cosign verify       --certificate-identity https://github.com/<owner>/wrkspc/.github/workflows/release.yml@refs/tags/vX.Y.Z       --certificate-oidc-issuer https://token.actions.githubusercontent.com       ghcr.io/<owner>/wrkspc-hub@<digest>
  ```

  The signature uses the ambient GitHub Actions OIDC identity; the
  certificate identity is the release workflow at the exact tag the
  artifacts were built from. See
  [Hub container deployment](../hub-container-deployment.md).

The release pipeline (`release.yml` plus the `release-tarball`,
`release-checksum`, `release-sign`, and `release-publish` recipes) is
in place; signing keys and the first published release land with the
first tagged version.

## macOS signing

Tarballs ship unsigned for now. The practical impact is small and
specific: binaries fetched with `curl` or built locally carry no
quarantine attribute, so Gatekeeper leaves them alone. Browser
downloads get quarantined; the documented, per-artifact removal is
the supported workaround.
