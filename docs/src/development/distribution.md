# Distribution

## Platforms

Seven release binaries: macOS (arm64 and x86_64), Linux (x86_64 glibc
and musl, and arm64), and Windows (x86_64 and arm64). Windows builds
run natively with the runner's MSVC toolchain and ship as `.zip`;
everything else ships as `.tar.gz`. There is no crates.io package yet:
the installable package is `wrkspc-cli`, and it is not published to
the registry until the versioned-release work lands.

## Channels

- **Checkout build** — `cargo build --release --locked` from a clone
  gives zero Gatekeeper friction on macOS.
- **GitHub release artifacts** — per-platform archives with
  `SHA256SUMS` and Sigstore attestations over every digest (keyless,
  issued to the release workflow's OIDC identity). Built in CI from
  the full `just quality` gate and the release profile (thin LTO);
  Linux targets cross-build with cargo-zigbuild. The GitHub Release
  and the container image both publish only after the operator's
  approval on the protected `release-signing` environment.
- **Container image** — the hub role ships as a hardened distroless
  image, published to GHCR as
  `ghcr.io/<owner>/wrkspc-hub` (`vX.Y.Z` + `latest`, multi-arch
  `linux/amd64`/`linux/arm64`, cosign keyless-signed), after the
  operator's approval. The image is
  assembled from the same release binaries as the archives — no
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
in place; `release-sign` remains an optional operator-side minisign
path whose key never enters GitHub.

## macOS signing

Artifacts carry keyless attestations (integrity and workflow-identity
provenance), not Apple Developer ID signatures, so Gatekeeper's
quarantine behavior is unchanged: binaries fetched with `curl` or
built locally carry no quarantine attribute and are left alone, while
browser downloads get quarantined. The documented, per-artifact
removal is the supported workaround.
