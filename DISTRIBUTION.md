# Distribution

Every stable release (`vX.Y.Z` tag) produces:

- **Platform binaries** — `wrkspc-<platform>.tar.gz` for macOS (arm64
  and x86_64), Linux x86_64 (gnu and musl), and Linux arm64; Windows
  x86_64 and arm64 ship as `wrkspc-<platform>.zip`. Every archive holds
  the single `wrkspc` binary plus the project license.
- **`SHA256SUMS`** — one file covering every tarball in the release.
- **Sigstore attestations** — every tarball and the checksums file carry
  an attestation over their digest, issued to the release workflow's
  ambient OIDC identity. Keyless: no long-lived signing key exists
  anywhere.
- **A container image** — `ghcr.io/beardedeagle/wrkspc-hub:<version>`,
  signed keyless with cosign over the same ambient GitHub OIDC identity.

## Verifying a release

Verify each archive against its attestation before use (the commands are
identical for `.tar.gz` and `.zip`):

```console
# 1. The archive's digest is attested by the release workflow.
gh attestation verify wrkspc-<platform>.<ext> --repo beardedeagle/wrkspc

# 2. The checksums file is attested too; verify it, then check the archive.
gh attestation verify SHA256SUMS --repo beardedeagle/wrkspc
sha256sum -c --ignore-missing SHA256SUMS
```

A successful verification prints the attestation, the workflow identity,
and the artifact digest. Do not use a tarball that fails either check.

To verify the container image instead:

```console
cosign verify ghcr.io/beardedeagle/wrkspc-hub:<version> \
  --certificate-identity-regexp 'https://github.com/beardedeagle/wrkspc/.github/workflows/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

The operator can additionally sign tarballs locally with a private
minisign key (`just release-sign`); that key never enters GitHub and is
documented separately in the vault.

