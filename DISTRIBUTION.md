# Distribution

Every stable release (`vX.Y.Z` tag) produces:

- **Platform tarballs** — `wrkspc-<platform>.tar.gz` for macOS (arm64),
  Linux x86_64 (gnu and musl), and Linux arm64, each holding the single
  `wrkspc` binary.
- **`SHA256SUMS`** — one file covering every tarball in the release.
- **Minisign signatures** — one `.minisig` file per tarball, produced with
  the maintainer's release key during the release pipeline.
- **A container image** — `ghcr.io/beardedeagle/wrkspc-hub:<version>`,
  signed keyless with cosign over the ambient GitHub OIDC identity.

## Verifying a release

The release signing key's public half is committed in this repository at
[`keys/minisign.pub`](keys/minisign.pub). Verify each tarball before use:

```console
# 1. The tarball's checksum matches the published SHA256SUMS.
sha256sum -c --ignore-missing SHA256SUMS

# 2. The signature is valid and was made by this key.
minisign -Vm wrkspc-<platform>.tar.gz -P keys/minisign.pub
```

A successful `minisign -Vm` reports `Signature and comment signature
verified` and `Trusted comment: signature from minisign secret key`. Do not
use a tarball that fails either check.

To verify the container image instead:

```console
cosign verify ghcr.io/beardedeagle/wrkspc-hub:<version> \
  --certificate-identity-regexp 'https://github.com/beardedeagle/wrkspc/.github/workflows/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```
