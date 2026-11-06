#!/bin/sh
# Publish the wrkspc-hub container image to GHCR.
# Exit 0: published, 1: refused, 2: pipeline error.
# Args: VERSION (semver, optional leading 'v' stripped).
# Env:  GITHUB_TOKEN, GITHUB_ACTOR, GITHUB_REPOSITORY_OWNER (set by Actions).
set -eu

fail() {
    printf 'publish-image: %s\n' "$*" >&2
    exit 2
}

refuse() {
    printf 'publish-image: %s\n' "$*" >&2
    exit 1
}

[ "$#" -eq 1 ] || fail 'usage: publish-image.sh VERSION'
version="${1#v}"

# Shape-check before the value reaches any shell construct: a tag or
# dispatch input is write-access-only, but a wrong shape must refuse
# loudly rather than produce a junk image tag. The whitelist requires
# digits between the dots — `1..3` and bare `.` runs are rejected, not
# just filtered by charset.
if ! printf '%s\n' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$'; then
    refuse "refusing non-semver version '$version'"
fi

: "${GITHUB_TOKEN:?publish-image: GITHUB_TOKEN is unset}"
: "${GITHUB_ACTOR:?publish-image: GITHUB_ACTOR is unset}"
: "${GITHUB_REPOSITORY_OWNER:?publish-image: GITHUB_REPOSITORY_OWNER is unset}"

image="ghcr.io/${GITHUB_REPOSITORY_OWNER}/wrkspc-hub"
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' 0

mkdir -p "$temporary/amd64/data" "$temporary/arm64/data"
tar -xzf "dist/wrkspc-x86_64-unknown-linux-gnu.tar.gz" -C "$temporary/amd64"
tar -xzf "dist/wrkspc-aarch64-unknown-linux-gnu.tar.gz" -C "$temporary/arm64"
touch "$temporary/amd64/data/.keep" "$temporary/arm64/data/.keep"

printf '%s' "$GITHUB_TOKEN" | docker login ghcr.io -u "$GITHUB_ACTOR" --password-stdin >/dev/null

# Two single-platform pushes (plain manifests, no attestation layer),
# merged into one multi-arch manifest list below. The owner segment
# follows the repository, so an org transfer republishes under the new
# namespace on the next tagged build.
docker buildx build --platform linux/amd64 --provenance=false \
    -f deploy/Dockerfile.publish \
    -t "${image}:v${version}-amd64" --push "$temporary/amd64"
docker buildx build --platform linux/arm64 --provenance=false \
    -f deploy/Dockerfile.publish \
    -t "${image}:v${version}-arm64" --push "$temporary/arm64"
docker buildx imagetools create \
    -t "${image}:v${version}" -t "${image}:latest" \
    "${image}:v${version}-amd64" "${image}:v${version}-arm64"

# Keyless cosign signature over the manifest-list digest. The binary is
# sha256-pinned (v3.1.4, linux-amd64, from cosign_checksums.txt); the
# signature uses the ambient GitHub Actions OIDC identity.
cosign_sha256='424e2c97d0b45fd7a6b608a87f090f35633f4c23ece5f092d48bba11aaf4bbfa'
curl -fsSL -o "$temporary/cosign" \
    'https://github.com/sigstore/cosign/releases/download/v3.1.4/cosign-linux-amd64'
printf '%s  %s\n' "$cosign_sha256" "$temporary/cosign" | sha256sum -c -
chmod +x "$temporary/cosign"
digest="$(docker buildx imagetools inspect "${image}:v${version}" --format '{{.Manifest.Digest}}')"
"$temporary/cosign" sign --yes "${image}@${digest}"
printf 'published %s:v%s (digest %s)\n' "$image" "$version" "$digest"
