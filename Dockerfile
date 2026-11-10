# syntax=docker/dockerfile:1

# wrkspc hub — containerized coordinator image, built from source.
#
# Containerized hub deployments are a first-class wrkspc target: the hub
# role is a passive mTLS acceptor with no harness control, so it packs
# into a minimal non-root image cleanly. Build anywhere:
#
#   docker build -t wrkspc-hub:local .
#
# The image expects the operator to provide at run time (see
# deploy/README.md for the full recipe):
#   * the hub database directory          -> /data (owned by uid 65532)
#   * the server certificate chain + key  -> /tls (server.crt, server.key)
#   * the client-trust CA                 -> /ca (ca.pem)
#   * the operator-managed serial
#     deny-list (optional)                -> /ops/revoke.txt
#
# The runtime stage is distroless: glibc and the CA bundle only — no
# shell, no package manager, no utilities. There is nothing to exec
# into; inspect the container with `docker logs` / `docker debug` and
# monitor the hub's surfaces from the host instead. The lab deployment
# path (devlocal) may prebuild the Linux binary with cargo-zigbuild and
# copy it beside a slimmer Dockerfile; this file is the project-owned
# definition and builds from source, so any group can reproduce the
# image from a checkout alone. Published GHCR images wrap the same
# release binaries via `deploy/Dockerfile.publish` instead of
# recompiling (see `.github/workflows/release.yml`).

# ---- build stage: compile the workspace inside the container ----
# The builder rides a mutable tag deliberately: a digest pin is
# per-platform, and this Dockerfile builds both linux/amd64 and
# linux/arm64 from one context. The RUNTIME layer (the shipped
# boundary) is digest-pinned below, and every dependency is locked +
# audited, so the builder image's mutability does not change what
# ships.
FROM rust:1.99.1-slim-bookworm AS build
WORKDIR /src
COPY . .
# --locked (the container build must respect Cargo.lock like every
# other gate) and the same path-remapping policy as the justfile-level
# RUSTFLAGS, adjusted for the image's cargo home (T09).
ENV RUSTFLAGS="--remap-path-prefix=/usr/local/cargo/registry=/registry/src --remap-path-prefix=/usr/local/cargo/git=/git --remap-path-prefix=/src=/wrkspc"
RUN cargo build --release --locked -p wrkspc-cli --bin wrkspc \
    && mkdir -p /data \
    && touch /data/.keep \
    && chown 65532:65532 /data/.keep

# ---- runtime stage: distroless, non-root, no shell ----
FROM gcr.io/distroless/cc-debian12@sha256:e5d81ddde149641e2a9ba55be4545bc125c67de07508b03ba4c22e6eb0ded5aa
COPY --from=build --chown=65532:65532 /src/target/release/wrkspc /usr/local/bin/wrkspc
COPY --from=build --chown=65532:65532 /data /data
# License notice retention (the project license requires the notice to
# accompany every distribution format).
COPY LICENSE /NOTICES/LICENSE
USER 65532:65532
WORKDIR /data
EXPOSE 8443 8082 8083
# No in-image HEALTHCHECK: the distroless image carries no shell or
# curl, so the liveness probe is an external monitor against the UI's
# /api/health (a bare 401 still proves the process is serving). See
# deploy/README.md.
ENTRYPOINT ["/usr/local/bin/wrkspc"]
CMD ["hub", "run", "--help"]
