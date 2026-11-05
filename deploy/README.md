# Containerized hub

The wrkspc hub role ships as a container image built from source by the
repository's `Dockerfile`. The hub is a passive mTLS acceptor with no
harness control, so the image is minimal and non-root.

## Build

```bash
docker build -t wrkspc-hub:local .
```

The build compiles the workspace inside a `rust:1.99-slim-bookworm`
stage (matching `rust-toolchain.toml`) and copies the single static
binary into a distroless runtime (`cc-debian12`, digest-pinned): glibc
and the CA bundle only — no shell, no package manager, no utilities,
running as the distroless `nonroot` user (uid 65532).

## Published image (GHCR)

Release builds publish a prebuilt-binary image to the GitHub Container
Registry as `ghcr.io/<owner>/wrkspc-hub` — the owner segment follows
the repository, so an org transfer moves the published package with
it. Tags: `vX.Y.Z` (and `latest` for the most recent release),
multi-arch (`linux/amd64`, `linux/arm64`), cosign keyless-signed
against the GitHub Actions OIDC identity.

```bash
docker pull ghcr.io/beardedeagle/wrkspc-hub:latest
```

The published image is assembled by `.github/workflows/release.yml`
from the same `just release-tarball` binaries the signed release
ships — the registry image never recompiles the workspace in CI.
`deploy/Dockerfile.publish` is that thin wrapper (binary + the
digest-pinned distroless runtime); the root `Dockerfile` (from source)
remains the reproducibility definition and the local `just
docker-build` path. Everything in this README — mounts, ports, TLS
posture, observability — applies to the published image unchanged.

## Run

The container expects four mounts, mirroring the wrkspc HLD's D-6
hosting record:

| Mount | Purpose |
| --- | --- |
| `/data` (writable) | the hub database (`wrkspc-hub.db` + WAL + self-generated UI token) |
| `/tls` (read-only) | `server.crt`, `server.key` — the hub's serving chain + key |
| `/ca` (read-only) | `ca.pem` — the CA root(s) client certificates must chain to |
| `/ops` (read-only, optional) | `revoke.txt` — operator-managed serial deny-list |

Minimal run (plain pilot mode, no TLS):

```bash
docker run --rm -p 8443:8443 \
  -v wrkspc-hub-data:/data \
  wrkspc-hub:local \
  hub run --db /data/wrkspc-hub.db --a2a 0.0.0.0:8082 --ui 0.0.0.0:8083
```

Full mTLS run:

```bash
docker run --rm -p 8443:8443 \
  -v wrkspc-hub-data:/data \
  -v ./tls:/tls:ro -v ./ca:/ca:ro -v ./ops:/ops:ro \
  wrkspc-hub:local \
  hub run --db /data/wrkspc-hub.db \
    --a2a 0.0.0.0:8082 --ui 0.0.0.0:8083 \
    --tls-cert /tls/server.crt --tls-key /tls/server.key --tls-ca /ca/ca.pem \
    --revoke-list /ops/revoke.txt \
    --card-url https://your-hub-fqdn:8443
```

The hub terminates TLS itself (hub-native rustls); the client
certificate must reach the accept loop end-to-end, so do not place a
terminating proxy in front of it.

`deploy/docker-compose.example.yml` shows the same shape declaratively,
hardened: `cap_drop: [ALL]`, `no-new-privileges`, read-only rootfs
(only `/data` writable), tmpfs `/tmp`. The lab's docker-lab deployment
keeps its own compose overlay (lab mounts and PKI paths) and may
prebuild the binary with cargo-zigbuild for faster iteration; the image
this Dockerfile produces is identical in behavior.

## Ports

All three ports are configurable; nothing is baked into the image:

| Variable | Default | Meaning |
| --- | --- | --- |
| `HUB_A2A_PORT` | `8082` | the container-internal A2A/TLS listener (`--a2a`); in TLS mode this listener serves the mTLS surface **and** the operator UI on the same port (path-routed) |
| `HUB_UI_PORT` | `8083` | the optional plaintext operator UI listener (`--ui`), for local use |
| `HUB_PUBLIC_PORT` | `8443` | the host port published for the TLS surface |

In TLS mode one published port carries everything: `/.well-known/*`,
`/jsonrpc`, `/peers/*`, `/presence`, `/roster`, and the delivery routes
require a client certificate (the hub requests one but does not require
it at the handshake; A2A routes reject connections that presented none),
while `/` and `/api/*` serve the operator UI behind the bearer token for
the browser. The separate `--ui` listener stays available for a
loopback-only operator surface.

The hub CLI accepts the same values as flags (`--a2a`, `--ui`, and
`--card-url` for the externally visible URL), so non-compose deployments
configure them the same way. `EXPOSE` lines in the image are metadata
only. The card URL must match the published port, or A2A clients that
read the card will dial the wrong address.

## Observability

The distroless runtime has no shell, so there is nothing to exec into.
Inspect with `docker logs`, and run the liveness probe from the host:

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8083/api/health
```

The endpoint answers 401 without a token, which still proves the
process is serving — treat any HTTP response as healthy and a
connection failure as unhealthy. The lab's compose overlay defines its
healthcheck the same way from the host side.

## Standard OS paths (non-container deploys)

The container mounts map onto standard OS paths for a bare-metal or VM
deploy of `wrkspc hub` (systemd-managed on Linux):

| Container mount | Standard path | Content |
| --- | --- | --- |
| `/data` | `/var/lib/wrkspc/` | `wrkspc-hub.db` + WAL + the self-generated `<db>.ui-token` (service-owned, 0700) |
| `/tls` | `/etc/wrkspc/tls/` | `server.crt`, `server.key` (root-owned, 0640/0600, readable by the service user) |
| `/ca` | `/etc/wrkspc/ca/` | `ca.pem` — the client-certificate trust root |
| `/ops` | `/etc/wrkspc/ops/` | `revoke.txt` — operator-managed serial deny-list |

The service user must be able to read everything under `/etc/wrkspc`
that the hub opens (`tls`/`ca`/`ops`) and write only `/var/lib/wrkspc`.

## Reverse proxies

The hub terminates TLS itself and its A2A surface relies on the
client-certificate identity stamp, so it must not sit behind a
TLS-terminating proxy (Caddy, nginx, ALB) — header forwarding breaks
cert-identity attribution. Where a load balancer is in front, use
L4/TLS passthrough (e.g. AWS NLB with TCP listeners).

## Cross-building release binaries

Release binaries for the hub image can be built from macOS with
cargo-zigbuild:

```bash
RUSTUP_TOOLCHAIN=1.99.0 cargo zigbuild -p wrkspc-cli --bin wrkspc \
  --target x86_64-unknown-linux-gnu --release
```

The explicit `RUSTUP_TOOLCHAIN` matters: mise's rust shim can resolve a
different toolchain than the repo's `rust-toolchain.toml` in background
shells, which fails with E0463. The repo's `just build-matrix
<level> <target>` recipes set it for you (see the `Justfile`).
