# Gates and automation

`just` owns every repo lifecycle action. Git hooks and GitHub
workflows are thin callers of `just` recipes — nothing runs the
toolchain directly. A new gate or CI step is a `just` recipe first.

## The battery

```console
$ just fmt-check        # formatting, read-only
$ just check            # compile the whole workspace, all targets
$ just clippy           # clippy, warnings denied (dev profile)
$ just clippy-release   # the same, release profile
$ just clippy-linux     # the same, x86_64-linux target
$ just test             # the full test suite
$ just no-user-paths    # release binary carries no builder paths
$ just deny-check       # dependency policy (cargo-deny)
$ just audit            # vulnerability audit (cargo-audit)
$ just osv              # OSV scan, pinned scanner, dated exceptions
$ just quality          # the full battery, in order
$ just ci-rust          # the CI battery: fmt, three clippies, build, test
```

Path hygiene is enforced structurally: every `just`-invoked build
carries `--remap-path-prefix` so compiler-emitted paths never leak
builder directories into the binary, and `no-user-paths` fails the
gate if one ever does.

## Hooks

`lefthook.yml` runs the same recipes locally:

- **pre-commit** — `fmt-check`, `clippy`, `lint-md` (in parallel)
- **pre-push** — `ci-rust`, `deny-check`, `audit`, `osv`

So what passes a push locally is what the gates run in CI. Install
with `lefthook install`.

## CI

- `ci.yml` runs `just ci-rust` on every push and PR.
- `security.yml` runs `just deny-check`, `just audit`, and
  `just osv` daily and on demand.
- `docs.yml` builds the book (`just docs`, which also embeds the
  rustdoc API reference at `/rustdoc`) whenever `docs/**` or the
  changelog changes.
- `codeql.yml` scans the Rust workspace with CodeQL on push, PR, and
  a weekly schedule.
- `workflow-security.yml` audits the Actions definitions with pinned
  zizmor whenever they change.
- `release.yml` runs `just quality` as its gate, then packages the
  seven-leg release matrix (macOS arm64/x86_64, Linux x86_64
  glibc+musl and arm64, Windows x86_64/arm64 as `.zip`), emits
  Sigstore attestations over every artifact, and only then — after
  the operator's approval on the protected `release-signing`
  environment — publishes the GitHub Release, followed by the GHCR
  container image.
- Compiling jobs in `ci.yml` carry a hand-rolled cargo cache in two
  entries: the registry (plus the cargo-installed tools) keyed on the
  lockfile, and a per-job target dir keyed on the manifests, Justfile,
  toolchain pin, and workflow itself. The release pipeline consumes
  no caches at all — a release build uses only freshly fetched, pinned
  inputs, so restored cache entries cannot poison the published
  binaries. `actions/cache` only — this repo pins official actions,
  never third-party cache actions.

## Docs

```console
$ just docs          # build the book into docs/book/, rustdoc API reference embedded
$ just docs-serve    # serve it locally with reload
$ just lint-md       # markdown lint across the repo, pinned CLI
```

`CHANGELOG.md` at the repo root is the single source of truth;
`just docs` copies it into the book at build time. The same build
runs `cargo doc --no-deps --workspace --locked` and embeds the
result at `/rustdoc` in the published site — the Rust API reference
frames it as a no-semver, product-internal surface (see
[Rust API](../reference/rust-api.md)).
