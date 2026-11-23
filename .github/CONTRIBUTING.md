# Contributing to wrkspc

wrkspc is an open, Apache-2.0 Rust workspace: a shared workspace where
humans and AI agents coordinate across harnesses, with visible
delivery and human moderation. External contributions are welcome.

By submitting a contribution you agree that it is licensed under the
project's [Apache-2.0 license](../LICENSE) (inbound = outbound).

## Ground rules

- **The Justfile is the single source of truth.** Git hooks (lefthook)
  and CI are thin callers of `just` recipes — a new gate or CI step is
  a `just` recipe first. Don't add a check to CI that isn't one.
- **The bar is enforced, not aspirational.** Formatting is checked,
  clippy runs with `-D warnings`, the dependency policy is denied by
  cargo-deny, advisories are audited, and the release binary is
  checked for builder paths. CI will not go green on a warning.
- **Real tests, not coverage theater.** New behavior needs a test that
  would fail without the change. Match the unit/integration style of
  the crate you touch.
- **Docs are part of the change.** The book (docs/src) must match the
  code it describes: check `reference/api.md` against the router,
  `reference/cli.md` against the clap structs, and the harnesses page
  against the skill registry before claiming a docs sweep is done.
- **Docstrings are rustdoc, not Google style.** `# Errors` /
  `# Panics` / `# Safety` (mandatory on `unsafe fn`) / `# Examples`
  sections, intra-doc links in backticks, and a `cargo doc --no-deps
  --workspace` run with zero warnings before a docstring change is
  done.

## Getting set up

You need a recent stable Rust toolchain (`rustup`, pinned per
`rust-toolchain.toml`), [`just`](https://just.systems), and
[`lefthook`](https://lefthook.dev) for the git hooks.

```sh
git clone git@github.com:beardedeagle/wrkspc.git
cd wrkspc
lefthook install     # wire up the pre-commit / pre-push hooks
just --list          # see every available recipe
```

## The development loop

- `just quality` — the full battery: format check, the three clippy
  profiles (dev, release, linux target), tests, builder-path audit,
  dependency policy, advisory audit, OSV scan.
- `just ci-rust` — the CI battery (what every push runs).
- `just docs` — build the book with the embedded rustdoc API
  reference; `just lint-md` lints every markdown file.

Run `just quality` before you push — it is a superset of what CI runs.

## Submitting a change

1. Branch off `main` (`git switch -c fix/short-description`).
2. Make the change together with its tests. Keep the diff focused —
   one logical change per pull request.
3. Run `just quality` and make sure it is green.
4. Write a clear commit message: an imperative subject and a body that
   explains *why*. No AI attribution or co-author trailers.
5. Open a pull request against `main`, fill in the template, and
   describe how you verified the change.

Main is ruleset-protected: required checks (ci-rust, Build book,
zizmor, CodeQL, analyze) plus a code-owner review must pass before
merge. Do not push directly to main — the ruleset refuses it.

## Reporting bugs and requesting features

Use the issue templates. For anything security-sensitive, follow
[SECURITY.md](./SECURITY.md) instead of opening a public issue.
