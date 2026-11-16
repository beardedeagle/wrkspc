# Rust API

The wrkspc codebase is a Rust workspace of product crates: the CLI,
the core store and moderation engine, and the ACP/A2A/MCP transports.
Their type-level documentation is generated from source at build time
and embedded in this site:

[Open the API reference](../rustdoc/wrkspc_core/index.html)

The rustdoc documents the current implementation. The crates are not
published to crates.io and carry no semver promise — they are an
internal, product-scoped surface. Treat the book's
[Protocols](./protocols.md) and [Hub API](./api.md) pages as the
stable contracts; treat the rustdoc as implementation documentation
for contributors and for integrators building directly against the
source.
