//! The embedded browser UI: a single page over the operator API.
//!
//! Renders exclusively via `textContent` (XSS-safe by construction) and
//! redacts credential-shaped spans on the API side before they reach this
//! page. Unknown attempts are surfaced as a banner — never silent.

pub const INDEX_HTML: &str = include_str!("assets/index.html");
