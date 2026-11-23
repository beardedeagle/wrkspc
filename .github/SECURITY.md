# Security Policy

## Supported Versions

wrkspc ships from a single supported line of development. Security
fixes land on the latest release and on `main`; there are no
separately maintained release branches. Always run the latest release.

| Version | Supported |
| ------- | --------- |
| latest release (`0.1.x`) | yes |
| `main` | yes |
| older tags | no |

## Reporting a Vulnerability

Report security issues privately. Do not open a public issue, pull
request, or discussion for security-sensitive material.

Use either private channel:

1. **GitHub private advisory (preferred).** Open a report at
   <https://github.com/beardedeagle/wrkspc/security/advisories/new>.
2. **Email.** Write to <randy@heroictek.com>.

Include:

- A description of the issue and its expected impact.
- The affected version or commit, operating system, and architecture.
- Reproduction steps or a minimal failing input when it is safe to
  share.
- A suggested remediation, when available.

Do not include credentials, private transcripts, database contents, or
other unnecessary sensitive data.

## System and Scope

wrkspc is a Rust workspace that ships one binary with two roles. The
**hub** owns the shared record — conversations, messages, deliveries,
moderation, membership, and the event feed — in a SQLite store, and
serves the A2A coordination surface and the operator UI. The **node**
runs per machine, enrolls over mTLS with a client certificate, polls
and claims deliveries (fence-gated), spawns harness ACP adapters, and
serves a loopback operator UI and MCP surface.

The trust boundary is explicit: the hub commits what should happen,
the node proves what did, and moderation gates delivery at claim time.
Node↔hub coordination is mTLS with client certificates; a banned
peer's certificate serial sits in a live deny-list. Credential-shaped
content is redacted before the feed reaches the UI, and every role
lowers core-dump limits before parsing TLS key material.

The operator is responsible for the environment the roles run in:
certificate issuance, the operator UI token, and what harness
sessions are bound and executed. Those controls must cover the actual
execution; moderation gates delivery, and execution of an operator-
gated turn is the node's intended function, not an enforcement gap.

Model output, prompts, tool arguments, message content, and bound
harness behavior must therefore be treated as untrusted data. Local
deployment does not make those inputs trusted.

## Reportable Findings and Severity Context

Reportable issues include bypass of the certificate-identity boundary,
a second node executing a delivery the claim fence should have
excluded, acceptance of messages from booted or banned peers,
redaction failure exposing credential material, injection through a
bindings.toml command or the skill installer, release-input
substitution or an attestation-verification bypass, and practical
resource exhaustion of a reachable surface.

Severity depends on the authority gained, affected asset, reachability
through a supported workflow, required operator action, and effective
containment. Documentation or local-only deployment do not by
themselves reduce a reachable boundary failure to non-reportable.

## Out of Scope and Limitations

Operator-authorized moderation actions and the execution of bound
harness sessions are intended functionality; their availability alone
does not establish a vulnerability. Findings remain in scope when an
implementation executes a different operation, exposes data beyond the
requested result, mishandles credentials or state, or bypasses an
actual enforced boundary. Intended functionality still warrants
surrounding hardening; specific residual risks require an explicit
owner decision.

## What to Expect

- An acknowledgement that the report was received, typically within a
  few days.
- An initial validity and severity assessment, with follow-up
  questions when needed.
- Private remediation and disclosure coordination for accepted
  findings, with credit unless the reporter requests anonymity.
