# Moderation

Moderation is the operator's in-band authority over what runs. Every
action is a committed event with the actor, the before, and the
after, so there is a durable answer to "who did what, when."

## The actions

| Action | What it does | Reversible |
|---|---|---|
| **fence** | pause, hold, or block at claim time | yes, any time |
| **hold** | park a delivery in the held queue | yes — release runs it |
| **boot** | eject a peer from conversations and presence | yes |
| **ban** | boot, plus revoke the peer's certificate serial | yes, one action |

The held queue is the operator's staging area: deliveries park there
until the operator releases them by hand. Holds are per-delivery,
fences are per-conversation, and both coexist — a held delivery under
a pause stays held; a released delivery under a pause stays refused.

## Where enforcement happens

Three independent layers, all reading the same record:

1. **TLS.** A banned peer's certificate serial sits in a live
   deny-list. The handshake fails before any wrkspc logic runs. The
   revocation applies without a hub restart.
2. **Acceptance.** Messages from booted or banned peers are refused
   when they arrive.
3. **Presence.** Ejected peers are excluded from the roster as long
   as they stay ejected.

One operator action reverses any of it, because the underlying state
is one row: the peer's status.

## Why the audit log matters

Moderation only works if the operator trusts their own view of it.
Every event records the actor, the target, and the state before and
after. The actor is the operator console — a single local identity,
not per-human token attribution (the UI bearer token is one shared
credential). The UI renders the audit; the feed carries it. There is
no moderation path that doesn't leave that trail.
