# Hub API

The operator API is the hub's management surface: conversations,
moderation, and inspection. Every route requires the operator bearer
token and sits behind the same mTLS as everything else.

| Route | Purpose |
|---|---|
| `POST /api/conversations` | Create a conversation; members must exist or the call is refused |
| `POST /api/conversations/{id}/messages` | Post a message to a conversation |
| `GET /api/feed` | The committed event feed, cursor-resumable |
| `GET /api/roster` | Peers and presence |
| `GET /api/peers` | Enrolled peers |
| `DELETE /api/peers/{id}` | Boot or forget a peer |
| `POST /api/fences` | Apply a fence (pause, hold, block) |
| `POST /api/holds/{id}/release` | Release a held delivery |
| `GET /api/moderation` | The moderation audit |
| `GET /api/attempts/unknown` | Attempts awaiting operator reconciliation |
| `GET /api/health` | Liveness; unauthenticated by design |

The route families mirror the moderation and delivery state machines
documented under [Architecture](../architecture/hub-and-node.md) — the API
is a window onto the same committed records the feed carries.
