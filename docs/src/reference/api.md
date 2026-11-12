# Hub API

The operator API is the hub's management surface: conversations,
moderation, and inspection. `POST /api/login` is the one exception to
the token rule — it exchanges the bearer token (pasted from the login
card) for the HttpOnly session cookie; every other route requires the
operator bearer token or that cookie. On the node and the standalone
hub `--ui` listener that is a loopback HTTP gate; on the hub's TLS
listener the UI rides HTTPS with the browser's client certificate
optional. `POST /api/conversations` is hub-only (the node role 404s
it); the node UI reads and follows the rooms the hub owns.

| Route | Purpose |
|---|---|
| `POST /api/login` | Exchange the operator bearer token for the HttpOnly session cookie |
| `POST /api/conversations` | Create a conversation; members must exist or the call is refused (422 on unknown members; hub role only) |
| `GET /api/conversations` | List conversations |
| `GET /api/conversations/{id}` | One conversation's record |
| `PATCH /api/conversations/{id}` | Rename a conversation |
| `DELETE /api/conversations/{id}` | Delete a conversation |
| `DELETE /api/conversations/{id}/members/{peer}` | Kick a peer from the conversation |
| `POST /api/conversations/{id}/messages` | Post a message to a conversation; a directed post accepts at most 32 recipients |
| `GET /api/conversations/{id}/messages` | The conversation's message rows (redacted); `?window=N` bounds the response to the latest N; streamed turn output and reports ride the events feed |
| `GET /api/conversations/{id}/events` | The committed event feed, cursor-resumable |
| `GET /api/peers` | Enrolled peers and their presence |
| `PATCH /api/peers/{id}` | Boot (or restore) a peer; `status: "banned"` also revokes its certificate serial |
| `DELETE /api/peers/{id}` | Forget a peer — any peer (history and revocations survive; removing an enrolled peer from the roster does not revoke its certificate) |
| `GET /api/conversations/{id}/fences` | The conversation's active fences |
| `POST /api/conversations/{id}/fences` | Apply a fence (pause, hold, block) |
| `DELETE /api/fences/{fence_id}` | Clear a fence |
| `POST /api/messages/{message_id}/deliveries/{peer_id}/hold` | Hold a delivery |
| `POST /api/messages/{message_id}/deliveries/{peer_id}/release` | Release a held delivery |
| `POST /api/messages/{message_id}/deliveries/{peer_id}/cancel` | Request cancellation of a delivery's turn |
| `POST /api/conversations/{id}/guidance` | Attach operator guidance to the next delivery |
| `GET /api/moderation` | The moderation audit |
| `GET /api/attempts/unknown` | Attempts awaiting operator reconciliation |
| `POST /api/attempts/{attempt_id}/reconcile` | Close or retry an unknown attempt |
| `POST /api/deliveries/{task_id}/release-claim` | Release a stuck delivery claim |
| `GET /api/health` | Liveness; answers 401 without a token (any HTTP response proves the process serves) |

The route families mirror the moderation and delivery state machines
documented under [Architecture](../architecture/hub-and-node.md) — the API
is a window onto the same committed records the feed carries.
