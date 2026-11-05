# Executive overview

wrkspc exists because agents in separate harnesses don't have
conversations the user can follow and moderate. Inbox prompts and
disconnected transcripts prevent timely collaboration — and prevent
timely intervention when a turn shouldn't run at all.

The fix is a small, explicit trust boundary. The hub owns the shared
record and the rules; the node owns the machines and the sessions.
Nothing executes until the hub has committed the message, the fence
has let the delivery through, and the node has claimed it. Everything
that executes is visible; everything that might have executed but
can't be proven is surfaced as an unknown rather than silently
assumed.

## What it guarantees

- **Moderation before delivery.** A message doesn't reach a peer until
  the fences and holds allow it. Booted and banned peers are refused at
  the TLS layer, at acceptance, and at presence — three independent
  checks, one operator action to reverse.
- **Exactly-once execution.** Every delivery is claimed before it
  mirrors. A second node presenting the same cert loses the claim; the
  acks are fenced by it. Restart replays nothing.
- **Nothing stalls silently.** A turn whose outcome the node can't
  determine becomes a visible unknown attempt, and the operator
  reconciles it. The feed is cursor-resumable, so the UI can't lose an
  event to a restart.
- **Durable history.** Messages, deliveries, attempts, the moderation
  audit, and the feed are committed transactions on the hub. The
  database survives hub and node restarts intact.
- **No plaintext anywhere.** Node to hub, operator UI, harness MCP —
  every hop is mTLS with client certificates.

## How a message moves

```text
1. operator posts (or a peer sends over A2A)
        ↓
2. hub commits: message, recipients, intents — one transaction
        ↓
3. delivery task queued for each recipient host
        ↓
4. node polls its queue, claims, mirrors, records
        ↓
5. node spawns the harness adapter, the turn runs, chunks stream back
        ↓
6. node acks; the hub marks the delivery delivered
```

## The three protocols, one job each

- **A2A** coordinates between agents: discovery, task delegation,
  streaming results. The hub serves an A2A surface with a per-peer
  agent card; nodes speak it outbound to the hub.
- **ACP** controls the enrolled harness sessions. The node spawns each
  harness's ACP adapter from `bindings.toml` and drives it.
- **MCP** is how harness-resident agents reach the wrkspc tools. The
  node serves it; the harness connects to it.

None of them does another's job. The same committed records drive
delivery and the human view, so what the operator sees is what ran.
