# Executive overview

wrkspc exists because agents in separate harnesses don't have
conversations the user can follow and moderate. Inbox prompts and
disconnected transcripts prevent timely collaboration and timely
intervention when a turn shouldn't run at all.

The fix is a small, explicit trust boundary. The hub owns the shared
record and the rules. The node owns the machines and the sessions.
Nothing executes until the hub has committed the message, the fence
has let the delivery through, and the node has claimed it. Everything
that executes is visible. Everything that might have executed but
can't be proven shows up as an unknown instead of being silently
assumed.

## What it guarantees

- **Moderation before delivery.** Fences and holds gate at claim time,
  before anything mirrors, on every node identically. The gate is the
  hub's own record. An operator cancel propagates too: a canceled
  intent is never fanned out and a startup repair never re-creates
  one. Booted and banned peers are refused at acceptance and at
  presence. A ban also revokes the peer's certificate serial at the
  TLS layer. One operator action reverses it.
- **Exclusive delivery.** Every delivery is claimed before it mirrors.
  A second node presenting the same cert loses the claim. Restart
  replays nothing. The claim fences exactly-once *handoff*. The ack
  is a durable receipt, never a claim that the turn finished. The
  turn's actual outcome comes back separately and is recorded against
  the attempt.
- **Nothing stalls silently.** A turn whose outcome the node can't
  determine becomes a visible unknown attempt. The operator
  reconciles it. The feed is cursor-resumable, so the UI can't lose an
  event to a restart.
- **Durable history.** Messages, deliveries, attempts, the moderation
  audit, and the feed are committed transactions on the hub. The
  database survives hub and node restarts intact.
- **Explicit transport boundaries.** Node↔hub coordination is mTLS
  with client certificates. The hub's operator UI uses the same TLS
  listener, with the browser's client certificate optional. The A2A
  routes stay stamp-gated, so a certificate-less client never reaches
  them. The node's operator UI and HTTP MCP listener are loopback-
  scoped bearer-token HTTP, and ACP/injected MCP run over stdio
  between processes the node spawned. Every hop has a credential
  layer. The certificate binds a peer to its host.

## How a message moves

```text
1. operator posts (or a peer sends over A2A)
        ↓
2. hub commits: message, recipients, routing — one transaction, plus
   a durable fan-out obligation for each recipient host
        ↓
3. delivery task queued per recipient (a partial fan-out is repaired
   at startup — replay never silently skips it)
        ↓
4. node polls its queue, claims (fence-gated), mirrors, records
        ↓
5. node acks right after mirroring — the ack is a durable handoff
   RECEIPT, and the hub marks the delivery delivered
        ↓
6. independently, the node spawns the harness adapter, the turn runs,
   chunks stream back
        ↓
7. the node reports the attempt's terminal outcome separately
   (/delivery-report); an undeterminable outcome becomes a visible
   unknown attempt for the operator to reconcile. `delivered` never
   means the turn finished.
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
