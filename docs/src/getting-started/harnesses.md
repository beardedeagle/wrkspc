# Harnesses

A harness is the agent host on a machine — Codex, Antigravity, Pi.
wrkspc never links against a harness; it spawns the harness's ACP
adapter as a child process and drives the session over stdio. That is
the whole integration surface.

## Registered harnesses

`wrkspc skill install` knows three harnesses out of the box:

| Harness | Adapter | Notes |
|---|---|---|
| `codex` | `codex-acp` (npm) | Spawned via `npx`; the binding pins the version |
| `antigravity` | `antigravity-acp` | Gemini's harness; per-session model pinning is configured through it |
| `pi` | `pi-acp` | ACP adapter for the Pi coding agent |

Unknown harnesses still work: anything with an ACP-capable command
binds the same way. The registry entry only controls what `skill
install` knows how to set up.

## How a session runs

1. The hub commits a delivery for the node.
2. The node claims it, then spawns the binding's `command`.
3. The adapter initializes the session; wrkspc streams the prompt in
   and the turn's chunks out.
4. On completion, the node reports the attempt; the hub marks the
   delivery delivered.

The node owns the child's lifecycle. A harness that dies mid-turn
becomes a failed attempt; a turn whose outcome can't be determined
becomes an unknown attempt, and the operator reconciles it.

## Antigravity MCP registration

Antigravity's MCP configuration is machine-global on current
versions. wrkspc registers its MCP tools once per machine through the
mechanism Antigravity provides, and every Antigravity session on that
machine can see them. Per-project scoping is a harness-side
limitation, not something wrkspc can supply.
