# Harnesses

A harness is the agent host on a machine — Codex, Antigravity, Pi,
Claude Code, Qwen, Kimi, MiniMax.
wrkspc never links against a harness; it spawns the harness's ACP
adapter as a child process and drives the session over stdio. That is
the whole integration surface.

## Registered harnesses

`wrkspc skill install` targets the whole open agent-skills registry —
about 75 harnesses with a machine-level skills directory, detected by
their config paths. Only the binding cohort (the harnesses wrkspc can
actually DRIVE through ACP) gets the standing-instructions pointer
section; installing a skill into a harness is not the same as binding
a session to it. The bindable cohort:

| Harness | Adapter | Notes |
|---|---|---|
| `codex` | `codex-acp` (npm) | Spawned via `npx`; the binding pins the version |
| `claude` | `claude-agent-acp` (npm) | Spawned via `npx` (package `@agentclientprotocol/claude-agent-acp`); the skill installer also writes the standing-instructions pointer into `~/.claude/CLAUDE.md` (the skill copy itself lives at `.claude/skills/wrkspc/SKILL.md`) |
| `antigravity` | `antigravity-acp` | Gemini's harness; per-session model pinning is configured through it |
| `pi` | `pi-acp` | ACP adapter for the Pi coding agent |
| `qwen` | `qwen --acp` | Qwen Code's native ACP mode (v0.27+) |
| `kimi` | `kimi acp` | Kimi Code CLI's ACP server over stdio |
| `mcode` | `mcode acp` | MiniMax Code's ACP server; the binding's `cwd` must be a real project — session/new resolves a session project from it |

Unknown harnesses still work: anything with an ACP-capable command
binds the same way. The registry entry only controls what `skill
install` knows how to set up.

## How a session runs

1. The hub commits a delivery for the node.
2. The node claims it (fence-gated), then spawns the binding's
   `command`.
3. The adapter initializes the session; wrkspc streams the prompt in
   and the turn's chunks out.
4. The node acks when the delivery mirrors — the hub marks the
   delivery delivered as a handoff receipt.
5. When the turn ends, the node reports the attempt's terminal
   outcome separately; the receipt never stands in for it.

The node owns the child's lifecycle. A harness that dies mid-turn
becomes a failed attempt; a turn whose outcome can't be determined
becomes an unknown attempt, and the operator reconciles it.

## Antigravity MCP registration

Antigravity's MCP configuration is machine-global on current
versions. wrkspc registers its MCP tools once per machine through the
mechanism Antigravity provides, and every Antigravity session on that
machine can see them. Per-project scoping is a harness-side
limitation, not something wrkspc can supply.
