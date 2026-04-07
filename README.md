# opencode-cursor-oauth

OpenCode plugin that connects to Cursor's API, giving you access to Cursor
models inside OpenCode with full tool-calling support.

## Install in OpenCode

Add this to `~/.config/opencode/opencode.json`:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "plugin": [
    "opencode-cursor-oauth"
  ],
  "provider": {
    "cursor": {
      "name": "Cursor"
    }
  }
}
```

The `cursor` provider stub is required because OpenCode drops providers that do
not already exist in its bundled provider catalog.

## Authenticate

```sh
opencode auth login --provider cursor
```

This opens Cursor OAuth in the browser. Tokens are stored in
`~/.local/share/opencode/auth.json` and refreshed automatically.

## Use

Start OpenCode and select any Cursor model. The plugin starts a local
OpenAI-compatible proxy on demand and routes requests through Cursor's gRPC API.

## Features

- **Native tool redirection** — Cursor's built-in tools (read, write, delete,
  fetch, shell, shell stream, grep, ls) are intercepted and redirected to
  OpenCode's MCP equivalents. Read, write, delete, fetch, and shell results are
  sent back as native protobuf types; grep and ls fall back to MCP text results.
  Unsupported native tools (diagnostics, background shell, etc.) are rejected
  with an explanatory message.
- **Parallel tool call batching** — multiple tool calls are accumulated and
  flushed as a single batch using protocol signals (checkpoint, stepCompleted,
  turnEnded, requestContextArgs) or an inactivity timeout, enabling true
  parallel execution of subagents.
- **Title generation** — OpenCode title-agent requests are handled via Cursor's
  NameAgent unary RPC instead of spinning up a full agent bridge.
- **Session scoping** — `x-session-affinity` and `x-parent-session-id` headers
  are combined with content hashing for collision-resistant bridge/conversation
  keys, isolating concurrent sessions and subagents.
- **`tool_choice` filtering** — tools are filtered per OpenAI `tool_choice`
  semantics (`none`, `auto`, `required`, or specific function name).
- **Disk-backed persistence** — conversation checkpoints and blob stores persist
  to `~/.local/share/opencode/cursor-conversations/`, surviving proxy restarts.
- **Undo / revisit** — content-addressed checkpoint history (up to 30 entries)
  restores prior conversation state when the turn fingerprint matches.
- **Auto-resume** — timeout failures retry up to 5 times; `resource_exhausted`
  retries up to 10 times with exponential backoff before rebuilding from the
  last checkpoint.
- **AI SDK stream fix** — `sdk-wrapper.ts` works around a bug in
  `@ai-sdk/openai-compatible` where hardcoded block IDs break reasoning/text
  interleaving. See [docs/ai-sdk-stream-interleaving-bug.md](docs/ai-sdk-stream-interleaving-bug.md).
- **Structured logging** — info/warn/error events are forwarded to OpenCode's
  plugin log API. Console output only appears with `CURSOR_PROXY_DEBUG=1`.

## Architecture

```
OpenCode  -->  /v1/chat/completions  -->  Bun.serve (proxy)
                                              |
                                     HTTP/2 Connect stream
                                              |
                                    Cursor gRPC backend
                                      /agent.v1.AgentService/Run
```

### Tool call flow

```
1. Cursor model receives OpenCode tools via RequestContext (as MCP tool defs)
2. Model tries native tools (readArgs, shellArgs, grepArgs, etc.)
3. Proxy redirects supported native tools to OpenCode MCP equivalents
4. Native protobuf results sent back for most tools (grep/ls use MCP fallback)
5. Model issues MCP tool call → mcpArgs exec message
6. Proxy accumulates tool calls, flushes batch as OpenAI tool_calls SSE chunk
7. OpenCode executes tools in parallel, sends results in follow-up request
8. Proxy resumes with mcpResult on the same H2 stream
```

### Key design choices

- **Persistent frame parser** — the Connect protocol frame parser lives in the
  bridge and survives across handler swaps on tool result resume, preventing
  buffer orphaning that caused silent stalls.
- **Bidirectional streaming** — a single HTTP/2 stream is kept open for the
  entire conversation turn; tool results are written back on the same stream
  without reconnecting.
- **Signal-based batching** — parallel tool calls are batched using protocol
  signals (checkpoint, stepCompleted, turnEnded, requestContextArgs) with an
  inactivity timeout fallback, ensuring all tool calls in a batch are dispatched
  together.
- **Disk-backed state** — conversation checkpoints and blob stores persist to
  disk, surviving proxy restarts and enabling undo/revisit.
- **Auto-resume** — timeout failures retry up to 5 times; `resource_exhausted`
  retries up to 10 times with exponential backoff before rebuilding from the
  last checkpoint.

## Develop locally

```sh
bun install
bun run build    # tsc — type-checked build
bun run bundle   # bun build — produces dist/index.js + dist/sdk-wrapper.js
bun run deploy   # bundle + copy to ~/.config/opencode/plugins/
```

### Testing strategy

```sh
bun test                                      # full unit + integration + fuzz suite
bun run test:smoke                            # smoke script against a real proxy instance
bun test test/conversation-replay-recovery.test.ts
bun test test/conversation-semantic-fuzz.test.ts
SEMANTIC_FUZZ_COUNT=50 bun test test/conversation-semantic-fuzz.test.ts
SEMANTIC_FUZZ_COUNT=50 SEMANTIC_FUZZ_FAILURE_POINT_COUNT=16 bun test test/conversation-semantic-fuzz.test.ts
```

The test suite is intentionally layered:

| Layer | Representative files | What it validates |
|------|-----------------------|-------------------|
| Pure unit/state logic | `protocol.test.ts`, `openai-messages.test.ts`, `native-tools.test.ts`, `event-queue.test.ts`, `cursor-session.test.ts` | Frame parsing, message parsing, tool redirection, queue behavior, error classification |
| Session/state-machine edge cases | `batch-flush.test.ts`, `openai-stream.test.ts`, `server-compaction.test.ts` | Batch flushing, SSE lifecycle, checkpoint history/archive restore, timeout and cancellation semantics |
| Fake Cursor backend integration | `network-failures.test.ts`, `conversation-happy-path.test.ts`, `conversation-advanced-coverage.test.ts` | End-to-end proxy behavior against a scriptable HTTP/2 Connect backend, including timeouts, retries, tool-call resumes, restart recovery, and multi-resume tool batches |
| Semantic replay recovery | `conversation-replay-recovery.test.ts` | Record a golden multi-turn conversation, then replay it while failing once at each semantic communication point and verify the reconstructed frontend conversation still matches |
| Seeded semantic fuzzing | `conversation-semantic-fuzz.test.ts` | Generate deterministic multi-turn/tool-call scenarios, sample semantic failure points, inject resets/destroys, and assert final conversation equality plus at-most-once frontend tool execution |
| Smoke script | `test/smoke.ts` | High-level plugin sanity checks: proxy startup, auth helpers, model discovery fallback, and export shape |

The reusable test harness lives in `test/support/`:

- `fake-cursor-backend.ts` implements a scriptable fake Cursor backend over HTTP/2 Connect + protobuf.
- `openai-conversation-driver.ts` acts as a fake OpenAI frontend, including retry semantics and tool-result caching by `tool_call_id`.
- `proxy-harness.ts` starts the local proxy with isolated runtime config and temporary conversation storage, and can restart it in-place to exercise disk-backed recovery.

### Fuzzing

The project uses **semantic fuzzing**, not raw byte fuzzing, as its primary fuzzing strategy.

- Scenarios are generated from fixed seeds so failures are reproducible.
- `SEMANTIC_FUZZ_COUNT` controls how many seeded scenarios are generated; if it is unset, the suite falls back to its built-in default seed count.
- `SEMANTIC_FUZZ_FAILURE_POINT_COUNT` controls how many semantic communication points are replayed per seed; if it is unset, the suite falls back to its built-in default sample size.
- Total replay work grows roughly as `seed_count * (1 + failure_point_count)`, since each seed runs one golden pass plus one replay per sampled failure point.
- Each seed produces a small conversation in the currently supported recoverable space: multiple turns, optional tool-use turns, and 1-2 tool batches per turn with 1-2 tool calls in each batch.
- The suite first runs a golden conversation, records the semantic communication points, then replays sampled points with a single injected upstream `reset` or `destroy`.
- The main invariants are:
  - final normalized conversation matches the golden run
  - per-turn assistant/reasoning output matches the golden run
  - frontend tool execution remains at-most-once for each unique `tool_call_id`

The semantic fuzz generator is intentionally bounded for CI stability. `SEMANTIC_FUZZ_COUNT` is capped at `100` and `SEMANTIC_FUZZ_FAILURE_POINT_COUNT` is capped at `64`. More complex multi-batch / multi-resume flows are still covered by dedicated deterministic tests such as `conversation-advanced-coverage.test.ts`.

### Pre-commit checks

A **husky** pre-commit hook runs **Biome** (lint + format) on every staged `.ts`
file in `src/` and `test/` via **lint-staged**. The hook is installed
automatically by `bun install` (via the `prepare` script).

```sh
bun run check       # lint + format check (no writes)
bun run check:fix   # lint + format with auto-fix
bun run typecheck   # tsc --noEmit
```

Cognitive complexity is enforced at a threshold of 25. Functions that
intentionally exceed it carry a `biome-ignore` suppression with a reason.

`bun run deploy` bundles the plugin into two self-contained JS files:

- `opencode-cursor-oauth.js` — main plugin (auth, proxy, model registry)
- `opencode-cursor-sdk.js` — AI SDK wrapper (stream interleaving fix)

`@opencode-ai/plugin` is kept external for the main plugin bundle. Files are
copied into `~/.config/opencode/plugins/` — no symlinks, survives OpenCode
updates.

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CURSOR_PROXY_DEBUG` | `0` | Set to `1` to enable verbose console logging |
| `CURSOR_API_URL` | `https://api2.cursor.sh` | Override Cursor API base URL |
| `CURSOR_AGENT_URL` | `https://api2.cursor.sh` | Override Cursor agent streaming URL |
| `SEMANTIC_FUZZ_COUNT` | `5` | Number of seeded semantic fuzz scenarios to generate when running `test/conversation-semantic-fuzz.test.ts` |
| `SEMANTIC_FUZZ_FAILURE_POINT_COUNT` | `8` | Number of semantic communication points to replay per fuzz seed in `test/conversation-semantic-fuzz.test.ts` |

## MITM proxy (tools/)

`tools/mitm-proxy.ts` is a transparent TLS relay for capturing raw Cursor
agent traffic for protocol analysis. See the file header for setup instructions.

## Requirements

- [OpenCode](https://opencode.ai)
- [Bun](https://bun.sh)
- Node.js 18+ compatible runtime APIs (the test harness uses `node:http2`)
- Active [Cursor](https://cursor.com) subscription
