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

- **Full native tool redirection** — Cursor's built-in tools (read, write,
  delete, fetch, shell, shell stream, grep, ls) are transparently redirected to
  OpenCode's MCP equivalents with native protobuf results sent back to Cursor.
- **Parallel tool call batching** — multiple tool calls are accumulated and
  flushed as a single batch using protocol signals (checkpoint, stepCompleted,
  turnEnded), enabling true parallel execution of subagents.
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
- **Auto-resume** — on timeout or `resource_exhausted`, the proxy rebuilds the
  request from the last checkpoint (up to 5 attempts).
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
3. Proxy redirects ALL native tools to OpenCode MCP equivalents
4. Native protobuf results are sent back to Cursor for redirected tools
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
  signals (checkpoint, stepCompleted, turnEnded) rather than timers, ensuring
  all tool calls in a batch are dispatched together.
- **Disk-backed state** — conversation checkpoints and blob stores persist to
  disk, surviving proxy restarts and enabling undo/revisit.
- **Auto-resume** — on timeout or `resource_exhausted`, the proxy automatically
  rebuilds the request from the last checkpoint (up to 5 attempts).

## Develop locally

```sh
bun install
bun run build    # tsc — type-checked build
bun run bundle   # bun build — produces dist/index.js + dist/sdk-wrapper.js
bun run deploy   # bundle + copy to ~/.config/opencode/plugins/
bun run test     # smoke tests
```

`bun run deploy` bundles the plugin into two self-contained JS files:

- `opencode-cursor-oauth.js` — main plugin (auth, proxy, model registry)
- `opencode-cursor-sdk.js` — AI SDK wrapper (stream interleaving fix)

Only `@opencode-ai/plugin` is kept external. Files are copied into
`~/.config/opencode/plugins/` — no symlinks, survives OpenCode updates.

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CURSOR_PROXY_DEBUG` | `0` | Set to `1` to enable verbose console logging |
| `CURSOR_API_URL` | `https://api2.cursor.sh` | Override Cursor API base URL |
| `CURSOR_AGENT_URL` | `https://agentn.us.api5.cursor.sh` | Override Cursor agent streaming URL |

## MITM proxy (tools/)

`tools/mitm-proxy.ts` is a transparent TLS relay for capturing raw Cursor
agent traffic for protocol analysis. See the file header for setup instructions.

## Requirements

- [OpenCode](https://opencode.ai)
- [Bun](https://bun.sh)
- Active [Cursor](https://cursor.com) subscription
