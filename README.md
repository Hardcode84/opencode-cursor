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

## How it works

1. OAuth — browser-based login to Cursor via PKCE.
2. Model discovery — queries Cursor's gRPC API for all available models.
3. Local proxy — translates `POST /v1/chat/completions` into Cursor's
   protobuf/HTTP/2 Connect protocol.
4. Native tool routing — redirects Cursor's built-in read/write/delete/fetch
   tools to OpenCode's MCP equivalents; rejects shell/grep with typed errors so
   the model falls back to MCP tools.

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
2. Model tries native tools (readArgs, shellArgs, etc.)
3. Proxy redirects read/write/delete/fetch to OpenCode MCP tools
4. Proxy rejects shell/grep/ls with typed errors (ShellRejected, etc.)
5. Model issues MCP tool call -> mcpArgs exec message
6. Proxy emits OpenAI tool_calls SSE chunk, keeps H2 stream alive
7. OpenCode executes tool, sends result in follow-up request
8. Proxy resumes with mcpResult on the same H2 stream
```

### Key design choices

- **Persistent frame parser** — the Connect protocol frame parser lives in the
  bridge and survives across handler swaps on tool result resume, preventing
  buffer orphaning that caused silent stalls.
- **Bidirectional streaming** — a single HTTP/2 stream is kept open for the
  entire conversation turn; tool results are written back on the same stream
  without reconnecting.
- **Disk-backed state** — conversation checkpoints and blob stores persist to
  disk, surviving proxy restarts and enabling undo/revisit.
- **Auto-resume** — on timeout or `resource_exhausted`, the proxy automatically
  rebuilds the request from the last checkpoint (up to 5 attempts).

## Develop locally

```sh
bun install
bun run build    # tsc — type-checked build
bun run bundle   # bun build — bundled for deployment
bun run deploy   # bundle + copy to ~/.config/opencode/node_modules/
```

`bun run deploy` bundles the plugin into self-contained JS files (only
`@opencode-ai/plugin` is external) and copies them into OpenCode's plugin
directory. No symlinks — survives OpenCode updates.

## MITM proxy (tools/)

`tools/mitm-proxy.ts` is a transparent TLS relay for capturing raw Cursor
agent traffic for protocol analysis. See the file header for setup instructions.

## Requirements

- [OpenCode](https://opencode.ai)
- [Bun](https://bun.sh)
- Active [Cursor](https://cursor.com) subscription
