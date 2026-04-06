# Architecture: OpenCode ↔ Proxy ↔ Cursor

## Overview

```
┌──────────┐   OpenAI HTTP/SSE    ┌───────────┐   HTTP/2 Connect+Protobuf  ┌──────────────┐
│ OpenCode │ ◄──────────────────► │   proxy   │ ◄────────────────────────► │ Cursor Server│
│ (client) │   localhost:4011     │ (proxy.ts)│   api2direct.cursor.sh     │  (agent.v1)  │
└──────────┘                      └───────────┘                            └──────────────┘
```

## Protocol Layers

### OpenCode ↔ Proxy: OpenAI-compatible REST + SSE

**Request (OpenCode → Proxy):**

```
POST /v1/chat/completions
Content-Type: application/json

{
  messages: [...],         // system + user + assistant + tool results
  model: "claude-4-opus",
  tools: [...],            // MCP tool definitions
  tool_choice: "auto",
  stream: true
}
```

**Response (Proxy → OpenCode):** SSE stream of `chat.completion.chunk` objects

```
data: {"choices":[{"delta":{"reasoning_content":"..."}}]}   // thinking
data: {"choices":[{"delta":{"content":"..."}}]}              // text
data: {"choices":[{"delta":{"tool_calls":[{...}]}}]}         // tool call
data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}// batch end
data: {"usage":{...}}                                        // token counts
data: [DONE]                                                 // stream end
```

### Proxy ↔ Cursor: HTTP/2 + Connect Protocol + Protobuf

**RPC:** `POST /agent.v1.AgentService/Run` (bidirectional streaming)

Both sides send framed protobuf messages. Each frame = 5-byte header (1 flag byte + 4-byte length) + protobuf payload.

## Message Types

### Proxy → Cursor (AgentClientMessage)


| Message                                  | When                               | Purpose                                                    |
| ---------------------------------------- | ---------------------------------- | ---------------------------------------------------------- |
| `AgentRunRequest`                        | First frame                        | Initial request with conversation state, model, checkpoint |
| `clientHeartbeat`                        | Every 5s                           | Keep H2 connection alive                                   |
| `execClientMessage.requestContextResult` | On `requestContextArgs`            | Provide MCP tool definitions + cloudRule                   |
| `execClientMessage.mcpResult`            | After OpenCode returns tool result | MCP tool execution result                                  |
| `execClientMessage.<native>Result`       | After OpenCode returns tool result | Native tool result (read/write/shell/etc)                  |
| `execClientControlMessage.streamClose`   | After each exec result             | Signal exec completion                                     |
| `kvClientMessage.getBlobResult`          | On blob request                    | Return cached blob data                                    |
| `kvClientMessage.setBlobResult`          | On blob store                      | Acknowledge blob stored                                    |
| `interactionResponse`                    | On query                           | Approve/reject searches, questions, etc                    |


### Cursor → Proxy (AgentServerMessage)


| Message                                | When                           | Purpose                                           |
| -------------------------------------- | ------------------------------ | ------------------------------------------------- |
| `interactionUpdate.textDelta`          | During generation              | Model text output                                 |
| `interactionUpdate.thinkingDelta`      | During generation              | Model thinking/reasoning                          |
| `interactionUpdate.tokenDelta`         | During generation              | Token count update                                |
| `interactionUpdate.toolCallStarted`    | Before tool exec               | Notification (more tools may follow)              |
| `interactionUpdate.toolCallCompleted`  | After tool result processed    | Notification                                      |
| `interactionUpdate.stepCompleted`      | After batch of tools           | **Batch boundary signal**                         |
| `interactionUpdate.turnEnded`          | After full turn                | **Batch boundary signal**                         |
| `interactionUpdate.heartbeat`          | Periodic (~10s)                | Keepalive (NOT a batch signal)                    |
| `conversationCheckpointUpdate`         | After significant state change | **Batch boundary signal** (when pendingExecs > 0) |
| `execServerMessage.requestContextArgs` | Start of turn                  | Request tool definitions                          |
| `execServerMessage.mcpArgs`            | Tool call                      | MCP tool invocation                               |
| `execServerMessage.<native>Args`       | Tool call                      | Native tool invocation (redirected to MCP)        |
| `execServerControlMessage.abort`       | Cancellation                   | Abort a running exec                              |
| `kvServerMessage.getBlobArgs`          | Checkpoint restore             | Request blob from local store                     |
| `kvServerMessage.setBlobArgs`          | Checkpoint save                | Store blob locally                                |
| `interactionQuery`                     | Web search, questions          | Approval request                                  |
| `endStream`                            | Stream end                     | Clean close or error                              |


## Lifecycles

### 1. Simple request (no tools)

```
OpenCode                    Proxy                         Cursor
  │                           │                              │
  │ POST /v1/chat/completions │                              │
  │ (messages, no tools)      │                              │
  │ ─────────────────────────>│                              │
  │                           │ H2: AgentRunRequest          │
  │                           │ ────────────────────────────>│
  │                           │                              │
  │                           │ H2: heartbeat (every 5s)     │
  │                           │ ────────────────────────────>│
  │                           │                              │
  │                           │   interactionUpdate.textDelta│
  │                           │<─────────────────────────────│
  │ SSE: content delta        │                              │
  │<──────────────────────────│                              │
  │                           │    ...more deltas...         │
  │                           │<─────────────────────────────│
  │ SSE: content delta        │                              │
  │<──────────────────────────│                              │
  │                           │    checkpoint                │
  │                           │<─────────────────────────────│
  │                           │    turnEnded                 │
  │                           │<─────────────────────────────│
  │                           │    endStream                 │
  │                           │<─────────────────────────────│
  │ SSE: finish_reason="stop" │                              │
  │ SSE: [DONE]               │                              │
  │<──────────────────────────│                              │
  │                           │ [bridge closes, H2 ends]     │
```

### 2. Tool call flow (the hard part)

```
OpenCode                    Proxy                         Cursor
  │                           │                              │
  │ POST /v1/chat/completions │                              │
  │ (messages + tool defs)    │                              │
  │ ─────────────────────────>│                              │
  │                           │ H2: AgentRunRequest          │
  │                           │ ────────────────────────────>│
  │                           │                              │
  │                           │    requestContextArgs        │
  │                           │<─────────────────────────────│
  │                           │ H2: requestContextResult     │
  │                           │    (tool defs + cloudRule)   │
  │                           │ ────────────────────────────>│
  │                           │                              │
  │                           │    textDelta (thinking...)   │
  │                           │<─────────────────────────────│
  │ SSE: reasoning_content    │                              │
  │<──────────────────────────│                              │
  │                           │                              │
  │                           │    textDelta (response text) │
  │                           │<─────────────────────────────│
  │ SSE: content              │                              │
  │<──────────────────────────│                              │
  │                           │                              │
  │                           │    toolCallStarted           │
  │                           │<─────────────────────────────│
  │                           │    mcpArgs (tool A)          │  ◄── tool call 1
  │                           │<─────────────────────────────│
  │ SSE: tool_calls[0]        │                              │
  │<──────────────────────────│                              │
  │                           │    mcpArgs (tool B)          │  ◄── tool call 2
  │                           │<─────────────────────────────│
  │ SSE: tool_calls[1]        │                              │
  │<──────────────────────────│                              │
  │                           │                              │
  │                           │    checkpoint ◄──────────────── batch boundary!
  │                           │<─────────────────────────────│
  │                           │                              │
  │                           │  [proxy sets checkpointAfter │
  │                           │   Exec=true, afterParse runs │
  │                           │   flushPendingExecs()]       │
  │                           │                              │
  │ SSE: finish=tool_calls    │  [bridge stored in           │
  │ SSE: [DONE]               │   activeBridges, SSE closed] │
  │<──────────────────────────│                              │
  │                           │                              │
  │  [OpenCode executes       │  [H2 connection stays alive, │
  │   tool A and tool B       │   heartbeats continue,       │
  │   via its MCP tools]      │   Cursor waits for results]  │
  │                           │                              │
  │ POST /v1/chat/completions │                              │
  │ (tool results for A & B)  │                              │
  │ ─────────────────────────>│                              │
  │                           │  [proxy finds activeBridge,  │
  │                           │   matches tool results]      │
  │                           │                              │
  │                           │ H2: mcpResult (tool A)       │
  │                           │ H2: streamClose (tool A)     │
  │                           │ ────────────────────────────>│
  │                           │ H2: mcpResult (tool B)       │
  │                           │ H2: streamClose (tool B)     │
  │                           │ ────────────────────────────>│
  │                           │                              │
  │                           │  [new SSE stream created     │
  │                           │   for continuation]          │
  │                           │                              │
  │                           │    textDelta (next response) │
  │                           │<─────────────────────────────│
  │ SSE: content              │                              │
  │<──────────────────────────│                              │
  │                           │    ...more tools or text...  │
```

### 3. The stall problem — flush/resume race

This is the failure mode that causes agents to hang:

```
                    Stream A (SSE #1)                    Cursor
                        │                                   │
 ◄── tool_calls[0,1]────│    mcpArgs A, mcpArgs B           │
                        │<──────────────────────────────────│
                        │                                   │
                        │    checkpoint                     │
                        │<──────────────────────────────────│
                        │                                   │
                        │  checkpointAfterExec=true         │
                        │  → flushPendingExecs()            │
                        │  → SSE closed, bridge stored      │
                        │                                   │
                        │  ┌───── RACE WINDOW ──────┐       │
                        │  │                        │       │
                        │  │  stepCompleted arrives │<──────│ ◄── consumed by Stream A
                        │  │  (SSE is closed, can't │       │     handler, but afterParse
                        │  │   act on it)           │       │     sees closed=true, no-op
                        │  │                        │       │
                        │  └────────────────────────┘       │
                        │                                   │
OpenCode sends results  │                                   │
─────────────────────>  │                                   │
                        │                                   │
                    Stream B (SSE #2) created               │
                        │                                   │
                        │  checkpointAfterExec=false ◄───── PROBLEM: no signal pending
                        │                                   │
                        │  H2: mcpResult A + B              │
                        │  ────────────────────────────────>│
                        │                                   │
                        │    mcpArgs C (new tool call)      │ ◄── Cursor sends new tools
                        │<──────────────────────────────────│
 ◄── tool_calls[2] ─────│                                   │
                        │                                   │
                        │  [waiting for checkpoint/         │
                        │   stepCompleted/turnEnded to      │
                        │   set checkpointAfterExec=true    │
                        │   and flush... but those signals  │
                        │   were already consumed by        │
                        │   Stream A's dead handler]        │
                        │                                   │
                        │  ════ STALL ═══════════════       │
                        │  SSE open, tool_calls sent        │
                        │  but never finish_reason sent     │
                        │  OpenCode waits forever           │
```

## Key State Machines

### Per-stream state (`StreamState`)

```
checkpointAfterExec: false ──[checkpoint received while pendingExecs>0]──► true
                             ──[stepCompleted while pendingExecs>0]──────► true
                             ──[turnEnded while pendingExecs>0]─────────► true
                             ──[requestContextArgs while pendingExecs>0]► true

afterParse runs after every H2 data chunk:
  if pendingExecs>0 && checkpointAfterExec && !closed → flushPendingExecs()
```

### Bridge lifecycle

```
[fresh request]
  │
  ├─ no tools called ──► text stream ──► endStream ──► bridge.onClose ──► SSE stop+DONE
  │
  └─ tools called ──► pendingExecs accumulated
                       │
                       ├─ batch boundary (checkpoint/step/turn/endStream)
                       │   └─► flushPendingExecs()
                       │       ├─ emit finish_reason=tool_calls + [DONE]
                       │       ├─ close SSE
                       │       └─ store bridge in activeBridges (H2 stays alive)
                       │
                       └─ [next request with tool results]
                           ├─ bridge alive? → handleToolResultResume()
                           │   ├─ send mcpResult for each matched result
                           │   ├─ create new SSE stream on same bridge
                           │   └─ loop back to "tools called" or "no tools"
                           │
                           └─ bridge dead? → start fresh bridge
```

### Inactivity timer

```
Timer phases:
  THINKING (30s) ─── waiting for first model output after request/resume
  STREAMING (15s) ── gap between model output tokens

Reset on: any onMessage from bridge (including server heartbeats ~10s)
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                     BUG: server heartbeats reset the timer, so it never fires
                     during the race window above

On timeout:
  if pendingExecs > 0 → flush them (safety net)
  elif has checkpoint  → auto-resume (up to 5 times)
  else                 → send error, close SSE
```

## Native Tool Redirection

Cursor's model calls native tools. Proxy intercepts and redirects to OpenCode's MCP:

```
Cursor native exec        →  Redirected to OpenCode MCP tool
─────────────────────────────────────────────────────────────
readArgs                  →  mcp: read(filePath, offset, limit)
writeArgs                 →  mcp: write(filePath, content)
deleteArgs                →  mcp: bash("rm -f ...")
shellArgs / shellStreamArgs → mcp: bash(command, ...)
lsArgs                    →  mcp: glob("*", path)
grepArgs                  →  mcp: grep(pattern, path, ...)
fetchArgs                 →  mcp: web_fetch(url)
mcpArgs                   →  mcp: <toolName>(args)  (pass-through)

Result flows back: OpenCode MCP result → proxy formats as native Cursor result type
```

## Files


| File                 | Role                                                                      |
| -------------------- | ------------------------------------------------------------------------- |
| `src/proxy.ts`       | Core: HTTP server, bridge management, protocol translation, SSE streaming |
| `src/index.ts`       | Plugin entrypoint: registers MCP tools, starts proxy                      |
| `src/logger.ts`      | Structured logging via OpenCode plugin API                                |
| `src/sdk-wrapper.ts` | Custom AI SDK wrapper (OpenAI-compatible provider for OpenCode)           |


