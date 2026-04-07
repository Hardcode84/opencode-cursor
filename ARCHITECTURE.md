# Architecture: OpenCode ↔ Proxy ↔ Cursor

## Overview

```
┌──────────┐   OpenAI HTTP/SSE    ┌───────────┐   HTTP/2 Connect+Protobuf  ┌──────────────┐
│ OpenCode │ ◄──────────────────► │   proxy   │ ◄────────────────────────► │ Cursor Server│
│ (client) │   localhost:4011     │           │      api2.cursor.sh        │  (agent.v1)  │
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

| Message | When | Purpose |
|---------|------|---------|
| `AgentRunRequest` | First frame | Initial request with conversation state, model, checkpoint |
| `clientHeartbeat` | Every 5s | Keep H2 connection alive |
| `execClientMessage.requestContextResult` | On `requestContextArgs` | Provide MCP tool definitions + cloudRule |
| `execClientMessage.mcpResult` | After OpenCode returns tool result | MCP tool execution result |
| `execClientMessage.<native>Result` | After OpenCode returns tool result | Native tool result (read/write/shell/etc) |
| `execClientControlMessage.streamClose` | After each exec result | Signal exec completion |
| `kvClientMessage.getBlobResult` | On blob request | Return cached blob data |
| `kvClientMessage.setBlobResult` | On blob store | Acknowledge blob stored |
| `interactionResponse` | On query | Approve/reject searches, questions, etc |

### Cursor → Proxy (AgentServerMessage)

| Message | When | Purpose |
|---------|------|---------|
| `interactionUpdate.textDelta` | During generation | Model text output |
| `interactionUpdate.thinkingDelta` | During generation | Model thinking/reasoning |
| `interactionUpdate.tokenDelta` | During generation | Token count update |
| `interactionUpdate.toolCallStarted` | Before tool exec | Notification (more tools may follow) |
| `interactionUpdate.toolCallCompleted` | After tool result processed | Notification |
| `interactionUpdate.stepCompleted` | After batch of tools | **Batch boundary signal** |
| `interactionUpdate.turnEnded` | After full turn | **Batch boundary signal** |
| `interactionUpdate.heartbeat` | Periodic (~10s) | Keepalive (NOT a batch signal) |
| `conversationCheckpointUpdate` | After significant state change | **Batch boundary signal** (when pendingExecs > 0) |
| `execServerMessage.requestContextArgs` | Start of turn | Request tool definitions |
| `execServerMessage.mcpArgs` | Tool call | MCP tool invocation |
| `execServerMessage.<native>Args` | Tool call | Native tool invocation (redirected to MCP) |
| `execServerControlMessage.abort` | Cancellation | Abort a running exec |
| `kvServerMessage.getBlobArgs` | Checkpoint restore | Request blob from local store |
| `kvServerMessage.setBlobArgs` | Checkpoint save | Store blob locally |
| `interactionQuery` | Web search, questions | Approval request |
| `endStream` | Stream end | Clean close or error |

## Internal Design

### The core problem

One Cursor H2 stream maps to **multiple** OpenAI request/response cycles.
OpenAI's tool-call protocol ends the response (finish_reason=tool_calls),
then the client sends results in a new request. Cursor's protocol keeps
the bidirectional stream alive — tool results flow back on the same connection.

The proxy must "cut" the SSE stream at tool-call batch boundaries, keep the
H2 connection alive, and resume SSE output when the next request arrives.

### The naive approach (historical — replaced)

In an earlier design, state lived inside the SSE `ReadableStream` closure. Each
HTTP response created a fresh `StreamState`. The bridge's message handler was
installed inside the SSE closure too. When the SSE closed and a new one opened,
both state and handler were replaced, causing a race: messages arriving between
SSE close and the next request were processed by the dead handler.

### The current design: session owns state, SSE writer is a consumer

```
┌──────────────────────────────────────────────────────────┐
│                     CursorSession                        │
│                                                          │
│  H2 stream ──► message handler (installed ONCE)          │
│                    │                                     │
│                    ├─ KV, exec responses,  ──► respond   │
│                    │  interactionQuery        immediately│
│                    │                                     │
│                    └─ text, toolCall,      ──► event     │
│                       batchReady, done        queue      │
│                                                          │
│  Batch state machine:                                    │
│    STREAMING ──► COLLECTING ──► FLUSHED ──► STREAMING    │
│                                                          │
│  Event queue: [text, toolCall, batchReady, ...]          │
│    (buffers when no SSE writer is reading)               │
│                                                          │
│  Methods:                                                │
│    next() → Promise<Event>     (consume from queue)      │
│    sendToolResults(results)    (send to Cursor, reset)   │
│    close()                                               │
│                                                          │
└──────────────────────────────────────────────────────────┘
         │ next()                      ▲ sendToolResults()
         ▼                             │
┌──────────────────┐          ┌──────────────────┐
│  SSE Writer #1   │          │  SSE Writer #2   │
│  reads events,   │          │  reads events    │
│  emits chunks,   │          │  (including any  │
│  closes on       │          │   buffered ones) │
│  batchReady      │          │                  │
└──────────────────┘          └──────────────────┘
```

**Principle**: The H2 message handler is installed once when the session is
created and never changes. SSE writers come and go; the session persists.

### Event types

```typescript
type SessionEvent =
  | { type: 'text'; text: string; isThinking: boolean }
  | { type: 'toolCall'; exec: PendingExec }
  | { type: 'batchReady' }
  | { type: 'usage'; outputTokens: number; totalTokens: number }
  | { type: 'done'; error?: string; retryHint?: RetryHint }
// RetryHint = 'blob_not_found' | 'resource_exhausted' | 'timeout'
```

Protocol-level messages (KV, requestContext, interactionQuery, exec responses)
are handled inline by the session — they never reach the event queue.

### Batch state machine

```
STREAMING ───[mcpArgs]──► COLLECTING ───[boundary]──► FLUSHED
    ▲                      ↑ [more mcpArgs]                │
    │                                                      │
    └──────────────────[sendToolResults()]─────────────────┘
```

**STREAMING**: Text deltas flow. No pending tool calls.

**COLLECTING**: Tool calls are arriving. `pendingExecs` grows.
`toolCall` events are emitted. Boundary signals are noted but don't
trigger flush yet — we wait for the H2 data chunk boundary (`afterParse`).

**FLUSHED**: First boundary signal (checkpoint/stepCompleted/turnEnded/
requestContextArgs/endStream) while in COLLECTING. Emit `batchReady`.
Ignore subsequent boundary signals for the same batch.
Wait for `sendToolResults()` to return to STREAMING.

Boundary signals:
- `checkpoint` while pendingExecs > 0
- `stepCompleted` while pendingExecs > 0
- `turnEnded` while pendingExecs > 0
- `requestContextArgs` while pendingExecs > 0
- `endStream` while pendingExecs > 0 (forced)

### SSE writer loop

The SSE writer is a simple consumer — no protocol knowledge:

```typescript
// pumpSession() in openai-stream.ts
async function pumpSession(session: CursorSession, ctx: SSECtx): Promise<PumpResult> {
  for (;;) {
    const event = await session.next()
    switch (event.type) {
      case 'text':       emit content/reasoning chunk; break
      case 'toolCall':   emit tool_calls chunk; break
      case 'usage':      emit usage chunk; break
      case 'batchReady': emit finish_reason=tool_calls + [DONE]; return 'batchReady'
      case 'done':       emit finish_reason=stop + [DONE]; return 'done'
    }
  }
}
```

### Why the race disappears

1. Checkpoint arrives → session enters FLUSHED, emits `batchReady`
2. SSE writer #1 reads `batchReady` → closes SSE
3. `stepCompleted` arrives → session is FLUSHED → **ignored, no event emitted**
4. New `mcpArgs C` arrives → session emits `toolCall(C)` → **buffered in queue**
5. New checkpoint arrives → session emits `batchReady` → **buffered in queue**
6. Tool results arrive → `sendToolResults()` → state resets to STREAMING
7. SSE writer #2 calls `next()` → **reads buffered `toolCall(C)` then `batchReady`**

No state is lost. No handler is replaced. The queue bridges the gap.

### Inactivity timer

Reset on recognized server messages (text, tool calls), NOT on H2 keepalives.
**Exception**: in FLUSHED state, the timer is an absolute deadline — it is
set once on entering FLUSHED and not restarted by server traffic.

```
Phases:
  THINKING (30s) — waiting for first model output
  STREAMING (15s) — gap between output tokens
  FLUSHED (10min) — absolute deadline waiting for client tool results

On thinking/streaming timeout:
  if COLLECTING with pending execs → force batchReady (safety net)
  elif has checkpoint → auto-resume (timeout: up to 5 attempts; resource_exhausted: up to 10 attempts with exponential backoff)
  else → emit done with error

On FLUSHED timeout:
  → emit done with "Tool result wait timed out", close session
```

## Sequence Diagrams

### 1. Simple request (no tools)

```
OpenCode                    Proxy                         Cursor
  │                           │                              │
  │ POST /v1/chat/completions │                              │
  │ ─────────────────────────>│                              │
  │                           │ H2: AgentRunRequest          │
  │                           │ ────────────────────────────>│
  │                           │                              │
  │                           │    textDelta                 │
  │                           │<─────────────────────────────│
  │ SSE: content              │                              │
  │<──────────────────────────│                              │
  │                           │    ...more deltas...         │
  │                           │<─────────────────────────────│
  │ SSE: content              │                              │
  │<──────────────────────────│                              │
  │                           │    endStream                 │
  │                           │<─────────────────────────────│
  │ SSE: finish_reason="stop" │                              │
  │ SSE: [DONE]               │                              │
  │<──────────────────────────│                              │
```

### 2. Tool call flow

```
OpenCode                    Proxy (CursorSession)         Cursor
  │                           │                              │
  │ POST /v1/chat/completions │                              │
  │ ─────────────────────────>│                              │
  │                           │ [create session + H2 stream] │
  │                           │ H2: AgentRunRequest          │
  │                           │ ────────────────────────────>│
  │                           │                              │
  │                           │    requestContextArgs        │
  │                           │<─────────────────────────────│
  │                           │ H2: requestContextResult     │  ◄─ handled inline
  │                           │ ────────────────────────────>│     by session
  │                           │                              │
  │                           │    textDelta                 │
  │                           │<─────────────────────────────│
  │                           │  → queue: text event         │
  │ SSE: content              │  ← SSE writer reads it       │
  │<──────────────────────────│                              │
  │                           │                              │
  │                           │    mcpArgs (tool A)          │
  │                           │<─────────────────────────────│
  │                           │  state: COLLECTING           │
  │                           │  → queue: toolCall event     │
  │ SSE: tool_calls[0]        │  ← SSE writer reads it       │
  │<──────────────────────────│                              │
  │                           │    mcpArgs (tool B)          │
  │                           │<─────────────────────────────│
  │                           │  → queue: toolCall event     │
  │ SSE: tool_calls[1]        │  ← SSE writer reads it       │
  │<──────────────────────────│                              │
  │                           │                              │
  │                           │    checkpoint                │
  │                           │<─────────────────────────────│
  │                           │  state: → FLUSHED            │
  │                           │  → queue: batchReady event   │
  │ SSE: finish=tool_calls    │  ← SSE writer reads it       │
  │ SSE: [DONE]               │    and closes                │
  │<──────────────────────────│                              │
  │                           │                              │
  │                           │    stepCompleted             │  ◄─ FLUSHED state:
  │                           │<─────────────────────────────│     ignored, no event
  │                           │                              │
  │  [execute tools A, B]     │  [session alive, no reader]  │
  │                           │                              │
  │ POST /v1/chat/completions │                              │
  │ (tool results A & B)      │                              │
  │ ─────────────────────────>│                              │
  │                           │ sendToolResults()            │
  │                           │  state: → STREAMING          │
  │                           │ H2: mcpResult A + streamClose│
  │                           │ H2: mcpResult B + streamClose│
  │                           │ ────────────────────────────>│
  │                           │                              │
  │                           │  [new SSE writer subscribes] │
  │                           │                              │
  │                           │    textDelta                 │
  │                           │<─────────────────────────────│
  │                           │  → queue: text event         │
  │ SSE: content              │  ← SSE writer reads it       │
  │<──────────────────────────│                              │
  │                           │    ...continues...           │
```

### 3. Buffered events across resume (race solved)

```
                      CursorSession                       Cursor
                          │                                  │
  state: FLUSHED          │                                  │
  queue: [batchReady]     │                                  │
  SSE writer reads it ────│                                  │
  SSE #1 closes           │                                  │
                          │                                  │
  [NO READER]             │    stepCompleted                 │
                          │<─────────────────────────────────│
                          │  FLUSHED → ignore                │
                          │                                  │
  [NO READER]             │    mcpArgs C (new tool!)         │
                          │<─────────────────────────────────│
                          │  state: → COLLECTING             │
                          │  → queue: [toolCall(C)]          │
                          │                                  │
  [NO READER]             │    checkpoint                    │
                          │<─────────────────────────────────│
                          │  state: → FLUSHED                │
                          │  → queue: [toolCall(C), batchReady] │
                          │                                  │
  Tool results arrive ───>│                                  │
  sendToolResults()       │  state: → STREAMING              │
  new SSE writer          │  H2: mcpResult A+B               │
                          │  ───────────────────────────────>│
                          │                                  │
  SSE writer calls next() │                                  │
  reads toolCall(C) ──────│ ◄── from buffer!                 │
  SSE: tool_calls[0]      │                                  │
                          │                                  │
  reads batchReady ───────│ ◄── from buffer!                 │
  SSE: finish=tool_calls  │                                  │
  SSE: [DONE]             │                                  │
  SSE #2 closes           │                                  │
```

## Native Tool Redirection

Cursor's model calls native tools. The session intercepts and redirects to OpenCode's MCP:

```
Cursor native exec          Redirected to OpenCode MCP tool
──────────────────────────────────────────────────────────
readArgs                  →  read(filePath, offset, limit)
writeArgs                 →  write(filePath, content)
deleteArgs                →  bash("rm -f ...")
shellArgs / shellStreamArgs → bash(command, ...)
lsArgs                    →  glob("*", path)
grepArgs                  →  grep(pattern, path, ...)
fetchArgs                 →  web_fetch(url)
mcpArgs                   →  <toolName>(args)  (pass-through)

Result flows back: OpenCode MCP result → formatted as native Cursor protobuf
```

## Files

| File | Responsibility |
|------|----------------|
| `src/protocol.ts` | Connect framing, constants |
| `src/cursor-session.ts` | **CursorSession**: H2 connection, message handler, batch state machine, event queue |
| `src/openai-stream.ts` | **SSE writer**: consumes session events, produces OpenAI-format SSE chunks |
| `src/server.ts` | HTTP routes, request parsing, session lifecycle, active session map |
| `src/cursor-messages.ts` | Cursor message processing: interaction updates, KV, exec dispatch, query responses |
| `src/native-tools.ts` | Native→MCP redirection, PendingExec, result formatting |
| `src/conversation-state.ts` | Checkpoint persistence, disk serialization, TTL eviction |
| `src/thinking-filter.ts` | Streaming thinking tag parser |
| `src/title.ts` | Title detection + Cursor NameAgent RPC |
| `src/index.ts` | Plugin entrypoint: OAuth hooks, `startProxy`, model catalog, SDK wrapper |
| `src/logger.ts` | Structured logging via OpenCode plugin API |
| `src/sdk-wrapper.ts` | Custom AI SDK wrapper (OpenAI-compatible provider) |
