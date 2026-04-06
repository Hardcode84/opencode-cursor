import { createHash, randomUUID } from "node:crypto";
import { create, fromBinary, fromJson, type JsonValue, toBinary } from "@bufbuild/protobuf";
import { ValueSchema } from "@bufbuild/protobuf/wkt";
import {
  getConversationState,
  invalidateConversationState,
  persistConversation,
  resolveConversationState,
  turnsFingerprint,
} from "./conversation-state";
import { CursorSession } from "./cursor-session";
import { errorDetails, logDebug, logError, logInfo, logWarn } from "./logger";
import {
  type OpenAIMessage,
  type OpenAIToolDef,
  parseMessages,
  selectToolsForChoice,
  type ToolResultInfo,
  textContent,
} from "./openai-messages";
import type { PumpResult } from "./openai-stream";
import {
  collectNonStreamingResponse,
  createSSECtx,
  pumpSession,
  SSE_HEADERS,
  type SSECtx,
} from "./openai-stream";
import {
  AgentClientMessageSchema,
  AgentConversationTurnStructureSchema,
  AgentRunRequestSchema,
  AssistantMessageSchema,
  ConversationActionSchema,
  ConversationStateStructureSchema,
  ConversationStepSchema,
  ConversationTurnStructureSchema,
  type McpToolDefinition,
  McpToolDefinitionSchema,
  ModelDetailsSchema,
  RequestContextSchema,
  ResumeActionSchema,
  UserMessageActionSchema,
  UserMessageSchema,
} from "./proto/agent_pb";
import { buildTitleSourceText, detectTitleRequest, handleTitleGenerationRequest } from "./title";

const MAX_BLOB_RETRIES = 2;
const MAX_AUTO_RESUMES = 5;
const SESSION_TTL_MS = 5 * 60 * 1000;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface ChatCompletionRequest {
  model: string;
  messages: OpenAIMessage[];
  stream?: boolean;
  temperature?: number;
  max_tokens?: number;
  tools?: OpenAIToolDef[];
  tool_choice?: unknown;
}

interface CursorRequestPayload {
  requestBytes: Uint8Array;
  blobStore: Map<string, Uint8Array>;
  mcpTools: McpToolDefinition[];
}

// ---------------------------------------------------------------------------
// Active sessions with TTL eviction
// ---------------------------------------------------------------------------

interface ActiveSession {
  session: CursorSession;
  convKey: string;
  storedAt: number;
}

const activeSessions = new Map<string, ActiveSession>();

function evictStaleSessions(): void {
  const cutoff = Date.now() - SESSION_TTL_MS;
  for (const [key, active] of activeSessions) {
    if (!active.session.alive) {
      activeSessions.delete(key);
      continue;
    }
    // Don't evict sessions waiting for client tool results
    if (active.session.flushedExecs.length > 0) continue;
    if (active.storedAt < cutoff) {
      active.session.close();
      activeSessions.delete(key);
    }
  }
}

/** Store a session, closing any existing entry for the same key to prevent orphans. */
function storeActiveSession(key: string, entry: ActiveSession): void {
  const existing = activeSessions.get(key);
  if (existing && existing.session !== entry.session) {
    existing.session.close();
  }
  activeSessions.set(key, entry);
}

setInterval(evictStaleSessions, 60_000).unref();

// ---------------------------------------------------------------------------
// HTTP server
// ---------------------------------------------------------------------------

let proxyServer: ReturnType<typeof Bun.serve> | undefined;
let proxyPort: number | undefined;
let proxyAccessTokenProvider: (() => Promise<string>) | undefined;
let proxyModels: Array<{ id: string; name: string }> = [];

function buildOpenAIModelList(models: ReadonlyArray<{ id: string; name: string }>) {
  return models.map((model) => ({
    id: model.id,
    object: "model" as const,
    created: 0,
    owned_by: "cursor",
  }));
}

export function getProxyPort(): number | undefined {
  return proxyPort;
}

export async function startProxy(
  getAccessToken: () => Promise<string>,
  models: ReadonlyArray<{ id: string; name: string }> = [],
): Promise<number> {
  proxyAccessTokenProvider = getAccessToken;
  proxyModels = models.map((m) => ({ id: m.id, name: m.name }));
  if (proxyServer && proxyPort) return proxyPort;

  proxyServer = Bun.serve({
    port: 0,
    idleTimeout: 255,
    async fetch(req) {
      const url = new URL(req.url);

      if (req.method === "GET" && url.pathname === "/v1/models") {
        return new Response(
          JSON.stringify({ object: "list", data: buildOpenAIModelList(proxyModels) }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      if (req.method === "POST" && url.pathname === "/v1/chat/completions") {
        try {
          const body = (await req.json()) as ChatCompletionRequest;
          if (!proxyAccessTokenProvider) throw new Error("Access token provider not configured");
          const accessToken = await proxyAccessTokenProvider();
          const sessionId = req.headers.get("x-session-affinity") ?? undefined;
          const parentSessionId = req.headers.get("x-parent-session-id") ?? undefined;
          return handleChatCompletion(body, accessToken, sessionId, parentSessionId);
        } catch (err) {
          logError("chat completion failed", errorDetails(err));
          const message = err instanceof Error ? err.message : String(err);
          return new Response(
            JSON.stringify({
              error: { message, type: "server_error", code: "internal_error" },
            }),
            { status: 500, headers: { "Content-Type": "application/json" } },
          );
        }
      }

      return jsonError("Not Found", "not_found", 404);
    },
  });

  proxyPort = proxyServer.port;
  if (!proxyPort) throw new Error("Failed to bind proxy to a port");
  return proxyPort;
}

export function stopProxy(): void {
  if (proxyServer) {
    proxyServer.stop();
    proxyServer = undefined;
    proxyPort = undefined;
    proxyAccessTokenProvider = undefined;
    proxyModels = [];
  }
  for (const { session, convKey } of activeSessions.values()) {
    const stored = getConversationState(convKey);
    if (stored) {
      for (const [k, v] of session.blobStore) stored.blobStore.set(k, v);
      stored.lastAccessMs = Date.now();
      persistConversation(convKey, stored);
    }
    session.close();
  }
  activeSessions.clear();
}

function buildMcpToolDefinitions(tools: OpenAIToolDef[]): McpToolDefinition[] {
  return tools.map((t) => {
    const fn = t.function;
    const jsonSchema: JsonValue =
      fn.parameters && typeof fn.parameters === "object"
        ? (fn.parameters as JsonValue)
        : { type: "object", properties: {}, required: [] };
    const inputSchema = toBinary(ValueSchema, fromJson(ValueSchema, jsonSchema));
    return create(McpToolDefinitionSchema, {
      name: fn.name,
      description: fn.description || "",
      providerIdentifier: "opencode",
      toolName: fn.name,
      inputSchema,
    });
  });
}

// ---------------------------------------------------------------------------
// Key derivation
// ---------------------------------------------------------------------------

function deriveBridgeKey(
  modelId: string,
  messages: OpenAIMessage[],
  sessionId?: string,
  parentSessionId?: string,
): string {
  const firstUserMsg = messages.find((m) => m.role === "user");
  const firstUserText = firstUserMsg ? textContent(firstUserMsg.content) : "";
  return createHash("sha256")
    .update(
      `bridge:${sessionId ?? ""}:${parentSessionId ?? ""}:${modelId}:${firstUserText.slice(0, 200)}`,
    )
    .digest("hex")
    .slice(0, 16);
}

function deriveConversationKey(
  messages: OpenAIMessage[],
  sessionId?: string,
  parentSessionId?: string,
): string {
  const systemParts = messages
    .filter((m) => m.role === "system")
    .map((m) => textContent(m.content));
  const systemText = systemParts.join("\n");
  return createHash("sha256")
    .update(`conv:${sessionId ?? ""}:${parentSessionId ?? ""}:${systemText.slice(0, 2000)}`)
    .digest("hex")
    .slice(0, 16);
}

// ---------------------------------------------------------------------------
// Cursor request construction
// ---------------------------------------------------------------------------

function buildCursorRequest(
  modelId: string,
  systemPrompt: string,
  userText: string,
  turns: Array<{ userText: string; assistantText: string }>,
  conversationId: string,
  checkpoint: Uint8Array | null,
  existingBlobStore?: Map<string, Uint8Array>,
): CursorRequestPayload {
  const blobStore = new Map<string, Uint8Array>(existingBlobStore ?? []);

  const systemJson = JSON.stringify({ role: "system", content: systemPrompt });
  const systemBytes = new TextEncoder().encode(systemJson);
  const systemBlobId = new Uint8Array(createHash("sha256").update(systemBytes).digest());
  blobStore.set(Buffer.from(systemBlobId).toString("hex"), systemBytes);

  let conversationState: ReturnType<typeof create<typeof ConversationStateStructureSchema>>;
  if (checkpoint) {
    conversationState = fromBinary(ConversationStateStructureSchema, checkpoint);
  } else {
    const turnBytes: Uint8Array[] = [];
    for (const turn of turns) {
      const userMsg = create(UserMessageSchema, {
        text: turn.userText,
        messageId: randomUUID(),
      });
      const userMsgBytes = toBinary(UserMessageSchema, userMsg);

      const stepBytes: Uint8Array[] = [];
      if (turn.assistantText) {
        const step = create(ConversationStepSchema, {
          message: {
            case: "assistantMessage",
            value: create(AssistantMessageSchema, { text: turn.assistantText }),
          },
        });
        stepBytes.push(toBinary(ConversationStepSchema, step));
      }

      const agentTurn = create(AgentConversationTurnStructureSchema, {
        userMessage: userMsgBytes,
        steps: stepBytes,
      });
      const turnStructure = create(ConversationTurnStructureSchema, {
        turn: { case: "agentConversationTurn", value: agentTurn },
      });
      turnBytes.push(toBinary(ConversationTurnStructureSchema, turnStructure));
    }

    conversationState = create(ConversationStateStructureSchema, {
      rootPromptMessagesJson: [systemBlobId],
      turns: turnBytes,
      todos: [],
      pendingToolCalls: [],
      previousWorkspaceUris: [],
      fileStates: {},
      fileStatesV2: {},
      summaryArchives: [],
      turnTimings: [],
      subagentStates: {},
      selfSummaryCount: 0,
      readPaths: [],
    });
  }

  const userMessage = create(UserMessageSchema, {
    text: userText,
    messageId: randomUUID(),
  });
  const action = create(ConversationActionSchema, {
    action: {
      case: "userMessageAction",
      value: create(UserMessageActionSchema, { userMessage }),
    },
  });

  const modelDetails = create(ModelDetailsSchema, {
    modelId,
    displayModelId: modelId,
    displayName: modelId,
  });

  const runRequest = create(AgentRunRequestSchema, {
    conversationState,
    action,
    modelDetails,
    conversationId,
  });

  const clientMessage = create(AgentClientMessageSchema, {
    message: { case: "runRequest", value: runRequest },
  });

  return {
    requestBytes: toBinary(AgentClientMessageSchema, clientMessage),
    blobStore,
    mcpTools: [],
  };
}

function buildResumeRequest(
  modelId: string,
  conversationId: string,
  checkpoint: Uint8Array | null,
  existingBlobStore: Map<string, Uint8Array>,
  mcpTools: McpToolDefinition[],
): CursorRequestPayload {
  const blobStore = new Map<string, Uint8Array>(existingBlobStore);

  const conversationState = checkpoint
    ? fromBinary(ConversationStateStructureSchema, checkpoint)
    : create(ConversationStateStructureSchema, {});

  const action = create(ConversationActionSchema, {
    action: {
      case: "resumeAction",
      value: create(ResumeActionSchema, {
        requestContext: create(RequestContextSchema, {
          tools: mcpTools,
        }),
      }),
    },
  });

  const modelDetails = create(ModelDetailsSchema, {
    modelId,
    displayModelId: modelId,
    displayName: modelId,
  });

  const runRequest = create(AgentRunRequestSchema, {
    conversationState,
    action,
    modelDetails,
    conversationId,
  });

  const clientMessage = create(AgentClientMessageSchema, {
    message: { case: "runRequest", value: runRequest },
  });

  return {
    requestBytes: toBinary(AgentClientMessageSchema, clientMessage),
    blobStore,
    mcpTools,
  };
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

function makeCheckpointCallback(
  convKey: string,
): (bytes: Uint8Array, blobStore: Map<string, Uint8Array>) => void {
  return (bytes, blobStore) => {
    // Re-resolve if the entry was evicted while session was alive
    const stored = getConversationState(convKey) ?? resolveConversationState(convKey);
    stored.checkpoint = bytes;
    for (const [k, v] of blobStore) stored.blobStore.set(k, v);
    stored.lastAccessMs = Date.now();
    persistConversation(convKey, stored);
  };
}

/**
 * Pump a session with auto-resume on timeout/resource_exhausted.
 * Returns the final PumpResult. On batchReady, stores the session.
 * On done/unrecoverable, closes the session.
 */
async function pumpWithAutoResume(
  ctx: SSECtx,
  session: CursorSession,
  modelId: string,
  bridgeKey: string,
  convKey: string,
): Promise<PumpResult> {
  let resumeCount = 0;
  let currentSession = session;

  for (let attempt = 0; attempt <= MAX_AUTO_RESUMES; attempt++) {
    const result = await pumpSession(currentSession, ctx);

    if (result.outcome === "done") {
      currentSession.close();
      return result;
    }

    if (result.outcome === "batchReady") {
      storeActiveSession(bridgeKey, {
        session: currentSession,
        convKey,
        storedAt: Date.now(),
      });
      return result;
    }

    const mcpTools = currentSession.mcpTools;
    const accessToken = currentSession.accessToken;
    currentSession.close();

    if (
      (result.retryHint === "timeout" || result.retryHint === "resource_exhausted") &&
      resumeCount < MAX_AUTO_RESUMES
    ) {
      resumeCount++;
      const isStepBoundary = result.retryHint === "resource_exhausted";
      if (isStepBoundary) {
        logDebug("step-boundary resume", { attempt: resumeCount });
      } else {
        logWarn("auto-resume", {
          hint: result.retryHint,
          attempt: resumeCount,
          max: MAX_AUTO_RESUMES,
        });
        ctx.sendChunk({
          content: `\n[Auto-resuming (attempt ${resumeCount}/${MAX_AUTO_RESUMES})...]\n`,
        });
      }
      const stored = getConversationState(convKey);
      if (stored?.checkpoint) {
        const payload = buildResumeRequest(
          modelId,
          stored.conversationId,
          stored.checkpoint,
          stored.blobStore,
          mcpTools,
        );
        currentSession = new CursorSession({
          accessToken,
          requestBytes: payload.requestBytes,
          blobStore: payload.blobStore,
          mcpTools: payload.mcpTools,
          convKey,
          onCheckpoint: makeCheckpointCallback(convKey),
        });
        continue;
      }
    }

    return result;
  }

  return { outcome: "done" };
}

/** Write an unrecoverable retry result to the SSE context. */
function writeRetryError(ctx: SSECtx, result: PumpResult): void {
  if (ctx.closed || result.outcome !== "retry") return;
  if (result.error) ctx.sendChunk({ content: `\n[Error: ${result.error}]` });
  ctx.sendChunk({}, "stop");
  ctx.sendDone();
}

// ---------------------------------------------------------------------------
// Chat completion handler
// ---------------------------------------------------------------------------

function handleChatCompletion(
  body: ChatCompletionRequest,
  accessToken: string,
  sessionId?: string,
  parentSessionId?: string,
): Response | Promise<Response> {
  if (detectTitleRequest(body)) {
    const sourceText = buildTitleSourceText(body.messages);
    if (sourceText) {
      logInfo("title request detected", { sourceLen: sourceText.length });
      return handleTitleGenerationRequest(
        sourceText,
        accessToken,
        body.model,
        body.stream !== false,
      );
    }
  }

  const { systemPrompt, userText, turns, toolResults } = parseMessages(body.messages);
  const modelId = body.model;
  const tools = selectToolsForChoice(body.tools ?? [], body.tool_choice);

  if (!userText && toolResults.length === 0) {
    return jsonError("No user message found", "missing_user_message");
  }

  if (body.stream === false && tools.length > 0) {
    return jsonError("Non-streaming responses with tools are not supported", "unsupported_mode");
  }

  const bridgeKey = deriveBridgeKey(modelId, body.messages, sessionId, parentSessionId);
  const convKey = deriveConversationKey(body.messages, sessionId, parentSessionId);
  const active = activeSessions.get(bridgeKey);

  // --- Tool-result resume ---
  if (toolResults.length > 0) {
    const resumeResponse = tryToolResultResume(active, toolResults, bridgeKey, modelId, convKey);
    if (resumeResponse) return resumeResponse;
  }

  // Clean up stale session
  if (active && activeSessions.has(bridgeKey)) {
    active.session.close();
    activeSessions.delete(bridgeKey);
  }

  // New conversation detection
  const isFirstMessage = turns.length === 0 && toolResults.length === 0;
  if (isFirstMessage) {
    logDebug(`new conversation - clearing stale state for key ${convKey}`);
    invalidateConversationState(convKey);
  }

  const stored = resolveConversationState(convKey);

  const fp = turnsFingerprint(turns);
  const historicCheckpoint = stored.checkpointHistory.get(fp);
  if (historicCheckpoint) {
    logDebug(`checkpoint-history hit for fp=${fp}`);
    stored.checkpoint = historicCheckpoint;
  } else if (stored.checkpoint && fp) {
    stored.checkpointHistory.set(fp, stored.checkpoint);
    if (stored.checkpointHistory.size > 30) {
      const oldest = stored.checkpointHistory.keys().next().value!;
      stored.checkpointHistory.delete(oldest);
    }
  }

  const mcpTools = buildMcpToolDefinitions(tools);
  const effectiveUserText =
    userText || (toolResults.length > 0 ? toolResults.map((r) => r.content).join("\n") : "");

  const payload = buildCursorRequest(
    modelId,
    systemPrompt,
    effectiveUserText,
    turns,
    stored.conversationId,
    stored.checkpoint,
    stored.blobStore,
  );
  payload.mcpTools = mcpTools;

  logInfo("chat completion request", {
    model: modelId,
    stream: body.stream !== false,
    tools: tools.length,
    userTextLen: effectiveUserText.length,
    hasCheckpoint: !!stored.checkpoint,
    messages: body.messages.length,
    turns: turns.length,
    blobs: stored.blobStore.size,
  });

  if (body.stream === false) {
    return handleNonStreamingResponse(payload, accessToken, modelId, convKey, systemPrompt);
  }

  return handleStreamingWithRetry(
    payload,
    accessToken,
    modelId,
    bridgeKey,
    convKey,
    systemPrompt,
    effectiveUserText,
    turns,
    mcpTools,
  );
}

// ---------------------------------------------------------------------------
// Tool-result resume validation
// ---------------------------------------------------------------------------

function jsonError(message: string, code: string, status = 400): Response {
  return new Response(JSON.stringify({ error: { message, type: "invalid_request_error", code } }), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function tryToolResultResume(
  active: ActiveSession | undefined,
  toolResults: ToolResultInfo[],
  bridgeKey: string,
  modelId: string,
  convKey: string,
): Response | null {
  if (!active) {
    logDebug("tool results received but no active session -- falling through to fresh request", {
      bridgeKey,
      toolResultIds: toolResults.map((r) => r.toolCallId),
    });
    return null;
  }

  activeSessions.delete(bridgeKey);
  const pendingIds = new Set(active.session.flushedExecs.map((e) => e.toolCallId));
  const newResults = toolResults.filter((r) => pendingIds.has(r.toolCallId));
  const orphanedCount = toolResults.length - newResults.length;

  if (orphanedCount > 0) {
    logWarn("tool results reference unknown pending exec IDs", {
      orphanedCount,
      knownIds: [...pendingIds],
      receivedIds: toolResults.map((r) => r.toolCallId),
    });
  }

  if (newResults.length === 0) {
    active.session.close();
    return jsonError(
      "None of the provided tool results match pending executions",
      "tool_results_mismatch",
    );
  }

  if (active.session.alive) {
    logDebug(
      `resume: session alive, ${newResults.length} new tool results (of ${toolResults.length} total)`,
    );
    active.session.sendToolResults(newResults);
    return handleResumeStream(active.session, modelId, bridgeKey, convKey);
  }

  logDebug("resume: session DEAD, falling through to fresh session");
  active.session.close();
  return null;
}

// ---------------------------------------------------------------------------
// Streaming with blob-not-found retries (auto-resume handled by shared loop)
// ---------------------------------------------------------------------------

interface StreamingPumpOpts {
  ctx: SSECtx;
  initialPayload: CursorRequestPayload;
  accessToken: string;
  modelId: string;
  bridgeKey: string;
  convKey: string;
  systemPrompt?: string;
  effectiveUserText?: string;
  turns?: Array<{ userText: string; assistantText: string }>;
  mcpTools?: McpToolDefinition[];
  onSession: (s: CursorSession) => void;
}

async function runStreamingPump(opts: StreamingPumpOpts): Promise<void> {
  const {
    ctx,
    accessToken,
    modelId,
    bridgeKey,
    convKey,
    systemPrompt,
    effectiveUserText,
    turns,
    mcpTools,
    onSession,
  } = opts;
  let currentPayload = opts.initialPayload;

  for (let blobAttempt = 0; blobAttempt <= MAX_BLOB_RETRIES; blobAttempt++) {
    const session = new CursorSession({
      accessToken,
      requestBytes: currentPayload.requestBytes,
      blobStore: currentPayload.blobStore,
      mcpTools: currentPayload.mcpTools,
      cloudRule: systemPrompt,
      convKey,
      onCheckpoint: makeCheckpointCallback(convKey),
    });
    onSession(session);

    const result = await pumpWithAutoResume(ctx, session, modelId, bridgeKey, convKey);

    if (result.outcome !== "retry") break;

    if (result.retryHint === "blob_not_found" && blobAttempt < MAX_BLOB_RETRIES) {
      if (blobAttempt === 0) {
        logWarn("blob not found - soft retry: nulling checkpoint", { convKey });
        const stored2 = resolveConversationState(convKey);
        stored2.checkpoint = null;
        persistConversation(convKey, stored2);
        currentPayload = buildCursorRequest(
          modelId,
          systemPrompt ?? "",
          effectiveUserText ?? "",
          turns ?? [],
          stored2.conversationId,
          null,
          stored2.blobStore,
        );
      } else {
        logWarn("blob not found again - hard retry: full invalidation", { convKey });
        invalidateConversationState(convKey);
        const fresh = resolveConversationState(convKey);
        currentPayload = buildCursorRequest(
          modelId,
          systemPrompt ?? "",
          effectiveUserText ?? "",
          turns ?? [],
          fresh.conversationId,
          null,
          fresh.blobStore,
        );
      }
      currentPayload.mcpTools = mcpTools ?? [];
      continue;
    }

    writeRetryError(ctx, result);
    break;
  }
}

function handleStreamingWithRetry(
  initialPayload: CursorRequestPayload,
  accessToken: string,
  modelId: string,
  bridgeKey: string,
  convKey: string,
  systemPrompt?: string,
  effectiveUserText?: string,
  turns?: Array<{ userText: string; assistantText: string }>,
  mcpTools?: McpToolDefinition[],
): Response {
  const completionId = `chatcmpl-${randomUUID().replace(/-/g, "").slice(0, 28)}`;
  const created = Math.floor(Date.now() / 1000);

  const ref = { session: undefined as CursorSession | undefined, cancelled: false };

  const stream = new ReadableStream({
    start(controller) {
      const ctx = createSSECtx(controller, modelId, completionId, created);

      void (async () => {
        try {
          await runStreamingPump({
            ctx,
            initialPayload,
            accessToken,
            modelId,
            bridgeKey,
            convKey,
            systemPrompt,
            effectiveUserText,
            turns,
            mcpTools,
            onSession(s) {
              ref.session = s;
              if (ref.cancelled) s.close();
            },
          });
        } catch (err) {
          logDebug("streaming pump interrupted", { error: String(err) });
        } finally {
          ref.session = undefined;
          ctx.close();
        }
      })();
    },
    cancel() {
      logDebug("ReadableStream cancel (streaming)", { bridgeKey, convKey });
      ref.cancelled = true;
      ref.session?.close();
      ref.session = undefined;
    },
  });

  return new Response(stream, { headers: SSE_HEADERS });
}

// ---------------------------------------------------------------------------
// Tool-result resume (session still alive, continue streaming)
// ---------------------------------------------------------------------------

function handleResumeStream(
  session: CursorSession,
  modelId: string,
  bridgeKey: string,
  convKey: string,
): Response {
  const completionId = `chatcmpl-${randomUUID().replace(/-/g, "").slice(0, 28)}`;
  const created = Math.floor(Date.now() / 1000);
  // Session is known upfront (unlike handleStreamingWithRetry); same cancel contract.
  const ref = { session: session as CursorSession | undefined, cancelled: false };

  const stream = new ReadableStream({
    start(controller) {
      const ctx = createSSECtx(controller, modelId, completionId, created);

      void (async () => {
        try {
          const result = await pumpWithAutoResume(ctx, session, modelId, bridgeKey, convKey);
          writeRetryError(ctx, result);
        } catch (err) {
          logDebug("resume pump interrupted", { error: String(err) });
        } finally {
          ref.session = undefined;
          ctx.close();
        }
      })();
    },
    cancel() {
      logDebug("ReadableStream cancel (resume)", { bridgeKey, convKey });
      ref.cancelled = true;
      ref.session?.close();
      ref.session = undefined;
    },
  });

  return new Response(stream, { headers: SSE_HEADERS });
}

// ---------------------------------------------------------------------------
// Non-streaming
// ---------------------------------------------------------------------------

async function handleNonStreamingResponse(
  payload: CursorRequestPayload,
  accessToken: string,
  modelId: string,
  convKey: string,
  systemPrompt?: string,
): Promise<Response> {
  const session = new CursorSession({
    accessToken,
    requestBytes: payload.requestBytes,
    blobStore: payload.blobStore,
    mcpTools: payload.mcpTools,
    cloudRule: systemPrompt,
    convKey,
    onCheckpoint: makeCheckpointCallback(convKey),
  });
  return collectNonStreamingResponse(session, modelId);
}
