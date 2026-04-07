import { createHash, randomUUID } from "node:crypto";
import { create, fromBinary, fromJson, type JsonValue, toBinary } from "@bufbuild/protobuf";
import { ValueSchema } from "@bufbuild/protobuf/wkt";
import {
  getConversationState,
  invalidateConversationState,
  persistConversation,
  resolveConversationState,
  type StoredConversation,
  type Turn,
  turnsFingerprint,
} from "./conversation-state";
import type { RetryHint } from "./cursor-session";
import { CursorSession } from "./cursor-session";
import { errorDetails, logDebug, logError, logInfo, logWarn } from "./logger";
import { MCP_TOOL_PREFIX } from "./native-tools";
import {
  type OpenAIMessage,
  type OpenAIToolDef,
  parseMessages,
  selectToolsForChoice,
  type ToolResultInfo,
  textContent,
} from "./openai-messages";
import {
  collectNonStreamingResponse,
  createSSECtx,
  type PumpResult,
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
import { type CursorRuntimeConfig, resolveRuntimeConfig } from "./runtime-config";
import { buildTitleSourceText, detectTitleRequest, handleTitleGenerationRequest } from "./title";

const MAX_BLOB_RETRIES = 2;
const MAX_TIMEOUT_AUTO_RESUMES = 5;
const MAX_RESOURCE_EXHAUSTED_AUTO_RESUMES = 10;
const CHECKPOINT_HISTORY_LIMIT = 30;
const CHECKPOINT_ARCHIVE_LIMIT = 60;

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

function isOpenAIMessageArray(value: unknown): value is OpenAIMessage[] {
  return (
    Array.isArray(value) &&
    value.every(
      (message) =>
        message !== null &&
        typeof message === "object" &&
        typeof (message as { role?: unknown }).role === "string",
    )
  );
}

function isChatCompletionRequest(value: unknown): value is ChatCompletionRequest {
  if (!value || typeof value !== "object") return false;
  const request = value as { model?: unknown; messages?: unknown };
  return typeof request.model === "string" && isOpenAIMessageArray(request.messages);
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
  const now = Date.now();
  for (const [key, active] of activeSessions) {
    const ttlCutoff = now - active.session.runtimeConfig.activeSessionTtlMs;
    const flushedCutoff = now - active.session.runtimeConfig.flushedSessionMaxLifetimeMs;
    if (!active.session.alive) {
      activeSessions.delete(key);
      continue;
    }
    if (active.session.flushedExecs.length > 0) {
      // Safety net: evict even flushed sessions after 60 min to prevent leaks
      // from clients that disconnect without cancelling the stream.
      if (active.storedAt < flushedCutoff) {
        logDebug("evicting long-lived flushed session", {
          bridgeKey: key,
          ageSec: Math.round((now - active.storedAt) / 1000),
        });
        active.session.close();
        activeSessions.delete(key);
      }
      continue;
    }
    if (active.storedAt < ttlCutoff) {
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
let proxyRuntimeConfig: CursorRuntimeConfig = resolveRuntimeConfig();

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
  runtimeConfig?: Partial<CursorRuntimeConfig>,
): Promise<number> {
  proxyAccessTokenProvider = getAccessToken;
  proxyModels = models.map((m) => ({ id: m.id, name: m.name }));
  proxyRuntimeConfig = resolveRuntimeConfig(runtimeConfig);
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
        return handleChatCompletionFetch(req);
      }

      return jsonError("Not Found", "not_found", 404);
    },
  });

  proxyPort = proxyServer.port;
  if (!proxyPort) throw new Error("Failed to bind proxy to a port");
  return proxyPort;
}

async function handleChatCompletionFetch(req: Request): Promise<Response> {
  try {
    const body = await req.json();
    if (!isChatCompletionRequest(body)) {
      return jsonError("Invalid chat completion request", "invalid_request");
    }
    if (!proxyAccessTokenProvider) throw new Error("Access token provider not configured");
    const accessToken = await proxyAccessTokenProvider();
    const sessionId = req.headers.get("x-session-affinity") ?? undefined;
    const parentSessionId = req.headers.get("x-parent-session-id") ?? undefined;
    const opencodeAgent = req.headers.get("x-opencode-agent") ?? undefined;
    return handleChatCompletion(
      body,
      accessToken,
      proxyRuntimeConfig,
      sessionId,
      parentSessionId,
      opencodeAgent,
    );
  } catch (err) {
    if (err instanceof SyntaxError) {
      return jsonError("Invalid JSON body", "invalid_json");
    }
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

export function stopProxy(): void {
  if (proxyServer) {
    proxyServer.stop();
    proxyServer = undefined;
    proxyPort = undefined;
    proxyAccessTokenProvider = undefined;
    proxyModels = [];
  }
  for (const { session, convKey } of activeSessions.values()) {
    const stored = getConversationState(convKey, session.runtimeConfig);
    if (stored) {
      for (const [k, v] of session.blobStore) stored.blobStore.set(k, v);
      stored.lastAccessMs = Date.now();
      persistConversation(convKey, stored, session.runtimeConfig);
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
      name: `${MCP_TOOL_PREFIX}${fn.name}`,
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

/**
 * Derive a stable conversation key for checkpoint storage.
 *
 * When session headers are present (OpenCode sets `x-session-affinity`), the
 * key is based solely on session/parent IDs — intentionally ignoring message
 * content so that compaction agents (which rewrite the system prompt) share
 * the same stored conversation as the original session.
 *
 * Without session headers (anonymous / legacy), the key incorporates the
 * system prompt text so different instructions get separate conversations.
 */
export function deriveConversationKey(
  messages: OpenAIMessage[],
  sessionId?: string,
  parentSessionId?: string,
): string {
  if (sessionId || parentSessionId) {
    return createHash("sha256")
      .update(`conv:${sessionId ?? ""}:${parentSessionId ?? ""}`)
      .digest("hex")
      .slice(0, 16);
  }

  const systemParts = messages
    .filter((m) => m.role === "system")
    .map((m) => textContent(m.content));
  const systemText = systemParts.join("\n");
  return createHash("sha256")
    .update(`conv:${sessionId ?? ""}:${parentSessionId ?? ""}:${systemText.slice(0, 2000)}`)
    .digest("hex")
    .slice(0, 16);
}

const OPENCODE_AGENT_COMPACTION = "compaction";

export function shouldBypassStoredCheckpoint(opencodeAgent?: string): boolean {
  return opencodeAgent === OPENCODE_AGENT_COMPACTION;
}

interface PreparedConversationState {
  checkpoint: Uint8Array | null;
  didReset: boolean;
}

function rememberCheckpoint(
  map: Map<string, Uint8Array>,
  fingerprint: string,
  checkpoint: Uint8Array,
  limit: number,
): void {
  if (!fingerprint) return;
  map.delete(fingerprint);
  map.set(fingerprint, checkpoint);
  while (map.size > limit) {
    const oldest = map.keys().next().value;
    if (!oldest) break;
    map.delete(oldest);
  }
}

function archiveCheckpointLineage(stored: StoredConversation, fingerprint: string): boolean {
  for (const [fp, cp] of stored.checkpointHistory) {
    rememberCheckpoint(stored.checkpointArchive, fp, cp, CHECKPOINT_ARCHIVE_LIMIT);
  }
  if (stored.checkpoint) {
    const key = fingerprint || "__current__";
    rememberCheckpoint(stored.checkpointArchive, key, stored.checkpoint, CHECKPOINT_ARCHIVE_LIMIT);
  }

  const didReset = stored.checkpoint !== null || stored.checkpointHistory.size > 0;
  stored.checkpoint = null;
  stored.checkpointHistory.clear();
  return didReset;
}

export function prepareStoredConversationForRequest(
  stored: StoredConversation,
  turns: Turn[],
  opencodeAgent?: string,
): PreparedConversationState {
  const fp = turnsFingerprint(turns);

  if (shouldBypassStoredCheckpoint(opencodeAgent)) {
    const didReset = archiveCheckpointLineage(stored, fp);
    return { checkpoint: null, didReset };
  }

  const historicCheckpoint = stored.checkpointHistory.get(fp);
  if (historicCheckpoint) {
    logDebug("checkpoint history hit", { fp });
    stored.checkpoint = historicCheckpoint;
    return { checkpoint: historicCheckpoint, didReset: false };
  }

  if (fp) {
    const archivedCheckpoint = stored.checkpointArchive.get(fp);
    if (archivedCheckpoint) {
      logDebug("checkpoint archive hit", { fp });
      stored.checkpoint = archivedCheckpoint;
      return { checkpoint: archivedCheckpoint, didReset: false };
    }
  }

  if (stored.checkpoint && fp) {
    rememberCheckpoint(stored.checkpointHistory, fp, stored.checkpoint, CHECKPOINT_HISTORY_LIMIT);
  }

  return { checkpoint: stored.checkpoint, didReset: false };
}

// ---------------------------------------------------------------------------
// Cursor request construction
// ---------------------------------------------------------------------------

function buildCursorRequest(
  modelId: string,
  systemPrompt: string,
  userText: string,
  turns: Turn[],
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
  const decodedCheckpoint = checkpoint
    ? decodeCheckpointState(checkpoint, "buildCursorRequest")
    : null;
  if (decodedCheckpoint) {
    conversationState = decodedCheckpoint;
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

  const runRequest = create(AgentRunRequestSchema, {
    conversationState,
    action,
    modelDetails: createModelDetails(modelId),
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

  const conversationState =
    (checkpoint ? decodeCheckpointState(checkpoint, "buildResumeRequest") : null) ??
    create(ConversationStateStructureSchema, {});

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

  const runRequest = create(AgentRunRequestSchema, {
    conversationState,
    action,
    modelDetails: createModelDetails(modelId),
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

function createModelDetails(modelId: string) {
  return create(ModelDetailsSchema, {
    modelId,
    displayModelId: modelId,
    displayName: modelId,
    maxMode: true,
  });
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

function decodeCheckpointState(
  checkpoint: Uint8Array,
  context: string,
): ReturnType<typeof create<typeof ConversationStateStructureSchema>> | null {
  try {
    return fromBinary(ConversationStateStructureSchema, checkpoint);
  } catch (error) {
    logWarn("Ignoring invalid stored checkpoint", {
      context,
      error: error instanceof Error ? error.message : String(error),
    });
    return null;
  }
}

function sanitizeStoredCheckpointForBuild(
  stored: StoredConversation,
  checkpoint: Uint8Array | null,
  convKey: string,
  runtimeConfig: Partial<CursorRuntimeConfig>,
  context: string,
): Uint8Array | null {
  if (!checkpoint) return null;
  if (decodeCheckpointState(checkpoint, context)) return checkpoint;
  stored.checkpoint = null;
  for (const [fp, candidate] of stored.checkpointHistory) {
    if (buffersEqual(candidate, checkpoint)) stored.checkpointHistory.delete(fp);
  }
  for (const [fp, candidate] of stored.checkpointArchive) {
    if (buffersEqual(candidate, checkpoint)) stored.checkpointArchive.delete(fp);
  }
  persistConversation(convKey, stored, runtimeConfig);
  return null;
}

function buffersEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) return false;
  for (let index = 0; index < left.length; index++) {
    if (left[index] !== right[index]) return false;
  }
  return true;
}

function makeCheckpointCallback(
  convKey: string,
  runtimeConfig: Partial<CursorRuntimeConfig>,
): (bytes: Uint8Array, blobStore: Map<string, Uint8Array>) => void {
  return (bytes, blobStore) => {
    // Re-resolve if the entry was evicted while session was alive
    const stored =
      getConversationState(convKey, runtimeConfig) ??
      resolveConversationState(convKey, runtimeConfig);
    stored.checkpoint = bytes;
    for (const [k, v] of blobStore) stored.blobStore.set(k, v);
    stored.lastAccessMs = Date.now();
    persistConversation(convKey, stored, runtimeConfig);
  };
}

function buildAutoResumePayload(options: {
  stored: StoredConversation | undefined;
  convKey: string;
  runtimeConfig: Partial<CursorRuntimeConfig>;
  modelId: string;
  mcpTools: McpToolDefinition[];
  rebuildRequest?: () => CursorRequestPayload;
  attempt: number;
}): CursorRequestPayload | null {
  const { stored, convKey, runtimeConfig, modelId, mcpTools, rebuildRequest, attempt } = options;
  if (stored?.checkpoint) {
    const safeCheckpoint = sanitizeStoredCheckpointForBuild(
      stored,
      stored.checkpoint,
      convKey,
      runtimeConfig,
      "auto-resume",
    );
    if (safeCheckpoint) {
      return buildResumeRequest(
        modelId,
        stored.conversationId,
        safeCheckpoint,
        stored.blobStore,
        mcpTools,
      );
    }
  }

  if (!rebuildRequest) return null;
  logDebug("no checkpoint for resume, rebuilding original request", {
    attempt,
  });
  const payload = rebuildRequest();
  payload.mcpTools = mcpTools;
  return payload;
}

export function resourceExhaustedBackoffDelayMs(
  attempt: number,
  runtimeConfig: Pick<
    CursorRuntimeConfig,
    "resourceExhaustedRetryDelayMs" | "resourceExhaustedRetryMaxDelayMs"
  >,
): number {
  if (attempt <= 0) return 0;
  const baseDelay = Math.max(0, Math.trunc(runtimeConfig.resourceExhaustedRetryDelayMs));
  if (baseDelay === 0) return 0;
  const maxDelay = Math.max(baseDelay, Math.trunc(runtimeConfig.resourceExhaustedRetryMaxDelayMs));
  return Math.min(baseDelay * 2 ** (attempt - 1), maxDelay);
}

function retryDelayMsForHint(
  runtimeConfig: CursorRuntimeConfig,
  result: PumpResult,
  attempt: number,
): number {
  if (result.outcome !== "retry") return 0;
  return result.retryHint === "resource_exhausted"
    ? resourceExhaustedBackoffDelayMs(attempt, runtimeConfig)
    : 0;
}

export function autoResumeAttemptLimit(retryHint: RetryHint | undefined): number {
  if (retryHint === "resource_exhausted") return MAX_RESOURCE_EXHAUSTED_AUTO_RESUMES;
  if (retryHint === "timeout") return MAX_TIMEOUT_AUTO_RESUMES;
  return 0;
}

async function sleepMs(ms: number): Promise<void> {
  if (ms <= 0) return;
  await new Promise<void>((resolve) => setTimeout(resolve, ms));
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
  rebuildRequest?: () => CursorRequestPayload,
): Promise<PumpResult> {
  let resumeCount = 0;
  let currentSession = session;

  while (true) {
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
    const cloudRule = currentSession.cloudRule;
    currentSession.close();
    const maxAutoResumes = autoResumeAttemptLimit(result.retryHint);

    if (
      (result.retryHint === "timeout" || result.retryHint === "resource_exhausted") &&
      resumeCount < maxAutoResumes
    ) {
      resumeCount++;
      const isStepBoundary = result.retryHint === "resource_exhausted";
      const retryDelayMs = retryDelayMsForHint(currentSession.runtimeConfig, result, resumeCount);
      if (isStepBoundary) {
        logDebug("step-boundary resume", {
          attempt: resumeCount,
          max: maxAutoResumes,
          delayMs: retryDelayMs,
        });
      } else {
        logWarn("auto-resume", {
          hint: result.retryHint,
          attempt: resumeCount,
          max: maxAutoResumes,
        });
        ctx.sendChunk({
          content: `\n[Auto-resuming (attempt ${resumeCount}/${maxAutoResumes})...]\n`,
        });
      }
      await sleepMs(retryDelayMs);
      if (ctx.closed) return { outcome: "done" };

      const stored = getConversationState(convKey, currentSession.runtimeConfig);
      const payload = buildAutoResumePayload({
        stored,
        convKey,
        runtimeConfig: currentSession.runtimeConfig,
        modelId,
        mcpTools,
        rebuildRequest,
        attempt: resumeCount,
      });

      if (payload) {
        currentSession = new CursorSession({
          accessToken,
          requestBytes: payload.requestBytes,
          blobStore: payload.blobStore,
          mcpTools: payload.mcpTools,
          cloudRule,
          convKey,
          runtimeConfig: currentSession.runtimeConfig,
          onCheckpoint: makeCheckpointCallback(convKey, currentSession.runtimeConfig),
        });
        continue;
      }
    }

    return result;
  }
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
  runtimeConfig: Partial<CursorRuntimeConfig>,
  sessionId?: string,
  parentSessionId?: string,
  opencodeAgent?: string,
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
        runtimeConfig,
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
    invalidateConversationState(convKey, runtimeConfig);
  }

  const stored = resolveConversationState(convKey, runtimeConfig);

  const preparedConversation = prepareStoredConversationForRequest(stored, turns, opencodeAgent);
  if (preparedConversation.didReset) {
    logInfo("resetting stored conversation for compaction request", {
      convKey,
      sessionId,
      parentSessionId,
    });
    persistConversation(convKey, stored, runtimeConfig);
  }

  const mcpTools = buildMcpToolDefinitions(tools);
  const effectiveUserText =
    userText || (toolResults.length > 0 ? toolResults.map((r) => r.content).join("\n") : "");
  const safeCheckpoint = sanitizeStoredCheckpointForBuild(
    stored,
    preparedConversation.checkpoint,
    convKey,
    runtimeConfig,
    "chat-completion",
  );

  const payload = buildCursorRequest(
    modelId,
    systemPrompt,
    effectiveUserText,
    turns,
    stored.conversationId,
    safeCheckpoint,
    stored.blobStore,
  );
  payload.mcpTools = mcpTools;

  logInfo("chat completion request", {
    model: modelId,
    stream: body.stream !== false,
    tools: tools.length,
    userTextLen: effectiveUserText.length,
    hasCheckpoint: !!preparedConversation.checkpoint,
    messages: body.messages.length,
    turns: turns.length,
    blobs: stored.blobStore.size,
  });

  if (body.stream === false) {
    return handleNonStreamingResponse(
      payload,
      accessToken,
      modelId,
      convKey,
      runtimeConfig,
      systemPrompt,
    );
  }

  return handleStreamingWithRetry(
    payload,
    accessToken,
    modelId,
    bridgeKey,
    convKey,
    runtimeConfig,
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
  const staleCount = toolResults.length - newResults.length;

  if (staleCount > 0) {
    logDebug("resume: filtered stale tool result IDs from prior turns", {
      staleCount,
      matchedCount: newResults.length,
      staleSample: toolResults
        .filter((r) => !pendingIds.has(r.toolCallId))
        .slice(0, 3)
        .map((r) => r.toolCallId),
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
    logDebug("resume: session alive", {
      matchedCount: newResults.length,
      totalReceived: toolResults.length,
    });
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
  runtimeConfig: Partial<CursorRuntimeConfig>;
  systemPrompt?: string;
  effectiveUserText?: string;
  turns?: Turn[];
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
      runtimeConfig: opts.runtimeConfig,
      onCheckpoint: makeCheckpointCallback(convKey, opts.runtimeConfig),
    });
    onSession(session);

    const rebuildRequest = () => {
      const stored = resolveConversationState(convKey, opts.runtimeConfig);
      const safeCheckpoint = sanitizeStoredCheckpointForBuild(
        stored,
        stored.checkpoint,
        convKey,
        opts.runtimeConfig,
        "stream-rebuild",
      );
      return buildCursorRequest(
        modelId,
        systemPrompt ?? "",
        effectiveUserText ?? "",
        turns ?? [],
        stored.conversationId,
        safeCheckpoint,
        stored.blobStore,
      );
    };
    const result = await pumpWithAutoResume(
      ctx,
      session,
      modelId,
      bridgeKey,
      convKey,
      rebuildRequest,
    );

    if (result.outcome !== "retry") break;

    if (result.retryHint === "blob_not_found" && blobAttempt < MAX_BLOB_RETRIES) {
      if (blobAttempt === 0) {
        logWarn("blob not found - soft retry: nulling checkpoint", { convKey });
        const stored2 = resolveConversationState(convKey, opts.runtimeConfig);
        stored2.checkpoint = null;
        persistConversation(convKey, stored2, opts.runtimeConfig);
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
        invalidateConversationState(convKey, opts.runtimeConfig);
        const fresh = resolveConversationState(convKey, opts.runtimeConfig);
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
  runtimeConfig: Partial<CursorRuntimeConfig>,
  systemPrompt?: string,
  effectiveUserText?: string,
  turns?: Turn[],
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
            runtimeConfig,
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
  runtimeConfig: Partial<CursorRuntimeConfig>,
  systemPrompt?: string,
): Promise<Response> {
  const session = new CursorSession({
    accessToken,
    requestBytes: payload.requestBytes,
    blobStore: payload.blobStore,
    mcpTools: payload.mcpTools,
    cloudRule: systemPrompt,
    convKey,
    runtimeConfig,
    onCheckpoint: makeCheckpointCallback(convKey, runtimeConfig),
  });
  return collectNonStreamingResponse(session, modelId);
}
