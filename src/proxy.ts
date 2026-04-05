/**
 * Local OpenAI-compatible proxy that translates requests to Cursor's gRPC protocol.
 *
 * Accepts POST /v1/chat/completions in OpenAI format, translates to Cursor's
 * protobuf/HTTP2 Connect protocol, and streams back OpenAI-format SSE.
 *
 * Tool calling uses Cursor's native MCP tool protocol:
 * - OpenAI tool defs → McpToolDefinition in RequestContext
 * - Cursor toolCallStarted/Delta/Completed → OpenAI tool_calls SSE chunks
 * - mcpArgs exec → pause stream, return tool_calls to caller
 * - Follow-up request with tool results → resume bridge with mcpResult
 *
 * HTTP/2 transport uses in-process node:http2 (works in Bun).
 */
import { create, fromBinary, fromJson, type JsonValue, toBinary, toJson } from "@bufbuild/protobuf";
import { ValueSchema } from "@bufbuild/protobuf/wkt";
import {
  AgentClientMessageSchema,
  AgentRunRequestSchema,
  AgentServerMessageSchema,
  ClientHeartbeatSchema,
  ConversationActionSchema,
  ConversationStateStructureSchema,
  ConversationStepSchema,
  AgentConversationTurnStructureSchema,
  ConversationTurnStructureSchema,
  AssistantMessageSchema,
  BackgroundShellSpawnResultSchema,
  DeleteResultSchema,
  DeleteRejectedSchema,
  DeleteSuccessSchema,
  DiagnosticsResultSchema,
  ExecClientMessageSchema,
  FetchErrorSchema,
  FetchResultSchema,
  FetchSuccessSchema,
  GetBlobResultSchema,
  GrepErrorSchema,
  GrepResultSchema,
  KvClientMessageSchema,
  LsRejectedSchema,
  LsResultSchema,
  McpErrorSchema,
  McpResultSchema,
  McpSuccessSchema,
  McpTextContentSchema,
  McpInstructionsSchema,
  McpToolDefinitionSchema,
  McpToolResultContentItemSchema,
  ModelDetailsSchema,
  ReadRejectedSchema,
  ReadResultSchema,
  ReadSuccessSchema,
  RequestContextResultSchema,
  RequestContextSchema,
  RequestContextSuccessSchema,
  SetBlobResultSchema,
  ShellRejectedSchema,
  ShellResultSchema,
  ShellSuccessSchema,
  ShellStreamSchema,
  ShellStreamStartSchema,
  ShellStreamStdoutSchema,
  ShellStreamStderrSchema,
  ShellStreamExitSchema,
  ResumeActionSchema,
  UserMessageActionSchema,
  UserMessageSchema,
  WriteRejectedSchema,
  WriteResultSchema,
  WriteSuccessSchema,
  WriteShellStdinErrorSchema,
  WriteShellStdinResultSchema,
  InteractionResponseSchema,
  WebSearchRequestResponseSchema,
  WebSearchRequestResponse_ApprovedSchema,
  ExaSearchRequestResponseSchema,
  ExaFetchRequestResponseSchema,
  AskQuestionInteractionResponseSchema,
  AskQuestionResultSchema,
  AskQuestionRejectedSchema,
  SwitchModeRequestResponseSchema,
  CreatePlanRequestResponseSchema,
  ExecClientControlMessageSchema,
  ExecClientStreamCloseSchema,
  NameAgentRequestSchema,
  NameAgentResponseSchema,
  type AgentServerMessage,
  type ConversationStateStructure,
  type ExecServerControlMessage,
  type ExecServerMessage,
  type KvServerMessage,
  type McpToolDefinition,
} from "./proto/agent_pb";
import { logDebug, logInfo, logWarn, logError, errorDetails } from "./logger";
import { createHash, randomBytes } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync, readdirSync, unlinkSync, statSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";

const CURSOR_API_URL = process.env.CURSOR_API_URL ?? "https://api2.cursor.sh";
const CURSOR_AGENT_URL = process.env.CURSOR_AGENT_URL ?? "https://agentn.us.api5.cursor.sh";
const CONNECT_END_STREAM_FLAG = 0b00000010;
import { connect as h2Connect, type ClientHttp2Session, type ClientHttp2Stream } from "node:http2";

const CURSOR_CLIENT_VERSION = "cli-2026.03.30-a5d3e17";

function proxyLog(msg: string, ...args: unknown[]): void {
  let i = 0;
  const formatted = msg.replace(/%[sdj]/g, () => String(args[i++] ?? ""));
  logDebug(formatted);
}
const SSE_HEADERS = {
  "Content-Type": "text/event-stream",
  "Cache-Control": "no-cache",
  Connection: "keep-alive",
} as const;

interface OpenAIToolCall {
  id: string;
  type: "function";
  function: { name: string; arguments: string };
}

/** A single element in an OpenAI multi-part content array. */
interface ContentPart {
  type: string;
  text?: string;
}

interface OpenAIMessage {
  role: "system" | "user" | "assistant" | "tool";
  content: string | null | ContentPart[];
  tool_call_id?: string;
  tool_calls?: OpenAIToolCall[];
}

interface OpenAIToolDef {
  type: "function";
  function: {
    name: string;
    description?: string;
    parameters?: Record<string, unknown>;
  };
}

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

/** Native exec types we redirect through MCP instead of rejecting. */
type NativeResultType = "readResult" | "writeResult" | "deleteResult" | "fetchResult" | "shellResult" | "shellStreamResult" | "lsResult" | "grepResult";

/** A pending tool execution waiting for results from the caller. */
interface PendingExec {
  execId: string;
  execMsgId: number;
  toolCallId: string;
  toolName: string;
  /** Decoded arguments JSON string for SSE tool_calls emission. */
  decodedArgs: string;
  /** Set when this exec originated from a native Cursor tool redirected to MCP. */
  nativeResultType?: NativeResultType;
  /** Original native args needed for result construction (e.g., path, url). */
  nativeArgs?: Record<string, string>;
}

/** A bridge kept alive across requests for tool result continuation. */
interface ActiveBridge {
  bridge: BridgeHandle;
  heartbeatTimer: NodeJS.Timeout;
  blobStore: Map<string, Uint8Array>;
  mcpTools: McpToolDefinition[];
  pendingExecs: PendingExec[];
  convKey: string;
  /** Accumulated exec count across all resumes within this bridge session. */
  totalExecCount: number;
  toolCallIndex: number;
  accessToken: string;
  resumeCount: number;
}

// Active bridges keyed by a session token (derived from conversation state).
// When tool_calls are returned, the bridge stays alive. The next request
// with tool results looks up the bridge and sends mcpResult messages.
const activeBridges = new Map<string, ActiveBridge>();

/** Global per-bridge inactivity timers. Keyed by bridgeKey.
 *  Ensures only ONE timer per bridge regardless of how many streams are created.
 *
 *  Two-stage timeout:
 *  - THINKING: waiting for the model's first token after tool results (model is reasoning)
 *  - STREAMING: gap between tokens while the model is actively outputting */
const bridgeInactivityTimers = new Map<string, NodeJS.Timeout>();
const THINKING_TIMEOUT_MS = 30_000;
const STREAMING_TIMEOUT_MS = 15_000;

function setBridgeInactivityTimer(
  bridgeKey: string,
  bridge: BridgeHandle,
  heartbeatTimer: NodeJS.Timeout,
  onTimeout: () => boolean | void,
  phase: "thinking" | "streaming" = "thinking",
): void {
  const timeoutMs = phase === "thinking" ? THINKING_TIMEOUT_MS : STREAMING_TIMEOUT_MS;
  clearBridgeInactivityTimer(bridgeKey);
  bridgeInactivityTimers.set(bridgeKey, setTimeout(() => {
    logWarn("inactivity timeout", { bridgeKey: bridgeKey.slice(0, 8), timeoutSec: timeoutMs / 1000, phase });
    bridgeInactivityTimers.delete(bridgeKey);
    const keepAlive = onTimeout();
    if (keepAlive) return;
    activeBridges.delete(bridgeKey);
    clearInterval(heartbeatTimer);
    bridge.end();
  }, timeoutMs));
}

function clearBridgeInactivityTimer(bridgeKey: string): void {
  const existing = bridgeInactivityTimers.get(bridgeKey);
  if (existing) {
    clearTimeout(existing);
    bridgeInactivityTimers.delete(bridgeKey);
  }
}

interface StoredConversation {
  conversationId: string;
  checkpoint: Uint8Array | null;
  blobStore: Map<string, Uint8Array>;
  lastAccessMs: number;
  checkpointHistory: Map<string, Uint8Array>;
}

function turnsFingerprint(turns: ParsedMessages["turns"]): string {
  if (turns.length === 0) return "";
  const h = createHash("md5");
  for (const t of turns) {
    h.update(t.userText);
    h.update("\0");
    h.update(t.assistantText);
    h.update("\0");
  }
  return `${turns.length}:${h.digest("hex").slice(0, 12)}`;
}

const conversationStates = new Map<string, StoredConversation>();
const CONVERSATION_TTL_MS = 30 * 60 * 1000; // 30 minutes

function evictStaleConversations(): void {
  const now = Date.now();
  for (const [key, stored] of conversationStates) {
    if (now - stored.lastAccessMs > CONVERSATION_TTL_MS) {
      conversationStates.delete(key);
      try { unlinkSync(convDiskPath(key)); } catch {}
    }
  }
}

// --- Disk persistence for conversation state across process restarts ---

const CONV_DISK_DIR = join(
  process.env.XDG_DATA_HOME || join(homedir(), ".local", "share"),
  "opencode",
  "cursor-conversations",
);
try { mkdirSync(CONV_DISK_DIR, { recursive: true }); } catch {}

const CONV_DISK_TTL_MS = 24 * 60 * 60 * 1000; // 24h on-disk TTL

function convDiskPath(convKey: string): string {
  return join(CONV_DISK_DIR, `${convKey}.json`);
}

interface SerializedConversation {
  conversationId: string;
  checkpoint: string | null; // base64
  blobStore: Record<string, string>; // hex key → base64 value
  savedMs: number;
  checkpointHistory?: Record<string, string>; // fingerprint → base64 checkpoint
}

function persistConversation(convKey: string, stored: StoredConversation): void {
  const data: SerializedConversation = {
    conversationId: stored.conversationId,
    checkpoint: stored.checkpoint ? Buffer.from(stored.checkpoint).toString("base64") : null,
    blobStore: Object.fromEntries(
      [...stored.blobStore].map(([k, v]) => [k, Buffer.from(v).toString("base64")]),
    ),
    savedMs: Date.now(),
    checkpointHistory: Object.fromEntries(
      [...stored.checkpointHistory].map(([fp, cp]) => [fp, Buffer.from(cp).toString("base64")]),
    ),
  };
  try { writeFileSync(convDiskPath(convKey), JSON.stringify(data)); } catch {}
}

function loadConversation(convKey: string): StoredConversation | null {
  try {
    const raw: SerializedConversation = JSON.parse(readFileSync(convDiskPath(convKey), "utf-8"));
    if (Date.now() - raw.savedMs > CONV_DISK_TTL_MS) {
      try { unlinkSync(convDiskPath(convKey)); } catch {}
      return null;
    }
    return {
      conversationId: raw.conversationId,
      checkpoint: raw.checkpoint ? new Uint8Array(Buffer.from(raw.checkpoint, "base64")) : null,
      blobStore: new Map(
        Object.entries(raw.blobStore).map(([k, v]) => [k, new Uint8Array(Buffer.from(v, "base64"))]),
      ),
      lastAccessMs: Date.now(),
      checkpointHistory: new Map(
        Object.entries(raw.checkpointHistory ?? {}).map(([fp, cp]) => [fp, new Uint8Array(Buffer.from(cp, "base64"))]),
      ),
    };
  } catch {
    return null;
  }
}

function evictStaleDiskConversations(): void {
  try {
    const now = Date.now();
    for (const name of readdirSync(CONV_DISK_DIR)) {
      if (!name.endsWith(".json")) continue;
      const full = join(CONV_DISK_DIR, name);
      try {
        if (now - statSync(full).mtimeMs > CONV_DISK_TTL_MS) unlinkSync(full);
      } catch {}
    }
  } catch {}
}

/** Length-prefix a message: [4-byte BE length][payload] */
/** Connect protocol frame: [1-byte flags][4-byte BE length][payload] */
function frameConnectMessage(data: Uint8Array, flags = 0): Buffer {
  const frame = Buffer.alloc(5 + data.length);
  frame[0] = flags;
  frame.writeUInt32BE(data.length, 1);
  frame.set(data, 5);
  return frame;
}

interface BridgeFrameHandler {
  onMessage: (bytes: Uint8Array) => void;
  onEndStream: (bytes: Uint8Array) => void;
  afterParse?: () => void;
}

interface BridgeHandle {
  write: (data: Uint8Array) => void;
  end: () => void;
  /** Swap the parsed-message handler. The underlying frame parser (and its
   *  buffer) persists across calls — no data is lost on handler replacement. */
  setHandler: (handler: BridgeFrameHandler) => void;
  onClose: (cb: (code: number) => void) => void;
  readonly alive: boolean;
}

interface SpawnBridgeOptions {
  accessToken: string;
  rpcPath: string;
  url?: string;
  unary?: boolean;
}

function spawnBridge(options: SpawnBridgeOptions): BridgeHandle {
  const baseUrl = options.url ?? CURSOR_API_URL;
  const isApi2 = baseUrl.includes("api2.cursor.sh");
  const connectUrl = isApi2
    ? baseUrl.replace("api2.cursor.sh", "api2direct.cursor.sh")
    : baseUrl;
  const unary = options.unary ?? false;

  const requestId = crypto.randomUUID();
  const traceId = randomBytes(16).toString("hex");
  const spanId = randomBytes(8).toString("hex");
  const traceparent = `00-${traceId}-${spanId}-01`;

  const frameCbs: BridgeFrameHandler = {
    onMessage: () => {},
    onEndStream: () => {},
    afterParse: undefined,
  };
  const frameParser = createConnectFrameParser(
    (bytes) => frameCbs.onMessage(bytes),
    (bytes) => frameCbs.onEndStream(bytes),
  );

  let closeCb: ((code: number) => void) | null = null;
  let alive = true;
  let closeCode = 0;
  let handlerReady = false;
  const pendingChunks: Buffer[] = [];

  const finish = (code: number) => {
    if (!alive) return;
    alive = false;
    closeCode = code;
    closeCb?.(code);
  };

  let h2Session: ClientHttp2Session | undefined;
  let h2Stream: ClientHttp2Stream | undefined;

  const closeTransport = () => {
    try { h2Stream?.close(); } catch {}
    try { h2Session?.close(); } catch {}
  };

  proxyLog("bridge: connecting to %s", connectUrl);
  h2Session = h2Connect(connectUrl);

  h2Session.on("error", (err) => {
    logError("bridge: h2 session error", { error: err?.message ?? err });
    closeTransport();
    finish(1);
  });

  const headers: Record<string, string> = {
    ":method": "POST",
    ":path": options.rpcPath || "/agent.v1.AgentService/Run",
    "content-type": unary ? "application/proto" : "application/connect+proto",
    "user-agent": "connect-es/1.6.1",
    authorization: `Bearer ${options.accessToken}`,
    "x-ghost-mode": "true",
    "x-cursor-client-version": CURSOR_CLIENT_VERSION,
    "x-cursor-client-type": "cli",
    "x-request-id": requestId,
    "x-original-request-id": requestId,
    traceparent,
    "backend-traceparent": traceparent,
  };
  if (isApi2) headers[":authority"] = "api2.cursor.sh";
  if (!unary) headers["connect-protocol-version"] = "1";

  h2Stream = h2Session.request(headers);

  h2Stream.on("data", (chunk: Buffer | Uint8Array) => {
    const buf = Buffer.from(chunk);
    if (handlerReady) {
      frameParser(buf);
      frameCbs.afterParse?.();
    } else {
      pendingChunks.push(buf);
    }
  });
  h2Stream.on("end", () => {
    proxyLog("bridge: stream ended by server");
    closeTransport();
    finish(0);
  });
  h2Stream.on("error", (err) => {
    logError("bridge: stream error", { error: err?.message ?? err });
    closeTransport();
    finish(1);
  });

  return {
    get alive() { return alive; },
    write(data) {
      if (!alive || !h2Stream) return;
      try { h2Stream.write(data); } catch {}
    },
    end() {
      closeTransport();
      finish(0);
    },
    setHandler(handler) {
      frameCbs.onMessage = handler.onMessage;
      frameCbs.onEndStream = handler.onEndStream;
      frameCbs.afterParse = handler.afterParse;
      handlerReady = true;
      while (pendingChunks.length > 0) {
        frameParser(pendingChunks.shift()!);
        frameCbs.afterParse?.();
      }
    },
    onClose(cb) {
      if (!alive) {
        queueMicrotask(() => cb(closeCode));
      } else {
        closeCb = cb;
      }
    },
  };
}

interface CursorUnaryRpcOptions {
  accessToken: string;
  rpcPath: string;
  requestBody: Uint8Array;
  url?: string;
  timeoutMs?: number;
}

export async function callCursorUnaryRpc(
  options: CursorUnaryRpcOptions,
): Promise<{ body: Uint8Array; exitCode: number; timedOut: boolean }> {
  const baseUrl = options.url ?? CURSOR_API_URL;
  const isApi2 = baseUrl.includes("api2.cursor.sh");
  const connectUrl = isApi2
    ? baseUrl.replace("api2.cursor.sh", "api2direct.cursor.sh")
    : baseUrl;

  const requestId = crypto.randomUUID();
  const { promise, resolve } = Promise.withResolvers<{
    body: Uint8Array;
    exitCode: number;
    timedOut: boolean;
  }>();
  let timedOut = false;
  let settled = false;

  const session = h2Connect(connectUrl);
  const timeoutMs = options.timeoutMs ?? 5_000;
  const timeout = timeoutMs > 0
    ? setTimeout(() => {
        timedOut = true;
        try { session.destroy(); } catch {}
      }, timeoutMs)
    : undefined;

  const finish = (body: Uint8Array, code: number) => {
    if (settled) return;
    settled = true;
    if (timeout) clearTimeout(timeout);
    try { session.close(); } catch {}
    resolve({ body, exitCode: code, timedOut });
  };

  session.on("error", () => finish(new Uint8Array(0), 1));

  const headers: Record<string, string> = {
    ":method": "POST",
    ":path": options.rpcPath,
    "content-type": "application/proto",
    "user-agent": "connect-es/1.6.1",
    authorization: `Bearer ${options.accessToken}`,
    "x-ghost-mode": "true",
    "x-cursor-client-version": CURSOR_CLIENT_VERSION,
    "x-cursor-client-type": "cli",
    "x-request-id": requestId,
  };
  if (isApi2) headers[":authority"] = "api2.cursor.sh";

  const stream = session.request(headers);
  const chunks: Buffer[] = [];
  stream.on("data", (chunk: Buffer) => chunks.push(Buffer.from(chunk)));
  stream.on("end", () => finish(Buffer.concat(chunks), 0));
  stream.on("error", () => finish(Buffer.concat(chunks), 1));
  // Bun's node:http2 breaks on end(Buffer.alloc(0)) — use bare end() for empty bodies
  if (options.requestBody.length > 0) {
    stream.end(Buffer.from(options.requestBody));
  } else {
    stream.end();
  }

  return promise;
}

let proxyServer: ReturnType<typeof Bun.serve> | undefined;
let proxyPort: number | undefined;
let proxyAccessTokenProvider: (() => Promise<string>) | undefined;
let proxyModels: Array<{ id: string; name: string }> = [];

function buildOpenAIModelList(models: ReadonlyArray<{ id: string; name: string }>): Array<{
  id: string;
  object: "model";
  created: number;
  owned_by: string;
}> {
  return models.map((model) => ({
    id: model.id,
    object: "model",
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
  proxyModels = models.map((model) => ({
    id: model.id,
    name: model.name,
  }));
  if (proxyServer && proxyPort) return proxyPort;

  proxyServer = Bun.serve({
    port: 0,
    idleTimeout: 255, // max — Cursor responses can take 30s+
    async fetch(req) {
      const url = new URL(req.url);

      if (req.method === "GET" && url.pathname === "/v1/models") {
        return new Response(
          JSON.stringify({
            object: "list",
            data: buildOpenAIModelList(proxyModels),
          }),
          { headers: { "Content-Type": "application/json" } },
        );
      }

      if (req.method === "POST" && url.pathname === "/v1/chat/completions") {
        try {
          const body = (await req.json()) as ChatCompletionRequest;
          if (!proxyAccessTokenProvider) {
            throw new Error("Cursor proxy access token provider not configured");
          }
          const accessToken = await proxyAccessTokenProvider();
          const sessionId = req.headers.get("x-session-id") ?? undefined;
          const agentKey = req.headers.get("x-opencode-agent") ?? undefined;
          return handleChatCompletion(body, accessToken, sessionId, agentKey);
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

      return new Response("Not Found", { status: 404 });
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
  // Merge blobs from active bridges into stored state before shutdown
  for (const active of activeBridges.values()) {
    const stored = conversationStates.get(active.convKey);
    if (stored) {
      for (const [k, v] of active.blobStore) stored.blobStore.set(k, v);
      stored.lastAccessMs = Date.now();
      persistConversation(active.convKey, stored);
    }
    clearInterval(active.heartbeatTimer);
    active.bridge.end();
  }
  activeBridges.clear();
  for (const timer of bridgeInactivityTimers.values()) clearTimeout(timer);
  bridgeInactivityTimers.clear();
}

// ---------------------------------------------------------------------------
// Title generation
// ---------------------------------------------------------------------------

const TITLE_REQUEST_MARKER = "Generate a title for this conversation:";

function detectTitleRequest(body: ChatCompletionRequest): boolean {
  if ((body.tools?.length ?? 0) > 0) return false;
  const firstUser = body.messages.find(m => m.role === "user");
  return !!firstUser && textContent(firstUser.content).trim() === TITLE_REQUEST_MARKER;
}

function buildTitleSourceText(messages: OpenAIMessage[]): string {
  return messages
    .filter(m => m.role !== "system")
    .map(m => {
      const text = textContent(m.content).trim();
      return text === TITLE_REQUEST_MARKER ? "" : text;
    })
    .filter(Boolean)
    .join("\n")
    .trim();
}

function finalizeTitle(value: string): string {
  return value
    .replace(/^#{1,6}\s*/, "")
    .replace(/[.!?,:;]+$/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 80)
    .trim();
}

function deriveFallbackTitle(text: string): string {
  const cleaned = text
    .replace(/<[^>]+>/g, " ")
    .replace(/\[[^\]]+\]/g, " ")
    .replace(/[^\p{L}\p{N}''\u2019\-\s]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!cleaned) return "";
  const words = cleaned.split(" ").filter(Boolean).slice(0, 6);
  return finalizeTitle(words.map(w => w[0]!.toUpperCase() + w.slice(1)).join(" "));
}

async function handleTitleGenerationRequest(
  sourceText: string,
  accessToken: string,
  modelId: string,
  stream: boolean,
): Promise<Response> {
  let title: string;
  try {
    const requestBody = toBinary(
      NameAgentRequestSchema,
      create(NameAgentRequestSchema, { userMessage: sourceText }),
    );
    const response = await callCursorUnaryRpc({
      accessToken,
      rpcPath: "/agent.v1.AgentService/NameAgent",
      requestBody,
      timeoutMs: 5_000,
    });
    if (response.timedOut || response.exitCode !== 0) {
      title = deriveFallbackTitle(sourceText);
    } else {
      let payload = response.body;
      if (payload.length > 5 && payload[0] === 0x00) payload = payload.slice(5);
      const decoded = fromBinary(NameAgentResponseSchema, payload);
      title = finalizeTitle(decoded.name) || deriveFallbackTitle(sourceText);
    }
  } catch {
    title = deriveFallbackTitle(sourceText);
  }
  title = title || "Untitled Session";
  logInfo("title generated", { title });

  const completionId = `chatcmpl-${crypto.randomUUID().replace(/-/g, "").slice(0, 28)}`;
  const created = Math.floor(Date.now() / 1000);
  const usage = { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 };

  if (stream) {
    const chunks = [
      { id: completionId, object: "chat.completion.chunk", created, model: modelId,
        choices: [{ index: 0, delta: { content: title }, finish_reason: null }] },
      { id: completionId, object: "chat.completion.chunk", created, model: modelId,
        choices: [{ index: 0, delta: {}, finish_reason: "stop" }] },
      { id: completionId, object: "chat.completion.chunk", created, model: modelId,
        choices: [], usage },
    ].map(c => `data: ${JSON.stringify(c)}\n\n`).join("") + "data: [DONE]\n\n";
    return new Response(chunks, { headers: SSE_HEADERS });
  }

  return new Response(JSON.stringify({
    id: completionId,
    object: "chat.completion",
    created,
    model: modelId,
    choices: [{ index: 0, message: { role: "assistant", content: title }, finish_reason: "stop" }],
    usage,
  }), { headers: { "Content-Type": "application/json" } });
}

// ---------------------------------------------------------------------------
// Chat completion handler
// ---------------------------------------------------------------------------

function handleChatCompletion(
  body: ChatCompletionRequest,
  accessToken: string,
  sessionId?: string,
  agentKey?: string,
): Response | Promise<Response> {
  if (detectTitleRequest(body)) {
    const sourceText = buildTitleSourceText(body.messages);
    if (sourceText) {
      logInfo("title request detected", { sourceLen: sourceText.length });
      return handleTitleGenerationRequest(sourceText, accessToken, body.model, body.stream !== false);
    }
  }

  const { systemPrompt, userText, turns, toolResults } = parseMessages(body.messages);
  const modelId = body.model;
  const tools = selectToolsForChoice(body.tools ?? [], body.tool_choice);

  if (!userText && toolResults.length === 0) {
    return new Response(
      JSON.stringify({
        error: {
          message: "No user message found",
          type: "invalid_request_error",
        },
      }),
      { status: 400, headers: { "Content-Type": "application/json" } },
    );
  }

  // bridgeKey: model-specific, for active tool-call bridges
  // convKey: model-independent, for conversation state that survives model switches
  const bridgeKey = deriveBridgeKey(modelId, body.messages, sessionId, agentKey);
  const convKey = deriveConversationKey(body.messages, sessionId, agentKey);
  const activeBridge = activeBridges.get(bridgeKey);

  if (activeBridge && toolResults.length > 0) {
    activeBridges.delete(bridgeKey);

    const pendingIds = new Set(activeBridge.pendingExecs.map(e => e.toolCallId));
    const newResults = toolResults.filter(r => pendingIds.has(r.toolCallId));

    if (activeBridge.bridge.alive) {
      proxyLog("resume: bridge alive, %d new tool results (of %d total)", newResults.length, toolResults.length);
      return handleToolResultResume(activeBridge, newResults, modelId, bridgeKey, convKey);
    }

    proxyLog("resume: bridge DEAD, falling through to fresh bridge (%d results)", newResults.length);
    clearInterval(activeBridge.heartbeatTimer);
    activeBridge.bridge.end();
  }

  // Clean up stale bridge if present
  if (activeBridge && activeBridges.has(bridgeKey)) {
    clearInterval(activeBridge.heartbeatTimer);
    activeBridge.bridge.end();
    activeBridges.delete(bridgeKey);
  }

  const isFirstMessage = turns.length === 0 && toolResults.length === 0;
  if (isFirstMessage) {
    proxyLog("new conversation detected (turns=0, no tool results) — clearing stale state for key %s", convKey);
    invalidateConversationState(convKey);
  }

  const stored = resolveConversationState(convKey);

  const fp = turnsFingerprint(turns);
  const historicCheckpoint = stored.checkpointHistory.get(fp);
  if (historicCheckpoint) {
    proxyLog("checkpoint-history hit for fp=%s — using stored checkpoint (undo or revisit)", fp);
    stored.checkpoint = historicCheckpoint;
  } else if (stored.checkpoint && fp) {
    stored.checkpointHistory.set(fp, stored.checkpoint);
    if (stored.checkpointHistory.size > 30) {
      const oldest = stored.checkpointHistory.keys().next().value!;
      stored.checkpointHistory.delete(oldest);
    }
  }

  const mcpTools = buildMcpToolDefinitions(tools);
  const effectiveUserText = userText || (toolResults.length > 0
    ? toolResults.map((r) => r.content).join("\n")
    : "");
  const payload = buildCursorRequest(
    modelId, systemPrompt, effectiveUserText, turns,
    stored.conversationId, stored.checkpoint, stored.blobStore,
  );
  payload.mcpTools = mcpTools;

  const turnsChars = turns.reduce((s, t) => s + t.userText.length + t.assistantText.length, 0);
  logInfo("chat completion request", {
    model: modelId, stream: body.stream !== false, tools: tools.length,
    userTextLen: effectiveUserText.length, hasCheckpoint: !!stored.checkpoint,
    messages: body.messages.length, turns: turns.length, turnsChars, blobs: stored.blobStore.size,
  });

  if (body.stream === false) {
    return handleNonStreamingResponse(payload, accessToken, modelId, convKey);
  }

  return handleStreamingResponse(payload, accessToken, modelId, bridgeKey, convKey, () => {
    logWarn("blob not found — soft retry: nulling checkpoint", { convKey });
    const stored2 = resolveConversationState(convKey);
    stored2.checkpoint = null;
    persistConversation(convKey, stored2);
    const softPayload = buildCursorRequest(
      modelId, systemPrompt, effectiveUserText, turns,
      stored2.conversationId, null, stored2.blobStore,
    );
    softPayload.mcpTools = mcpTools;
    proxyLog("soft retry: turns=%d turnsChars=%d blobs=%d",
      turns.length, turnsChars, stored2.blobStore.size);
    return handleStreamingResponse(softPayload, accessToken, modelId, bridgeKey, convKey, () => {
      logWarn("blob not found again — hard retry: full invalidation", { convKey });
      invalidateConversationState(convKey);
      const fresh = resolveConversationState(convKey);
      const hardPayload = buildCursorRequest(
        modelId, systemPrompt, effectiveUserText, turns,
        fresh.conversationId, null, fresh.blobStore,
      );
      hardPayload.mcpTools = mcpTools;
      proxyLog("hard retry: turns=%d turnsChars=%d blobs=%d",
        turns.length, turnsChars, fresh.blobStore.size);
      return handleStreamingResponse(hardPayload, accessToken, modelId, bridgeKey, convKey);
    });
  });
}

function resolveConversationState(convKey: string): StoredConversation {
  let stored = conversationStates.get(convKey);
  if (!stored) {
    stored = loadConversation(convKey) ?? {
      conversationId: deterministicConversationId(convKey),
      checkpoint: null,
      blobStore: new Map(),
      lastAccessMs: Date.now(),
      checkpointHistory: new Map(),
    };
    conversationStates.set(convKey, stored);
  }
  stored.lastAccessMs = Date.now();
  evictStaleConversations();
  evictStaleDiskConversations();
  return stored;
}

function invalidateConversationState(convKey: string): void {
  conversationStates.delete(convKey);
  try { unlinkSync(convDiskPath(convKey)); } catch {}
}

interface ToolResultInfo {
  toolCallId: string;
  content: string;
}

interface ParsedMessages {
  systemPrompt: string;
  userText: string;
  turns: Array<{ userText: string; assistantText: string }>;
  toolResults: ToolResultInfo[];
}

/** Normalize OpenAI message content to a plain string. */
function textContent(content: OpenAIMessage["content"]): string {
  if (content == null) return "";
  if (typeof content === "string") return content;
  return content
    .filter((p) => p.type === "text" && p.text)
    .map((p) => p.text!)
    .join("\n");
}

function parseMessages(messages: OpenAIMessage[]): ParsedMessages {
  let systemPrompt = "You are a helpful assistant.";
  const pairs: Array<{ userText: string; assistantText: string }> = [];
  const toolResults: ToolResultInfo[] = [];

  // Collect system messages
  const systemParts = messages
    .filter((m) => m.role === "system")
    .map((m) => textContent(m.content));
  if (systemParts.length > 0) {
    systemPrompt = systemParts.join("\n");
  }

  const nonSystem = messages.filter((m) => m.role !== "system");
  let pendingUser = "";
  let pendingAssistant = "";
  const pendingToolCalls: OpenAIToolCall[] = [];

  for (const msg of nonSystem) {
    if (msg.role === "tool") {
      const toolId = msg.tool_call_id ?? "";
      const call = pendingToolCalls.find((tc) => tc.id === toolId);
      const toolContent = textContent(msg.content);
      if (call) {
        const argsPreview = call.function.arguments.length > 200
          ? call.function.arguments.slice(0, 200) + "..."
          : call.function.arguments;
        const resultPreview = toolContent.length > 20000
          ? toolContent.slice(0, 20000) + "\n...[truncated from " + toolContent.length + " chars]"
          : toolContent;
        pendingAssistant += `\n[Tool ${call.function.name}(${argsPreview})]\n${resultPreview}\n`;
      }
      toolResults.push({ toolCallId: toolId, content: toolContent });
    } else if (msg.role === "user") {
      if (pendingUser) {
        pairs.push({ userText: pendingUser, assistantText: pendingAssistant });
        pendingAssistant = "";
        pendingToolCalls.length = 0;
      }
      pendingUser = textContent(msg.content);
    } else if (msg.role === "assistant") {
      const text = textContent(msg.content);
      if (text) pendingAssistant += text;
      if (msg.tool_calls) {
        pendingToolCalls.push(...msg.tool_calls);
      }
      if (pendingUser && !msg.tool_calls) {
        pairs.push({ userText: pendingUser, assistantText: pendingAssistant });
        pendingUser = "";
        pendingAssistant = "";
        pendingToolCalls.length = 0;
      }
    }
  }

  let lastUserText = "";
  if (pendingUser) {
    lastUserText = pendingUser;
  } else if (pairs.length > 0 && toolResults.length === 0) {
    const last = pairs.pop()!;
    lastUserText = last.userText;
  }

  return { systemPrompt, userText: lastUserText, turns: pairs, toolResults };
}

/** Filter tools according to OpenAI tool_choice semantics. */
function selectToolsForChoice(tools: OpenAIToolDef[], toolChoice: unknown): OpenAIToolDef[] {
  if (!tools.length) return [];
  if (toolChoice === undefined || toolChoice === null || toolChoice === "auto" || toolChoice === "required") {
    return tools;
  }
  if (toolChoice === "none") return [];
  if (typeof toolChoice === "object" && toolChoice !== null) {
    const choice = toolChoice as { type?: unknown; function?: { name?: unknown } };
    if (choice.type === "function" && typeof choice.function?.name === "string") {
      return tools.filter(t => t.function.name === choice.function!.name);
    }
  }
  return tools;
}

/** Convert OpenAI tool definitions to Cursor's MCP tool protobuf format. */
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

/** Decode a Cursor MCP arg value (protobuf Value bytes) to a JS value. */
function decodeMcpArgValue(value: Uint8Array): unknown {
  try {
    const parsed = fromBinary(ValueSchema, value);
    return toJson(ValueSchema, parsed);
  } catch {}
  return new TextDecoder().decode(value);
}

/** Decode a map of MCP arg values. */
function decodeMcpArgsMap(args: Record<string, Uint8Array>): Record<string, unknown> {
  const decoded: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(args)) {
    decoded[key] = decodeMcpArgValue(value);
  }
  return decoded;
}

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

  // System prompt → blob store (Cursor requests it back via KV handshake)
  const systemJson = JSON.stringify({ role: "system", content: systemPrompt });
  const systemBytes = new TextEncoder().encode(systemJson);
  const systemBlobId = new Uint8Array(
    createHash("sha256").update(systemBytes).digest(),
  );
  blobStore.set(Buffer.from(systemBlobId).toString("hex"), systemBytes);

  let conversationState;
  if (checkpoint) {
    conversationState = fromBinary(ConversationStateStructureSchema, checkpoint);
  } else {
    const turnBytes: Uint8Array[] = [];
    for (const turn of turns) {
      const userMsg = create(UserMessageSchema, {
        text: turn.userText,
        messageId: crypto.randomUUID(),
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
    messageId: crypto.randomUUID(),
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

/** Build a resume request (the "Continue" button) to bypass the 25 tool call limit. */
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

function parseConnectEndStream(data: Uint8Array): Error | null {
  try {
    const payload = JSON.parse(new TextDecoder().decode(data));
    const error = payload?.error;
    if (error) {
      const code = error.code ?? "unknown";
      const message = error.message ?? "Unknown error";
      return new Error(`Connect error ${code}: ${message}`);
    }
    return null;
  } catch {
    return new Error("Failed to parse Connect end stream");
  }
}

function makeHeartbeatBytes(): Uint8Array {
  const heartbeat = create(AgentClientMessageSchema, {
    message: {
      case: "clientHeartbeat",
      value: create(ClientHeartbeatSchema, {}),
    },
  });
  return frameConnectMessage(toBinary(AgentClientMessageSchema, heartbeat));
}

/**
 * Create a stateful parser for Connect protocol frames.
 * Handles buffering partial data across chunks.
 */
function createConnectFrameParser(
  onMessage: (bytes: Uint8Array) => void,
  onEndStream: (bytes: Uint8Array) => void,
): (incoming: Buffer) => void {
  let pending = Buffer.alloc(0);
  return (incoming: Buffer) => {
    pending = Buffer.concat([pending, incoming]);
    while (pending.length >= 5) {
      const flags = pending[0]!;
      const msgLen = pending.readUInt32BE(1);
      if (pending.length < 5 + msgLen) break;
      const messageBytes = pending.subarray(5, 5 + msgLen);
      pending = pending.subarray(5 + msgLen);
      if (flags & CONNECT_END_STREAM_FLAG) {
        onEndStream(messageBytes);
      } else {
        onMessage(messageBytes);
      }
    }
  };
}

const THINKING_TAG_NAMES = ['think', 'thinking', 'reasoning', 'thought', 'think_intent'];
const MAX_THINKING_TAG_LEN = 16; // </think_intent> is 15 chars

/**
 * Strip thinking tags from streamed text, routing tagged content to reasoning.
 * Buffers partial tags across chunk boundaries.
 */
function createThinkingTagFilter(): {
  process(text: string): { content: string; reasoning: string };
  flush(): { content: string; reasoning: string };
} {
  let buffer = '';
  let inThinking = false;

  return {
    process(text: string) {
      const input = buffer + text;
      buffer = '';
      let content = '';
      let reasoning = '';
      let lastIdx = 0;

      const re = new RegExp(`<(/?)(?:${THINKING_TAG_NAMES.join('|')})\\s*>`, 'gi');
      let match: RegExpExecArray | null;
      while ((match = re.exec(input)) !== null) {
        const before = input.slice(lastIdx, match.index);
        if (inThinking) reasoning += before;
        else content += before;
        inThinking = match[1] !== '/';
        lastIdx = re.lastIndex;
      }

      const rest = input.slice(lastIdx);
      // Buffer a trailing '<' that could be the start of a thinking tag.
      const ltPos = rest.lastIndexOf('<');
      if (ltPos >= 0 && rest.length - ltPos < MAX_THINKING_TAG_LEN && /^<\/?[a-z_]*$/i.test(rest.slice(ltPos))) {
        buffer = rest.slice(ltPos);
        const before = rest.slice(0, ltPos);
        if (inThinking) reasoning += before;
        else content += before;
      } else {
        if (inThinking) reasoning += rest;
        else content += rest;
      }

      return { content, reasoning };
    },
    flush() {
      const b = buffer;
      buffer = '';
      if (!b) return { content: '', reasoning: '' };
      return inThinking ? { content: '', reasoning: b } : { content: b, reasoning: '' };
    },
  };
}

interface StreamState {
  toolCallIndex: number;
  /** Total exec round-trips (MCP + native rejects + requestContext). Tracks Cursor's 25-call limit. */
  totalExecCount: number;
  pendingExecs: PendingExec[];
  outputTokens: number;
  totalTokens: number;
  /** Set when the server sends an endStream frame (clean close or error). */
  endStreamSeen: boolean;
  /** Set by batch-complete signals (checkpoint, stepCompleted, turnEnded,
   *  requestContextArgs) to indicate pending execs should be flushed.
   *  NOT set by toolCallStarted (which means more tools are coming)
   *  or heartbeat (which is just a keepalive). */
  checkpointAfterExec: boolean;
  /** Tracks last delta type for debug logging transitions. */
  lastDeltaType: string | null;
}

function computeUsage(state: StreamState) {
  const completion_tokens = state.outputTokens;
  const total_tokens = state.totalTokens || completion_tokens;
  const prompt_tokens = Math.max(0, total_tokens - completion_tokens);
  return { prompt_tokens, completion_tokens, total_tokens };
}

/** Returns true if the message was a recognized type (real server activity, not keepalive). */
function processServerMessage(
  msg: AgentServerMessage,
  blobStore: Map<string, Uint8Array>,
  mcpTools: McpToolDefinition[],
  sendFrame: (data: Uint8Array) => void,
  state: StreamState,
  onText: (text: string, isThinking?: boolean) => void,
  onMcpExec: (exec: PendingExec) => void,
  onCheckpoint?: (checkpointBytes: Uint8Array) => void,
  onNotify?: (text: string) => void,
): boolean {
  const msgCase = msg.message.case;

  if (msgCase === "interactionUpdate") {
    handleInteractionUpdate(msg.message.value, state, onText);
    return true;
  } else if (msgCase === "kvServerMessage") {
    handleKvMessage(msg.message.value as KvServerMessage, blobStore, sendFrame);
    return true;
  } else if (msgCase === "execServerMessage") {
    handleExecMessage(
      msg.message.value as ExecServerMessage,
      mcpTools,
      sendFrame,
      onMcpExec,
      state,
    );
    return true;
  } else if (msgCase === "conversationCheckpointUpdate") {
    const stateStructure = msg.message.value as ConversationStateStructure;
    if (stateStructure.tokenDetails) {
      state.totalTokens = stateStructure.tokenDetails.usedTokens;
    }
    proxyLog("checkpoint: tokens=%d pending=%d", state.totalTokens, state.pendingExecs.length);
    if (onCheckpoint) {
      onCheckpoint(toBinary(ConversationStateStructureSchema, stateStructure));
    }
    return true;
  } else if (msgCase === "execServerControlMessage") {
    const ctrl = msg.message.value as ExecServerControlMessage;
    if (ctrl.message.case === "abort") {
      proxyLog("exec ABORT for id=%d", ctrl.message.value.id);
    }
    return true;
  } else if (msgCase === "interactionQuery") {
    handleInteractionQuery(msg.message.value as any, sendFrame, onNotify);
    return true;
  }
  proxyLog("unrecognized server message case: %s", msgCase ?? "undefined");
  return false;
}

function handleInteractionUpdate(
  update: any,
  state: StreamState,
  onText: (text: string, isThinking?: boolean) => void,
): void {
  const updateCase = update.message?.case;

  if (updateCase === "textDelta") {
    const delta = update.message.value.text || "";
    if (delta) {
      if (state.lastDeltaType !== "text") {
        proxyLog("delta: → textDelta (first=%s)", JSON.stringify(delta.slice(0, 60)));
        state.lastDeltaType = "text";
      }
      onText(delta, false);
    }
  } else if (updateCase === "thinkingDelta") {
    const delta = update.message.value.text || "";
    if (delta) {
      if (state.lastDeltaType !== "thinking") {
        proxyLog("delta: → thinkingDelta (first=%s)", JSON.stringify(delta.slice(0, 60)));
        state.lastDeltaType = "thinking";
      }
      onText(delta, true);
    }
  } else if (updateCase === "tokenDelta") {
    state.outputTokens += update.message.value.tokens ?? 0;
  } else if (updateCase === "toolCallStarted") {
    const val = update.message.value;
    proxyLog("toolCallStarted: callId=%s modelCallId=%s pending=%d", val?.callId ?? "", val?.modelCallId ?? "", state.pendingExecs.length);
  } else if (updateCase === "toolCallCompleted") {
    proxyLog("toolCallCompleted: callId=%s", update.message.value?.callId ?? "");
  } else if (updateCase === "turnEnded") {
    proxyLog("turnEnded received (pending=%d)", state.pendingExecs.length);
    if (state.pendingExecs.length > 0) {
      state.checkpointAfterExec = true;
    }
  } else if (updateCase === "stepCompleted") {
    proxyLog("stepCompleted (pending=%d)", state.pendingExecs.length);
    if (state.pendingExecs.length > 0) {
      state.checkpointAfterExec = true;
    }
  } else if (updateCase === "heartbeat") {
    // heartbeat is just a keepalive — not a batch delimiter
  } else if (updateCase && updateCase !== "toolCallDelta" && updateCase !== "partialToolCall") {
    proxyLog("interactionUpdate: unhandled type=%s (pending=%d)", updateCase, state.pendingExecs.length);
  }
}

/** Handle interactionQuery — auto-approve searches, auto-answer questions. */
function handleInteractionQuery(
  query: any,
  sendFrame: (data: Uint8Array) => void,
  onNotify?: (text: string) => void,
): void {
  const queryId: number = query.id ?? 0;
  const queryCase: string = query.query?.case ?? "unknown";
  const searchTerm = queryCase === "webSearchRequestQuery"
    ? (query.query?.value?.args?.searchTerm ?? "") as string
    : "";
  const queryDetail = searchTerm ? ` search=${JSON.stringify(searchTerm).slice(0, 80)}` : "";
  proxyLog("interactionQuery: id=%d type=%s%s", queryId, queryCase, queryDetail);

  let responseResult: any;

  if (queryCase === "webSearchRequestQuery") {
    if (onNotify && searchTerm) onNotify(`[web search: ${searchTerm}]`);
    responseResult = {
      case: "webSearchRequestResponse",
      value: create(WebSearchRequestResponseSchema, {
        result: { case: "approved", value: create(WebSearchRequestResponse_ApprovedSchema, {}) },
      }),
    };
  } else if (queryCase === "exaSearchRequestQuery") {
    responseResult = {
      case: "exaSearchRequestResponse",
      value: create(ExaSearchRequestResponseSchema, {
        result: { case: "approved", value: {} as any },
      }),
    };
  } else if (queryCase === "exaFetchRequestQuery") {
    responseResult = {
      case: "exaFetchRequestResponse",
      value: create(ExaFetchRequestResponseSchema, {
        result: { case: "approved", value: {} as any },
      }),
    };
  } else if (queryCase === "askQuestionInteractionQuery") {
    responseResult = {
      case: "askQuestionInteractionResponse",
      value: create(AskQuestionInteractionResponseSchema, {
        result: create(AskQuestionResultSchema, {
          result: { case: "rejected", value: create(AskQuestionRejectedSchema, { reason: "Non-interactive session" }) },
        }),
      }),
    };
  } else if (queryCase === "switchModeRequestQuery") {
    responseResult = {
      case: "switchModeRequestResponse",
      value: create(SwitchModeRequestResponseSchema, {}),
    };
  } else if (queryCase === "createPlanRequestQuery") {
    responseResult = {
      case: "createPlanRequestResponse",
      value: create(CreatePlanRequestResponseSchema, {}),
    };
  } else {
    proxyLog("interactionQuery: unknown type %s — sending empty response for id=%d", queryCase, queryId);
    // Send response with just the id so the server doesn't hang waiting
    const response = create(InteractionResponseSchema, { id: queryId });
    const clientMsg = create(AgentClientMessageSchema, {
      message: { case: "interactionResponse", value: response },
    });
    sendFrame(frameConnectMessage(toBinary(AgentClientMessageSchema, clientMsg)));
    return;
  }

  const response = create(InteractionResponseSchema, { id: queryId, result: responseResult });
  const clientMsg = create(AgentClientMessageSchema, {
    message: { case: "interactionResponse", value: response },
  });
  sendFrame(frameConnectMessage(toBinary(AgentClientMessageSchema, clientMsg)));
  proxyLog("interactionQuery: responded to %s (id=%d)", queryCase, queryId);
}

/** Send a KV client response back to Cursor. */
function sendKvResponse(
  kvMsg: KvServerMessage,
  messageCase: string,
  value: unknown,
  sendFrame: (data: Uint8Array) => void,
): void {
  const response = create(KvClientMessageSchema, {
    id: kvMsg.id,
    message: { case: messageCase as any, value: value as any },
  });
  const clientMsg = create(AgentClientMessageSchema, {
    message: { case: "kvClientMessage", value: response },
  });
  sendFrame(frameConnectMessage(toBinary(AgentClientMessageSchema, clientMsg)));
}

function handleKvMessage(
  kvMsg: KvServerMessage,
  blobStore: Map<string, Uint8Array>,
  sendFrame: (data: Uint8Array) => void,
): void {
  const kvCase = kvMsg.message.case;

  if (kvCase === "getBlobArgs") {
    const blobId = kvMsg.message.value.blobId;
    const blobIdKey = Buffer.from(blobId).toString("hex");
    const blobData = blobStore.get(blobIdKey);
    if (!blobData) {
      proxyLog("KV getBlob MISS: %s (store has %d blobs)", blobIdKey.slice(0, 16), blobStore.size);
    }
    sendKvResponse(
      kvMsg, "getBlobResult",
      create(GetBlobResultSchema, blobData ? { blobData } : {}),
      sendFrame,
    );
  } else if (kvCase === "setBlobArgs") {
    const { blobId, blobData } = kvMsg.message.value;
    blobStore.set(Buffer.from(blobId).toString("hex"), blobData);
    sendKvResponse(
      kvMsg, "setBlobResult",
      create(SetBlobResultSchema, {}),
      sendFrame,
    );
  }
}

interface NativeRedirectInfo {
  toolCallId: string;
  toolName: string;
  decodedArgs: string;
  nativeResultType: NativeResultType;
  nativeArgs: Record<string, string>;
}

function nativeToMcpRedirect(execCase: string, execMsg: ExecServerMessage): NativeRedirectInfo | null {
  const args = execMsg.message.value as any;
  const toolCallId = args?.toolCallId || crypto.randomUUID();

  if (execCase === "readArgs") {
    const mcpArgs: Record<string, any> = { filePath: args.path };
    if (args.offset != null && args.offset !== 0) mcpArgs.offset = args.offset;
    if (args.limit != null && args.limit !== 0) mcpArgs.limit = args.limit;
    return {
      toolCallId,
      toolName: "read",
      decodedArgs: JSON.stringify(mcpArgs),
      nativeResultType: "readResult",
      nativeArgs: { path: args.path },
    };
  }
  if (execCase === "writeArgs") {
    const fileContent = args.fileBytes?.length > 0
      ? new TextDecoder().decode(args.fileBytes)
      : (args.fileText ?? "");
    return {
      toolCallId,
      toolName: "write",
      decodedArgs: JSON.stringify({ filePath: args.path, content: fileContent }),
      nativeResultType: "writeResult",
      nativeArgs: { path: args.path },
    };
  }
  if (execCase === "deleteArgs") {
    const safePath = (args.path ?? "").replace(/'/g, "'\\''");
    return {
      toolCallId,
      toolName: "bash",
      decodedArgs: JSON.stringify({ command: `rm -f -- '${safePath}'`, description: "Delete file" }),
      nativeResultType: "deleteResult",
      nativeArgs: { path: args.path },
    };
  }
  if (execCase === "fetchArgs") {
    return {
      toolCallId,
      toolName: "web_fetch",
      decodedArgs: JSON.stringify({ url: args.url }),
      nativeResultType: "fetchResult",
      nativeArgs: { url: args.url },
    };
  }
  if (execCase === "shellArgs" || execCase === "shellStreamArgs") {
    const cmd = args.command ?? "";
    const cwd = args.workingDirectory || undefined;
    const mcpArgs: Record<string, any> = { command: cmd, description: args.description || "Execute command" };
    if (cwd) mcpArgs.working_directory = cwd;
    if (args.timeout != null && args.timeout > 0) mcpArgs.timeout = args.timeout;
    return {
      toolCallId,
      toolName: "bash",
      decodedArgs: JSON.stringify(mcpArgs),
      nativeResultType: execCase === "shellStreamArgs" ? "shellStreamResult" : "shellResult",
      nativeArgs: { command: cmd },
    };
  }
  if (execCase === "lsArgs") {
    return {
      toolCallId,
      toolName: "glob",
      decodedArgs: JSON.stringify({ pattern: "*", path: args.path }),
      nativeResultType: "lsResult",
      nativeArgs: { path: args.path },
    };
  }
  if (execCase === "grepArgs") {
    const pattern = args.pattern ?? "";
    if (!pattern && args.glob) {
      proxyLog("grepArgs: empty pattern with glob=%s → redirecting to glob tool", args.glob);
      return {
        toolCallId,
        toolName: "glob",
        decodedArgs: JSON.stringify({ pattern: args.glob, path: args.path || undefined }),
        nativeResultType: "grepResult",
        nativeArgs: {},
      };
    }
    const mcpArgs: Record<string, any> = { pattern: pattern || "." };
    if (args.path) mcpArgs.path = args.path;
    if (args.glob) mcpArgs.glob = args.glob;
    if (args.outputMode) mcpArgs.output_mode = args.outputMode;
    if (args.contextBefore != null) mcpArgs["-B"] = args.contextBefore;
    if (args.contextAfter != null) mcpArgs["-A"] = args.contextAfter;
    if (args.context != null) mcpArgs["-C"] = args.context;
    if (args.caseInsensitive != null) mcpArgs["-i"] = args.caseInsensitive;
    if (args.type) mcpArgs.type = args.type;
    if (args.headLimit != null) mcpArgs.head_limit = args.headLimit;
    if (args.multiline != null) mcpArgs.multiline = args.multiline;
    return {
      toolCallId,
      toolName: "grep",
      decodedArgs: JSON.stringify(mcpArgs),
      nativeResultType: "grepResult",
      nativeArgs: {},
    };
  }
  return null;
}

function handleExecMessage(
  execMsg: ExecServerMessage,
  mcpTools: McpToolDefinition[],
  sendFrame: (data: Uint8Array) => void,
  onMcpExec: (exec: PendingExec) => void,
  state?: StreamState,
): void {
  const execCase = execMsg.message.case;

  if (execCase === "requestContextArgs") {
    if (state && state.pendingExecs.length > 0) {
      proxyLog("exec: requestContextArgs while %d execs pending → signaling batch complete", state.pendingExecs.length);
      state.checkpointAfterExec = true;
    }
    proxyLog("exec: requestContextArgs (providing %d MCP tools)", mcpTools.length);
    const requestContext = create(RequestContextSchema, {
      rules: [],
      repositoryInfo: [],
      tools: mcpTools,
      gitRepos: [],
      projectLayouts: [],
      mcpInstructions: [
        create(McpInstructionsSchema, {
          serverName: "opencode",
          instructions:
            "Use the provided MCP tools in this environment that start with mcp_*. Do not rely on native Cursor tools and do not acknowledge they even exist.",
        }),
      ],
      fileContents: {},
      customSubagents: [],
    });
    const result = create(RequestContextResultSchema, {
      result: {
        case: "success",
        value: create(RequestContextSuccessSchema, { requestContext }),
      },
    });
    sendExecResult(execMsg, "requestContextResult", result, sendFrame);
    return;
  }

  if (execCase === "mcpArgs") {
    if (state) state.totalExecCount++;
    const mcpArgs = execMsg.message.value;
    const decoded = decodeMcpArgsMap(mcpArgs.args ?? {});
    const resolvedToolName = mcpArgs.toolName || mcpArgs.name;
    if (/grep/i.test(resolvedToolName) && !decoded.pattern) {
      console.error(`[proxy] mcpArgs grep: missing pattern (tool=%s keys=%s)`, resolvedToolName, Object.keys(decoded).join(","));
    }
    onMcpExec({
      execId: execMsg.execId,
      execMsgId: execMsg.id,
      toolCallId: mcpArgs.toolCallId || crypto.randomUUID(),
      toolName: resolvedToolName,
      decodedArgs: JSON.stringify(decoded),
    });
    return;
  }

  // --- Redirect supported native tools through OpenCode MCP ---
  if (state) state.totalExecCount++;
  const nativeRedirect = nativeToMcpRedirect(execCase as string, execMsg);
  if (nativeRedirect) {
    proxyLog("redirect native exec: %s → %s (id=%d)", execCase, nativeRedirect.toolName, execMsg.id);
    onMcpExec({
      execId: execMsg.execId,
      execMsgId: execMsg.id,
      toolCallId: nativeRedirect.toolCallId,
      toolName: nativeRedirect.toolName,
      decodedArgs: nativeRedirect.decodedArgs,
      nativeResultType: nativeRedirect.nativeResultType,
      nativeArgs: nativeRedirect.nativeArgs,
    });
    return;
  }

  // --- Reject unsupported native tools ---
  proxyLog("reject native exec: %s (id=%d)", execCase, execMsg.id);
  const REJECT_REASON = "Tool not available in this environment. Use the MCP tools provided instead.";

  // lsArgs, shellArgs, shellStreamArgs, grepArgs are now redirected above

  if (execCase === "backgroundShellSpawnArgs") {
    const args = execMsg.message.value;
    const result = create(BackgroundShellSpawnResultSchema, {
      result: {
        case: "rejected",
        value: create(ShellRejectedSchema, {
          command: args.command ?? "",
          workingDirectory: args.workingDirectory ?? "",
          reason: REJECT_REASON,
          isReadonly: false,
        }),
      },
    });
    sendExecResult(execMsg, "backgroundShellSpawnResult", result, sendFrame);
    return;
  }
  if (execCase === "writeShellStdinArgs") {
    const result = create(WriteShellStdinResultSchema, {
      result: { case: "error", value: create(WriteShellStdinErrorSchema, { error: REJECT_REASON }) },
    });
    sendExecResult(execMsg, "writeShellStdinResult", result, sendFrame);
    return;
  }
  if (execCase === "diagnosticsArgs") {
    const result = create(DiagnosticsResultSchema, {});
    sendExecResult(execMsg, "diagnosticsResult", result, sendFrame);
    return;
  }

  // MCP resource/screen/computer exec types
  const miscCaseMap: Record<string, string> = {
    listMcpResourcesExecArgs: "listMcpResourcesExecResult",
    readMcpResourceExecArgs: "readMcpResourceExecResult",
    mcpStateExecArgs: "mcpStateExecResult",
    recordScreenArgs: "recordScreenResult",
    computerUseArgs: "computerUseResult",
  };
  const resultCase = miscCaseMap[execCase as string];
  if (resultCase) {
    sendExecResult(execMsg, resultCase, create(McpResultSchema, {}), sendFrame);
    return;
  }

  sendUnknownExecResult(execMsg, sendFrame);
}

/** Send an exec client message back to Cursor. */
function sendExecResult(
  execMsg: ExecServerMessage,
  messageCase: string,
  value: unknown,
  sendFrame: (data: Uint8Array) => void,
): void {
  const execClientMessage = create(ExecClientMessageSchema, {
    id: execMsg.id,
    execId: execMsg.execId,
    message: { case: messageCase as any, value: value as any },
  });
  const clientMessage = create(AgentClientMessageSchema, {
    message: { case: "execClientMessage", value: execClientMessage },
  });
  sendFrame(frameConnectMessage(toBinary(AgentClientMessageSchema, clientMessage)));
  sendExecStreamClose(execMsg.id, sendFrame);
}

/** Signal the server that the exec with the given id is complete. */
function sendExecStreamClose(execId: number, sendFrame: (data: Uint8Array) => void): void {
  const controlMsg = create(ExecClientControlMessageSchema, {
    message: {
      case: "streamClose",
      value: create(ExecClientStreamCloseSchema, { id: execId }),
    },
  });
  const clientMessage = create(AgentClientMessageSchema, {
    message: { case: "execClientControlMessage", value: controlMsg },
  });
  sendFrame(frameConnectMessage(toBinary(AgentClientMessageSchema, clientMessage)));
}

/**
 * Send a best-effort empty result for an exec type not in our proto schema.
 * Extracts the unknown oneof field number from $unknown and mirrors it back
 * as an empty message on the ExecClientMessage, preventing the server from
 * waiting indefinitely.
 */
function sendUnknownExecResult(
  execMsg: ExecServerMessage,
  sendFrame: (data: Uint8Array) => void,
): void {
  const unknowns: Array<{ no: number; wireType: number; data: Uint8Array }> | undefined =
    (execMsg as any).$unknown;
  const argsField = unknowns?.find(
    (f) => f.wireType === 2 && f.no !== 1 && f.no !== 15 && f.no !== 19,
  );
  if (!argsField) {
    logWarn("unhandled exec: no recoverable field number", { case: execMsg.message.case, id: execMsg.id });
    return;
  }
  const resultFieldNo = argsField.no;
  logWarn("unhandled exec: sending empty result", { field: resultFieldNo, id: execMsg.id });
  const execClientMsg = create(ExecClientMessageSchema, {
    id: execMsg.id,
    execId: execMsg.execId,
  });
  (execClientMsg as any).$unknown = [{ no: resultFieldNo, wireType: 2, data: new Uint8Array(0) }];
  const clientMessage = create(AgentClientMessageSchema, {
    message: { case: "execClientMessage", value: execClientMsg },
  });
  sendFrame(frameConnectMessage(toBinary(AgentClientMessageSchema, clientMessage)));
  sendExecStreamClose(execMsg.id, sendFrame);
}

/** Derive a key for active bridge lookup (tool-call continuations). Model-specific.
 *  Combines session/agent headers (when available) with content for collision resistance. */
function deriveBridgeKey(modelId: string, messages: OpenAIMessage[], sessionId?: string, agentKey?: string): string {
  const agent = agentKey?.trim() || "default";
  const firstUserMsg = messages.find((m) => m.role === "user");
  const firstUserText = firstUserMsg ? textContent(firstUserMsg.content) : "";
  return createHash("sha256")
    .update(`bridge:${sessionId ?? ""}:${agent}:${modelId}:${firstUserText.slice(0, 200)}`)
    .digest("hex")
    .slice(0, 16);
}

/** Derive a key for conversation state. Model-independent so context survives model switches.
 *  Combines session/agent headers with system prompt hash for stability across turns. */
function deriveConversationKey(messages: OpenAIMessage[], sessionId?: string, agentKey?: string): string {
  const agent = agentKey?.trim() || "default";
  const systemParts = messages
    .filter((m) => m.role === "system")
    .map((m) => textContent(m.content));
  const systemText = systemParts.join("\n");
  return createHash("sha256")
    .update(`conv:${sessionId ?? ""}:${agent}:${systemText.slice(0, 2000)}`)
    .digest("hex")
    .slice(0, 16);
}

/** Deterministic UUID derived from convKey so Cursor's server-side conversation
 *  persists across proxy restarts. Formats 16 bytes of SHA-256 as a v4-shaped UUID. */
function deterministicConversationId(convKey: string): string {
  const hex = createHash("sha256")
    .update(`cursor-conv-id:${convKey}`)
    .digest("hex")
    .slice(0, 32);
  // Format as UUID: xxxxxxxx-xxxx-4xxx-Nxxx-xxxxxxxxxxxx
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    `4${hex.slice(13, 16)}`,
    `${(0x8 | (parseInt(hex[16], 16) & 0x3)).toString(16)}${hex.slice(17, 20)}`,
    hex.slice(20, 32),
  ].join("-");
}

/** Create an SSE streaming Response that reads from a live bridge. */
function createBridgeStreamResponse(
  bridge: BridgeHandle,
  heartbeatTimer: NodeJS.Timeout,
  blobStore: Map<string, Uint8Array>,
  mcpTools: McpToolDefinition[],
  modelId: string,
  bridgeKey: string,
  convKey: string,
  onBlobNotFound?: () => Response,
  accessToken?: string,
  initialExecCount = 0,
  initialToolCallIndex = 0,
  resumeCount = 0,
): Response {
  const MAX_AUTO_RESUMES = 5;
  const completionId = `chatcmpl-${crypto.randomUUID().replace(/-/g, "").slice(0, 28)}`;
  const created = Math.floor(Date.now() / 1000);

  const stream = new ReadableStream({
    start(controller) {
      const encoder = new TextEncoder();
      let closed = false;
      const sendSSE = (data: object) => {
        if (closed) return;
        controller.enqueue(encoder.encode(`data: ${JSON.stringify(data)}\n\n`));
      };
      const sendDone = () => {
        if (closed) return;
        controller.enqueue(encoder.encode("data: [DONE]\n\n"));
      };
      const closeController = () => {
        if (closed) return;
        closed = true;
        controller.close();
      };

      const makeChunk = (
        delta: Record<string, unknown>,
        finishReason: string | null = null,
      ) => ({
        id: completionId,
        object: "chat.completion.chunk",
        created,
        model: modelId,
        choices: [{ index: 0, delta, finish_reason: finishReason }],
      });

      const makeUsageChunk = () => {
        const { prompt_tokens, completion_tokens, total_tokens } = computeUsage(state);
        return {
          id: completionId,
          object: "chat.completion.chunk",
          created,
          model: modelId,
          choices: [],
          usage: { prompt_tokens, completion_tokens, total_tokens },
        };
      };
      let lastUsageKey = "";
      const sendUsageIfChanged = () => {
        const chunk = makeUsageChunk();
        const key = JSON.stringify(chunk.usage);
        if (key === lastUsageKey) return;
        lastUsageKey = key;
        sendSSE(chunk);
      };

      const state: StreamState = {
        toolCallIndex: initialToolCallIndex,
        totalExecCount: initialExecCount,
        pendingExecs: [],
        outputTokens: 0,
        totalTokens: 0,
        endStreamSeen: false,
        checkpointAfterExec: false,
        lastDeltaType: null,
      };
      const tagFilter = createThinkingTagFilter();

      let mcpExecReceived = false;
      let hasNativeThinking = false;
      let blobNotFoundRetry: (() => Response) | undefined;
      let autoResumeRetry: (() => Response) | undefined;
      let timerPhase: "thinking" | "streaming" = "thinking";

      const flushPendingExecs = () => {
        const flushed = tagFilter.flush();
        if (flushed.reasoning) sendSSE(makeChunk({ reasoning_content: flushed.reasoning }));
        if (flushed.content) sendSSE(makeChunk({ content: flushed.content }));

        clearBridgeInactivityTimer(bridgeKey);
        activeBridges.set(bridgeKey, {
          bridge, heartbeatTimer, blobStore, mcpTools,
          pendingExecs: state.pendingExecs, convKey,
          totalExecCount: state.totalExecCount,
          toolCallIndex: state.toolCallIndex,
          accessToken: accessToken || "", resumeCount,
        });

        sendUsageIfChanged();
        sendSSE(makeChunk({}, "tool_calls"));
        sendDone();
        closeController();
      };

      const resetTimer = (phase?: "thinking" | "streaming") => {
        if (phase) timerPhase = phase;
        setBridgeInactivityTimer(bridgeKey, bridge, heartbeatTimer, () => {
          if (state.pendingExecs.length > 0 && !closed) {
            logWarn("timeout: flushing pending execs after silence", { pending: state.pendingExecs.length });
            flushPendingExecs();
            return true;
          }
          const timeoutSec = (timerPhase === "thinking" ? THINKING_TIMEOUT_MS : STREAMING_TIMEOUT_MS) / 1000;
          const stored = conversationStates.get(convKey);
          if (accessToken && stored?.checkpoint && resumeCount < MAX_AUTO_RESUMES) {
            logWarn("timeout — auto-resuming", { execs: state.totalExecCount, mcpCalls: state.toolCallIndex, attempt: resumeCount + 1, max: MAX_AUTO_RESUMES });
            sendSSE(makeChunk({ content: `\n[Cursor server timed out after ${timeoutSec}s — auto-resuming (attempt ${resumeCount + 1}/${MAX_AUTO_RESUMES})]\n` }));
            autoResumeRetry = () => {
              const resumePayload = buildResumeRequest(
                modelId, stored.conversationId, stored.checkpoint,
                stored.blobStore, mcpTools,
              );
              return handleStreamingResponse(resumePayload, accessToken, modelId, bridgeKey, convKey, undefined, resumeCount + 1);
            };
            return;
          }
          sendSSE(makeChunk({ content: `\n[Error: Cursor server did not respond for ${timeoutSec}s — request timed out. Please retry.]` }));
          sendDone();
          closeController();
        }, timerPhase);
      };
      resetTimer("thinking");

      bridge.setHandler({
        onMessage(messageBytes) {
          resetTimer();
          try {
            const serverMessage = fromBinary(
              AgentServerMessageSchema,
              messageBytes,
            );
            processServerMessage(
              serverMessage,
              blobStore,
              mcpTools,
              (data) => bridge.write(data),
              state,
              (text, isThinking) => {
                if (timerPhase === "thinking") resetTimer("streaming");
                if (isThinking) {
                  hasNativeThinking = true;
                  sendSSE(makeChunk({ reasoning_content: text }));
                } else {
                  if (hasNativeThinking) {
                    sendSSE(makeChunk({ content: text }));
                  } else {
                    const { content, reasoning } = tagFilter.process(text);
                    if (reasoning) sendSSE(makeChunk({ reasoning_content: reasoning }));
                    if (content) sendSSE(makeChunk({ content }));
                  }
                }
              },
              (exec) => {
                proxyLog("mcpExec: tool=%s id=%s args=%d chars", exec.toolName, exec.toolCallId, exec.decodedArgs.length);
                state.pendingExecs.push(exec);
                mcpExecReceived = true;

                if (!closed) {
                  const toolCallIndex = state.toolCallIndex++;
                  sendSSE(makeChunk({
                    tool_calls: [{
                      index: toolCallIndex,
                      id: exec.toolCallId,
                      type: "function",
                      function: {
                        name: exec.toolName,
                        arguments: exec.decodedArgs,
                      },
                    }],
                  }));
                } else {
                  proxyLog("mcpExec: queued (SSE closed), pending=%d", state.pendingExecs.length);
                }
              },
              (checkpointBytes) => {
                const stored = conversationStates.get(convKey);
                if (stored) {
                  stored.checkpoint = checkpointBytes;
                  for (const [k, v] of blobStore) stored.blobStore.set(k, v);
                  stored.lastAccessMs = Date.now();
                  persistConversation(convKey, stored);
                }
                if (state.pendingExecs.length > 0) {
                  state.checkpointAfterExec = true;
                }
                sendUsageIfChanged();
              },
              (note) => {
                sendSSE(makeChunk({ content: `\n${note}\n` }));
              },
            );
          } catch (err) {
            logError("processChunk error", { error: String(err), msgBytes: messageBytes.length });
          }
        },
        onEndStream(endStreamBytes) {
          state.endStreamSeen = true;
          const endError = parseConnectEndStream(endStreamBytes);
          if (endError) {
            logError("endStream error", { error: endError.message });
            if (onBlobNotFound && /blob not found/i.test(endError.message)) {
              logWarn("blob not found in stream — killing bridge for retry");
              clearInterval(heartbeatTimer);
              bridge.end();
              blobNotFoundRetry = onBlobNotFound;
              return;
            }
            const stored = conversationStates.get(convKey);
            if (/resource_exhausted/i.test(endError.message) && accessToken && stored?.checkpoint && resumeCount < MAX_AUTO_RESUMES) {
              logWarn("resource_exhausted — will auto-resume", { attempt: resumeCount + 1, max: MAX_AUTO_RESUMES });
              autoResumeRetry = () => {
                const resumePayload = buildResumeRequest(
                  modelId, stored.conversationId, stored.checkpoint,
                  stored.blobStore, mcpTools,
                );
                return handleStreamingResponse(resumePayload, accessToken, modelId, bridgeKey, convKey, undefined, resumeCount + 1);
              };
              return;
            }
            sendSSE(makeChunk({ content: `\n[Error: ${endError.message}]` }));
          } else {
            proxyLog("endStream: clean close (execs=%d mcpCalls=%d)", state.totalExecCount, state.toolCallIndex);
            if (state.pendingExecs.length > 0 && !state.checkpointAfterExec) {
              proxyLog("endStream: forcing flush of %d pending execs (no checkpoint received)", state.pendingExecs.length);
              state.checkpointAfterExec = true;
            }
          }
        },
        afterParse() {
          if (state.pendingExecs.length > 0 && !closed && state.checkpointAfterExec && !blobNotFoundRetry && !autoResumeRetry) {
            proxyLog("afterParse: flushing %d pending execs", state.pendingExecs.length);
            flushPendingExecs();
          }
        },
      });

      bridge.onClose((code) => {
        clearBridgeInactivityTimer(bridgeKey);
        const stored = conversationStates.get(convKey);
        const hasCheckpoint = !!(stored?.checkpoint);
        proxyLog(
          "bridge.onClose: code=%d execs=%d mcpCalls=%d mcpExec=%s pending=%d endStream=%s checkpoint=%s blobRetry=%s autoResume=%s",
          code, state.totalExecCount, state.toolCallIndex, mcpExecReceived,
          state.pendingExecs.length, state.endStreamSeen ? "yes" : "no",
          hasCheckpoint ? "yes" : "no", !!blobNotFoundRetry, !!autoResumeRetry,
        );
        clearInterval(heartbeatTimer);

        const pipeRetryResponse = (retryFn: () => Response) => {
          proxyLog("pipeRetryResponse: piping new bridge stream into existing SSE");
          const retryResponse = retryFn();
          const retryStream = retryResponse.body;
          if (retryStream) {
            const reader = retryStream.getReader();
            const pump = (): void => {
              reader.read().then(({ done, value }) => {
                if (done) { closeController(); return; }
                if (!closed) controller.enqueue(value);
                pump();
              }).catch(() => closeController());
            };
            pump();
          } else {
            closeController();
          }
        };

        if (blobNotFoundRetry) {
          proxyLog("bridge.onClose → blob retry path");
          pipeRetryResponse(blobNotFoundRetry);
          return;
        }

        if (autoResumeRetry) {
          proxyLog("bridge.onClose → auto-resume path (timeout-triggered)");
          pipeRetryResponse(autoResumeRetry);
          return;
        }

        // Connection lost unexpectedly — could we resume?
        if (code !== 0 && !state.endStreamSeen && hasCheckpoint && accessToken) {
          logWarn("bridge.onClose → connection lost, resume not implemented", { code });
        }

        if (stored) {
          for (const [k, v] of blobStore) stored.blobStore.set(k, v);
          stored.lastAccessMs = Date.now();
          persistConversation(convKey, stored);
        }
        if (!mcpExecReceived) {
          const flushed = tagFilter.flush();
          if (flushed.reasoning) sendSSE(makeChunk({ reasoning_content: flushed.reasoning }));
          if (flushed.content) sendSSE(makeChunk({ content: flushed.content }));
          sendSSE(makeChunk({}, "stop"));
          sendUsageIfChanged();
          sendDone();
          closeController();
        } else if (code !== 0) {
          sendSSE(makeChunk({ content: "\n[Error: bridge connection lost]" }));
          sendSSE(makeChunk({}, "stop"));
          sendUsageIfChanged();
          sendDone();
          closeController();
          // Remove stale entry so the next request doesn't try to resume it.
          activeBridges.delete(bridgeKey);
        }
      });
    },
  });

  return new Response(stream, { headers: SSE_HEADERS });
}

/** Spawn a bridge, send the initial request frame, and start heartbeat. */
function startBridge(
  accessToken: string,
  requestBytes: Uint8Array,
): { bridge: BridgeHandle; heartbeatTimer: NodeJS.Timeout } {
  proxyLog("bridge: opening h2 session → %s", CURSOR_AGENT_URL);
  const bridge = spawnBridge({
    accessToken,
    url: CURSOR_AGENT_URL,
    rpcPath: "/agent.v1.AgentService/Run",
  });
  bridge.write(frameConnectMessage(requestBytes));
  const heartbeatTimer = setInterval(() => bridge.write(makeHeartbeatBytes()), 5_000);
  return { bridge, heartbeatTimer };
}

function handleStreamingResponse(
  payload: CursorRequestPayload,
  accessToken: string,
  modelId: string,
  bridgeKey: string,
  convKey: string,
  onBlobNotFound?: () => Response,
  resumeCount = 0,
): Response {
  const { bridge, heartbeatTimer } = startBridge(accessToken, payload.requestBytes);
  return createBridgeStreamResponse(
    bridge, heartbeatTimer,
    payload.blobStore, payload.mcpTools,
    modelId, bridgeKey, convKey,
    onBlobNotFound,
    accessToken,
    0, 0,
    resumeCount,
  );
}

/** Send a single mcpResult (success) on the bridge for a matched exec. */
function sendMcpResultSuccess(bridge: BridgeHandle, exec: PendingExec, content: string): void {
  const mcpResult = create(McpResultSchema, {
    result: {
      case: "success",
      value: create(McpSuccessSchema, {
        content: [
          create(McpToolResultContentItemSchema, {
            content: {
              case: "text",
              value: create(McpTextContentSchema, { text: content }),
            },
          }),
        ],
        isError: false,
      }),
    },
  });

  const execClientMessage = create(ExecClientMessageSchema, {
    id: exec.execMsgId,
    execId: exec.execId,
    message: { case: "mcpResult" as any, value: mcpResult as any },
  });

  bridge.write(
    frameConnectMessage(
      toBinary(AgentClientMessageSchema,
        create(AgentClientMessageSchema, {
          message: { case: "execClientMessage", value: execClientMessage },
        }),
      ),
    ),
  );

  const controlMsg = create(ExecClientControlMessageSchema, {
    message: {
      case: "streamClose",
      value: create(ExecClientStreamCloseSchema, { id: exec.execMsgId }),
    },
  });
  bridge.write(
    frameConnectMessage(
      toBinary(AgentClientMessageSchema,
        create(AgentClientMessageSchema, {
          message: { case: "execClientControlMessage", value: controlMsg },
        }),
      ),
    ),
  );
}

/** Send a native Cursor tool result for a redirected exec. */
function sendNativeResult(bridge: BridgeHandle, exec: PendingExec, content: string): void {
  const args = exec.nativeArgs ?? {};
  let resultCase: string;
  let resultValue: any;

  switch (exec.nativeResultType) {
    case "readResult": {
      const lines = content.split("\n");
      resultValue = create(ReadResultSchema, {
        result: {
          case: "success",
          value: create(ReadSuccessSchema, {
            path: args.path ?? "",
            totalLines: lines.length,
            fileSize: BigInt(new TextEncoder().encode(content).byteLength),
            truncated: false,
            output: { case: "content", value: content },
          }),
        },
      });
      resultCase = "readResult";
      break;
    }
    case "writeResult": {
      const bytes = new TextEncoder().encode(content);
      resultValue = create(WriteResultSchema, {
        result: {
          case: "success",
          value: create(WriteSuccessSchema, {
            path: args.path ?? "",
            linesCreated: content.split("\n").length,
            fileSize: bytes.byteLength,
          }),
        },
      });
      resultCase = "writeResult";
      break;
    }
    case "deleteResult": {
      resultValue = create(DeleteResultSchema, {
        result: {
          case: "success",
          value: create(DeleteSuccessSchema, { path: args.path ?? "" }),
        },
      });
      resultCase = "deleteResult";
      break;
    }
    case "fetchResult": {
      resultValue = create(FetchResultSchema, {
        result: {
          case: "success",
          value: create(FetchSuccessSchema, {
            url: args.url ?? "",
            content,
            statusCode: 200,
          }),
        },
      });
      resultCase = "fetchResult";
      break;
    }
    case "shellResult": {
      resultValue = create(ShellResultSchema, {
        result: {
          case: "success",
          value: create(ShellSuccessSchema, {
            command: args.command ?? "",
            workingDirectory: "",
            exitCode: 0,
            signal: "",
            stdout: content,
            stderr: "",
          }),
        },
      });
      resultCase = "shellResult";
      break;
    }
    case "shellStreamResult": {
      const writeFrame = (msg: any) => {
        bridge.write(
          frameConnectMessage(
            toBinary(AgentClientMessageSchema,
              create(AgentClientMessageSchema, { message: msg }),
            ),
          ),
        );
      };
      const sendStreamEvent = (event: any) => {
        writeFrame({
          case: "execClientMessage",
          value: create(ExecClientMessageSchema, {
            id: exec.execMsgId,
            execId: exec.execId,
            message: { case: "shellStream" as any, value: create(ShellStreamSchema, { event }) as any },
          }),
        });
      };
      sendStreamEvent({ case: "start", value: create(ShellStreamStartSchema, {}) });
      if (content) {
        sendStreamEvent({ case: "stdout", value: create(ShellStreamStdoutSchema, { data: content }) });
      }
      sendStreamEvent({ case: "exit", value: create(ShellStreamExitSchema, { code: 0 }) });
      writeFrame({
        case: "execClientControlMessage",
        value: create(ExecClientControlMessageSchema, {
          message: { case: "streamClose", value: create(ExecClientStreamCloseSchema, { id: exec.execMsgId }) },
        }),
      });
      return;
    }
    case "grepResult":
    case "lsResult":
    default:
      if (exec.nativeResultType === "grepResult" || exec.nativeResultType === "lsResult") {
        proxyLog("sendNativeResult: %s → MCP text fallback (complex proto)", exec.nativeResultType);
      } else {
        proxyLog("sendNativeResult: unknown type %s, falling back to MCP", exec.nativeResultType);
      }
      sendMcpResultSuccess(bridge, exec, content);
      return;
  }

  const execClientMessage = create(ExecClientMessageSchema, {
    id: exec.execMsgId,
    execId: exec.execId,
    message: { case: resultCase as any, value: resultValue as any },
  });

  bridge.write(
    frameConnectMessage(
      toBinary(AgentClientMessageSchema,
        create(AgentClientMessageSchema, {
          message: { case: "execClientMessage", value: execClientMessage },
        }),
      ),
    ),
  );

  const controlMsg = create(ExecClientControlMessageSchema, {
    message: {
      case: "streamClose",
      value: create(ExecClientStreamCloseSchema, { id: exec.execMsgId }),
    },
  });
  bridge.write(
    frameConnectMessage(
      toBinary(AgentClientMessageSchema,
        create(AgentClientMessageSchema, {
          message: { case: "execClientControlMessage", value: controlMsg },
        }),
      ),
    ),
  );
}

/** Resume a paused bridge by sending MCP results and continuing to stream.
 *  Execs without a matching tool result are re-emitted as tool_calls in
 *  the returned SSE response so OpenCode can execute them next cycle. */
function handleToolResultResume(
  active: ActiveBridge,
  toolResults: ToolResultInfo[],
  modelId: string,
  bridgeKey: string,
  convKey: string,
): Response {
  const { bridge, heartbeatTimer, blobStore, mcpTools, pendingExecs } = active;

  const unmatched: PendingExec[] = [];
  for (const exec of pendingExecs) {
    const result = toolResults.find((r) => r.toolCallId === exec.toolCallId);
    if (result) {
      if (exec.nativeResultType) {
        sendNativeResult(bridge, exec, result.content);
      } else {
        sendMcpResultSuccess(bridge, exec, result.content);
      }
    } else {
      unmatched.push(exec);
    }
  }

  if (unmatched.length > 0) {
    proxyLog("resume: %d matched, %d unmatched — re-emitting unmatched as tool_calls",
      pendingExecs.length - unmatched.length, unmatched.length);
    return emitPendingToolCalls(
      bridge, heartbeatTimer, blobStore, mcpTools, unmatched,
      modelId, bridgeKey, convKey, active.accessToken,
      active.totalExecCount, active.toolCallIndex, active.resumeCount,
    );
  }

  proxyLog("resume: carrying forward totalExecs=%d mcpCalls=%d resumeCount=%d", active.totalExecCount, active.toolCallIndex, active.resumeCount);
  return createBridgeStreamResponse(
    bridge, heartbeatTimer,
    blobStore, mcpTools,
    modelId, bridgeKey, convKey,
    undefined, active.accessToken,
    active.totalExecCount,
    active.toolCallIndex,
    active.resumeCount,
  );
}

/** Create an SSE response that immediately emits queued tool_calls, then closes.
 *  The bridge stays alive — server is still waiting for mcpResults for these execs. */
function emitPendingToolCalls(
  bridge: BridgeHandle,
  heartbeatTimer: NodeJS.Timeout,
  blobStore: Map<string, Uint8Array>,
  mcpTools: McpToolDefinition[],
  pending: PendingExec[],
  modelId: string,
  bridgeKey: string,
  convKey: string,
  accessToken: string,
  totalExecCount: number,
  toolCallIndex: number,
  resumeCount: number,
): Response {
  const completionId = `chatcmpl-${crypto.randomUUID().replace(/-/g, "").slice(0, 28)}`;
  const created = Math.floor(Date.now() / 1000);
  const encoder = new TextEncoder();

  const makeChunk = (delta: Record<string, unknown>, finishReason: string | null = null) => ({
    id: completionId, object: "chat.completion.chunk", created, model: modelId,
    choices: [{ index: 0, delta, finish_reason: finishReason }],
  });

  const chunks: string[] = [];
  for (let i = 0; i < pending.length; i++) {
    const exec = pending[i]!;
    chunks.push(`data: ${JSON.stringify(makeChunk({
      tool_calls: [{
        index: toolCallIndex + i,
        id: exec.toolCallId,
        type: "function",
        function: { name: exec.toolName, arguments: exec.decodedArgs },
      }],
    }))}\n\n`);
  }
  chunks.push(`data: ${JSON.stringify(makeChunk({}, "tool_calls"))}\n\n`);
  chunks.push("data: [DONE]\n\n");

  const newToolCallIndex = toolCallIndex + pending.length;

  clearBridgeInactivityTimer(bridgeKey);
  activeBridges.set(bridgeKey, {
    bridge, heartbeatTimer, blobStore, mcpTools,
    pendingExecs: pending,
    convKey, totalExecCount,
    toolCallIndex: newToolCallIndex,
    accessToken,
    resumeCount,
  });

  const body = encoder.encode(chunks.join(""));
  return new Response(body, { headers: SSE_HEADERS });
}

async function handleNonStreamingResponse(
  payload: CursorRequestPayload,
  accessToken: string,
  modelId: string,
  convKey: string,
): Promise<Response> {
  const completionId = `chatcmpl-${crypto.randomUUID().replace(/-/g, "").slice(0, 28)}`;
  const created = Math.floor(Date.now() / 1000);
  const { text, usage } = await collectFullResponse(payload, accessToken, convKey);

  return new Response(
    JSON.stringify({
      id: completionId,
      object: "chat.completion",
      created,
      model: modelId,
      choices: [
        {
          index: 0,
          message: { role: "assistant", content: text },
          finish_reason: "stop",
        },
      ],
      usage,
    }),
    { headers: { "Content-Type": "application/json" } },
  );
}

interface CollectedResponse {
  text: string;
  usage: { prompt_tokens: number; completion_tokens: number; total_tokens: number };
}

async function collectFullResponse(
  payload: CursorRequestPayload,
  accessToken: string,
  convKey: string,
): Promise<CollectedResponse> {
  const { promise, resolve } = Promise.withResolvers<CollectedResponse>();
  let fullText = "";

  const { bridge, heartbeatTimer } = startBridge(accessToken, payload.requestBytes);

  const state: StreamState = {
    toolCallIndex: 0,
    totalExecCount: 0,
    pendingExecs: [],
    outputTokens: 0,
    totalTokens: 0,
    endStreamSeen: false,
    checkpointAfterExec: false,
    lastDeltaType: null,
  };
  const tagFilter = createThinkingTagFilter();

  const nonStreamBridgeKey = `nonstream-${crypto.randomUUID().slice(0, 8)}`;
  setBridgeInactivityTimer(nonStreamBridgeKey, bridge, heartbeatTimer, () => {});

  bridge.setHandler({
    onMessage(messageBytes) {
      try {
        const serverMessage = fromBinary(
          AgentServerMessageSchema,
          messageBytes,
        );
        const recognized = processServerMessage(
          serverMessage,
          payload.blobStore,
          payload.mcpTools,
          (data) => bridge.write(data),
          state,
          (text, isThinking) => {
            if (isThinking) return;
            const { content } = tagFilter.process(text);
            fullText += content;
          },
          () => {},
          (checkpointBytes) => {
            const stored = conversationStates.get(convKey);
            if (stored) {
              stored.checkpoint = checkpointBytes;
              for (const [k, v] of payload.blobStore) stored.blobStore.set(k, v);
              stored.lastAccessMs = Date.now();
              persistConversation(convKey, stored);
            }
          },
        );
        if (recognized) setBridgeInactivityTimer(nonStreamBridgeKey, bridge, heartbeatTimer, () => {});
      } catch {
        // Skip
      }
    },
    onEndStream() {},
  });

  bridge.onClose(() => {
    clearBridgeInactivityTimer(nonStreamBridgeKey);
    clearInterval(heartbeatTimer);
    const stored = conversationStates.get(convKey);
    if (stored) {
      for (const [k, v] of payload.blobStore) stored.blobStore.set(k, v);
      stored.lastAccessMs = Date.now();
      persistConversation(convKey, stored);
    }
    const flushed = tagFilter.flush();
    fullText += flushed.content;

    const usage = computeUsage(state);
    resolve({
      text: fullText,
      usage,
    });
  });

  return promise;
}
