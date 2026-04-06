import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  AgentClientMessageSchema,
  AgentServerMessageSchema,
  ClientHeartbeatSchema,
  type McpToolDefinition,
} from "./proto/agent_pb";
import { frameConnectMessage, createConnectFrameParser, parseConnectEndStream } from "./protocol";
import {
  type BridgeWriter,
  type PendingExec,
  sendMcpResultSuccess,
  sendNativeResult,
} from "./native-tools";
import { type StreamState, processServerMessage } from "./cursor-messages";
import { logError, logWarn } from "./logger";
import { connect as h2Connect, type ClientHttp2Session, type ClientHttp2Stream } from "node:http2";
import { randomBytes, randomUUID } from "node:crypto";

const CURSOR_API_URL = process.env.CURSOR_API_URL ?? "https://api2.cursor.sh";
const CURSOR_AGENT_URL = process.env.CURSOR_AGENT_URL ?? "https://agentn.us.api5.cursor.sh";
const CURSOR_CLIENT_VERSION = "cli-2026.03.30-a5d3e17";
const THINKING_TIMEOUT_MS = 30_000;
const STREAMING_TIMEOUT_MS = 15_000;
const CLOSE_OK = 0;
const CLOSE_ERR = 1;
const MAX_QUEUE_DEPTH = 10_000;

export type RetryHint = "blob_not_found" | "resource_exhausted" | "timeout";

export type SessionEvent =
  | { type: "text"; text: string; isThinking: boolean }
  | { type: "toolCall"; exec: PendingExec }
  | { type: "batchReady" }
  | { type: "usage"; outputTokens: number; totalTokens: number }
  | { type: "done"; error?: string; retryHint?: RetryHint };

function resolveCursorH2Target(baseUrl: string): { connectUrl: string; authority?: string } {
  const isApi2 = baseUrl.includes("api2.cursor.sh");
  return {
    connectUrl: isApi2 ? baseUrl.replace("api2.cursor.sh", "api2direct.cursor.sh") : baseUrl,
    authority: isApi2 ? "api2.cursor.sh" : undefined,
  };
}

export function classifyConnectError(errorMessage: string): RetryHint | undefined {
  if (/blob not found/i.test(errorMessage)) return "blob_not_found";
  if (/resource_exhausted/i.test(errorMessage)) return "resource_exhausted";
  return undefined;
}

class EventQueue<T> {
  private buffer: T[] = [];
  private waiters: Array<(value: T) => void> = [];

  get length(): number {
    return this.buffer.length;
  }

  push(event: T): void {
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter(event);
    } else {
      if (this.buffer.length >= MAX_QUEUE_DEPTH) {
        logWarn("EventQueue overflow, dropping event", { depth: this.buffer.length });
        return;
      }
      this.buffer.push(event);
    }
  }

  /** Push unconditionally (bypasses high-water mark). Used for terminal events. */
  pushForce(event: T): void {
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter(event);
    } else {
      this.buffer.push(event);
    }
  }

  next(): Promise<T> {
    const head = this.buffer.shift();
    if (head !== undefined) return Promise.resolve(head);
    return new Promise((resolve) => {
      this.waiters.push(resolve);
    });
  }
}

export interface SessionOptions {
  accessToken: string;
  requestBytes: Uint8Array;
  blobStore: Map<string, Uint8Array>;
  mcpTools: McpToolDefinition[];
  cloudRule?: string;
  convKey: string;
  onCheckpoint?: (bytes: Uint8Array, blobStore: Map<string, Uint8Array>) => void;
}

function makeHeartbeatFrame(): Buffer {
  const heartbeat = create(AgentClientMessageSchema, {
    message: {
      case: "clientHeartbeat",
      value: create(ClientHeartbeatSchema, {}),
    },
  });
  return frameConnectMessage(toBinary(AgentClientMessageSchema, heartbeat));
}

export class CursorSession implements BridgeWriter {
  private readonly queue = new EventQueue<SessionEvent>();
  private readonly streamState: StreamState;
  private batchState: "streaming" | "collecting" | "flushed" = "streaming";
  private pendingExecs: PendingExec[] = [];
  private _alive = true;
  private h2Session: ClientHttp2Session;
  private h2Stream: ClientHttp2Stream;
  private heartbeatTimer: ReturnType<typeof setInterval>;
  private inactivityTimer: ReturnType<typeof setTimeout> | null = null;
  private timerPhase: "thinking" | "streaming" = "thinking";
  private doneEventSent = false;
  private _flushedExecs: PendingExec[] = [];

  readonly blobStore: Map<string, Uint8Array>;
  readonly accessToken: string;
  readonly options: SessionOptions;

  constructor(options: SessionOptions) {
    this.options = options;
    this.blobStore = options.blobStore;
    this.accessToken = options.accessToken;

    this.streamState = {
      toolCallIndex: 0,
      totalExecCount: 0,
      pendingExecs: this.pendingExecs,
      outputTokens: 0,
      totalTokens: 0,
      endStreamSeen: false,
      checkpointAfterExec: false,
      lastDeltaType: null,
    };

    const { connectUrl, authority } = resolveCursorH2Target(CURSOR_AGENT_URL);
    const requestId = randomUUID();
    const traceId = randomBytes(16).toString("hex");
    const spanId = randomBytes(8).toString("hex");
    const traceparent = `00-${traceId}-${spanId}-01`;

    const frameParser = createConnectFrameParser(
      (bytes) => this.handleMessage(bytes),
      (bytes) => this.handleEndStream(bytes),
    );

    this.h2Session = h2Connect(connectUrl);
    this.h2Session.on("error", (err) => {
      logError("CursorSession: h2 session error", { error: err?.message ?? err });
      this.closeTransport();
      this.finish(CLOSE_ERR);
    });

    const headers: Record<string, string> = {
      ":method": "POST",
      ":path": "/agent.v1.AgentService/Run",
      "content-type": "application/connect+proto",
      "user-agent": "connect-es/1.6.1",
      authorization: `Bearer ${this.accessToken}`,
      "x-ghost-mode": "true",
      "x-cursor-client-version": CURSOR_CLIENT_VERSION,
      "x-cursor-client-type": "cli",
      "x-request-id": requestId,
      "x-original-request-id": requestId,
      traceparent,
      "backend-traceparent": traceparent,
      "connect-protocol-version": "1",
    };
    if (authority) headers[":authority"] = authority;

    this.h2Stream = this.h2Session.request(headers);
    this.write(frameConnectMessage(options.requestBytes));

    this.heartbeatTimer = setInterval(() => {
      this.write(makeHeartbeatFrame());
    }, 5_000);

    this.h2Stream.on("data", (chunk: Buffer | Uint8Array) => {
      frameParser(Buffer.from(chunk));
      this.afterParse();
    });
    this.h2Stream.on("end", () => {
      this.closeTransport();
      this.finish(CLOSE_OK);
    });
    this.h2Stream.on("error", (err) => {
      logError("CursorSession: h2 stream error", { error: err?.message ?? err });
      this.closeTransport();
      this.finish(CLOSE_ERR);
    });

    this.resetInactivityTimer();
  }

  get alive(): boolean {
    return this._alive;
  }

  get flushedExecs(): PendingExec[] {
    return [...this._flushedExecs];
  }

  get mcpTools(): McpToolDefinition[] {
    return this.options.mcpTools;
  }

  next(): Promise<SessionEvent> {
    return this.queue.next();
  }

  write(data: Uint8Array): void {
    if (!this._alive) return;
    try {
      this.h2Stream.write(data);
    } catch {
      /* ignore write-after-close */
    }
  }

  sendToolResults(results: Array<{ toolCallId: string; content: string }>): void {
    const remaining: PendingExec[] = [];
    for (const exec of this.pendingExecs) {
      const match = results.find((r) => r.toolCallId === exec.toolCallId);
      if (match) {
        if (exec.nativeResultType) {
          sendNativeResult(this, exec, match.content);
        } else {
          sendMcpResultSuccess(this, exec, match.content);
        }
      } else {
        remaining.push(exec);
      }
    }
    this.pendingExecs.length = 0;
    this.pendingExecs.push(...remaining);

    if (remaining.length > 0) {
      for (const exec of remaining) {
        this.queue.push({ type: "toolCall", exec });
      }
      this.batchState = "flushed";
      this._flushedExecs = [...remaining];
      this.queue.push({ type: "batchReady" });
    } else {
      this.batchState = "streaming";
    }

    this.timerPhase = "thinking";
    this.resetInactivityTimer();
    this.afterParse();
  }

  close(): void {
    this.closeTransport();
    this.finish(CLOSE_OK);
  }

  private pushDone(event: Extract<SessionEvent, { type: "done" }>): void {
    if (this.doneEventSent) return;
    this.doneEventSent = true;
    this.queue.pushForce(event);
  }

  private closeTransport(): void {
    try { this.h2Stream?.close(); } catch { /* ignore */ }
    try { this.h2Session?.close(); } catch { /* ignore */ }
  }

  private clearInactivityTimer(): void {
    if (this.inactivityTimer) {
      clearTimeout(this.inactivityTimer);
      this.inactivityTimer = null;
    }
  }

  private finish(code: number): void {
    if (this._alive) {
      this._alive = false;
      clearInterval(this.heartbeatTimer);
      this.clearInactivityTimer();
      this.closeTransport();
    }
    if (!this.doneEventSent) {
      if (this.pendingExecs.length > 0) {
        this.pushDone({ type: "done", error: "session closed with pending tool calls" });
      } else if (code !== CLOSE_OK) {
        this.pushDone({ type: "done", error: "bridge connection lost" });
      } else {
        this.pushDone({ type: "done" });
      }
    }
  }

  // Timer is paused in FLUSHED state (waiting for client tool results, not server)
  private resetInactivityTimer(): void {
    this.clearInactivityTimer();
    if (this.batchState === "flushed") return;
    const ms = this.timerPhase === "thinking" ? THINKING_TIMEOUT_MS : STREAMING_TIMEOUT_MS;
    this.inactivityTimer = setTimeout(() => {
      this.inactivityTimer = null;
      if (this.batchState === "collecting" && this.pendingExecs.length > 0) {
        this.batchState = "flushed";
        this._flushedExecs = [...this.pendingExecs];
        this.clearInactivityTimer();
        this.queue.push({ type: "batchReady" });
        return;
      }
      this.pushDone({ type: "done", error: "Cursor server timed out", retryHint: "timeout" });
      this.close();
    }, ms);
  }

  private handleMessage(messageBytes: Uint8Array): void {
    try {
      const msg = fromBinary(AgentServerMessageSchema, messageBytes);
      const recognized = processServerMessage(
        msg,
        this.blobStore,
        this.options.mcpTools,
        this.options.cloudRule,
        (data) => this.write(data),
        this.streamState,
        (text, isThinking) => {
          if (this.timerPhase === "thinking") this.timerPhase = "streaming";
          this.queue.push({ type: "text", text, isThinking: !!isThinking });
        },
        (exec) => {
          this.pendingExecs.push(exec);
          this.streamState.toolCallIndex++;
          if (this.batchState === "streaming" || this.batchState === "flushed") {
            this.batchState = "collecting";
          }
          this.queue.push({ type: "toolCall", exec });
          this.resetInactivityTimer();
        },
        (bytes) => {
          this.options.onCheckpoint?.(bytes, this.blobStore);
          if (this.pendingExecs.length > 0 && this.batchState === "collecting") {
            this.streamState.checkpointAfterExec = true;
          }
          this.queue.push({
            type: "usage",
            outputTokens: this.streamState.outputTokens,
            totalTokens: this.streamState.totalTokens,
          });
        },
        (note) => {
          this.queue.push({ type: "text", text: "\n" + note + "\n", isThinking: false });
        },
      );
      if (recognized) this.resetInactivityTimer();
    } catch (err) {
      logError("CursorSession: processServerMessage failed", { error: String(err) });
      this.pushDone({ type: "done", error: "Failed to process server message" });
      this.close();
    }
  }

  private handleEndStream(endStreamBytes: Uint8Array): void {
    this.streamState.endStreamSeen = true;
    const err = parseConnectEndStream(endStreamBytes);
    if (err) {
      const hint = classifyConnectError(err.message);
      this.pushDone({ type: "done", error: err.message, retryHint: hint });
      this.finish(CLOSE_ERR);
      return;
    }
    if (this.pendingExecs.length > 0 && this.batchState === "collecting") {
      this.streamState.checkpointAfterExec = true;
    }
  }

  private afterParse(): void {
    if (this.streamState.checkpointAfterExec && this.batchState === "collecting") {
      this.batchState = "flushed";
      this.streamState.checkpointAfterExec = false;
      this._flushedExecs = [...this.pendingExecs];
      this.clearInactivityTimer();
      this.queue.push({ type: "batchReady" });
    }
    if (
      this.streamState.endStreamSeen &&
      this.batchState !== "collecting" &&
      !this.doneEventSent &&
      this.pendingExecs.length === 0
    ) {
      this.pushDone({ type: "done" });
    }
  }
}

// --- Unary RPC (used by title.ts, models.ts) ---

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
  const { connectUrl, authority } = resolveCursorH2Target(options.url ?? CURSOR_API_URL);
  const requestId = randomUUID();
  const { promise, resolve } = Promise.withResolvers<{
    body: Uint8Array;
    exitCode: number;
    timedOut: boolean;
  }>();
  let timedOut = false;
  let settled = false;

  const session = h2Connect(connectUrl);
  const timeoutMs = options.timeoutMs ?? 5_000;
  const timeout =
    timeoutMs > 0
      ? setTimeout(() => {
          timedOut = true;
          try { session.destroy(); } catch { /* ignore */ }
        }, timeoutMs)
      : undefined;

  const finish = (body: Uint8Array, code: number) => {
    if (settled) return;
    settled = true;
    if (timeout) clearTimeout(timeout);
    try { session.close(); } catch { /* ignore */ }
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
  if (authority) headers[":authority"] = authority;

  const stream = session.request(headers);
  const chunks: Buffer[] = [];
  stream.on("data", (chunk: Buffer) => chunks.push(Buffer.from(chunk)));
  stream.on("end", () => finish(Buffer.concat(chunks), 0));
  stream.on("error", () => finish(Buffer.concat(chunks), 1));
  if (options.requestBody.length > 0) {
    stream.end(Buffer.from(options.requestBody));
  } else {
    stream.end();
  }

  return promise;
}
