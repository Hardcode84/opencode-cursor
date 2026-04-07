import http2, {
  constants as http2Constants,
  type IncomingHttpHeaders,
  type ServerHttp2Stream,
} from "node:http2";
import type { AddressInfo } from "node:net";
import { create, fromBinary, fromJson, toBinary } from "@bufbuild/protobuf";
import { ValueSchema } from "@bufbuild/protobuf/wkt";
import { encodeVarint } from "../../src/models";
import {
  type AgentClientMessage,
  AgentClientMessageSchema,
  AgentServerMessageSchema,
  type ConversationStateStructure,
  ConversationStateStructureSchema,
  ExecServerMessageSchema,
  GetUsableModelsResponseSchema,
  InteractionUpdateSchema,
  McpArgsSchema,
  ModelDetailsSchema,
  NameAgentResponseSchema,
  RequestContextArgsSchema,
  TextDeltaUpdateSchema,
  ThinkingDeltaUpdateSchema,
  TokenDeltaUpdateSchema,
} from "../../src/proto/agent_pb";
import {
  AvailableModelsResponse_AvailableModelSchema,
  AvailableModelsResponseSchema,
} from "../../src/proto/aiserver_pb";
import {
  CONNECT_END_STREAM_FLAG,
  createConnectFrameParser,
  frameConnectMessage,
} from "../../src/protocol";

export interface FakeCursorModel {
  id: string;
  name: string;
  reasoning?: boolean;
  contextWindow?: number;
  maxTokens?: number;
}

export interface FakeUnaryRequest {
  path: string;
  headers: IncomingHttpHeaders;
  body: Uint8Array;
}

type RunHandler = (connection: FakeRunConnection) => void | Promise<void>;

interface Waiter {
  predicate: (message: AgentClientMessage) => boolean;
  resolve: (message: AgentClientMessage) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

function frameUnaryPayload(payload: Uint8Array): Buffer {
  return Buffer.from(frameConnectMessage(payload));
}

function encodeTokenLimit(limit: number): Uint8Array {
  const varint = encodeVarint(limit);
  const out = new Uint8Array(1 + varint.length);
  out[0] = 0x08; // field 1, wire type 0
  out.set(varint, 1);
  return out;
}

function encodeValue(value: unknown): Uint8Array {
  return toBinary(ValueSchema, fromJson(ValueSchema, value));
}

export class FakeRunConnection {
  readonly clientMessages: AgentClientMessage[] = [];
  private readonly waiters: Waiter[] = [];
  private readCursor = 0;
  private closedResolvers: Array<() => void> = [];
  private _closed = false;

  constructor(
    private readonly stream: ServerHttp2Stream,
    readonly headers: IncomingHttpHeaders,
  ) {
    const frameParser = createConnectFrameParser(
      (bytes) => {
        const message = fromBinary(AgentClientMessageSchema, bytes);
        this.clientMessages.push(message);
        this.resolveWaiters();
      },
      () => {
        // Client-side endStream frames are unexpected here, but they still count
        // as transport activity for tests waiting on disconnect behavior.
      },
    );

    this.stream.on("data", (chunk: Buffer | Uint8Array) => {
      frameParser(Buffer.from(chunk));
    });
    this.stream.on("close", () => this.markClosed());
    this.stream.on("aborted", () => this.markClosed());
    this.stream.on("error", () => this.markClosed());
  }

  get closed(): boolean {
    return this._closed;
  }

  waitForClientMessage(
    predicate: (message: AgentClientMessage) => boolean = () => true,
    timeoutMs = 1_000,
  ): Promise<AgentClientMessage> {
    for (let i = this.readCursor; i < this.clientMessages.length; i++) {
      const candidate = this.clientMessages[i]!;
      if (predicate(candidate)) {
        this.readCursor = i + 1;
        return Promise.resolve(candidate);
      }
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.removeWaiter(waiter);
        reject(new Error(`Timed out waiting for client message after ${timeoutMs}ms`));
      }, timeoutMs);
      const waiter: Waiter = { predicate, resolve, reject, timer };
      this.waiters.push(waiter);
    });
  }

  waitForClientMessageCase(messageCase: string, timeoutMs = 1_000): Promise<AgentClientMessage> {
    return this.waitForClientMessage((message) => message.message.case === messageCase, timeoutMs);
  }

  waitForClose(timeoutMs = 1_000): Promise<void> {
    if (this._closed) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`Timed out waiting for stream close after ${timeoutMs}ms`));
      }, timeoutMs);
      this.closedResolvers.push(() => {
        clearTimeout(timer);
        resolve();
      });
    });
  }

  sendRequestContextArgs(execId = 1): void {
    const requestContextArgs = create(RequestContextArgsSchema, {});
    const execMessage = create(ExecServerMessageSchema, {
      id: execId,
      execId: `exec-request-context-${execId}`,
      message: { case: "requestContextArgs", value: requestContextArgs },
    });
    this.sendServerMessage({
      message: { case: "execServerMessage", value: execMessage },
    });
  }

  sendMcpToolCall(
    toolName: string,
    args: Record<string, unknown> = {},
    options: { toolCallId?: string; execId?: number } = {},
  ): void {
    const execId = options.execId ?? 1;
    const mcpArgs = create(McpArgsSchema, {
      name: toolName,
      toolCallId: options.toolCallId ?? `${toolName}-call-${execId}`,
      args: Object.fromEntries(
        Object.entries(args).map(([key, value]) => [key, encodeValue(value)]),
      ),
    });
    const execMessage = create(ExecServerMessageSchema, {
      id: execId,
      execId: `exec-${execId}`,
      message: { case: "mcpArgs", value: mcpArgs },
    });
    this.sendServerMessage({
      message: { case: "execServerMessage", value: execMessage },
    });
  }

  sendTextDelta(text: string): void {
    this.sendInteractionUpdate({
      message: { case: "textDelta", value: create(TextDeltaUpdateSchema, { text }) },
    });
  }

  sendThinkingDelta(text: string): void {
    this.sendInteractionUpdate({
      message: { case: "thinkingDelta", value: create(ThinkingDeltaUpdateSchema, { text }) },
    });
  }

  sendTokenDelta(tokens: number): void {
    this.sendInteractionUpdate({
      message: { case: "tokenDelta", value: create(TokenDeltaUpdateSchema, { tokens }) },
    });
  }

  sendCheckpoint(state: Partial<ConversationStateStructure> = {}): void {
    this.sendServerMessage({
      message: {
        case: "conversationCheckpointUpdate",
        value: create(ConversationStateStructureSchema, state),
      },
    });
  }

  sendEndStreamError(code: string, message: string): void {
    const payload = new TextEncoder().encode(JSON.stringify({ error: { code, message } }));
    this.writeFrame(payload, CONNECT_END_STREAM_FLAG);
    try {
      this.stream.end();
    } catch {
      /* ignore */
    }
  }

  sendEndStreamOk(): void {
    const payload = new TextEncoder().encode(JSON.stringify({}));
    this.writeFrame(payload, CONNECT_END_STREAM_FLAG);
    try {
      this.stream.end();
    } catch {
      /* ignore */
    }
  }

  writeRaw(chunk: Uint8Array | Buffer): void {
    if (this._closed) return;
    this.stream.write(Buffer.from(chunk));
  }

  writeSplitFrame(payload: Uint8Array, splitAt: number, flags = 0): void {
    const frame = frameConnectMessage(payload, flags);
    const cut = Math.max(1, Math.min(splitAt, frame.length - 1));
    this.writeRaw(frame.subarray(0, cut));
    this.writeRaw(frame.subarray(cut));
  }

  resetStream(errorCode = http2Constants.NGHTTP2_CANCEL): void {
    if (this._closed) return;
    try {
      this.stream.close(errorCode);
    } catch {
      /* ignore */
    }
  }

  destroy(error?: Error): void {
    if (this._closed) return;
    try {
      this.stream.destroy(error);
    } catch {
      /* ignore */
    }
  }

  private sendInteractionUpdate(
    update: Parameters<typeof create<typeof InteractionUpdateSchema>>[1],
  ): void {
    this.sendServerMessage({
      message: {
        case: "interactionUpdate",
        value: create(InteractionUpdateSchema, update),
      },
    });
  }

  private sendServerMessage(
    message: Parameters<typeof create<typeof AgentServerMessageSchema>>[1],
  ): void {
    this.writeFrame(toBinary(AgentServerMessageSchema, create(AgentServerMessageSchema, message)));
  }

  private writeFrame(payload: Uint8Array, flags = 0): void {
    if (this._closed) return;
    this.stream.write(frameConnectMessage(payload, flags));
  }

  private markClosed(): void {
    if (this._closed) return;
    this._closed = true;
    for (const waiter of this.waiters.splice(0)) {
      clearTimeout(waiter.timer);
      waiter.reject(new Error("Stream closed before expected client message"));
    }
    for (const resolve of this.closedResolvers.splice(0)) {
      resolve();
    }
  }

  private removeWaiter(target: Waiter): void {
    const index = this.waiters.indexOf(target);
    if (index >= 0) this.waiters.splice(index, 1);
  }

  private resolveWaiters(): void {
    for (let i = 0; i < this.waiters.length; ) {
      const waiter = this.waiters[i]!;
      let resolved = false;
      for (let j = this.readCursor; j < this.clientMessages.length; j++) {
        const candidate = this.clientMessages[j]!;
        if (!waiter.predicate(candidate)) continue;
        this.readCursor = j + 1;
        clearTimeout(waiter.timer);
        this.waiters.splice(i, 1);
        waiter.resolve(candidate);
        resolved = true;
        break;
      }
      if (!resolved) i++;
    }
  }
}

export class FakeCursorBackend {
  private readonly server = http2.createServer();
  private readonly runHandlers: RunHandler[] = [];
  private availableModels: FakeCursorModel[] = [
    { id: "test-model", name: "Test Model", reasoning: true },
  ];
  private usableModels: FakeCursorModel[] = [
    { id: "test-model", name: "Test Model", reasoning: true },
  ];
  private tokenLimits = new Map<string, number>([["test-model", 200_000]]);
  private title = "Generated Title";

  readonly unaryRequests: FakeUnaryRequest[] = [];
  readonly runConnections: FakeRunConnection[] = [];

  private constructor() {
    this.server.on("stream", (stream, headers) => {
      void this.handleStream(stream, headers);
    });
  }

  static async start(): Promise<FakeCursorBackend> {
    const backend = new FakeCursorBackend();
    await new Promise<void>((resolve) => backend.server.listen(0, "127.0.0.1", resolve));
    return backend;
  }

  get apiUrl(): string {
    const address = this.server.address() as AddressInfo;
    return `http://127.0.0.1:${address.port}`;
  }

  get agentUrl(): string {
    return this.apiUrl;
  }

  get runCount(): number {
    return this.runConnections.length;
  }

  setAvailableModels(models: FakeCursorModel[]): void {
    this.availableModels = [...models];
  }

  setUsableModels(models: FakeCursorModel[]): void {
    this.usableModels = [...models];
  }

  setTokenLimit(modelId: string, limit: number): void {
    this.tokenLimits.set(modelId, limit);
  }

  setTitle(title: string): void {
    this.title = title;
  }

  enqueueRun(handler: RunHandler): void {
    this.runHandlers.push(handler);
  }

  async close(): Promise<void> {
    for (const connection of this.runConnections) {
      connection.resetStream();
    }
    await new Promise<void>((resolve, reject) =>
      this.server.close((error) => (error ? reject(error) : resolve())),
    );
  }

  private async handleStream(
    stream: ServerHttp2Stream,
    headers: IncomingHttpHeaders,
  ): Promise<void> {
    const path = String(headers[":path"] ?? "");
    if (path === "/agent.v1.AgentService/Run") {
      stream.respond({
        ":status": 200,
        "content-type": "application/connect+proto",
      });
      const connection = new FakeRunConnection(stream, headers);
      this.runConnections.push(connection);
      const handler = this.runHandlers.shift();
      if (!handler) {
        connection.sendEndStreamError("test_backend_error", "No run handler queued");
        return;
      }
      try {
        await handler(connection);
      } catch (error) {
        connection.sendEndStreamError(
          "test_backend_error",
          error instanceof Error ? error.message : String(error),
        );
      }
      return;
    }

    const body = await this.readBody(stream);
    this.unaryRequests.push({ path, headers, body });
    this.respondUnary(stream, path);
  }

  private async readBody(stream: ServerHttp2Stream): Promise<Uint8Array> {
    const chunks: Buffer[] = [];
    await new Promise<void>((resolve) => {
      stream.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
      stream.on("end", resolve);
    });
    return new Uint8Array(Buffer.concat(chunks));
  }

  private respondUnary(stream: ServerHttp2Stream, path: string): void {
    if (path === "/aiserver.v1.AiService/AvailableModels") {
      const body = frameUnaryPayload(
        toBinary(
          AvailableModelsResponseSchema,
          create(AvailableModelsResponseSchema, {
            models: this.availableModels.map((model) =>
              create(AvailableModelsResponse_AvailableModelSchema, {
                name: model.id,
                clientDisplayName: model.name,
                supportsThinking: model.reasoning ?? true,
              }),
            ),
          }),
        ),
      );
      stream.respond({ ":status": 200, "content-type": "application/connect+proto" });
      stream.end(body);
      return;
    }

    if (path === "/agent.v1.AgentService/GetUsableModels") {
      const body = frameUnaryPayload(
        toBinary(
          GetUsableModelsResponseSchema,
          create(GetUsableModelsResponseSchema, {
            models: this.usableModels.map((model) =>
              create(ModelDetailsSchema, {
                modelId: model.id,
                displayModelId: model.id,
                displayName: model.name,
                displayNameShort: model.name,
                aliases: [],
                maxMode: true,
              }),
            ),
          }),
        ),
      );
      stream.respond({ ":status": 200, "content-type": "application/connect+proto" });
      stream.end(body);
      return;
    }

    if (path === "/aiserver.v1.AiService/GetEffectiveTokenLimit") {
      const body = frameUnaryPayload(
        encodeTokenLimit(this.tokenLimits.get("test-model") ?? 200_000),
      );
      stream.respond({ ":status": 200, "content-type": "application/connect+proto" });
      stream.end(body);
      return;
    }

    if (path === "/agent.v1.AgentService/NameAgent") {
      const body = frameUnaryPayload(
        toBinary(NameAgentResponseSchema, create(NameAgentResponseSchema, { name: this.title })),
      );
      stream.respond({ ":status": 200, "content-type": "application/connect+proto" });
      stream.end(body);
      return;
    }

    stream.respond({ ":status": 404 });
    stream.end();
  }
}
