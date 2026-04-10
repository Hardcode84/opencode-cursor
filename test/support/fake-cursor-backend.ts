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
  AgentConversationTurnStructureSchema,
  AgentRunRequestSchema,
  AgentServerMessageSchema,
  AssistantMessageSchema,
  type ConversationStateStructure,
  ConversationStateStructureSchema,
  ConversationStepSchema,
  ConversationTurnStructureSchema,
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
  UserMessageSchema,
} from "../../src/proto/agent_pb";
import {
  AvailableModelsResponse_AvailableModelSchema,
  AvailableModelsResponseSchema,
} from "../../src/proto/aiserver_pb";
import {
  CONNECT_END_STREAM_FLAG,
  createConnectFrameParser,
  decodeConnectUnaryBody,
  frameConnectMessage,
} from "../../src/protocol";

export interface FakeCursorModel {
  id: string;
  name: string;
  reasoning?: boolean;
}

export interface FakeUnaryRequest {
  path: string;
  headers: IncomingHttpHeaders;
  body: Uint8Array;
}

export interface FakeConversationTurnSnapshot {
  userText: string;
  assistantText: string;
}

export interface FakeRunRequestSnapshot {
  conversationId: string;
  modelId: string;
  maxMode: boolean;
  actionCase: string;
  userText: string;
  turns: FakeConversationTurnSnapshot[];
  pendingToolCalls: string[];
  raw: AgentClientMessage;
}

export interface FakeMcpResultSnapshot {
  execMessageId: number;
  execId: string;
  text: string;
  raw: AgentClientMessage;
}

export interface FakeCommunicationPoint {
  ordinal: number;
  label: string;
  connectionIndex: number;
}

type FailureMode = "reset" | "destroy";

type RunHandler = (connection: FakeRunConnection) => void | Promise<void>;

interface Waiter {
  cursorKey: string;
  predicate: (message: AgentClientMessage) => boolean;
  resolve: (message: AgentClientMessage) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

const MAX_UNARY_BODY_BYTES = 1_000_000;
const UNARY_BODY_TIMEOUT_MS = 1_000;

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

function buildConversationState(
  turns: FakeConversationTurnSnapshot[],
  options: { pendingToolCalls?: string[] } = {},
): ConversationStateStructure {
  const turnBytes = turns.map((turn, index) => {
    const userMessageBytes = toBinary(
      UserMessageSchema,
      create(UserMessageSchema, {
        text: turn.userText,
        messageId: `fake-user-${index + 1}`,
      }),
    );
    const stepBytes = turn.assistantText
      ? [
          toBinary(
            ConversationStepSchema,
            create(ConversationStepSchema, {
              message: {
                case: "assistantMessage",
                value: create(AssistantMessageSchema, { text: turn.assistantText }),
              },
            }),
          ),
        ]
      : [];
    return toBinary(
      ConversationTurnStructureSchema,
      create(ConversationTurnStructureSchema, {
        turn: {
          case: "agentConversationTurn",
          value: create(AgentConversationTurnStructureSchema, {
            userMessage: userMessageBytes,
            steps: stepBytes,
          }),
        },
      }),
    );
  });

  return create(ConversationStateStructureSchema, {
    turns: turnBytes,
    rootPromptMessagesJson: [],
    todos: [],
    pendingToolCalls: options.pendingToolCalls ?? [],
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

function decodeConversationTurns(
  state: ConversationStateStructure | undefined,
): FakeConversationTurnSnapshot[] {
  if (!state) return [];
  return state.turns.flatMap((turnBytes) => {
    const turn = fromBinary(ConversationTurnStructureSchema, turnBytes);
    if (turn.turn.case !== "agentConversationTurn") return [];
    const agentTurn = turn.turn.value;
    const userMessage = fromBinary(UserMessageSchema, agentTurn.userMessage);
    const assistantText = agentTurn.steps
      .map((stepBytes) => fromBinary(ConversationStepSchema, stepBytes))
      .flatMap((step) =>
        step.message.case === "assistantMessage" ? [step.message.value.text || ""] : [],
      )
      .join("");
    return [
      {
        userText: userMessage.text || "",
        assistantText,
      },
    ];
  });
}

function decodeRunRequest(message: AgentClientMessage): FakeRunRequestSnapshot {
  if (message.message.case !== "runRequest") {
    throw new Error(`Expected runRequest, got ${message.message.case ?? "undefined"}`);
  }
  const runRequest = fromBinary(
    AgentRunRequestSchema,
    toBinary(AgentRunRequestSchema, message.message.value),
  );
  const actionCase = runRequest.action?.action.case ?? "";
  const userText =
    actionCase === "userMessageAction"
      ? runRequest.action?.action.value.userMessage?.text || ""
      : "";
  return {
    conversationId: runRequest.conversationId || "",
    modelId: runRequest.modelDetails?.modelId || "",
    maxMode: runRequest.modelDetails?.maxMode ?? false,
    actionCase,
    userText,
    turns: decodeConversationTurns(runRequest.conversationState),
    pendingToolCalls: [...(runRequest.conversationState?.pendingToolCalls ?? [])],
    raw: message,
  };
}

function decodeMcpResult(message: AgentClientMessage): FakeMcpResultSnapshot {
  if (
    message.message.case !== "execClientMessage" ||
    message.message.value.message.case !== "mcpResult"
  ) {
    throw new Error(
      `Expected execClientMessage.mcpResult, got ${message.message.case ?? "undefined"}`,
    );
  }
  const result = message.message.value.message.value;
  const text =
    result.result.case === "success"
      ? result.result.value.content
          .flatMap((item) => (item.content.case === "text" ? [item.content.value.text] : []))
          .join("")
      : "";
  return {
    execMessageId: message.message.value.id,
    execId: message.message.value.execId,
    text,
    raw: message,
  };
}

export class FakeRunConnection {
  readonly clientMessages: AgentClientMessage[] = [];
  private readonly waiters: Waiter[] = [];
  private readonly cursors = new Map<string, number>();
  private closedResolvers: Array<() => void> = [];
  private _closed = false;
  private _interrupted = false;
  private contextTag = "";

  constructor(
    private readonly backend: FakeCursorBackend,
    private readonly stream: ServerHttp2Stream,
    readonly connectionIndex: number,
    readonly headers: IncomingHttpHeaders,
  ) {
    const frameParser = createConnectFrameParser(
      (bytes) => {
        const message = fromBinary(AgentClientMessageSchema, bytes);
        this.clientMessages.push(message);
        this.resolveWaiters();
        this.recordClientPoint(message);
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

  get interrupted(): boolean {
    return this._interrupted || this._closed;
  }

  setContextTag(tag: string): void {
    this.contextTag = tag;
  }

  waitForClientMessage(
    predicate: (message: AgentClientMessage) => boolean = () => true,
    timeoutMs = 1_000,
    cursorKey = "any",
  ): Promise<AgentClientMessage> {
    const immediateMatch = this.findMatchingMessage(predicate, cursorKey);
    if (immediateMatch) {
      this.cursors.set(cursorKey, immediateMatch.index + 1);
      return Promise.resolve(immediateMatch.message);
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.removeWaiter(waiter);
        reject(new Error(`Timed out waiting for client message after ${timeoutMs}ms`));
      }, timeoutMs);
      const waiter: Waiter = { cursorKey, predicate, resolve, reject, timer };
      this.waiters.push(waiter);
    });
  }

  waitForClientMessageCase(messageCase: string, timeoutMs = 1_000): Promise<AgentClientMessage> {
    return this.waitForClientMessage(
      (message) => message.message.case === messageCase,
      timeoutMs,
      `case:${messageCase}`,
    );
  }

  async waitForRunRequest(timeoutMs = 1_000): Promise<FakeRunRequestSnapshot> {
    return decodeRunRequest(
      await this.waitForClientMessage(
        (message) => message.message.case === "runRequest",
        timeoutMs,
        "runRequest",
      ),
    );
  }

  async waitForRequestContextResult(
    execMessageId?: number,
    timeoutMs = 1_000,
  ): Promise<AgentClientMessage> {
    return this.waitForClientMessage(
      (message) => {
        if (message.message.case !== "execClientMessage") return false;
        if (message.message.value.message.case !== "requestContextResult") return false;
        return execMessageId == null || message.message.value.id === execMessageId;
      },
      timeoutMs,
      "requestContextResult",
    );
  }

  async waitForMcpResult(
    execMessageId?: number,
    timeoutMs = 1_000,
  ): Promise<FakeMcpResultSnapshot> {
    const message = await this.waitForClientMessage(
      (candidate) => {
        if (candidate.message.case !== "execClientMessage") return false;
        if (candidate.message.value.message.case !== "mcpResult") return false;
        return execMessageId == null || candidate.message.value.id === execMessageId;
      },
      timeoutMs,
      "mcpResult",
    );
    return decodeMcpResult(message);
  }

  async waitForExecStreamClose(
    execMessageId?: number,
    timeoutMs = 1_000,
  ): Promise<AgentClientMessage> {
    return this.waitForClientMessage(
      (message) => {
        if (message.message.case !== "execClientControlMessage") return false;
        if (message.message.value.message.case !== "streamClose") return false;
        return execMessageId == null || message.message.value.message.value.id === execMessageId;
      },
      timeoutMs,
      "execStreamClose",
    );
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

  sendConversationCheckpoint(
    turns: FakeConversationTurnSnapshot[],
    options: { pendingToolCalls?: string[] } = {},
  ): void {
    this.sendCheckpoint(buildConversationState(turns, options));
  }

  sendEndStreamError(code: string, message: string): void {
    if (this.failAtPoint(`server.endStream.error.${code}`)) return;
    const payload = new TextEncoder().encode(JSON.stringify({ error: { code, message } }));
    this.writeFrame(payload, CONNECT_END_STREAM_FLAG);
    try {
      this.stream.end();
    } catch {
      /* ignore */
    }
  }

  sendEndStreamOk(): void {
    if (this.failAtPoint("server.endStream.ok")) return;
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
    if (this.failAtPoint(describeServerMessage(message))) return;
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

  private findMatchingMessage(
    predicate: (message: AgentClientMessage) => boolean,
    cursorKey: string,
  ): { message: AgentClientMessage; index: number } | null {
    for (
      let index = this.cursors.get(cursorKey) ?? 0;
      index < this.clientMessages.length;
      index++
    ) {
      const candidate = this.clientMessages[index]!;
      if (predicate(candidate)) {
        return { message: candidate, index };
      }
    }
    return null;
  }

  private recordClientPoint(message: AgentClientMessage): void {
    this.failAtPoint(describeClientMessage(message));
  }

  private failAtPoint(label: string): boolean {
    const failed = this.backend.recordPoint(
      this,
      this.contextTag ? `${this.contextTag}:${label}` : label,
    );
    if (failed) {
      this._interrupted = true;
    }
    return failed;
  }

  private resolveWaiters(): void {
    for (let i = 0; i < this.waiters.length; ) {
      const waiter = this.waiters[i]!;
      const match = this.findMatchingMessage(waiter.predicate, waiter.cursorKey);
      if (!match) {
        i++;
        continue;
      }
      this.cursors.set(waiter.cursorKey, match.index + 1);
      clearTimeout(waiter.timer);
      this.waiters.splice(i, 1);
      waiter.resolve(match.message);
    }
  }
}

export class FakeCursorBackend {
  private readonly server = http2.createServer();
  private readonly runHandlers: RunHandler[] = [];
  private defaultRunHandler: RunHandler | undefined;
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
  readonly communicationPoints: FakeCommunicationPoint[] = [];
  private failOnceAtOrdinal: number | null = null;
  private failureMode: FailureMode = "reset";
  private failureInjected = false;

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

  latestRunConnection(): FakeRunConnection | undefined {
    return this.runConnections.at(-1);
  }

  get didInjectFailure(): boolean {
    return this.failureInjected;
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

  setRunHandler(handler: RunHandler): void {
    this.defaultRunHandler = handler;
  }

  setFailOnceAtPoint(ordinal: number, mode: FailureMode = "reset"): void {
    this.failOnceAtOrdinal = ordinal;
    this.failureMode = mode;
    this.failureInjected = false;
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
      const connection = new FakeRunConnection(
        this,
        stream,
        this.runConnections.length + 1,
        headers,
      );
      this.runConnections.push(connection);
      const handler = this.runHandlers.shift() ?? this.defaultRunHandler;
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

    let body: Uint8Array;
    try {
      body = await this.readBody(stream);
    } catch {
      try {
        stream.respond({ ":status": 400 });
        stream.end();
      } catch {
        /* ignore */
      }
      return;
    }
    this.unaryRequests.push({ path, headers, body });
    this.respondUnary(stream, path, body);
  }

  private async readBody(stream: ServerHttp2Stream): Promise<Uint8Array> {
    const chunks: Buffer[] = [];
    let totalBytes = 0;
    return new Promise<Uint8Array>((resolve, reject) => {
      const cleanup = () => {
        clearTimeout(timer);
        stream.off("data", onData);
        stream.off("end", onEnd);
        stream.off("error", onError);
        stream.off("aborted", onAborted);
        stream.off("close", onClose);
      };
      const fail = (error: Error) => {
        cleanup();
        reject(error);
      };
      const onData = (chunk: Buffer | Uint8Array) => {
        const buffer = Buffer.from(chunk);
        totalBytes += buffer.length;
        if (totalBytes > MAX_UNARY_BODY_BYTES) {
          fail(new Error("Unary request body exceeded maximum size"));
          return;
        }
        chunks.push(buffer);
      };
      const onEnd = () => {
        cleanup();
        resolve(new Uint8Array(Buffer.concat(chunks)));
      };
      const onError = (error: Error) => fail(error);
      const onAborted = () => fail(new Error("Unary request stream aborted"));
      const onClose = () => fail(new Error("Unary request stream closed before end"));
      const timer = setTimeout(() => {
        fail(new Error("Timed out reading unary request body"));
      }, UNARY_BODY_TIMEOUT_MS);

      stream.on("data", onData);
      stream.on("end", onEnd);
      stream.on("error", onError);
      stream.on("aborted", onAborted);
      stream.on("close", onClose);
    });
  }

  private respondUnary(stream: ServerHttp2Stream, path: string, requestBody: Uint8Array): void {
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
      const modelId = decodeTokenLimitModelId(requestBody);
      const responseBody = frameUnaryPayload(
        encodeTokenLimit(this.tokenLimits.get(modelId ?? "test-model") ?? 200_000),
      );
      stream.respond({ ":status": 200, "content-type": "application/connect+proto" });
      stream.end(responseBody);
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

  recordPoint(connection: FakeRunConnection, label: string): boolean {
    const point: FakeCommunicationPoint = {
      ordinal: this.communicationPoints.length,
      label,
      connectionIndex: connection.connectionIndex,
    };
    this.communicationPoints.push(point);

    if (this.failOnceAtOrdinal === point.ordinal && !this.failureInjected && !connection.closed) {
      this.failureInjected = true;
      if (this.failureMode === "destroy") {
        connection.destroy(new Error(`Injected failure at ${label}`));
      } else {
        connection.resetStream();
      }
      return true;
    }

    return false;
  }
}

function describeClientMessage(message: AgentClientMessage): string {
  if (message.message.case === "execClientMessage") {
    return `client.execClientMessage.${message.message.value.message.case ?? "unknown"}`;
  }
  if (message.message.case === "execClientControlMessage") {
    return `client.execClientControlMessage.${message.message.value.message.case ?? "unknown"}`;
  }
  if (message.message.case === "kvClientMessage") {
    return `client.kvClientMessage.${message.message.value.message.case ?? "unknown"}`;
  }
  return `client.${message.message.case ?? "unknown"}`;
}

function describeServerMessage(
  message: Parameters<typeof create<typeof AgentServerMessageSchema>>[1],
): string {
  if (message.message.case === "execServerMessage") {
    return `server.execServerMessage.${message.message.value.message.case ?? "unknown"}`;
  }
  if (message.message.case === "interactionUpdate") {
    return `server.interactionUpdate.${message.message.value.message.case ?? "unknown"}`;
  }
  return `server.${message.message.case ?? "unknown"}`;
}

function decodeTokenLimitModelId(body: Uint8Array): string | null {
  const payload = decodeConnectUnaryBody(body) ?? body;
  let offset = 0;
  if (payload[offset++] !== 0x0a) return null;
  const outer = readLengthDelimited(payload, offset);
  if (!outer) return null;
  offset = 0;
  if (outer.bytes[offset++] !== 0x0a) return null;
  const inner = readLengthDelimited(outer.bytes, offset);
  if (!inner) return null;
  return new TextDecoder().decode(inner.bytes);
}

function readLengthDelimited(
  bytes: Uint8Array,
  offset: number,
): { bytes: Uint8Array; nextOffset: number } | null {
  const length = readVarint(bytes, offset);
  if (!length) return null;
  const endOffset = length.nextOffset + length.value;
  if (endOffset > bytes.length) return null;
  return {
    bytes: bytes.subarray(length.nextOffset, endOffset),
    nextOffset: endOffset,
  };
}

function readVarint(
  bytes: Uint8Array,
  offset: number,
): { value: number; nextOffset: number } | null {
  let value = 0;
  let shift = 0;
  let nextOffset = offset;
  while (nextOffset < bytes.length) {
    const byte = bytes[nextOffset++]!;
    value |= (byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) {
      return { value, nextOffset };
    }
    shift += 7;
  }
  return null;
}
