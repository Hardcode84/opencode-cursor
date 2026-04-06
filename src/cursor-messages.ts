/**
 * Handlers for Cursor server messages (AgentServerMessage).
 *
 * Processes interactionUpdate (text/thinking deltas, tool lifecycle),
 * execServerMessage (tool calls, request context), kvServerMessage (blobs),
 * interactionQuery (web search, questions), and checkpoints.
 */
import { create, fromBinary, toBinary, toJson } from "@bufbuild/protobuf";
import { ValueSchema } from "@bufbuild/protobuf/wkt";
import {
  AgentClientMessageSchema,
  BackgroundShellSpawnResultSchema,
  ConversationStateStructureSchema,
  DiagnosticsResultSchema,
  ExecClientControlMessageSchema,
  ExecClientMessageSchema,
  ExecClientStreamCloseSchema,
  GetBlobResultSchema,
  InteractionResponseSchema,
  KvClientMessageSchema,
  McpErrorSchema,
  McpInstructionsSchema,
  McpResultSchema,
  McpToolDefinitionSchema,
  RequestContextResultSchema,
  RequestContextSchema,
  RequestContextSuccessSchema,
  SetBlobResultSchema,
  ShellRejectedSchema,
  WebSearchRequestResponseSchema,
  WebSearchRequestResponse_ApprovedSchema,
  ExaSearchRequestResponseSchema,
  ExaFetchRequestResponseSchema,
  AskQuestionInteractionResponseSchema,
  AskQuestionResultSchema,
  AskQuestionRejectedSchema,
  SwitchModeRequestResponseSchema,
  CreatePlanRequestResponseSchema,
  WriteShellStdinErrorSchema,
  WriteShellStdinResultSchema,
  type AgentServerMessage,
  type ConversationStateStructure,
  type ExecServerControlMessage,
  type ExecServerMessage,
  type KvServerMessage,
  type McpToolDefinition,
} from "./proto/agent_pb";
import { frameConnectMessage } from "./protocol";
import { type PendingExec, fixMcpArgNames, nativeToMcpRedirect } from "./native-tools";
import { logDebug, logWarn } from "./logger";

function proxyLog(msg: string, ...args: unknown[]): void {
  let i = 0;
  logDebug(msg.replace(/%[sdj]/g, () => String(args[i++] ?? "")));
}

// ── Types ──

export interface StreamState {
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

export function computeUsage(state: StreamState) {
  const completion_tokens = state.outputTokens;
  const total_tokens = state.totalTokens || completion_tokens;
  const prompt_tokens = Math.max(0, total_tokens - completion_tokens);
  return { prompt_tokens, completion_tokens, total_tokens };
}

// ── MCP arg decoding ──

function decodeMcpArgValue(value: Uint8Array): unknown {
  try {
    const parsed = fromBinary(ValueSchema, value);
    return toJson(ValueSchema, parsed);
  } catch {}
  return new TextDecoder().decode(value);
}

function decodeMcpArgsMap(args: Record<string, Uint8Array>): Record<string, unknown> {
  const decoded: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(args)) {
    decoded[key] = decodeMcpArgValue(value);
  }
  return decoded;
}

// ── Main dispatcher ──

/** Returns true if the message was a recognized type (real server activity, not keepalive). */
export function processServerMessage(
  msg: AgentServerMessage,
  blobStore: Map<string, Uint8Array>,
  mcpTools: McpToolDefinition[],
  cloudRule: string | undefined,
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
      cloudRule,
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

// ── Interaction updates ──

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

// ── Interaction queries ──

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

// ── KV (blob store) ──

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

// ── Exec messages (tool calls) ──

export function handleExecMessage(
  execMsg: ExecServerMessage,
  mcpTools: McpToolDefinition[],
  cloudRule: string | undefined,
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
      cloudRule: cloudRule || undefined,
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
    fixMcpArgNames(resolvedToolName, decoded);
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

// ── Exec result helpers ──

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
