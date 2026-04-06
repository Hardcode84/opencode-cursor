/**
 * Native Cursor tool → OpenCode MCP tool redirection and result formatting.
 *
 * When Cursor's model calls native tools (readArgs, shellArgs, etc.), we intercept
 * them and redirect to OpenCode's MCP equivalents. When results come back, we
 * format them as the native Cursor protobuf types the server expects.
 */
import { create, toBinary } from "@bufbuild/protobuf";
import { logDebug } from "./logger";
import {
  AgentClientMessageSchema,
  DeleteResultSchema,
  DeleteSuccessSchema,
  ExecClientControlMessageSchema,
  ExecClientMessageSchema,
  ExecClientStreamCloseSchema,
  type ExecServerMessage,
  FetchResultSchema,
  FetchSuccessSchema,
  McpResultSchema,
  McpSuccessSchema,
  McpTextContentSchema,
  McpToolResultContentItemSchema,
  ReadResultSchema,
  ReadSuccessSchema,
  ShellResultSchema,
  ShellStreamExitSchema,
  ShellStreamSchema,
  ShellStreamStartSchema,
  ShellStreamStdoutSchema,
  ShellSuccessSchema,
  WriteResultSchema,
  WriteSuccessSchema,
} from "./proto/agent_pb";
import { frameConnectMessage } from "./protocol";

function proxyLog(msg: string, ...args: unknown[]): void {
  let i = 0;
  logDebug(msg.replace(/%[sdj]/g, () => String(args[i++] ?? "")));
}

// ── Types ──

export type NativeResultType =
  | "readResult"
  | "writeResult"
  | "deleteResult"
  | "fetchResult"
  | "shellResult"
  | "shellStreamResult"
  | "lsResult"
  | "grepResult";

export interface PendingExec {
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

export interface BridgeWriter {
  write: (data: Uint8Array) => void;
}

interface NativeRedirectInfo {
  toolCallId: string;
  toolName: string;
  decodedArgs: string;
  nativeResultType: NativeResultType;
  nativeArgs: Record<string, string>;
}

// ── Argument fixup ──

/** Fix common argument name mismatches between Cursor native tools and OpenCode MCP tools.
 *  Cursor's model sometimes uses native arg names (e.g. `path`) instead of
 *  the MCP schema names (e.g. `filePath`). Mutates `args` in place. */
export function fixMcpArgNames(toolName: string, args: Record<string, unknown>): void {
  if (toolName === "read") {
    if (args.filePath == null && args.path != null) {
      args.filePath = args.path;
      delete args.path;
    }
  } else if (toolName === "write" || toolName === "edit") {
    if (args.filePath == null && args.path != null) {
      args.filePath = args.path;
      delete args.path;
    }
    if (toolName === "write" && args.content == null && args.file_content != null) {
      args.content = args.file_content;
      delete args.file_content;
    }
  } else if (toolName === "glob") {
    if (args.pattern == null && args.glob_pattern != null) {
      args.pattern = args.glob_pattern;
      delete args.glob_pattern;
    }
    if (args.path == null && args.target_directory != null) {
      args.path = args.target_directory;
      delete args.target_directory;
    }
  } else if (toolName === "grep") {
    if (args.pattern == null) {
      args.pattern = ".";
    }
  }
}

// ── Native → MCP redirection ──

export function nativeToMcpRedirect(
  execCase: string,
  execMsg: ExecServerMessage,
): NativeRedirectInfo | null {
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
    const fileContent =
      args.fileBytes?.length > 0 ? new TextDecoder().decode(args.fileBytes) : (args.fileText ?? "");
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
      decodedArgs: JSON.stringify({
        command: `rm -f -- '${safePath}'`,
        description: "Delete file",
      }),
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
    const mcpArgs: Record<string, any> = {
      command: cmd,
      description: args.description || "Execute command",
    };
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

// ── Result formatting ──

/** Send a single mcpResult (success) on the bridge for a matched exec. */
export function sendMcpResultSuccess(
  bridge: BridgeWriter,
  exec: PendingExec,
  content: string,
): void {
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
      toBinary(
        AgentClientMessageSchema,
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
      toBinary(
        AgentClientMessageSchema,
        create(AgentClientMessageSchema, {
          message: { case: "execClientControlMessage", value: controlMsg },
        }),
      ),
    ),
  );
}

/** Send a native Cursor tool result for a redirected exec. */
export function sendNativeResult(bridge: BridgeWriter, exec: PendingExec, content: string): void {
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
            toBinary(AgentClientMessageSchema, create(AgentClientMessageSchema, { message: msg })),
          ),
        );
      };
      const sendStreamEvent = (event: any) => {
        writeFrame({
          case: "execClientMessage",
          value: create(ExecClientMessageSchema, {
            id: exec.execMsgId,
            execId: exec.execId,
            message: {
              case: "shellStream" as any,
              value: create(ShellStreamSchema, { event }) as any,
            },
          }),
        });
      };
      sendStreamEvent({ case: "start", value: create(ShellStreamStartSchema, {}) });
      if (content) {
        sendStreamEvent({
          case: "stdout",
          value: create(ShellStreamStdoutSchema, { data: content }),
        });
      }
      sendStreamEvent({ case: "exit", value: create(ShellStreamExitSchema, { code: 0 }) });
      writeFrame({
        case: "execClientControlMessage",
        value: create(ExecClientControlMessageSchema, {
          message: {
            case: "streamClose",
            value: create(ExecClientStreamCloseSchema, { id: exec.execMsgId }),
          },
        }),
      });
      return;
    }
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
      toBinary(
        AgentClientMessageSchema,
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
      toBinary(
        AgentClientMessageSchema,
        create(AgentClientMessageSchema, {
          message: { case: "execClientControlMessage", value: controlMsg },
        }),
      ),
    ),
  );
}
