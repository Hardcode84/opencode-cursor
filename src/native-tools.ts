/**
 * Native Cursor tool → OpenCode MCP tool redirection and result formatting.
 *
 * When Cursor's model calls native tools (readArgs, shellArgs, etc.), we intercept
 * them and redirect to OpenCode's MCP equivalents. When results come back, we
 * format them as the native Cursor protobuf types the server expects.
 */
import { create, toBinary } from "@bufbuild/protobuf";
import { logDebugFmt } from "./logger";
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
  type GrepContentMatch,
  GrepContentMatchSchema,
  type GrepContentResult,
  GrepContentResultSchema,
  type GrepCountResult,
  GrepCountResultSchema,
  type GrepFileCount,
  GrepFileCountSchema,
  type GrepFileMatch,
  GrepFileMatchSchema,
  type GrepFilesResult,
  GrepFilesResultSchema,
  type GrepResult,
  GrepResultSchema,
  GrepSuccessSchema,
  type GrepUnionResult,
  GrepUnionResultSchema,
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

// ── MCP tool naming ──

export const MCP_TOOL_PREFIX = "mcp_opencode_";

export function stripMcpToolPrefix(name: string): string {
  return name.startsWith(MCP_TOOL_PREFIX) ? name.slice(MCP_TOOL_PREFIX.length) : name;
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

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: maps all native Cursor tool types to MCP equivalents
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
    const rawPath: string = args.path ?? "";
    if (!rawPath) {
      return {
        toolCallId,
        toolName: "bash",
        decodedArgs: JSON.stringify({ command: "true", description: "No-op delete (empty path)" }),
        nativeResultType: "deleteResult" as const,
        nativeArgs: { path: "" },
      };
    }
    // Single-quote wrapping is POSIX-safe for all characters except NUL
    // (which can't appear in real file paths). Strip NUL as defense-in-depth.
    const safePath = rawPath.replace(/\0/g, "").replace(/'/g, "'\\''");
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
      logDebugFmt("grepArgs: empty pattern with glob=%s -> redirecting to glob tool", args.glob);
      return {
        toolCallId,
        toolName: "glob",
        decodedArgs: JSON.stringify({ pattern: args.glob, path: args.path || undefined }),
        nativeResultType: "grepResult",
        nativeArgs: {
          pattern: args.glob ?? "",
          path: args.path ?? "",
          outputMode: "files_with_matches",
        },
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
      nativeArgs: {
        pattern: pattern || ".",
        path: args.path ?? "",
        outputMode: args.outputMode || "content",
        ...(args.multiline ? { multiline: "true" } : undefined),
        ...(args.headLimit != null ? { headLimit: String(args.headLimit) } : undefined),
      },
    };
  }
  return null;
}

// ── Grep result parsing ──

const VALID_OUTPUT_MODES: ReadonlySet<string> = new Set(["content", "files_with_matches", "count"]);

export interface GrepBuildResult {
  resultCase: "grepResult";
  resultValue: GrepResult;
}

/**
 * Build a GrepResult proto from MCP grep text output.
 * Returns null when the output can't be reliably parsed, signaling
 * the caller to fall back to MCP text. Null reasons:
 * - multiline mode (output isn't line-based ripgrep format)
 * - unknown outputMode (can't choose a parser)
 * - non-empty content that yielded zero parsed items (likely not ripgrep output)
 */
export function buildGrepResult(
  content: string,
  args: Record<string, string>,
): GrepBuildResult | null {
  const pattern = args.pattern ?? "";
  const path = args.path ?? "";
  const outputMode = args.outputMode || "content";

  if (args.multiline === "true") return null;
  if (!VALID_OUTPUT_MODES.has(outputMode)) return null;

  const clientTruncatedHint = args.headLimit != null && args.headLimit !== "";

  let unionResult:
    | { case: "count"; value: GrepCountResult }
    | { case: "files"; value: GrepFilesResult }
    | { case: "content"; value: GrepContentResult };

  if (outputMode === "count") {
    unionResult = buildCountResult(content, clientTruncatedHint);
  } else if (outputMode === "files_with_matches") {
    unionResult = buildFilesResult(content, clientTruncatedHint);
  } else {
    unionResult = buildContentResult(content, clientTruncatedHint);
  }

  // Non-empty MCP output that yielded zero parsed items likely isn't
  // ripgrep-shaped (error text, JSON, wrapper banners). Fall back to MCP text.
  if (content.trim() && isEmptyResult(unionResult)) return null;

  const workspaceResults: { [key: string]: GrepUnionResult } = {};
  workspaceResults[path || "."] = create(GrepUnionResultSchema, {
    result: unionResult,
  });

  return {
    resultCase: "grepResult",
    resultValue: create(GrepResultSchema, {
      result: {
        case: "success",
        value: create(GrepSuccessSchema, {
          pattern,
          path,
          outputMode,
          workspaceResults,
        }),
      },
    }),
  };
}

function isEmptyResult(
  r:
    | { case: "count"; value: GrepCountResult }
    | { case: "files"; value: GrepFilesResult }
    | { case: "content"; value: GrepContentResult },
): boolean {
  switch (r.case) {
    case "count":
      return r.value.counts.length === 0;
    case "files":
      return r.value.files.length === 0;
    case "content":
      return r.value.matches.length === 0;
  }
}

function buildCountResult(
  content: string,
  clientTruncatedHint: boolean,
): { case: "count"; value: GrepCountResult } {
  const counts: GrepFileCount[] = [];
  let totalMatches = 0;
  for (const raw of content.split("\n")) {
    const line = raw.replace(/\r$/, "");
    if (!line) continue;
    const sep = line.lastIndexOf(":");
    if (sep === -1) continue;
    const tail = line.slice(sep + 1);
    if (!/^\d+$/.test(tail)) continue;
    const file = line.slice(0, sep);
    const count = Number.parseInt(tail, 10);
    counts.push(create(GrepFileCountSchema, { file, count }));
    totalMatches += count;
  }
  return {
    case: "count" as const,
    value: create(GrepCountResultSchema, {
      counts,
      totalFiles: counts.length,
      totalMatches,
      clientTruncated: clientTruncatedHint,
      ripgrepTruncated: false,
    }),
  };
}

function buildFilesResult(
  content: string,
  clientTruncatedHint: boolean,
): { case: "files"; value: GrepFilesResult } {
  const files = content
    .split("\n")
    .map((l) => l.replace(/\r$/, "").trim())
    .filter(Boolean);
  return {
    case: "files" as const,
    value: create(GrepFilesResultSchema, {
      files,
      totalFiles: files.length,
      clientTruncated: clientTruncatedHint,
      ripgrepTruncated: false,
    }),
  };
}

function buildContentResult(
  content: string,
  clientTruncatedHint: boolean,
): { case: "content"; value: GrepContentResult } {
  const fileMatches: GrepFileMatch[] = [];
  let currentFile = "";
  let currentMatches: GrepContentMatch[] = [];
  let totalLines = 0;
  let totalMatchedLines = 0;

  const flushFile = () => {
    if (currentFile && currentMatches.length > 0) {
      fileMatches.push(create(GrepFileMatchSchema, { file: currentFile, matches: currentMatches }));
    }
    currentMatches = [];
  };

  for (const raw of content.split("\n")) {
    const line = raw.replace(/\r$/, "");
    if (line === "--" || line === "") continue;

    // Match lines: "file:lineNum:text" -- colon separators.
    // Regex backtracking handles colons in filenames correctly since
    // \d+ requires digits after the separator.
    const matchHit = line.match(/^(.+?):(\d+):(.*)/);
    if (matchHit) {
      const file = matchHit[1]!;
      const lineNum = Number.parseInt(matchHit[2]!, 10);
      const text = matchHit[3]!;
      if (file !== currentFile) {
        flushFile();
        currentFile = file;
      }
      totalLines++;
      totalMatchedLines++;
      currentMatches.push(
        create(GrepContentMatchSchema, {
          lineNumber: lineNum,
          content: text,
          isContextLine: false,
        }),
      );
      continue;
    }

    // Context lines: "file-lineNum-text" -- hyphen separators.
    // Hyphens in filenames are common (e.g. my-2-component.ts), so prefer
    // matching against the known currentFile prefix when available.
    const ctxParsed = parseContextLine(line, currentFile);
    if (ctxParsed) {
      if (ctxParsed.file !== currentFile) {
        flushFile();
        currentFile = ctxParsed.file;
      }
      totalLines++;
      currentMatches.push(
        create(GrepContentMatchSchema, {
          lineNumber: ctxParsed.lineNum,
          content: ctxParsed.text,
          isContextLine: true,
        }),
      );
    }
  }
  flushFile();

  return {
    case: "content" as const,
    value: create(GrepContentResultSchema, {
      matches: fileMatches,
      totalLines,
      totalMatchedLines,
      clientTruncated: clientTruncatedHint,
      ripgrepTruncated: false,
    }),
  };
}

/** Parse a context line using currentFile as a prefix hint to avoid
 *  ambiguity from hyphens in filenames. Falls back to naive regex. */
function parseContextLine(
  line: string,
  currentFile: string,
): { file: string; lineNum: number; text: string } | null {
  if (currentFile) {
    const prefix = `${currentFile}-`;
    if (line.startsWith(prefix)) {
      const m = line.slice(prefix.length).match(/^(\d+)-(.*)/s);
      if (m) {
        return { file: currentFile, lineNum: Number.parseInt(m[1]!, 10), text: m[2]! };
      }
    }
  }
  const m = line.match(/^(.+?)-(\d+)-(.*)/);
  if (m) {
    return { file: m[1]!, lineNum: Number.parseInt(m[2]!, 10), text: m[3]! };
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
    case "grepResult": {
      try {
        const built = buildGrepResult(content, args);
        if (!built) {
          const reason =
            args.multiline === "true"
              ? "multiline"
              : !VALID_OUTPUT_MODES.has(args.outputMode || "content")
                ? "unknown_mode"
                : "unparseable";
          logDebugFmt("sendNativeResult: grepResult -> MCP text fallback (%s)", reason);
          sendMcpResultSuccess(bridge, exec, content);
          return;
        }
        resultValue = built.resultValue;
        resultCase = built.resultCase;
      } catch {
        logDebugFmt("sendNativeResult: grepResult proto build failed -> MCP text fallback");
        sendMcpResultSuccess(bridge, exec, content);
        return;
      }
      break;
    }
    default:
      if (exec.nativeResultType === "lsResult") {
        logDebugFmt("sendNativeResult: lsResult -> MCP text fallback");
      } else {
        logDebugFmt(
          "sendNativeResult: unknown type %s, falling back to MCP",
          exec.nativeResultType,
        );
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
