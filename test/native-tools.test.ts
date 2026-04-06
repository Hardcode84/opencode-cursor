import { describe, expect, test } from "bun:test";
import { buildGrepResult, fixMcpArgNames, nativeToMcpRedirect } from "../src/native-tools";
import type {
  ExecServerMessage,
  GrepContentResult,
  GrepCountResult,
  GrepFilesResult,
  GrepResult,
  GrepSuccess,
} from "../src/proto/agent_pb";

/** Minimal mock matching the fields nativeToMcpRedirect actually reads. */
function mockExec(value: Record<string, unknown>, id = 1, execId = "exec-1"): ExecServerMessage {
  return { message: { value }, id, execId } as unknown as ExecServerMessage;
}

describe("fixMcpArgNames", () => {
  test("read: remaps path → filePath", () => {
    const args: Record<string, unknown> = { path: "/foo.txt" };
    fixMcpArgNames("read", args);
    expect(args.filePath).toBe("/foo.txt");
    expect(args.path).toBeUndefined();
  });

  test("read: no-op when filePath already present", () => {
    const args: Record<string, unknown> = { filePath: "/a.txt", path: "/b.txt" };
    fixMcpArgNames("read", args);
    expect(args.filePath).toBe("/a.txt");
  });

  test("write: remaps path → filePath", () => {
    const args: Record<string, unknown> = { path: "/out.txt" };
    fixMcpArgNames("write", args);
    expect(args.filePath).toBe("/out.txt");
    expect(args.path).toBeUndefined();
  });

  test("write: remaps file_content → content", () => {
    const args: Record<string, unknown> = { file_content: "hello" };
    fixMcpArgNames("write", args);
    expect(args.content).toBe("hello");
    expect(args.file_content).toBeUndefined();
  });

  test("edit: remaps path → filePath", () => {
    const args: Record<string, unknown> = { path: "/f.ts" };
    fixMcpArgNames("edit", args);
    expect(args.filePath).toBe("/f.ts");
    expect(args.path).toBeUndefined();
  });

  test("glob: remaps glob_pattern → pattern", () => {
    const args: Record<string, unknown> = { glob_pattern: "*.ts" };
    fixMcpArgNames("glob", args);
    expect(args.pattern).toBe("*.ts");
    expect(args.glob_pattern).toBeUndefined();
  });

  test("glob: remaps target_directory → path", () => {
    const args: Record<string, unknown> = { target_directory: "/src" };
    fixMcpArgNames("glob", args);
    expect(args.path).toBe("/src");
    expect(args.target_directory).toBeUndefined();
  });

  test("grep: sets default pattern when missing", () => {
    const args: Record<string, unknown> = {};
    fixMcpArgNames("grep", args);
    expect(args.pattern).toBe(".");
  });

  test("grep: preserves existing pattern", () => {
    const args: Record<string, unknown> = { pattern: "TODO" };
    fixMcpArgNames("grep", args);
    expect(args.pattern).toBe("TODO");
  });

  test("unknown tool: no mutation", () => {
    const args: Record<string, unknown> = { path: "/foo", custom: 42 };
    fixMcpArgNames("unknown_tool", args);
    expect(args.path).toBe("/foo");
    expect(args.custom).toBe(42);
  });
});

describe("nativeToMcpRedirect", () => {
  test("readArgs → read tool", () => {
    const r = nativeToMcpRedirect("readArgs", mockExec({ path: "/f.txt", toolCallId: "tc-1" }));
    expect(r).not.toBeNull();
    expect(r!.toolName).toBe("read");
    expect(r!.toolCallId).toBe("tc-1");
    expect(r!.nativeResultType).toBe("readResult");
    expect(JSON.parse(r!.decodedArgs).filePath).toBe("/f.txt");
  });

  test("readArgs includes offset and limit when non-zero", () => {
    const r = nativeToMcpRedirect(
      "readArgs",
      mockExec({ path: "/f.txt", offset: 10, limit: 50, toolCallId: "tc-1" }),
    );
    const args = JSON.parse(r!.decodedArgs);
    expect(args.offset).toBe(10);
    expect(args.limit).toBe(50);
  });

  test("readArgs omits offset/limit when zero", () => {
    const r = nativeToMcpRedirect(
      "readArgs",
      mockExec({ path: "/f.txt", offset: 0, limit: 0, toolCallId: "tc-1" }),
    );
    const args = JSON.parse(r!.decodedArgs);
    expect(args.offset).toBeUndefined();
    expect(args.limit).toBeUndefined();
  });

  test("writeArgs with fileText", () => {
    const r = nativeToMcpRedirect(
      "writeArgs",
      mockExec({
        path: "/o.txt",
        fileText: "hello",
        fileBytes: new Uint8Array(0),
        toolCallId: "tc-2",
      }),
    );
    expect(r!.toolName).toBe("write");
    expect(r!.nativeResultType).toBe("writeResult");
    const args = JSON.parse(r!.decodedArgs);
    expect(args.filePath).toBe("/o.txt");
    expect(args.content).toBe("hello");
  });

  test("writeArgs with fileBytes", () => {
    const bytes = new TextEncoder().encode("byte content");
    const r = nativeToMcpRedirect(
      "writeArgs",
      mockExec({ path: "/o.txt", fileBytes: bytes, toolCallId: "tc-2" }),
    );
    expect(JSON.parse(r!.decodedArgs).content).toBe("byte content");
  });

  test("deleteArgs → bash rm", () => {
    const r = nativeToMcpRedirect(
      "deleteArgs",
      mockExec({ path: "/tmp/f.txt", toolCallId: "tc-3" }),
    );
    expect(r!.toolName).toBe("bash");
    expect(r!.nativeResultType).toBe("deleteResult");
    const args = JSON.parse(r!.decodedArgs);
    expect(args.command).toContain("rm -f");
    expect(args.command).toContain("/tmp/f.txt");
  });

  test("deleteArgs: path with single quotes is escaped", () => {
    const r = nativeToMcpRedirect(
      "deleteArgs",
      mockExec({ path: "/tmp/it's a file.txt", toolCallId: "tc-3" }),
    );
    const cmd = JSON.parse(r!.decodedArgs).command as string;
    expect(cmd).not.toContain("it's a");
    expect(cmd).toContain("it");
  });

  test("deleteArgs: empty path → no-op bash true", () => {
    const r = nativeToMcpRedirect("deleteArgs", mockExec({ path: "", toolCallId: "tc-3" }));
    expect(r).not.toBeNull();
    expect(r!.toolName).toBe("bash");
    expect(JSON.parse(r!.decodedArgs).command).toBe("true");
    expect(r!.nativeResultType).toBe("deleteResult");
  });

  test("deleteArgs: null path → no-op bash true", () => {
    const r = nativeToMcpRedirect("deleteArgs", mockExec({ toolCallId: "tc-3" }));
    expect(r).not.toBeNull();
    expect(r!.toolName).toBe("bash");
    expect(JSON.parse(r!.decodedArgs).command).toBe("true");
  });

  test("fetchArgs → web_fetch", () => {
    const r = nativeToMcpRedirect(
      "fetchArgs",
      mockExec({ url: "https://example.com", toolCallId: "tc-4" }),
    );
    expect(r!.toolName).toBe("web_fetch");
    expect(r!.nativeResultType).toBe("fetchResult");
    expect(JSON.parse(r!.decodedArgs).url).toBe("https://example.com");
  });

  test("shellArgs → bash with shellResult", () => {
    const r = nativeToMcpRedirect(
      "shellArgs",
      mockExec({ command: "ls -la", description: "list files", toolCallId: "tc-5" }),
    );
    expect(r!.toolName).toBe("bash");
    expect(r!.nativeResultType).toBe("shellResult");
    expect(JSON.parse(r!.decodedArgs).command).toBe("ls -la");
  });

  test("shellStreamArgs → bash with shellStreamResult", () => {
    const r = nativeToMcpRedirect(
      "shellStreamArgs",
      mockExec({ command: "cat f", toolCallId: "tc-6" }),
    );
    expect(r!.toolName).toBe("bash");
    expect(r!.nativeResultType).toBe("shellStreamResult");
  });

  test("shellArgs with cwd and timeout", () => {
    const r = nativeToMcpRedirect(
      "shellArgs",
      mockExec({
        command: "make",
        workingDirectory: "/proj",
        timeout: 30000,
        toolCallId: "tc-5",
      }),
    );
    const args = JSON.parse(r!.decodedArgs);
    expect(args.working_directory).toBe("/proj");
    expect(args.timeout).toBe(30000);
  });

  test("lsArgs → glob", () => {
    const r = nativeToMcpRedirect("lsArgs", mockExec({ path: "/src", toolCallId: "tc-7" }));
    expect(r!.toolName).toBe("glob");
    expect(r!.nativeResultType).toBe("lsResult");
    const args = JSON.parse(r!.decodedArgs);
    expect(args.pattern).toBe("*");
    expect(args.path).toBe("/src");
  });

  test("grepArgs → grep", () => {
    const r = nativeToMcpRedirect(
      "grepArgs",
      mockExec({ pattern: "TODO", path: "/src", toolCallId: "tc-grep-1" }),
    );
    expect(r!.toolName).toBe("grep");
    expect(r!.nativeResultType).toBe("grepResult");
    const args = JSON.parse(r!.decodedArgs);
    expect(args.pattern).toBe("TODO");
    expect(args.path).toBe("/src");
  });

  test("grepArgs forwards all optional fields", () => {
    const r = nativeToMcpRedirect(
      "grepArgs",
      mockExec({
        pattern: "err",
        glob: "*.log",
        outputMode: "files_with_matches",
        contextBefore: 2,
        contextAfter: 3,
        caseInsensitive: true,
        type: "ts",
        headLimit: 50,
        multiline: true,
        toolCallId: "tc-grep-2",
      }),
    );
    const args = JSON.parse(r!.decodedArgs);
    expect(args.glob).toBe("*.log");
    expect(args.output_mode).toBe("files_with_matches");
    expect(args["-B"]).toBe(2);
    expect(args["-A"]).toBe(3);
    expect(args["-i"]).toBe(true);
    expect(args.type).toBe("ts");
    expect(args.head_limit).toBe(50);
    expect(args.multiline).toBe(true);
  });

  test("grepArgs: empty pattern with glob → redirects to glob tool", () => {
    const r = nativeToMcpRedirect(
      "grepArgs",
      mockExec({ pattern: "", glob: "*.ts", toolCallId: "tc-grep-3" }),
    );
    expect(r!.toolName).toBe("glob");
    expect(r!.nativeResultType).toBe("grepResult");
    expect(JSON.parse(r!.decodedArgs).pattern).toBe("*.ts");
  });

  test("grepArgs: no pattern and no glob → default pattern", () => {
    const r = nativeToMcpRedirect("grepArgs", mockExec({ toolCallId: "tc-grep-4" }));
    expect(r!.toolName).toBe("grep");
    expect(JSON.parse(r!.decodedArgs).pattern).toBe(".");
  });

  test("unknown exec type → null", () => {
    expect(nativeToMcpRedirect("unknownArgs", mockExec({ toolCallId: "tc-9" }))).toBeNull();
  });

  test("grepArgs stores pattern/path/outputMode in nativeArgs", () => {
    const r = nativeToMcpRedirect(
      "grepArgs",
      mockExec({ pattern: "TODO", path: "/src", outputMode: "count", toolCallId: "tc-grep-5" }),
    );
    expect(r!.nativeArgs!.pattern).toBe("TODO");
    expect(r!.nativeArgs!.path).toBe("/src");
    expect(r!.nativeArgs!.outputMode).toBe("count");
  });

  test("grepArgs: multiline and headLimit stored in nativeArgs", () => {
    const r = nativeToMcpRedirect(
      "grepArgs",
      mockExec({
        pattern: "x",
        multiline: true,
        headLimit: 50,
        toolCallId: "tc-grep-6",
      }),
    );
    expect(r!.nativeArgs!.multiline).toBe("true");
    expect(r!.nativeArgs!.headLimit).toBe("50");
  });

  test("grepArgs: nativeArgs omits multiline/headLimit when not set", () => {
    const r = nativeToMcpRedirect("grepArgs", mockExec({ pattern: "x", toolCallId: "tc-grep-7" }));
    expect(r!.nativeArgs!.multiline).toBeUndefined();
    expect(r!.nativeArgs!.headLimit).toBeUndefined();
  });

  test("grepArgs: glob redirect stores glob pattern in nativeArgs", () => {
    const r = nativeToMcpRedirect(
      "grepArgs",
      mockExec({ pattern: "", glob: "*.ts", path: "/proj", toolCallId: "tc-grep-8" }),
    );
    expect(r!.nativeArgs).toEqual({
      pattern: "*.ts",
      path: "/proj",
      outputMode: "files_with_matches",
    });
  });
});

// ── buildGrepResult ──

function grepSuccess(r: ReturnType<typeof buildGrepResult>): GrepSuccess {
  expect(r).not.toBeNull();
  const result = r!.resultValue as GrepResult;
  expect(result.result.case).toBe("success");
  return result.result.value as GrepSuccess;
}

describe("buildGrepResult: content mode", () => {
  const args = { pattern: "TODO", path: "/src", outputMode: "content" };

  test("parses ripgrep content output with file:line:text format", () => {
    const content = [
      "src/a.ts:10:// TODO: fix this",
      "src/a.ts:11:// TODO: and this",
      "src/b.ts:5:  TODO item",
    ].join("\n");

    const r = buildGrepResult(content, args);
    expect(r).not.toBeNull();
    expect(r!.resultCase).toBe("grepResult");
    const success = grepSuccess(r);
    expect(success.pattern).toBe("TODO");
    expect(success.path).toBe("/src");
    expect(success.outputMode).toBe("content");

    const ws = success.workspaceResults["/src"];
    expect(ws).toBeDefined();
    expect(ws!.result.case).toBe("content");
    const contentResult = ws!.result.value as GrepContentResult;
    expect(contentResult.matches).toHaveLength(2);
    expect(contentResult.matches[0]!.file).toBe("src/a.ts");
    expect(contentResult.matches[0]!.matches).toHaveLength(2);
    expect(contentResult.matches[0]!.matches[0]!.lineNumber).toBe(10);
    expect(contentResult.matches[0]!.matches[0]!.content).toBe("// TODO: fix this");
    expect(contentResult.matches[0]!.matches[0]!.isContextLine).toBe(false);
    expect(contentResult.matches[1]!.file).toBe("src/b.ts");
    expect(contentResult.totalMatchedLines).toBe(3);
    expect(contentResult.totalLines).toBe(3);
  });

  test("parses context lines (file-line-text format)", () => {
    const content = [
      "src/a.ts-9-  const x = 1;",
      "src/a.ts:10:// TODO: fix",
      "src/a.ts-11-  return x;",
    ].join("\n");

    const r = buildGrepResult(content, args);
    const ws = grepSuccess(r).workspaceResults["/src"];
    const contentResult = ws!.result.value as GrepContentResult;
    expect(contentResult.matches[0]!.matches).toHaveLength(3);
    expect(contentResult.matches[0]!.matches[0]!.isContextLine).toBe(true);
    expect(contentResult.matches[0]!.matches[0]!.lineNumber).toBe(9);
    expect(contentResult.matches[0]!.matches[1]!.isContextLine).toBe(false);
    expect(contentResult.matches[0]!.matches[2]!.isContextLine).toBe(true);
  });

  test("context lines with hyphens in filename use currentFile prefix", () => {
    const content = [
      "src/my-2-component.ts:10:export function foo() {",
      "src/my-2-component.ts-11-  return bar;",
      "src/my-2-component.ts:12:export function baz() {",
    ].join("\n");

    const r = buildGrepResult(content, args);
    const ws = grepSuccess(r).workspaceResults["/src"];
    const contentResult = ws!.result.value as GrepContentResult;
    expect(contentResult.matches[0]!.file).toBe("src/my-2-component.ts");
    expect(contentResult.matches[0]!.matches).toHaveLength(3);
    expect(contentResult.matches[0]!.matches[1]!.isContextLine).toBe(true);
    expect(contentResult.matches[0]!.matches[1]!.lineNumber).toBe(11);
    expect(contentResult.matches[0]!.matches[1]!.content).toBe("  return bar;");
  });

  test("match lines with colons in filename parse correctly", () => {
    const content = "src/foo:bar.ts:10:match text";
    const r = buildGrepResult(content, args);
    const ws = grepSuccess(r).workspaceResults["/src"];
    const contentResult = ws!.result.value as GrepContentResult;
    expect(contentResult.matches[0]!.file).toBe("src/foo:bar.ts");
    expect(contentResult.matches[0]!.matches[0]!.lineNumber).toBe(10);
    expect(contentResult.matches[0]!.matches[0]!.content).toBe("match text");
  });

  test("skips separator lines and empty lines", () => {
    const content = "src/a.ts:1:line1\n--\nsrc/a.ts:5:line5\n\n";
    const r = buildGrepResult(content, args);
    const ws = grepSuccess(r).workspaceResults["/src"];
    const contentResult = ws!.result.value as GrepContentResult;
    expect(contentResult.matches[0]!.matches).toHaveLength(2);
  });

  test("handles empty content", () => {
    const r = buildGrepResult("", args);
    const ws = grepSuccess(r).workspaceResults["/src"];
    const contentResult = ws!.result.value as GrepContentResult;
    expect(contentResult.matches).toHaveLength(0);
    expect(contentResult.totalLines).toBe(0);
  });

  test("strips trailing CR from lines", () => {
    const content = "src/a.ts:10:match\r\nsrc/a.ts:11:other\r\n";
    const r = buildGrepResult(content, args);
    const ws = grepSuccess(r).workspaceResults["/src"];
    const contentResult = ws!.result.value as GrepContentResult;
    expect(contentResult.matches[0]!.matches[0]!.content).toBe("match");
    expect(contentResult.matches[0]!.matches[1]!.content).toBe("other");
  });

  test("headLimit sets clientTruncated hint", () => {
    const r = buildGrepResult("src/a.ts:1:hit", { ...args, headLimit: "50" });
    const ws = grepSuccess(r).workspaceResults["/src"];
    const contentResult = ws!.result.value as GrepContentResult;
    expect(contentResult.clientTruncated).toBe(true);
  });
});

describe("buildGrepResult: files_with_matches mode", () => {
  const args = { pattern: "TODO", path: "/proj", outputMode: "files_with_matches" };

  test("parses file list", () => {
    const content = "src/a.ts\nsrc/b.ts\nlib/c.js\n";
    const r = buildGrepResult(content, args);
    const ws = grepSuccess(r).workspaceResults["/proj"];
    expect(ws!.result.case).toBe("files");
    const filesResult = ws!.result.value as GrepFilesResult;
    expect(filesResult.files).toEqual(["src/a.ts", "src/b.ts", "lib/c.js"]);
    expect(filesResult.totalFiles).toBe(3);
  });

  test("handles empty result", () => {
    const r = buildGrepResult("", args);
    const ws = grepSuccess(r).workspaceResults["/proj"];
    const filesResult = ws!.result.value as GrepFilesResult;
    expect(filesResult.files).toHaveLength(0);
    expect(filesResult.totalFiles).toBe(0);
  });

  test("strips CRLF and whitespace", () => {
    const content = "src/a.ts\r\n  src/b.ts  \r\n";
    const r = buildGrepResult(content, args);
    const ws = grepSuccess(r).workspaceResults["/proj"];
    const filesResult = ws!.result.value as GrepFilesResult;
    expect(filesResult.files).toEqual(["src/a.ts", "src/b.ts"]);
  });
});

describe("buildGrepResult: count mode", () => {
  const args = { pattern: "TODO", path: ".", outputMode: "count" };

  test("parses file:count lines", () => {
    const content = "src/a.ts:5\nsrc/b.ts:12\nlib/c.js:1\n";
    const r = buildGrepResult(content, args);
    const ws = grepSuccess(r).workspaceResults["."];
    expect(ws!.result.case).toBe("count");
    const countResult = ws!.result.value as GrepCountResult;
    expect(countResult.counts).toHaveLength(3);
    expect(countResult.counts[0]!.file).toBe("src/a.ts");
    expect(countResult.counts[0]!.count).toBe(5);
    expect(countResult.counts[2]!.count).toBe(1);
    expect(countResult.totalFiles).toBe(3);
    expect(countResult.totalMatches).toBe(18);
  });

  test("skips lines with non-integer tail", () => {
    const content = "src/a.ts:5\ngarbage line\nsrc/b.ts:abc\nsrc/c.ts:3\n";
    const r = buildGrepResult(content, args);
    const ws = grepSuccess(r).workspaceResults["."];
    const countResult = ws!.result.value as GrepCountResult;
    expect(countResult.counts).toHaveLength(2);
    expect(countResult.counts[0]!.file).toBe("src/a.ts");
    expect(countResult.counts[1]!.file).toBe("src/c.ts");
    expect(countResult.totalMatches).toBe(8);
  });

  test("handles empty result", () => {
    const r = buildGrepResult("", args);
    const ws = grepSuccess(r).workspaceResults["."];
    const countResult = ws!.result.value as GrepCountResult;
    expect(countResult.counts).toHaveLength(0);
    expect(countResult.totalMatches).toBe(0);
  });

  test("handles file with colons in name", () => {
    const content = "src/foo:bar.ts:7\n";
    const r = buildGrepResult(content, args);
    const ws = grepSuccess(r).workspaceResults["."];
    const countResult = ws!.result.value as GrepCountResult;
    expect(countResult.counts[0]!.file).toBe("src/foo:bar.ts");
    expect(countResult.counts[0]!.count).toBe(7);
  });
});

describe("buildGrepResult: fallback cases", () => {
  test("returns null for multiline mode", () => {
    const r = buildGrepResult("some output", {
      pattern: "x",
      path: "/src",
      outputMode: "content",
      multiline: "true",
    });
    expect(r).toBeNull();
  });

  test("returns null for unknown outputMode", () => {
    const r = buildGrepResult("some output", {
      pattern: "x",
      path: "/src",
      outputMode: "json",
    });
    expect(r).toBeNull();
  });

  test("returns null when content is non-empty but nothing parsed (content mode)", () => {
    const r = buildGrepResult("error: something went wrong\nstderr output", {
      pattern: "x",
      path: "/src",
      outputMode: "content",
    });
    expect(r).toBeNull();
  });

  test("returns null when content is non-empty but nothing parsed (count mode)", () => {
    const r = buildGrepResult("some garbage output", {
      pattern: "x",
      path: ".",
      outputMode: "count",
    });
    expect(r).toBeNull();
  });

  test("empty content returns empty success (not null)", () => {
    const r = buildGrepResult("", { pattern: "x", path: "/src", outputMode: "content" });
    expect(r).not.toBeNull();
    const ws = grepSuccess(r).workspaceResults["/src"];
    const contentResult = ws!.result.value as GrepContentResult;
    expect(contentResult.matches).toHaveLength(0);
  });

  test("uses '.' when path is empty", () => {
    const r = buildGrepResult("", { pattern: "x", path: "", outputMode: "content" });
    expect(grepSuccess(r).workspaceResults["."]).toBeDefined();
  });
});
