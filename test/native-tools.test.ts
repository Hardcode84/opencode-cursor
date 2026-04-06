import { describe, expect, test } from "bun:test";
import { fixMcpArgNames, nativeToMcpRedirect } from "../src/native-tools";
import type { ExecServerMessage } from "../src/proto/agent_pb";

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
      mockExec({ pattern: "TODO", path: "/src", toolCallId: "tc-8" }),
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
        toolCallId: "tc-8",
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
      mockExec({ pattern: "", glob: "*.ts", toolCallId: "tc-8" }),
    );
    expect(r!.toolName).toBe("glob");
    expect(r!.nativeResultType).toBe("grepResult");
    expect(JSON.parse(r!.decodedArgs).pattern).toBe("*.ts");
  });

  test("grepArgs: no pattern and no glob → default pattern", () => {
    const r = nativeToMcpRedirect("grepArgs", mockExec({ toolCallId: "tc-8" }));
    expect(r!.toolName).toBe("grep");
    expect(JSON.parse(r!.decodedArgs).pattern).toBe(".");
  });

  test("unknown exec type → null", () => {
    expect(nativeToMcpRedirect("unknownArgs", mockExec({ toolCallId: "tc-9" }))).toBeNull();
  });
});
