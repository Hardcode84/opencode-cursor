import { describe, expect, test } from "bun:test";
import {
  type OpenAIMessage,
  type OpenAIToolDef,
  parseMessages,
  selectToolsForChoice,
  textContent,
} from "../src/openai-messages";

/** Build an OpenAIMessage with sensible defaults so tests only specify what matters. */
function msg(overrides: Partial<OpenAIMessage> & Pick<OpenAIMessage, "role">): OpenAIMessage {
  return { content: null, ...overrides };
}

describe("textContent", () => {
  test("returns string content as-is", () => {
    expect(textContent("hello")).toBe("hello");
  });

  test("returns empty string for null", () => {
    expect(textContent(null)).toBe("");
  });

  test("joins array of text parts", () => {
    const parts = [
      { type: "text", text: "line 1" },
      { type: "text", text: "line 2" },
    ];
    expect(textContent(parts)).toBe("line 1\nline 2");
  });

  test("filters out non-text parts", () => {
    const parts = [
      { type: "text", text: "keep" },
      { type: "image_url" },
      { type: "text", text: "also keep" },
    ];
    expect(textContent(parts)).toBe("keep\nalso keep");
  });

  test("returns empty string for empty array", () => {
    expect(textContent([])).toBe("");
  });

  test("drops text parts with empty string", () => {
    const parts = [
      { type: "text", text: "" },
      { type: "text", text: "a" },
    ];
    expect(textContent(parts)).toBe("a");
  });
});

describe("parseMessages", () => {
  test("extracts system prompt", () => {
    const result = parseMessages([
      msg({ role: "system", content: "You are a pirate." }),
      msg({ role: "user", content: "Hello" }),
    ]);
    expect(result.systemPrompt).toBe("You are a pirate.");
    expect(result.userText).toBe("Hello");
  });

  test("joins multiple system messages", () => {
    const result = parseMessages([
      msg({ role: "system", content: "Rule 1" }),
      msg({ role: "system", content: "Rule 2" }),
      msg({ role: "user", content: "Go" }),
    ]);
    expect(result.systemPrompt).toBe("Rule 1\nRule 2");
  });

  test("default system prompt when none provided", () => {
    const result = parseMessages([msg({ role: "user", content: "Hi" })]);
    expect(result.systemPrompt).toBe("You are a helpful assistant.");
  });

  test("multi-turn conversation", () => {
    const result = parseMessages([
      msg({ role: "user", content: "Q1" }),
      msg({ role: "assistant", content: "A1" }),
      msg({ role: "user", content: "Q2" }),
      msg({ role: "assistant", content: "A2" }),
      msg({ role: "user", content: "Q3" }),
    ]);
    expect(result.turns).toHaveLength(2);
    expect(result.turns[0]).toEqual({ userText: "Q1", assistantText: "A1" });
    expect(result.turns[1]).toEqual({ userText: "Q2", assistantText: "A2" });
    expect(result.userText).toBe("Q3");
  });

  test("tool result resume: extracts results and keeps userText", () => {
    const result = parseMessages([
      msg({ role: "user", content: "Do stuff" }),
      msg({
        role: "assistant",
        content: null,
        tool_calls: [
          { id: "tc-1", type: "function", function: { name: "read", arguments: '{"path":"/f"}' } },
        ],
      }),
      msg({ role: "tool", content: "file contents", tool_call_id: "tc-1" }),
    ]);
    expect(result.toolResults).toHaveLength(1);
    expect(result.toolResults[0]!.toolCallId).toBe("tc-1");
    expect(result.toolResults[0]!.content).toBe("file contents");
    expect(result.userText).toBe("Do stuff");
    // No turns closed because the assistant message had tool_calls (awaiting results)
    expect(result.turns).toHaveLength(0);
  });

  test("tool round-trip inlined in history when followed by new user message", () => {
    const result = parseMessages([
      msg({ role: "user", content: "Q1" }),
      msg({
        role: "assistant",
        content: "Let me check.",
        tool_calls: [
          { id: "tc-1", type: "function", function: { name: "read", arguments: '{"path":"/f"}' } },
        ],
      }),
      msg({ role: "tool", content: "file data", tool_call_id: "tc-1" }),
      msg({ role: "user", content: "Q2" }),
    ]);
    expect(result.userText).toBe("Q2");
    expect(result.turns).toHaveLength(1);
    expect(result.turns[0]!.assistantText).toContain("Let me check.");
    expect(result.turns[0]!.assistantText).toContain("[Tool read");
    expect(result.turns[0]!.assistantText).toContain("file data");
    expect(result.toolResults).toHaveLength(1);
  });

  test("malformed assistant tool calls do not crash parsing", () => {
    const result = parseMessages([
      msg({ role: "user", content: "Q1" }),
      msg({
        role: "assistant",
        content: "Calling something.",
        tool_calls: [{ id: "tc-bad", type: "function" } as any],
      }),
      msg({ role: "tool", content: "tool output", tool_call_id: "tc-bad" }),
      msg({ role: "user", content: "Q2" }),
    ]);

    expect(result.userText).toBe("Q2");
    expect(result.turns).toHaveLength(1);
    expect(result.turns[0]!.assistantText).toContain("[Tool unknown()]");
    expect(result.turns[0]!.assistantText).toContain("tool output");
  });

  test("single user message with no history", () => {
    const result = parseMessages([msg({ role: "user", content: "Just one question" })]);
    expect(result.userText).toBe("Just one question");
    expect(result.turns).toHaveLength(0);
    expect(result.toolResults).toHaveLength(0);
  });

  test("empty messages array", () => {
    const result = parseMessages([]);
    expect(result.userText).toBe("");
    expect(result.turns).toHaveLength(0);
    expect(result.systemPrompt).toBe("You are a helpful assistant.");
  });
});

describe("selectToolsForChoice", () => {
  const tools: OpenAIToolDef[] = [
    { type: "function", function: { name: "read", description: "Read a file" } },
    { type: "function", function: { name: "write", description: "Write a file" } },
    { type: "function", function: { name: "bash", description: "Run command" } },
  ];

  test("auto → all tools", () => {
    expect(selectToolsForChoice(tools, "auto")).toEqual(tools);
  });

  test("required → all tools", () => {
    expect(selectToolsForChoice(tools, "required")).toEqual(tools);
  });

  test("undefined → all tools", () => {
    expect(selectToolsForChoice(tools, undefined)).toEqual(tools);
  });

  test("null → all tools", () => {
    expect(selectToolsForChoice(tools, null)).toEqual(tools);
  });

  test("none → empty", () => {
    expect(selectToolsForChoice(tools, "none")).toEqual([]);
  });

  test("specific function → filtered to one", () => {
    const result = selectToolsForChoice(tools, {
      type: "function",
      function: { name: "write" },
    });
    expect(result).toHaveLength(1);
    expect(result[0]!.function.name).toBe("write");
  });

  test("specific function not found → empty", () => {
    const result = selectToolsForChoice(tools, {
      type: "function",
      function: { name: "nonexistent" },
    });
    expect(result).toHaveLength(0);
  });

  test("empty tools array → empty regardless of choice", () => {
    expect(selectToolsForChoice([], "auto")).toEqual([]);
    expect(selectToolsForChoice([], "required")).toEqual([]);
  });

  test("unknown string choice → all tools (fallthrough)", () => {
    expect(selectToolsForChoice(tools, "something_else")).toEqual(tools);
  });

  test("malformed object: missing function.name → all tools (fallthrough)", () => {
    expect(selectToolsForChoice(tools, { type: "function" })).toEqual(tools);
    expect(selectToolsForChoice(tools, { type: "function", function: {} })).toEqual(tools);
  });

  test("malformed object: wrong type field → all tools (fallthrough)", () => {
    expect(selectToolsForChoice(tools, { type: "tool", function: { name: "read" } })).toEqual(
      tools,
    );
  });

  test("numeric choice → all tools (fallthrough)", () => {
    expect(selectToolsForChoice(tools, 42)).toEqual(tools);
  });
});
