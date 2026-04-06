import { describe, expect, test } from "bun:test";
import { createThinkingTagFilter } from "../src/thinking-filter";

describe("createThinkingTagFilter", () => {
  test("passes through text without tags", () => {
    const filter = createThinkingTagFilter();
    const result = filter.process("hello world");
    expect(result.content).toBe("hello world");
    expect(result.reasoning).toBe("");
  });

  test("strips <think> tags, routes inner to reasoning", () => {
    const filter = createThinkingTagFilter();
    const result = filter.process("before<think>inner</think>after");
    expect(result.content).toBe("beforeafter");
    expect(result.reasoning).toBe("inner");
  });

  test.each([
    "thinking",
    "reasoning",
    "thought",
    "think_intent",
  ] as const)("handles <%s> variant", (tag) => {
    const filter = createThinkingTagFilter();
    const result = filter.process(`a<${tag}>b</${tag}>c`);
    expect(result.content).toBe("ac");
    expect(result.reasoning).toBe("b");
  });

  test("case insensitive", () => {
    const filter = createThinkingTagFilter();
    const result = filter.process("a<THINK>b</THINK>c<Thinking>d</Thinking>e");
    expect(result.content).toBe("ace");
    expect(result.reasoning).toBe("bd");
  });

  test("multiple thinking blocks in one chunk", () => {
    const filter = createThinkingTagFilter();
    const result = filter.process("a<think>1</think>b<think>2</think>c");
    expect(result.content).toBe("abc");
    expect(result.reasoning).toBe("12");
  });

  test("buffers partial opening tag across chunks", () => {
    const filter = createThinkingTagFilter();
    const r1 = filter.process("hello<thin");
    expect(r1.content).toBe("hello");
    expect(r1.reasoning).toBe("");

    const r2 = filter.process("k>secret</think>world");
    expect(r2.content).toBe("world");
    expect(r2.reasoning).toBe("secret");
  });

  test("buffers partial closing tag across chunks", () => {
    const filter = createThinkingTagFilter();
    const r1 = filter.process("<think>start");
    expect(r1.reasoning).toBe("start");

    const r2 = filter.process("mid</thi");
    expect(r2.reasoning).toBe("mid");

    const r3 = filter.process("nk>end");
    expect(r3.content).toBe("end");
    expect(r3.reasoning).toBe("");
  });

  test("flush emits buffered content (outside thinking)", () => {
    const filter = createThinkingTagFilter();
    filter.process("text<thi");
    const flushed = filter.flush();
    expect(flushed.content).toBe("<thi");
    expect(flushed.reasoning).toBe("");
  });

  test("flush emits buffered reasoning (inside thinking)", () => {
    const filter = createThinkingTagFilter();
    filter.process("<think>partial");
    filter.process(" thought</thi");
    const flushed = filter.flush();
    expect(flushed.reasoning).toBe("</thi");
    expect(flushed.content).toBe("");
  });

  test("flush on clean state returns empty", () => {
    const filter = createThinkingTagFilter();
    filter.process("clean text");
    const flushed = filter.flush();
    expect(flushed.content).toBe("");
    expect(flushed.reasoning).toBe("");
  });

  test("empty input", () => {
    const filter = createThinkingTagFilter();
    const result = filter.process("");
    expect(result.content).toBe("");
    expect(result.reasoning).toBe("");
  });

  test("streaming character-by-character", () => {
    const filter = createThinkingTagFilter();
    const input = "ok<think>hmm</think>done";
    let content = "";
    let reasoning = "";
    for (const ch of input) {
      const result = filter.process(ch);
      content += result.content;
      reasoning += result.reasoning;
    }
    const flushed = filter.flush();
    content += flushed.content;
    reasoning += flushed.reasoning;
    expect(content).toBe("okdone");
    expect(reasoning).toBe("hmm");
  });

  test("non-thinking angle brackets pass through", () => {
    const filter = createThinkingTagFilter();
    const result = filter.process("a < b > c <div>html</div>");
    expect(result.content).toBe("a < b > c <div>html</div>");
    expect(result.reasoning).toBe("");
  });
});
