import { describe, expect, test } from "bun:test";
import { collectNonStreamingResponse, pumpSession, type SSECtx } from "../src/openai-stream";

type FakeEvent =
  | { type: "text"; text: string; isThinking: boolean }
  | { type: "usage"; outputTokens: number; totalTokens: number }
  | {
      type: "done";
      error?: string;
      retryHint?: "blob_not_found" | "resource_exhausted" | "timeout";
    };

class FakeSession {
  private index = 0;
  closed = false;

  constructor(
    private readonly events: FakeEvent[],
    readonly outputTokens: number,
    readonly totalTokens: number,
  ) {}

  async next(): Promise<FakeEvent> {
    return this.events[this.index++]!;
  }

  close(): void {
    this.closed = true;
  }
}

function createCtx() {
  const chunks: Array<{ delta: Record<string, unknown>; finishReason: string | null | undefined }> =
    [];
  const usages: Array<{
    prompt_tokens: number;
    completion_tokens: number;
    total_tokens: number;
  }> = [];
  let closed = false;
  let doneCount = 0;

  const ctx: SSECtx = {
    sendChunk(delta, finishReason) {
      chunks.push({ delta, finishReason });
    },
    sendUsage(usage) {
      usages.push(usage);
    },
    sendDone() {
      closed = true;
      doneCount++;
    },
    close() {
      closed = true;
    },
    get closed() {
      return closed;
    },
  };

  return {
    ctx,
    chunks,
    usages,
    get doneCount() {
      return doneCount;
    },
  };
}

describe("pumpSession usage", () => {
  test("derives prompt tokens from total tokens", async () => {
    const session = new FakeSession(
      [{ type: "usage", outputTokens: 12, totalTokens: 40 }, { type: "done" }],
      12,
      40,
    );
    const recorder = createCtx();

    const result = await pumpSession(session as any, recorder.ctx);

    expect(result).toEqual({ outcome: "done" });
    expect(recorder.usages).toEqual([
      { prompt_tokens: 28, completion_tokens: 12, total_tokens: 40 },
    ]);
    expect(recorder.doneCount).toBe(1);
  });

  test("emits an improved usage snapshot at completion", async () => {
    const session = new FakeSession(
      [{ type: "usage", outputTokens: 10, totalTokens: 30 }, { type: "done" }],
      15,
      45,
    );
    const recorder = createCtx();

    const result = await pumpSession(session as any, recorder.ctx);

    expect(result).toEqual({ outcome: "done" });
    expect(recorder.usages).toEqual([
      { prompt_tokens: 20, completion_tokens: 10, total_tokens: 30 },
      { prompt_tokens: 30, completion_tokens: 15, total_tokens: 45 },
    ]);
  });

  test("falls back to session counters when no usage event was emitted", async () => {
    const session = new FakeSession(
      [{ type: "text", text: "hello", isThinking: false }, { type: "done" }],
      5,
      17,
    );
    const recorder = createCtx();

    const result = await pumpSession(session as any, recorder.ctx);

    expect(result).toEqual({ outcome: "done" });
    expect(recorder.usages).toEqual([
      { prompt_tokens: 12, completion_tokens: 5, total_tokens: 17 },
    ]);
  });

  test("omits usage when total tokens are unknown", async () => {
    const session = new FakeSession(
      [{ type: "text", text: "hello", isThinking: false }, { type: "done" }],
      5,
      0,
    );
    const recorder = createCtx();

    const result = await pumpSession(session as any, recorder.ctx);

    expect(result).toEqual({ outcome: "done" });
    expect(recorder.usages).toEqual([]);
  });

  test("does not emit fake zero usage", async () => {
    const session = new FakeSession(
      [{ type: "usage", outputTokens: 0, totalTokens: 0 }, { type: "done" }],
      0,
      0,
    );
    const recorder = createCtx();

    await pumpSession(session as any, recorder.ctx);

    expect(recorder.usages).toEqual([]);
  });
});

describe("collectNonStreamingResponse usage", () => {
  test("includes the best available usage snapshot", async () => {
    const session = new FakeSession(
      [{ type: "text", text: "answer", isThinking: false }, { type: "done" }],
      7,
      20,
    );

    const response = await collectNonStreamingResponse(session as any, "test-model");
    const body = (await response.json()) as {
      usage?: { prompt_tokens: number; completion_tokens: number; total_tokens: number };
      choices: Array<{ message: { content: string } }>;
    };

    expect(body.choices[0]?.message.content).toBe("answer");
    expect(body.usage).toEqual({ prompt_tokens: 13, completion_tokens: 7, total_tokens: 20 });
    expect(session.closed).toBe(true);
  });

  test("omits usage when the total token count is unknown", async () => {
    const session = new FakeSession(
      [{ type: "text", text: "answer", isThinking: false }, { type: "done" }],
      7,
      0,
    );

    const response = await collectNonStreamingResponse(session as any, "test-model");
    const body = (await response.json()) as {
      usage?: { prompt_tokens: number; completion_tokens: number; total_tokens: number };
      choices: Array<{ message: { content: string } }>;
    };

    expect(body.choices[0]?.message.content).toBe("answer");
    expect(body.usage).toBeUndefined();
    expect(session.closed).toBe(true);
  });
});
