import { describe, expect, test } from "bun:test";
import { fixInterleavingTransform } from "../src/sdk-wrapper";

type StreamPart = {
  type: string;
  id?: string;
  delta?: string;
};

async function runTransform(parts: StreamPart[]): Promise<StreamPart[]> {
  const transform = fixInterleavingTransform();
  const writer = transform.writable.getWriter();
  const reader = transform.readable.getReader();

  const writeAll = (async () => {
    for (const part of parts) {
      await writer.write(part);
    }
    await writer.close();
  })();

  const output: StreamPart[] = [];
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    output.push(value);
  }
  await writeAll;
  return output;
}

describe("fixInterleavingTransform", () => {
  test("drops stale text-end after text was closed on reasoning transition", async () => {
    const output = await runTransform([
      { type: "text-start", id: "txt-0" },
      { type: "text-delta", id: "txt-0", delta: "hello" },
      { type: "reasoning-start", id: "reasoning-0" },
      { type: "reasoning-delta", id: "reasoning-0", delta: "thinking" },
      // Upstream flush can still emit txt-0 even though the wrapper already
      // closed the active text part on reasoning-start.
      { type: "text-end", id: "txt-0" },
    ]);

    expect(output).toEqual([
      { type: "text-start", id: "txt-0" },
      { type: "text-delta", id: "txt-0", delta: "hello" },
      { type: "text-end", id: "txt-0" },
      { type: "reasoning-start", id: "reasoning-0" },
      { type: "reasoning-delta", id: "reasoning-0", delta: "thinking" },
    ]);
  });

  test("reopens text with a fresh id after reasoning ends", async () => {
    const output = await runTransform([
      { type: "text-start", id: "txt-0" },
      { type: "text-delta", id: "txt-0", delta: "before" },
      { type: "reasoning-start", id: "reasoning-0" },
      { type: "reasoning-delta", id: "reasoning-0", delta: "thought" },
      { type: "reasoning-end", id: "reasoning-0" },
      { type: "text-delta", id: "txt-0", delta: "after" },
      { type: "text-end", id: "txt-0" },
    ]);

    expect(output).toEqual([
      { type: "text-start", id: "txt-0" },
      { type: "text-delta", id: "txt-0", delta: "before" },
      { type: "text-end", id: "txt-0" },
      { type: "reasoning-start", id: "reasoning-0" },
      { type: "reasoning-delta", id: "reasoning-0", delta: "thought" },
      { type: "reasoning-end", id: "reasoning-0" },
      { type: "text-start", id: "txt-1" },
      { type: "text-delta", id: "txt-1", delta: "after" },
      { type: "text-end", id: "txt-1" },
    ]);
  });
});
