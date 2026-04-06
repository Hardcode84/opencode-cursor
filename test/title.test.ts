import { describe, expect, mock, test } from "bun:test";

mock.module("../src/cursor-session", () => ({
  callCursorUnaryRpc: async () => ({
    body: new Uint8Array(0),
    exitCode: 1,
    timedOut: true,
  }),
}));

const { handleTitleGenerationRequest } = await import("../src/title");

describe("handleTitleGenerationRequest", () => {
  test("omits fake zero usage in non-streaming title responses", async () => {
    const response = await handleTitleGenerationRequest(
      "Investigate token usage",
      "token",
      "model",
      false,
    );
    const body = (await response.json()) as {
      usage?: unknown;
      choices: Array<{ message: { content: string } }>;
    };

    expect(body.usage).toBeUndefined();
    expect(body.choices[0]?.message.content.length).toBeGreaterThan(0);
  });

  test("omits fake zero usage chunks in streaming title responses", async () => {
    const response = await handleTitleGenerationRequest(
      "Investigate token usage",
      "token",
      "model",
      true,
    );
    const bodyText = await response.text();

    expect(bodyText.includes('"usage"')).toBe(false);
    expect(bodyText.includes("data: [DONE]")).toBe(true);
  });
});
