import { describe, expect, test } from "bun:test";
import { prettyCursorModelName, resolveCursorModelName } from "../src/model-names";

describe("model display names", () => {
  test("pretty-prints common Cursor model ids", () => {
    expect(prettyCursorModelName("composer-2")).toBe("Composer 2");
    expect(prettyCursorModelName("claude-4.6-sonnet")).toBe("Claude Sonnet 4.6");
    expect(prettyCursorModelName("gpt-5.4-mini")).toBe("GPT-5.4 Mini");
    expect(prettyCursorModelName("gemini-3.1-pro")).toBe("Gemini 3.1 Pro");
  });

  test("keeps already-pretty discovered names", () => {
    expect(resolveCursorModelName("claude-4.6-sonnet", "Claude 4.6 Sonnet")).toBe(
      "Claude 4.6 Sonnet",
    );
    expect(resolveCursorModelName("composer-2", "Composer Two Experimental")).toBe(
      "Composer Two Experimental",
    );
  });

  test("prettifies raw discovered names", () => {
    expect(resolveCursorModelName("claude-4.6-sonnet", "claude-4.6-sonnet")).toBe(
      "Claude Sonnet 4.6",
    );
    expect(resolveCursorModelName("composer-2", "composer-2")).toBe("Composer 2");
  });
});
