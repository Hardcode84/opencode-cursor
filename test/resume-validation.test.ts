import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { startProxy, stopProxy } from "../src/server";

let port: number;
const BASE = () => `http://localhost:${port}/v1/chat/completions`;

function chatBody(overrides: Record<string, unknown> = {}) {
  return {
    model: "test-model",
    stream: true,
    messages: [{ role: "user", content: "hello" }],
    ...overrides,
  };
}

beforeAll(async () => {
  port = await startProxy(async () => "fake-token", [{ id: "test-model", name: "Test" }]);
});

afterAll(() => stopProxy());

describe("resume validation", () => {
  test("missing user message returns 400 with code", async () => {
    const res = await fetch(BASE(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ model: "test-model", stream: true, messages: [] }),
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("missing_user_message");
  });

  test("non-streaming with tools returns 400 with code", async () => {
    const res = await fetch(BASE(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(
        chatBody({
          stream: false,
          tools: [{ type: "function", function: { name: "foo", parameters: {} } }],
        }),
      ),
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("unsupported_mode");
  });

  test("tool results with no active session returns 400 session_not_found", async () => {
    const res = await fetch(BASE(), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-session-affinity": "orphan-session",
      },
      body: JSON.stringify(
        chatBody({
          messages: [
            { role: "user", content: "hello" },
            {
              role: "assistant",
              content: null,
              tool_calls: [
                {
                  id: "call_orphan",
                  type: "function",
                  function: { name: "foo", arguments: '{"x":1}' },
                },
              ],
            },
            { role: "tool", tool_call_id: "call_orphan", content: "result" },
          ],
        }),
      ),
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string; message: string } };
    expect(body.error.code).toBe("session_not_found");
    expect(body.error.message).toContain("No active session");
  });

  test("validation error includes type, code, and message fields", async () => {
    const res = await fetch(BASE(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ model: "test-model", stream: true, messages: [] }),
    });
    const body = (await res.json()) as { error: { type: string; code: string; message: string } };
    expect(body.error.type).toBe("invalid_request_error");
    expect(typeof body.error.code).toBe("string");
    expect(typeof body.error.message).toBe("string");
  });
});
