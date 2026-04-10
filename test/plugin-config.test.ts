import { describe, expect, test } from "bun:test";
import type { PluginInput } from "@opencode-ai/plugin";
import { CursorAuthPlugin } from "../src/index";
import { CURSOR_MAX_MODE_HEADER } from "../src/max-mode";

function createPluginInput(): PluginInput {
  return {
    client: {} as PluginInput["client"],
    project: {} as PluginInput["project"],
    directory: "/tmp/opencode-cursor-test",
    worktree: "/tmp/opencode-cursor-test",
    serverUrl: new URL("http://localhost"),
    $: {} as PluginInput["$"],
  };
}

async function invokeChatHeaders(options?: {
  providerID?: string;
  providerOptions?: Record<string, unknown>;
  modelOptions?: Record<string, unknown>;
}) {
  const hooks = await CursorAuthPlugin(createPluginInput());
  const hook = hooks["chat.headers"];
  if (!hook) throw new Error("chat.headers hook missing");

  const headers: Record<string, string> = {};
  await hook(
    {
      sessionID: "session-123",
      agent: "assistant",
      model: {
        providerID: options?.providerID ?? "cursor",
        options: options?.modelOptions ?? {},
      } as any,
      provider: {
        options: options?.providerOptions ?? {},
      } as any,
      message: {} as any,
    },
    { headers },
  );
  return headers;
}

describe("plugin config max mode hook", () => {
  test("provider options set the Cursor max-mode header", async () => {
    const headers = await invokeChatHeaders({
      providerOptions: { maxMode: false },
    });

    expect(headers["x-session-affinity"]).toBe("session-123");
    expect(headers["x-opencode-agent"]).toBe("assistant");
    expect(headers[CURSOR_MAX_MODE_HEADER]).toBe("false");
  });

  test("model options override provider max mode", async () => {
    const headers = await invokeChatHeaders({
      providerOptions: { maxMode: true },
      modelOptions: { max_mode: false },
    });

    expect(headers[CURSOR_MAX_MODE_HEADER]).toBe("false");
  });

  test("non-cursor models do not receive the Cursor max-mode header", async () => {
    const headers = await invokeChatHeaders({
      providerID: "openai",
      providerOptions: { maxMode: false },
      modelOptions: { maxMode: false },
    });

    expect(headers[CURSOR_MAX_MODE_HEADER]).toBeUndefined();
  });
});
