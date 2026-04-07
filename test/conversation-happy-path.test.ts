import { afterEach, describe, expect, test } from "bun:test";
import type { OpenAIToolDef } from "../src/openai-messages";
import { FakeCursorBackend, type FakeRunRequestSnapshot } from "./support/fake-cursor-backend";
import { OpenAIConversationDriver } from "./support/openai-conversation-driver";
import { type ProxyHarness, startProxyHarness } from "./support/proxy-harness";

let backend: FakeCursorBackend | undefined;
let proxy: ProxyHarness | undefined;

afterEach(async () => {
  await proxy?.close();
  await backend?.close();
  proxy = undefined;
  backend = undefined;
});

describe("conversation happy path", () => {
  test("simulates a multi-turn OpenAI conversation with reasoning and a tool-call resume", async () => {
    backend = await FakeCursorBackend.start();
    const runSnapshots: FakeRunRequestSnapshot[] = [];
    const toolResultTexts: string[] = [];

    const turn1AssistantText = "Echo: hello alpha.";
    const turn2HistoryAssistantText =
      'Invoking echo_tool. \n[Tool echo_tool({"text":"beta payload"})]\n' +
      "tool-result::beta payload\n" +
      "Tool says: tool-result::beta payload.";

    backend.enqueueRun(async (connection) => {
      const run = await connection.waitForRunRequest();
      runSnapshots.push(run);

      connection.sendRequestContextArgs(101);
      await connection.waitForRequestContextResult(101);

      connection.sendThinkingDelta("Thinking about hello alpha.");
      connection.sendTextDelta("Echo: ");
      connection.sendThinkingDelta("Planning concise echo.");
      connection.sendTextDelta("hello alpha.");
      connection.sendEndStreamOk();
    });

    backend.enqueueRun(async (connection) => {
      const run = await connection.waitForRunRequest();
      runSnapshots.push(run);

      connection.sendRequestContextArgs(201);
      await connection.waitForRequestContextResult(201);

      connection.sendThinkingDelta("Need the echo tool.");
      connection.sendTextDelta("Invoking echo_tool. ");
      connection.sendMcpToolCall(
        "echo_tool",
        { text: "beta payload" },
        { toolCallId: "echo-beta", execId: 202 },
      );
      connection.sendConversationCheckpoint([
        { userText: "hello alpha", assistantText: turn1AssistantText },
      ]);

      const mcpResult = await connection.waitForMcpResult(202);
      toolResultTexts.push(mcpResult.text);
      await connection.waitForExecStreamClose(202);

      connection.sendThinkingDelta("Tool completed successfully.");
      connection.sendTextDelta(`Tool says: ${mcpResult.text}.`);
      connection.sendConversationCheckpoint([
        { userText: "hello alpha", assistantText: turn1AssistantText },
        {
          userText: "please use the tool for beta",
          assistantText: turn2HistoryAssistantText,
        },
      ]);
      connection.sendEndStreamOk();
    });

    backend.enqueueRun(async (connection) => {
      const run = await connection.waitForRunRequest();
      runSnapshots.push(run);

      const userHistory = run.turns.map((turn) => turn.userText).join(" | ");
      const sawToolHistory = run.turns.some((turn) =>
        turn.assistantText.includes('[Tool echo_tool({"text":"beta payload"})]'),
      );

      connection.sendRequestContextArgs(301);
      await connection.waitForRequestContextResult(301);

      connection.sendThinkingDelta("Summarizing prior turns.");
      connection.sendTextDelta(
        `History users: ${userHistory}. Tool history: ${sawToolHistory ? "present" : "missing"}.`,
      );
      connection.sendEndStreamOk();
    });

    proxy = await startProxyHarness({
      runtimeConfig: {
        apiUrl: backend.apiUrl,
        agentUrl: backend.agentUrl,
      },
    });

    const tools: OpenAIToolDef[] = [
      {
        type: "function",
        function: {
          name: "echo_tool",
          description: "Echo text back to the caller",
          parameters: {
            type: "object",
            properties: {
              text: { type: "string" },
            },
            required: ["text"],
          },
        },
      },
    ];

    const driver = new OpenAIConversationDriver({
      baseUrl: proxy.baseUrl,
      model: "test-model",
      sessionId: "happy-path-session",
      tools,
      toolExecutors: {
        echo_tool(args) {
          const text = typeof args === "object" && args && "text" in args ? String(args.text) : "";
          return `tool-result::${text}`;
        },
      },
      initialMessages: [{ role: "system", content: "You are a deterministic test assistant." }],
    });

    const turn1 = await driver.runTurn("hello alpha");
    const turn2 = await driver.runTurn("please use the tool for beta");
    const turn3 = await driver.runTurn("what have we discussed so far?");

    expect(backend.runCount).toBe(3);
    expect(runSnapshots.length).toBe(3);

    expect(runSnapshots[0]!.actionCase).toBe("userMessageAction");
    expect(runSnapshots[0]!.userText).toBe("hello alpha");
    expect(runSnapshots[0]!.turns).toEqual([]);

    expect(runSnapshots[1]!.userText).toBe("please use the tool for beta");
    expect(runSnapshots[1]!.turns.length).toBe(1);
    expect(runSnapshots[1]!.turns[0]!.userText).toBe("hello alpha");
    expect(runSnapshots[1]!.turns[0]!.assistantText).toBe(turn1AssistantText);

    expect(runSnapshots[2]!.userText).toBe("what have we discussed so far?");
    expect(runSnapshots[2]!.turns.length).toBe(2);
    expect(runSnapshots[2]!.turns[1]!.assistantText).toContain(
      '[Tool echo_tool({"text":"beta payload"})]',
    );
    expect(runSnapshots[2]!.turns[1]!.assistantText).toContain("tool-result::beta payload");

    expect(turn1.requests.length).toBe(1);
    expect(turn1.requests[0]!.reasoning).toBe("Thinking about hello alpha.Planning concise echo.");
    expect(turn1.requests[0]!.content).toBe(turn1AssistantText);
    expect(
      turn1.requests[0]!.events.map((event) => event.kind).filter((kind) => kind !== "finish"),
    ).toEqual(["reasoning", "content", "reasoning", "content"]);

    expect(turn2.requests.length).toBe(2);
    expect(turn2.requests[0]!.finishReason).toBe("tool_calls");
    expect(turn2.requests[0]!.toolCalls).toHaveLength(1);
    expect(turn2.requests[0]!.toolCalls[0]!.function.name).toBe("echo_tool");
    expect(turn2.toolExecutions).toHaveLength(1);
    expect(turn2.toolExecutions[0]!.result).toBe("tool-result::beta payload");
    expect(toolResultTexts).toEqual(["tool-result::beta payload"]);
    expect(turn2.assistantText).toBe("Invoking echo_tool. Tool says: tool-result::beta payload.");
    expect(turn2.reasoningText).toBe("Need the echo tool.Tool completed successfully.");

    expect(turn3.requests.length).toBe(1);
    expect(turn3.assistantText).toBe(
      "History users: hello alpha | please use the tool for beta. Tool history: present.",
    );
    expect(turn3.reasoningText).toBe("Summarizing prior turns.");

    expect(driver.messages.map((message) => message.role)).toEqual([
      "system",
      "user",
      "assistant",
      "user",
      "assistant",
      "tool",
      "assistant",
      "user",
      "assistant",
    ]);
  }, 15_000);
});
