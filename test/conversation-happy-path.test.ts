import { afterEach, describe, expect, test } from "bun:test";
import { readEchoToolText } from "./support/conversation-test-helpers";
import { FakeCursorBackend, type FakeRunRequestSnapshot } from "./support/fake-cursor-backend";
import {
  createHappyPathTools,
  executeHappyPathConversation,
  HAPPY_PATH_INITIAL_MESSAGES,
  HAPPY_PATH_SESSION_ID,
  HAPPY_PATH_TURN_1_ASSISTANT_TEXT,
  HAPPY_PATH_TURN_2_TEXT,
  HAPPY_PATH_TURN_3_TEXT,
  installHappyPathScenario,
  validateHappyPathResponse,
} from "./support/happy-path-scenario";
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
    const scenario = installHappyPathScenario(backend, {
      runSnapshots: [] as FakeRunRequestSnapshot[],
      toolResultTexts: [],
    });

    proxy = await startProxyHarness({
      runtimeConfig: {
        apiUrl: backend.apiUrl,
        agentUrl: backend.agentUrl,
      },
    });

    const driver = new OpenAIConversationDriver({
      baseUrl: proxy.baseUrl,
      model: "test-model",
      sessionId: HAPPY_PATH_SESSION_ID,
      tools: createHappyPathTools(),
      toolExecutors: {
        echo_tool(args) {
          return `tool-result::${readEchoToolText(args)}`;
        },
      },
      initialMessages: HAPPY_PATH_INITIAL_MESSAGES,
      responseValidator: validateHappyPathResponse,
    });

    const { turn1, turn2, turn3 } = await executeHappyPathConversation(driver);

    expect(backend.runCount).toBe(3);
    expect(scenario.runSnapshots.length).toBe(3);

    expect(scenario.runSnapshots[0]!.actionCase).toBe("userMessageAction");
    expect(scenario.runSnapshots[0]!.userText).toBe("hello alpha");
    expect(scenario.runSnapshots[0]!.turns).toEqual([]);

    expect(scenario.runSnapshots[1]!.userText).toBe(HAPPY_PATH_TURN_2_TEXT);
    expect(scenario.runSnapshots[1]!.turns.length).toBe(1);
    expect(scenario.runSnapshots[1]!.turns[0]!.userText).toBe("hello alpha");
    expect(scenario.runSnapshots[1]!.turns[0]!.assistantText).toBe(
      HAPPY_PATH_TURN_1_ASSISTANT_TEXT,
    );

    expect(scenario.runSnapshots[2]!.userText).toBe(HAPPY_PATH_TURN_3_TEXT);
    expect(scenario.runSnapshots[2]!.turns.length).toBe(2);
    expect(scenario.runSnapshots[2]!.turns[1]!.assistantText).toContain(
      '[Tool echo_tool({"text":"beta payload"})]',
    );
    expect(scenario.runSnapshots[2]!.turns[1]!.assistantText).toContain(
      "tool-result::beta payload",
    );

    expect(turn1.requests.length).toBe(1);
    expect(turn1.requests[0]!.reasoning).toBe("Thinking about hello alpha.Planning concise echo.");
    expect(turn1.requests[0]!.content).toBe(HAPPY_PATH_TURN_1_ASSISTANT_TEXT);
    expect(
      turn1.requests[0]!.events.map((event) => event.kind).filter((kind) => kind !== "finish"),
    ).toEqual(["reasoning", "content", "reasoning", "content"]);

    expect(turn2.requests.length).toBe(2);
    expect(turn2.requests[0]!.finishReason).toBe("tool_calls");
    expect(turn2.requests[0]!.toolCalls).toHaveLength(1);
    expect(turn2.requests[0]!.toolCalls[0]!.function.name).toBe("echo_tool");
    expect(turn2.toolExecutions).toHaveLength(1);
    expect(turn2.toolExecutions[0]!.result).toBe("tool-result::beta payload");
    expect(scenario.toolResultTexts).toEqual(["tool-result::beta payload"]);
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
