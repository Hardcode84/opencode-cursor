import { describe, expect, test } from "bun:test";
import {
  type FakeCommunicationPoint,
  FakeCursorBackend,
  type FakeRunRequestSnapshot,
} from "./support/fake-cursor-backend";
import {
  createHappyPathTools,
  executeHappyPathConversation,
  HAPPY_PATH_INITIAL_MESSAGES,
  HAPPY_PATH_SESSION_ID,
  installHappyPathScenario,
  validateHappyPathResponse,
} from "./support/happy-path-scenario";
import {
  type NormalizedConversationMessage,
  normalizeConversationMessages,
  OpenAIConversationDriver,
} from "./support/openai-conversation-driver";
import { startProxyHarness } from "./support/proxy-harness";

interface ReplayRunResult {
  normalizedMessages: NormalizedConversationMessage[];
  turnSummary: Array<{ assistantText: string; reasoningText: string }>;
  points: FakeCommunicationPoint[];
  didInjectFailure: boolean;
  runSnapshots: FakeRunRequestSnapshot[];
}

describe("conversation replay recovery", () => {
  test("reconstructs the golden conversation after a single upstream reset at every semantic point", async () => {
    const golden = await runHappyPathReplay();
    expect(golden.points.length).toBeGreaterThan(0);

    const goldenMessages = JSON.stringify(golden.normalizedMessages);
    const goldenTurns = JSON.stringify(golden.turnSummary);
    const failures: string[] = [];

    for (const point of golden.points) {
      const replay = await runHappyPathReplay(point.ordinal);
      if (!replay.didInjectFailure) {
        failures.push(`Point ${point.ordinal} (${point.label}) did not inject a failure`);
        continue;
      }
      if (JSON.stringify(replay.normalizedMessages) !== goldenMessages) {
        failures.push(`Message mismatch after failing at point ${point.ordinal} (${point.label})`);
        continue;
      }
      if (JSON.stringify(replay.turnSummary) !== goldenTurns) {
        failures.push(
          `Turn summary mismatch after failing at point ${point.ordinal} (${point.label})`,
        );
      }
    }

    if (failures.length > 0) {
      throw new Error(failures.join("\n"));
    }
  }, 120_000);
});

async function runHappyPathReplay(failPointOrdinal?: number): Promise<ReplayRunResult> {
  const backend = await FakeCursorBackend.start();
  const scenario = installHappyPathScenario(backend);
  if (failPointOrdinal != null) {
    backend.setFailOnceAtPoint(failPointOrdinal);
  }

  const proxy = await startProxyHarness({
    runtimeConfig: {
      apiUrl: backend.apiUrl,
      agentUrl: backend.agentUrl,
    },
  });

  try {
    const driver = new OpenAIConversationDriver({
      baseUrl: proxy.baseUrl,
      model: "test-model",
      sessionId: HAPPY_PATH_SESSION_ID,
      tools: createHappyPathTools(),
      toolExecutors: {
        echo_tool(args) {
          const text = typeof args === "object" && args && "text" in args ? String(args.text) : "";
          return `tool-result::${text}`;
        },
      },
      initialMessages: HAPPY_PATH_INITIAL_MESSAGES,
      maxRequestRetries: 10,
      responseValidator: validateHappyPathResponse,
    });

    const outcome = await executeHappyPathConversation(driver);

    return {
      normalizedMessages: normalizeConversationMessages(driver.messages),
      turnSummary: [
        { assistantText: outcome.turn1.assistantText, reasoningText: outcome.turn1.reasoningText },
        { assistantText: outcome.turn2.assistantText, reasoningText: outcome.turn2.reasoningText },
        { assistantText: outcome.turn3.assistantText, reasoningText: outcome.turn3.reasoningText },
      ],
      points: [...backend.communicationPoints],
      didInjectFailure: backend.didInjectFailure,
      runSnapshots: [...scenario.runSnapshots],
    };
  } finally {
    await proxy.close();
    await backend.close();
  }
}
