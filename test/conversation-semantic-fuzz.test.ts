import { describe, expect, test } from "bun:test";
import {
  createSemanticFuzzTools,
  executeSemanticFuzzScenario,
  generateSemanticFuzzScenario,
  installSemanticFuzzScenario,
  type SemanticFuzzScenario,
  sampleSemanticFailurePoints,
  summarizeSemanticTurns,
  validateSemanticFuzzResponse,
} from "./support/conversation-semantic-fuzz";
import {
  type FakeCommunicationPoint,
  FakeCursorBackend,
  type FakeRunRequestSnapshot,
} from "./support/fake-cursor-backend";
import {
  type NormalizedConversationMessage,
  normalizeConversationMessages,
  OpenAIConversationDriver,
} from "./support/openai-conversation-driver";
import { startProxyHarness } from "./support/proxy-harness";

interface SemanticFuzzRunResult {
  normalizedMessages: NormalizedConversationMessage[];
  turnSummary: ReturnType<typeof summarizeSemanticTurns>;
  points: FakeCommunicationPoint[];
  didInjectFailure: boolean;
  runSnapshots: FakeRunRequestSnapshot[];
  executorInvocations: number;
  observedToolResults: string[];
}

const DEFAULT_SEMANTIC_FUZZ_COUNT = 5;
const SEMANTIC_FUZZ_SEEDS = resolveSemanticFuzzSeeds(process.env.SEMANTIC_FUZZ_COUNT);

describe("conversation semantic fuzz", () => {
  test("matches the golden conversation across seeded semantic failure replays", async () => {
    const failures: string[] = [];

    for (const seed of SEMANTIC_FUZZ_SEEDS) {
      failures.push(...(await evaluateSemanticSeed(seed)));
    }

    if (failures.length > 0) {
      throw new Error(failures.join("\n"));
    }
  }, 180_000);
});

async function evaluateSemanticSeed(seed: number): Promise<string[]> {
  const scenario = generateSemanticFuzzScenario(seed);
  const golden = await runSemanticFuzzScenario(scenario);
  expect(golden.executorInvocations).toBe(scenario.uniqueToolCallCount);
  expect(golden.observedToolResults.length).toBe(scenario.uniqueToolCallCount);
  expect(golden.runSnapshots.length).toBe(scenario.turns.length);

  const sampledPoints = sampleSemanticFailurePoints(golden.points, seed);
  expect(sampledPoints.length).toBeGreaterThan(0);

  const failures: string[] = [];
  for (const point of sampledPoints) {
    const replay = await runSemanticFuzzScenario(scenario, {
      failPointOrdinal: point.ordinal,
      failureMode: point.ordinal % 2 === 0 ? "reset" : "destroy",
    });
    const failure = compareSemanticReplay(scenario, golden, replay, point, seed);
    if (failure) failures.push(failure);
  }
  return failures;
}

async function runSemanticFuzzScenario(
  scenario: SemanticFuzzScenario,
  options: {
    failPointOrdinal?: number;
    failureMode?: "reset" | "destroy";
  } = {},
): Promise<SemanticFuzzRunResult> {
  const backend = await FakeCursorBackend.start();
  const state = installSemanticFuzzScenario(backend, scenario);
  if (options.failPointOrdinal != null) {
    backend.setFailOnceAtPoint(options.failPointOrdinal, options.failureMode ?? "reset");
  }

  const proxy = await startProxyHarness({
    runtimeConfig: {
      apiUrl: backend.apiUrl,
      agentUrl: backend.agentUrl,
    },
  });

  try {
    let executorInvocations = 0;
    const driver = new OpenAIConversationDriver({
      baseUrl: proxy.baseUrl,
      model: "test-model",
      sessionId: scenario.sessionId,
      tools: createSemanticFuzzTools(),
      toolExecutors: {
        echo_tool(args) {
          executorInvocations++;
          const text = typeof args === "object" && args && "text" in args ? String(args.text) : "";
          return `tool-result::${text}`;
        },
      },
      initialMessages: scenario.initialMessages,
      maxRequestRetries: 10,
      responseValidator: (messages, trace) =>
        validateSemanticFuzzResponse(scenario, messages, trace),
    });

    const turns = await executeSemanticFuzzScenario(driver, scenario);

    return {
      normalizedMessages: normalizeConversationMessages(driver.messages),
      turnSummary: summarizeSemanticTurns(turns),
      points: [...backend.communicationPoints],
      didInjectFailure: backend.didInjectFailure,
      runSnapshots: [...state.runSnapshots],
      executorInvocations,
      observedToolResults: [...state.observedToolResults],
    };
  } finally {
    await proxy.close();
    await backend.close();
  }
}

function compareSemanticReplay(
  scenario: SemanticFuzzScenario,
  golden: SemanticFuzzRunResult,
  replay: SemanticFuzzRunResult,
  point: FakeCommunicationPoint,
  seed: number,
): string | null {
  if (!replay.didInjectFailure) {
    return `Seed ${seed}: point ${point.ordinal} (${point.label}) did not inject a failure`;
  }
  if (replay.executorInvocations !== scenario.uniqueToolCallCount) {
    return `Seed ${seed}: point ${point.ordinal} (${point.label}) executed tools ${replay.executorInvocations} times instead of ${scenario.uniqueToolCallCount}`;
  }
  if (JSON.stringify(replay.normalizedMessages) !== JSON.stringify(golden.normalizedMessages)) {
    return `Seed ${seed}: message mismatch after point ${point.ordinal} (${point.label})`;
  }
  if (JSON.stringify(replay.turnSummary) !== JSON.stringify(golden.turnSummary)) {
    return `Seed ${seed}: turn mismatch after point ${point.ordinal} (${point.label})`;
  }
  return null;
}

function resolveSemanticFuzzSeeds(rawCount: string | undefined): number[] {
  const trimmed = rawCount?.trim();
  if (!trimmed) return buildSemanticFuzzSeeds(DEFAULT_SEMANTIC_FUZZ_COUNT);

  const count = Number(trimmed);
  if (!Number.isInteger(count) || count <= 0) {
    throw new Error("SEMANTIC_FUZZ_COUNT must be a positive integer");
  }

  return buildSemanticFuzzSeeds(count);
}

function buildSemanticFuzzSeeds(count: number): number[] {
  return Array.from({ length: count }, (_, index) => semanticFuzzSeedAt(index));
}

function semanticFuzzSeedAt(index: number): number {
  return 11 + index * 18 + Math.floor(index / 3) * 6;
}
