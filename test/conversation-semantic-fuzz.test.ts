import { describe, expect, test } from "bun:test";
import { isDeepStrictEqual } from "node:util";
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
import { readEchoToolText } from "./support/conversation-test-helpers";
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
const MAX_SEMANTIC_FUZZ_COUNT = 100;
const DEFAULT_SEMANTIC_FUZZ_FAILURE_POINT_COUNT = 8;
const MAX_SEMANTIC_FUZZ_FAILURE_POINT_COUNT = 64;
const SEMANTIC_FUZZ_CONFIG = resolveSemanticFuzzConfig(
  process.env.SEMANTIC_FUZZ_COUNT,
  process.env.SEMANTIC_FUZZ_FAILURE_POINT_COUNT,
);
const SEMANTIC_FUZZ_TIMEOUT_MS = Math.max(
  180_000,
  30_000 + SEMANTIC_FUZZ_CONFIG.seeds.length * (SEMANTIC_FUZZ_CONFIG.failurePointCount + 1) * 3_000,
);

describe("conversation semantic fuzz", () => {
  test(
    "matches the golden conversation across seeded semantic failure replays",
    async () => {
      const failures: string[] = [];

      for (const seed of SEMANTIC_FUZZ_CONFIG.seeds) {
        failures.push(...(await evaluateSemanticSeed(seed)));
      }

      if (failures.length > 0) {
        throw new Error(failures.join("\n"));
      }
    },
    SEMANTIC_FUZZ_TIMEOUT_MS,
  );
});

async function evaluateSemanticSeed(seed: number): Promise<string[]> {
  const scenario = generateSemanticFuzzScenario(seed);
  const golden = await runSemanticFuzzScenario(scenario);
  expect(golden.executorInvocations).toBe(scenario.uniqueToolCallCount);
  expect(golden.observedToolResults.length).toBe(scenario.uniqueToolCallCount);
  expect(golden.runSnapshots.length).toBe(scenario.turns.length);

  const sampledPoints = sampleSemanticFailurePoints(
    golden.points,
    seed,
    SEMANTIC_FUZZ_CONFIG.failurePointCount,
  );
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
          return `tool-result::${readEchoToolText(args)}`;
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
  if (!isDeepStrictEqual(replay.normalizedMessages, golden.normalizedMessages)) {
    return `Seed ${seed}: message mismatch after point ${point.ordinal} (${point.label})`;
  }
  if (!isDeepStrictEqual(replay.turnSummary, golden.turnSummary)) {
    return `Seed ${seed}: turn mismatch after point ${point.ordinal} (${point.label})`;
  }
  const replayToolResultSet = [...new Set(replay.observedToolResults)].sort();
  const goldenToolResultSet = [...new Set(golden.observedToolResults)].sort();
  if (!isDeepStrictEqual(replayToolResultSet, goldenToolResultSet)) {
    return `Seed ${seed}: backend-observed tool result set diverged after point ${point.ordinal} (${point.label})`;
  }
  return null;
}

function resolveSemanticFuzzConfig(
  rawSeedCount: string | undefined,
  rawFailurePointCount: string | undefined,
): { seeds: number[]; failurePointCount: number } {
  return {
    seeds: buildSemanticFuzzSeeds(
      resolvePositiveInteger(rawSeedCount, {
        envName: "SEMANTIC_FUZZ_COUNT",
        defaultValue: DEFAULT_SEMANTIC_FUZZ_COUNT,
        maxValue: MAX_SEMANTIC_FUZZ_COUNT,
      }),
    ),
    failurePointCount: resolvePositiveInteger(rawFailurePointCount, {
      envName: "SEMANTIC_FUZZ_FAILURE_POINT_COUNT",
      defaultValue: DEFAULT_SEMANTIC_FUZZ_FAILURE_POINT_COUNT,
      maxValue: MAX_SEMANTIC_FUZZ_FAILURE_POINT_COUNT,
    }),
  };
}

function resolvePositiveInteger(
  rawCount: string | undefined,
  options: {
    envName: string;
    defaultValue: number;
    maxValue: number;
  },
): number {
  const trimmed = rawCount?.trim();
  if (!trimmed) return options.defaultValue;

  const count = Number(trimmed);
  if (!Number.isInteger(count) || count <= 0) {
    throw new Error(`${options.envName} must be a positive integer`);
  }
  if (count > options.maxValue) {
    throw new Error(`${options.envName} must be <= ${options.maxValue}`);
  }
  return count;
}

function buildSemanticFuzzSeeds(count: number): number[] {
  return Array.from({ length: count }, (_, index) => semanticFuzzSeedAt(index));
}

function semanticFuzzSeedAt(index: number): number {
  return 11 + index * 18 + Math.floor(index / 3) * 6;
}
