import type { OpenAIMessage, OpenAIToolDef } from "../../src/openai-messages";
import type {
  FakeCommunicationPoint,
  FakeCursorBackend,
  FakeRunConnection,
  FakeRunRequestSnapshot,
} from "./fake-cursor-backend";
import type {
  ConversationDriverRequestTrace,
  ConversationDriverTurnTrace,
  OpenAIConversationDriver,
} from "./openai-conversation-driver";

export interface SemanticFuzzToolCall {
  id: string;
  text: string;
  result: string;
}

export interface SemanticFuzzBatch {
  introReasoning: string;
  introContent: string;
  toolCalls: SemanticFuzzToolCall[];
}

export interface SemanticFuzzTurn {
  userText: string;
  batches: SemanticFuzzBatch[];
  finalReasoning: string;
  finalContent: string;
  historyAssistantText: string;
}

export interface SemanticFuzzScenario {
  seed: number;
  sessionId: string;
  initialMessages: OpenAIMessage[];
  turns: SemanticFuzzTurn[];
  uniqueToolCallCount: number;
}

export interface SemanticFuzzScenarioState {
  runSnapshots: FakeRunRequestSnapshot[];
  observedToolResults: string[];
}

export function generateSemanticFuzzScenario(seed: number): SemanticFuzzScenario {
  const rng = createRng(seed);
  const turnCount = 2 + rng.nextInt(3);
  const turns: SemanticFuzzTurn[] = [];
  let uniqueToolCallCount = 0;

  for (let turnIndex = 0; turnIndex < turnCount; turnIndex++) {
    turns.push(buildTurn(seed, turnIndex, rng));
  }

  if (turns.every((turn) => turn.batches.length === 0)) {
    const forcedTurnIndex = rng.nextInt(turns.length);
    turns[forcedTurnIndex] = buildTurn(seed, forcedTurnIndex, createRng(seed ^ 0x9e3779b9), {
      forceToolBatch: true,
    });
  }

  for (const turn of turns) {
    for (const batch of turn.batches) {
      uniqueToolCallCount += batch.toolCalls.length;
    }
  }

  return {
    seed,
    sessionId: `semantic-fuzz-session-${seed}`,
    initialMessages: [
      {
        role: "system",
        content: `You are a deterministic semantic fuzz assistant for seed ${seed}.`,
      },
    ],
    turns,
    uniqueToolCallCount,
  };
}

export function createSemanticFuzzTools(): OpenAIToolDef[] {
  return [
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
}

export function installSemanticFuzzScenario(
  backend: FakeCursorBackend,
  scenario: SemanticFuzzScenario,
  state: SemanticFuzzScenarioState = { runSnapshots: [], observedToolResults: [] },
): SemanticFuzzScenarioState {
  backend.setRunHandler(async (connection) => {
    await handleSemanticFuzzConnection(connection, scenario, state);
  });

  return state;
}

export async function executeSemanticFuzzScenario(
  driver: OpenAIConversationDriver,
  scenario: SemanticFuzzScenario,
): Promise<ConversationDriverTurnTrace[]> {
  const turns: ConversationDriverTurnTrace[] = [];
  for (const turn of scenario.turns) {
    turns.push(await driver.runTurn(turn.userText));
  }
  return turns;
}

export function validateSemanticFuzzResponse(
  scenario: SemanticFuzzScenario,
  messages: ReadonlyArray<OpenAIMessage>,
  trace: ConversationDriverRequestTrace,
): string | null {
  const latestUserText = findLatestUserText(messages);
  if (!latestUserText) return "Semantic fuzz request missing latest user";
  const turn = scenario.turns.find((candidate) => candidate.userText === latestUserText);
  if (!turn) return `Unknown semantic fuzz turn for ${JSON.stringify(latestUserText)}`;

  const afterUser = messagesAfterLatestUser(messages);
  const toolAssistants = afterUser.filter(
    (message) => message.role === "assistant" && Array.isArray(message.tool_calls),
  );
  const toolMessages = afterUser.filter((message) => message.role === "tool");

  if (toolAssistants.length > turn.batches.length) {
    return `Too many completed tool batches for ${turn.userText}`;
  }

  for (const [batchIndex, assistant] of toolAssistants.entries()) {
    const batch = turn.batches[batchIndex]!;
    if ((assistant.content ?? "") !== batch.introContent) {
      return `Unexpected assistant tool prelude for ${turn.userText} batch ${batchIndex + 1}`;
    }
    const actualIds = (assistant.tool_calls ?? []).map((toolCall) => toolCall.id);
    const expectedIds = batch.toolCalls.map((toolCall) => toolCall.id);
    if (JSON.stringify(actualIds) !== JSON.stringify(expectedIds)) {
      return `Unexpected tool call IDs for ${turn.userText} batch ${batchIndex + 1}`;
    }
  }

  const expectedCompletedToolResults = turn.batches
    .slice(0, toolAssistants.length)
    .flatMap((batch) => batch.toolCalls.map((toolCall) => toolCall.result));
  const actualCompletedToolResults = toolMessages.map((message) =>
    typeof message.content === "string" ? message.content : JSON.stringify(message.content),
  );
  if (JSON.stringify(actualCompletedToolResults) !== JSON.stringify(expectedCompletedToolResults)) {
    return `Unexpected completed tool results for ${turn.userText}`;
  }

  if (toolAssistants.length < turn.batches.length) {
    const batch = turn.batches[toolAssistants.length]!;
    const actualTraceIds = trace.toolCalls.map((toolCall) => toolCall.id);
    const expectedTraceIds = batch.toolCalls.map((toolCall) => toolCall.id);
    const actualTraceArgs = trace.toolCalls.map((toolCall) => toolCall.function.arguments);
    const expectedTraceArgs = batch.toolCalls.map((toolCall) =>
      JSON.stringify({ text: toolCall.text }),
    );
    return trace.finishReason === "tool_calls" &&
      trace.reasoning === batch.introReasoning &&
      trace.content === batch.introContent &&
      JSON.stringify(actualTraceIds) === JSON.stringify(expectedTraceIds) &&
      JSON.stringify(actualTraceArgs) === JSON.stringify(expectedTraceArgs)
      ? null
      : `Tool batch response incomplete for ${turn.userText}`;
  }

  return trace.finishReason === "stop" &&
    trace.reasoning === turn.finalReasoning &&
    trace.content === turn.finalContent
    ? null
    : `Final response incomplete for ${turn.userText}`;
}

export function sampleSemanticFailurePoints(
  points: FakeCommunicationPoint[],
  seed: number,
  maxSampleCount = 8,
): FakeCommunicationPoint[] {
  if (points.length <= maxSampleCount) return [...points];

  const pickedOrdinals = new Set<number>();
  const preferredPatterns = [
    /^client\.runRequest$/,
    /server\.execServerMessage\.requestContextArgs$/,
    /server\.interactionUpdate\.thinkingDelta$/,
    /server\.interactionUpdate\.textDelta$/,
    /server\.execServerMessage\.mcpArgs$/,
    /server\.conversationCheckpointUpdate$/,
    /server\.endStream\.ok$/,
  ];

  for (const pattern of preferredPatterns) {
    if (pickedOrdinals.size >= maxSampleCount) break;
    const match = points.find((point) => pattern.test(point.label));
    if (match) pickedOrdinals.add(match.ordinal);
  }

  const safePoints = points.filter(
    (point) =>
      point.label === "client.runRequest" ||
      point.label.includes(":server.") ||
      point.label.startsWith("server."),
  );
  const rng = createRng(seed ^ 0x85ebca6b);
  while (pickedOrdinals.size < Math.min(maxSampleCount, safePoints.length)) {
    pickedOrdinals.add(safePoints[rng.nextInt(safePoints.length)]!.ordinal);
  }

  return points
    .filter((point) => pickedOrdinals.has(point.ordinal))
    .sort((a, b) => a.ordinal - b.ordinal);
}

export function summarizeSemanticTurns(turns: ConversationDriverTurnTrace[]) {
  return turns.map((turn) => ({
    userText: turn.userText,
    assistantText: turn.assistantText,
    reasoningText: turn.reasoningText,
    requestCount: turn.requests.length,
  }));
}

async function handleSemanticFuzzConnection(
  connection: FakeRunConnection,
  scenario: SemanticFuzzScenario,
  state: SemanticFuzzScenarioState,
): Promise<void> {
  const run = await connection.waitForRunRequest();
  const turnIndex = resolveTurnIndex(scenario, run.userText);
  const turn = scenario.turns[turnIndex]!;

  assertExpectedRunHistory(scenario, turnIndex, run);
  state.runSnapshots.push(run);
  connection.setContextTag(`seed${scenario.seed}:turn${turnIndex + 1}`);
  if (connection.interrupted) return;

  if (!(await exchangeRequestContext(connection, 10_000 + turnIndex))) return;
  if (shouldStartAtFinalOnly(run, turnIndex, turn)) {
    await emitFinalTurnResponse(connection, scenario, turnIndex, turn);
    return;
  }

  await playSemanticBatches(connection, scenario, state, turnIndex, turn);
  if (connection.interrupted) return;
  await emitFinalTurnResponse(connection, scenario, turnIndex, turn);
}

function buildTurn(
  seed: number,
  turnIndex: number,
  rng: Rng,
  overrides: { forceToolBatch?: boolean } = {},
): SemanticFuzzTurn {
  const turnLabel = `seed${seed}-turn${turnIndex + 1}`;
  const useToolBatch = overrides.forceToolBatch ?? rng.nextBoolean();
  const batches = useToolBatch ? [buildBatch(seed, turnIndex, rng)] : [];
  const toolSummary =
    batches.length > 0
      ? batches.flatMap((batch) => batch.toolCalls.map((toolCall) => toolCall.result)).join(" | ")
      : "no-tools";
  const finalReasoning = `Finalize ${turnLabel}.`;
  const finalContent = `Answer ${turnLabel}: ${toolSummary}.`;
  return {
    userText: `${turnLabel}-user`,
    batches,
    finalReasoning,
    finalContent,
    historyAssistantText: historyAssistantText(batches, finalContent),
  };
}

function buildBatch(seed: number, turnIndex: number, rng: Rng): SemanticFuzzBatch {
  const turnLabel = `seed${seed}-turn${turnIndex + 1}`;
  const callCount = 1 + rng.nextInt(2);
  const toolCalls: SemanticFuzzToolCall[] = [];
  for (let callIndex = 0; callIndex < callCount; callIndex++) {
    const text = `${turnLabel}-tool-${callIndex + 1}`;
    toolCalls.push({
      id: `${turnLabel}-tool-call-${callIndex + 1}`,
      text,
      result: `tool-result::${text}`,
    });
  }
  return {
    introReasoning: `Plan ${turnLabel} batch 1.`,
    introContent: `Calling tools for ${turnLabel}. `,
    toolCalls,
  };
}

function completedHistoryTurns(
  scenario: SemanticFuzzScenario,
  turnCount: number,
): Array<{ userText: string; assistantText: string }> {
  return scenario.turns.slice(0, turnCount).map((turn) => ({
    userText: turn.userText,
    assistantText: turn.historyAssistantText,
  }));
}

async function playSemanticBatches(
  connection: FakeRunConnection,
  scenario: SemanticFuzzScenario,
  state: SemanticFuzzScenarioState,
  turnIndex: number,
  turn: SemanticFuzzTurn,
): Promise<void> {
  for (const batch of turn.batches) {
    emitBatchPrelude(connection, batch);
    if (connection.interrupted) return;

    emitBatchToolCalls(connection, turnIndex, batch);
    if (connection.interrupted) return;

    connection.sendConversationCheckpoint(completedHistoryTurns(scenario, turnIndex));
    if (connection.interrupted) return;

    await awaitBatchResults(connection, state, turnIndex, batch);
    if (connection.interrupted) return;
  }
}

function emitBatchPrelude(connection: FakeRunConnection, batch: SemanticFuzzBatch): void {
  connection.sendThinkingDelta(batch.introReasoning);
  if (connection.interrupted) return;
  connection.sendTextDelta(batch.introContent);
}

function emitBatchToolCalls(
  connection: FakeRunConnection,
  turnIndex: number,
  batch: SemanticFuzzBatch,
): void {
  for (const [callIndex, toolCall] of batch.toolCalls.entries()) {
    connection.sendMcpToolCall(
      "echo_tool",
      { text: toolCall.text },
      {
        toolCallId: toolCall.id,
        execId: toolExecId(turnIndex, callIndex),
      },
    );
    if (connection.interrupted) return;
  }
}

async function awaitBatchResults(
  connection: FakeRunConnection,
  state: SemanticFuzzScenarioState,
  turnIndex: number,
  batch: SemanticFuzzBatch,
): Promise<void> {
  for (const [callIndex, toolCall] of batch.toolCalls.entries()) {
    const execId = toolExecId(turnIndex, callIndex);
    const mcpResult = await connection.waitForMcpResult(execId);
    if (mcpResult.text !== toolCall.result) {
      throw new Error(
        `Unexpected tool result for ${toolCall.id}: expected ${toolCall.result}, got ${mcpResult.text}`,
      );
    }
    state.observedToolResults.push(mcpResult.text);
    if (connection.interrupted) return;
    await connection.waitForExecStreamClose(execId);
    if (connection.interrupted) return;
  }
}

async function emitFinalTurnResponse(
  connection: FakeRunConnection,
  scenario: SemanticFuzzScenario,
  turnIndex: number,
  turn: SemanticFuzzTurn,
): Promise<void> {
  connection.sendThinkingDelta(turn.finalReasoning);
  if (connection.interrupted) return;
  connection.sendTextDelta(turn.finalContent);
  if (connection.interrupted) return;
  connection.sendConversationCheckpoint(completedHistoryTurns(scenario, turnIndex + 1));
  if (connection.interrupted) return;
  connection.sendEndStreamOk();
}

function expectedRunHistory(
  scenario: SemanticFuzzScenario,
  turnIndex: number,
  run: FakeRunRequestSnapshot,
): Array<{ userText: string; assistantText: string }> {
  const previousTurns = completedHistoryTurns(scenario, turnIndex);
  const currentTurn = scenario.turns[turnIndex]!;
  const completedCurrentTurn =
    run.turns.length === turnIndex + 1 &&
    run.turns[turnIndex]?.userText === currentTurn.userText &&
    run.turns[turnIndex]?.assistantText === currentTurn.historyAssistantText;
  return completedCurrentTurn ? completedHistoryTurns(scenario, turnIndex + 1) : previousTurns;
}

function historyAssistantText(batches: SemanticFuzzBatch[], finalContent: string): string {
  let text = "";
  for (const batch of batches) {
    text += batch.introContent;
    for (const toolCall of batch.toolCalls) {
      text += `\n[Tool echo_tool(${JSON.stringify({ text: toolCall.text })})]\n${toolCall.result}\n`;
    }
  }
  text += finalContent;
  return text;
}

function resolveTurnIndex(scenario: SemanticFuzzScenario, userText: string): number {
  const turnIndex = scenario.turns.findIndex((turn) => turn.userText === userText);
  if (turnIndex >= 0) return turnIndex;
  throw new Error(`Unknown semantic fuzz turn for user ${JSON.stringify(userText)}`);
}

function assertExpectedRunHistory(
  scenario: SemanticFuzzScenario,
  turnIndex: number,
  run: FakeRunRequestSnapshot,
): void {
  const expectedTurns = expectedRunHistory(scenario, turnIndex, run);
  if (JSON.stringify(run.turns) === JSON.stringify(expectedTurns)) return;
  throw new Error(
    `Unexpected run history for seed ${scenario.seed} turn ${turnIndex + 1}: expected ${JSON.stringify(expectedTurns)}, got ${JSON.stringify(run.turns)}`,
  );
}

function shouldStartAtFinalOnly(
  run: FakeRunRequestSnapshot,
  turnIndex: number,
  turn: SemanticFuzzTurn,
): boolean {
  return (
    run.turns[turnIndex]?.assistantText === turn.historyAssistantText && turn.batches.length > 0
  );
}

function findLatestUserText(messages: ReadonlyArray<OpenAIMessage>): string | null {
  for (let index = messages.length - 1; index >= 0; index--) {
    const message = messages[index]!;
    if (message.role !== "user") continue;
    return typeof message.content === "string" ? message.content : JSON.stringify(message.content);
  }
  return null;
}

function messagesAfterLatestUser(
  messages: ReadonlyArray<OpenAIMessage>,
): ReadonlyArray<OpenAIMessage> {
  for (let index = messages.length - 1; index >= 0; index--) {
    if (messages[index]!.role === "user") return messages.slice(index + 1);
  }
  return [];
}

async function exchangeRequestContext(
  connection: FakeRunConnection,
  execMessageId: number,
): Promise<boolean> {
  connection.sendRequestContextArgs(execMessageId);
  if (connection.interrupted) return false;
  await connection.waitForRequestContextResult(execMessageId);
  return !connection.interrupted;
}

interface Rng {
  next: () => number;
  nextInt: (limit: number) => number;
  nextBoolean: () => boolean;
}

function toolExecId(turnIndex: number, callIndex: number): number {
  return 20_000 + turnIndex * 100 + callIndex;
}

function createRng(seed: number): Rng {
  let state = seed >>> 0;
  const next = () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  return {
    next,
    nextInt(limit: number) {
      return Math.floor(next() * limit);
    },
    nextBoolean() {
      return next() >= 0.5;
    },
  };
}
