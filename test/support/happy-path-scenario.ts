import type { OpenAIMessage } from "../../src/openai-messages";
import {
  createEchoTools,
  exchangeRequestContext,
  findLatestUserText,
  formatEchoToolHistory,
} from "./conversation-test-helpers";
import type {
  FakeCursorBackend,
  FakeRunConnection,
  FakeRunRequestSnapshot,
} from "./fake-cursor-backend";
import type {
  ConversationDriverRequestTrace,
  ConversationDriverTurnTrace,
  OpenAIConversationDriver,
} from "./openai-conversation-driver";

export const HAPPY_PATH_SESSION_ID = "happy-path-session";
export const HAPPY_PATH_INITIAL_MESSAGES = [
  { role: "system", content: "You are a deterministic test assistant." as const },
];
export const HAPPY_PATH_TURN_1_TEXT = "hello alpha";
export const HAPPY_PATH_TURN_2_TEXT = "please use the tool for beta";
export const HAPPY_PATH_TURN_3_TEXT = "what have we discussed so far?";
export const HAPPY_PATH_TURN_1_ASSISTANT_TEXT = "Echo: hello alpha.";
export const HAPPY_PATH_TURN_2_HISTORY_ASSISTANT_TEXT =
  "Invoking echo_tool. " +
  formatEchoToolHistory("beta payload", "tool-result::beta payload") +
  "Tool says: tool-result::beta payload.";

export interface HappyPathScenarioState {
  runSnapshots: FakeRunRequestSnapshot[];
  toolResultTexts: string[];
}

export interface HappyPathConversationOutcome {
  turn1: ConversationDriverTurnTrace;
  turn2: ConversationDriverTurnTrace;
  turn3: ConversationDriverTurnTrace;
}

export function createHappyPathTools() {
  return createEchoTools();
}

export function validateHappyPathResponse(
  messages: ReadonlyArray<OpenAIMessage>,
  trace: ConversationDriverRequestTrace,
): string | null {
  const latestUserText = findLatestUserText(messages);
  if (!latestUserText) return "Happy-path request missing latest user";
  const hasToolResult = hasToolResultAfterLatestUser(messages);

  if (latestUserText === HAPPY_PATH_TURN_1_TEXT) {
    return trace.finishReason === "stop" &&
      trace.content === HAPPY_PATH_TURN_1_ASSISTANT_TEXT &&
      trace.reasoning === "Thinking about hello alpha.Planning concise echo."
      ? null
      : "Turn 1 response incomplete";
  }

  if (latestUserText === HAPPY_PATH_TURN_2_TEXT && !hasToolResult) {
    const toolCall = trace.toolCalls[0];
    return trace.finishReason === "tool_calls" &&
      trace.content === "Invoking echo_tool. " &&
      trace.reasoning === "Need the echo tool." &&
      trace.toolCalls.length === 1 &&
      toolCall?.id === "echo-beta" &&
      toolCall.function.name === "echo_tool" &&
      toolCall.function.arguments === '{"text":"beta payload"}'
      ? null
      : "Turn 2 tool request incomplete";
  }

  if (latestUserText === HAPPY_PATH_TURN_2_TEXT && hasToolResult) {
    return trace.finishReason === "stop" &&
      trace.content === "Tool says: tool-result::beta payload." &&
      trace.reasoning === "Tool completed successfully."
      ? null
      : "Turn 2 follow-up response incomplete";
  }

  if (latestUserText === HAPPY_PATH_TURN_3_TEXT) {
    return trace.finishReason === "stop" &&
      trace.content ===
        "History users: hello alpha | please use the tool for beta. Tool history: present." &&
      trace.reasoning === "Summarizing prior turns."
      ? null
      : "Turn 3 summary incomplete";
  }

  return `Unexpected happy-path user turn ${JSON.stringify(latestUserText)}`;
}

export function installHappyPathScenario(
  backend: FakeCursorBackend,
  state: HappyPathScenarioState = { runSnapshots: [], toolResultTexts: [] },
): HappyPathScenarioState {
  backend.setRunHandler(async (connection) => {
    const run = await connection.waitForRunRequest();
    const turnKey = identifyTurn(run);
    connection.setContextTag(turnKey);
    state.runSnapshots.push(run);
    if (connection.interrupted) return;

    if (turnKey === "turn1") {
      await handleTurn1(connection);
      return;
    }
    if (turnKey === "turn2") {
      await handleTurn2(connection, state);
      return;
    }
    await handleTurn3(connection, run);
  });
  return state;
}

export async function executeHappyPathConversation(
  driver: OpenAIConversationDriver,
): Promise<HappyPathConversationOutcome> {
  return {
    turn1: await driver.runTurn(HAPPY_PATH_TURN_1_TEXT),
    turn2: await driver.runTurn(HAPPY_PATH_TURN_2_TEXT),
    turn3: await driver.runTurn(HAPPY_PATH_TURN_3_TEXT),
  };
}

async function handleTurn1(connection: FakeRunConnection): Promise<void> {
  if (!(await exchangeRequestContext(connection, 101))) return;
  connection.sendThinkingDelta("Thinking about hello alpha.");
  if (connection.interrupted) return;
  connection.sendTextDelta("Echo: ");
  if (connection.interrupted) return;
  connection.sendThinkingDelta("Planning concise echo.");
  if (connection.interrupted) return;
  connection.sendTextDelta("hello alpha.");
  if (connection.interrupted) return;
  connection.sendEndStreamOk();
}

async function handleTurn2(
  connection: FakeRunConnection,
  state: HappyPathScenarioState,
): Promise<void> {
  if (!(await exchangeRequestContext(connection, 201))) return;
  connection.sendThinkingDelta("Need the echo tool.");
  if (connection.interrupted) return;
  connection.sendTextDelta("Invoking echo_tool. ");
  if (connection.interrupted) return;
  connection.sendMcpToolCall(
    "echo_tool",
    { text: "beta payload" },
    { toolCallId: "echo-beta", execId: 202 },
  );
  if (connection.interrupted) return;
  connection.sendConversationCheckpoint([
    { userText: HAPPY_PATH_TURN_1_TEXT, assistantText: HAPPY_PATH_TURN_1_ASSISTANT_TEXT },
  ]);
  if (connection.interrupted) return;

  const mcpResult = await connection.waitForMcpResult(202);
  state.toolResultTexts.push(mcpResult.text);
  if (connection.interrupted) return;
  await connection.waitForExecStreamClose(202);
  if (connection.interrupted) return;

  connection.sendThinkingDelta("Tool completed successfully.");
  if (connection.interrupted) return;
  connection.sendTextDelta(`Tool says: ${mcpResult.text}.`);
  if (connection.interrupted) return;
  connection.sendConversationCheckpoint([
    { userText: HAPPY_PATH_TURN_1_TEXT, assistantText: HAPPY_PATH_TURN_1_ASSISTANT_TEXT },
    {
      userText: HAPPY_PATH_TURN_2_TEXT,
      assistantText: HAPPY_PATH_TURN_2_HISTORY_ASSISTANT_TEXT,
    },
  ]);
  if (connection.interrupted) return;
  connection.sendEndStreamOk();
}

async function handleTurn3(
  connection: FakeRunConnection,
  run: FakeRunRequestSnapshot,
): Promise<void> {
  const userHistory = run.turns.map((turn) => turn.userText).join(" | ");
  const sawToolHistory = run.turns.some((turn) =>
    turn.assistantText.includes('[Tool echo_tool({"text":"beta payload"})]'),
  );
  if (!(await exchangeRequestContext(connection, 301))) return;
  connection.sendThinkingDelta("Summarizing prior turns.");
  if (connection.interrupted) return;
  connection.sendTextDelta(
    `History users: ${userHistory}. Tool history: ${sawToolHistory ? "present" : "missing"}.`,
  );
  if (connection.interrupted) return;
  connection.sendEndStreamOk();
}

function identifyTurn(run: FakeRunRequestSnapshot): "turn1" | "turn2" | "turn3" {
  if (run.userText === HAPPY_PATH_TURN_1_TEXT) return "turn1";
  if (run.userText === HAPPY_PATH_TURN_2_TEXT) return "turn2";
  if (run.userText === HAPPY_PATH_TURN_3_TEXT) return "turn3";
  throw new Error(
    `Unexpected happy-path run request: user=${JSON.stringify(run.userText)} turns=${run.turns.length}`,
  );
}

function hasToolResultAfterLatestUser(messages: ReadonlyArray<OpenAIMessage>): boolean {
  for (let index = messages.length - 1; index >= 0; index--) {
    const message = messages[index]!;
    if (message.role === "tool") return true;
    if (message.role === "user") return false;
  }
  return false;
}
