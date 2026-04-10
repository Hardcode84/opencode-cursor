import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { isDeepStrictEqual } from "node:util";
import { turnsFingerprint } from "../src/conversation-state";
import type { OpenAIMessage } from "../src/openai-messages";
import { deriveConversationKey, OPENCODE_AGENT_COMPACTION } from "../src/server";
import {
  createEchoTools,
  exchangeRequestContextOrThrow,
  formatEchoToolHistory,
  readEchoToolText,
} from "./support/conversation-test-helpers";
import {
  type FakeConversationTurnSnapshot,
  FakeCursorBackend,
  type FakeMcpResultSnapshot,
  type FakeRunRequestSnapshot,
} from "./support/fake-cursor-backend";
import {
  type ConversationDriverOptions,
  OpenAIConversationDriver,
} from "./support/openai-conversation-driver";
import { type ProxyHarness, startProxyHarness } from "./support/proxy-harness";

let backend: FakeCursorBackend | undefined;
let proxy: ProxyHarness | undefined;

const MODEL_ID = "test-model";
const SESSION_ID = "compaction-e2e-session";
const PRIMARY_SYSTEM = "You are a helpful build assistant.";
const COMPACTION_SYSTEM = "You compress prior conversation into a short durable summary.";
const BASELINE_USER = "Remember that alpha is 7 and beta is 9.";
const BASELINE_ASSISTANT = "Stored the facts: alpha is 7 and beta is 9.";
const COMPACTION_USER = "Compress the conversation so only alpha survives.";
const COMPACTION_ASSISTANT = "Compaction checkpoint saved.";
const SUMMARY_USER = "Conversation summary";
const SUMMARY_ASSISTANT = "alpha is 7.";
const FOLLOW_UP_USER = "What is alpha?";
const FOLLOW_UP_ASSISTANT = "Alpha is 7.";
const TOOL_USER = "Please use the echo tool for alpha.";
const TOOL_CALL_ID = "compaction-tool-call";
const TOOL_ARG_TEXT = "alpha tool";
const TOOL_RESULT = "tool-result::alpha tool";
const TOOL_ASSISTANT_PREFIX = "Using echo tool.";
const TOOL_ASSISTANT_FINAL = `Tool says: ${TOOL_RESULT}.`;
const TOOL_ASSISTANT_WITH_HISTORY = `${TOOL_ASSISTANT_PREFIX}${formatEchoToolHistory(
  TOOL_ARG_TEXT,
  TOOL_RESULT,
)}${TOOL_ASSISTANT_FINAL}`;
const TOOL_SUMMARY_ASSISTANT = `Echo tool already returned ${TOOL_RESULT}.`;
const TOOL_COMPACTION_ASSISTANT = "Compaction summary with tool saved.";
const TOOL_FOLLOW_UP_ASSISTANT = "Summary kept the prior tool result.";
const ARCHIVED_FOLLOW_UP_USER = "Continue the original verbose thread.";
const ANONYMOUS_FOLLOW_UP_ASSISTANT = "Anonymous sessions stayed isolated.";
const ARCHIVED_RESTORE_ASSISTANT = "Restored archived verbose history.";
const SECOND_VERBOSE_USER = "What value did beta have?";
const SECOND_VERBOSE_ASSISTANT = "Beta was 9.";
const PARENT_A = "parent-a";
const PARENT_B = "parent-b";
const PARENT_A_FOLLOW_UP_ASSISTANT = "Parent A kept verbose history.";
const PARENT_B_FOLLOW_UP_ASSISTANT = "Parent B kept summary history.";
const MULTI_ARCHIVE_RESTORE_ASSISTANT = "Restored archived multi-turn verbose history.";
const RECOVERED_AFTER_INVALID_ARCHIVED_CHECKPOINT = "Recovered after invalid archived checkpoint.";
const INVALID_ARCHIVED_CHECKPOINT_BASE64 = Buffer.from([0x0f]).toString("base64");
const PRIMARY_SYSTEM_MESSAGES = [
  { role: "system", content: PRIMARY_SYSTEM },
] satisfies OpenAIMessage[];
const BASELINE_TURN: FakeConversationTurnSnapshot = {
  userText: BASELINE_USER,
  assistantText: BASELINE_ASSISTANT,
};
const SUMMARY_TURN: FakeConversationTurnSnapshot = {
  userText: SUMMARY_USER,
  assistantText: SUMMARY_ASSISTANT,
};
const SECOND_VERBOSE_TURN: FakeConversationTurnSnapshot = {
  userText: SECOND_VERBOSE_USER,
  assistantText: SECOND_VERBOSE_ASSISTANT,
};
const TOOL_SUMMARY_TURN: FakeConversationTurnSnapshot = {
  userText: SUMMARY_USER,
  assistantText: TOOL_SUMMARY_ASSISTANT,
};
// Compaction requests share the same session/parent affinity as the primary
// thread, but switch proxy behavior via the exported compaction agent token.
const COMPACTION_DRIVER_CONTEXT = { opencodeAgent: OPENCODE_AGENT_COMPACTION } as const;

afterEach(async () => {
  await proxy?.close();
  await backend?.close();
  proxy = undefined;
  backend = undefined;
});

interface PersistedConversationFile {
  checkpoint: string | null;
  checkpointArchive?: Record<string, string>;
}

function createDriver(
  options: Omit<ConversationDriverOptions, "baseUrl" | "model">,
): OpenAIConversationDriver {
  return new OpenAIConversationDriver({
    baseUrl: proxy!.baseUrl,
    model: MODEL_ID,
    ...options,
  });
}

function queueCheckpointRun(
  runSnapshots: FakeRunRequestSnapshot[],
  execMessageId: number,
  assistantText: string,
  turns: FakeConversationTurnSnapshot[],
  pendingToolCalls: string[] = [],
): void {
  backend!.enqueueRun(async (connection) => {
    runSnapshots.push(await connection.waitForRunRequest());
    await exchangeRequestContextOrThrow(connection, execMessageId);
    connection.sendTextDelta(assistantText);
    connection.sendConversationCheckpoint(turns, { pendingToolCalls });
    connection.sendEndStreamOk();
  });
}

function queueTextOnlyRun(
  runSnapshots: FakeRunRequestSnapshot[],
  execMessageId: number,
  assistantText: string,
): void {
  backend!.enqueueRun(async (connection) => {
    runSnapshots.push(await connection.waitForRunRequest());
    await exchangeRequestContextOrThrow(connection, execMessageId);
    connection.sendTextDelta(assistantText);
    connection.sendEndStreamOk();
  });
}

function queueSnapshotExpectationRun(
  runSnapshots: FakeRunRequestSnapshot[],
  execMessageId: number,
  expectedTurns: FakeConversationTurnSnapshot[],
  expectedPendingToolCalls: string[],
  successText: string,
  failureLabel: string,
): void {
  backend!.enqueueRun(async (connection) => {
    const snapshot = await connection.waitForRunRequest();
    runSnapshots.push(snapshot);
    await exchangeRequestContextOrThrow(connection, execMessageId);
    connection.sendTextDelta(
      matchesSnapshot(snapshot, expectedTurns, expectedPendingToolCalls)
        ? successText
        : unexpectedSnapshotMessage(failureLabel, snapshot),
    );
    connection.sendEndStreamOk();
  });
}

function matchesSnapshot(
  snapshot: FakeRunRequestSnapshot,
  expectedTurns: FakeConversationTurnSnapshot[],
  expectedPendingToolCalls: string[],
): boolean {
  return (
    isDeepStrictEqual(snapshot.turns, expectedTurns) &&
    isDeepStrictEqual(snapshot.pendingToolCalls, expectedPendingToolCalls)
  );
}

function unexpectedSnapshotMessage(failureLabel: string, snapshot: FakeRunRequestSnapshot): string {
  return `${failureLabel} ${JSON.stringify({
    conversationId: snapshot.conversationId,
    userText: snapshot.userText,
    turns: snapshot.turns,
    pendingToolCalls: snapshot.pendingToolCalls,
  })}`;
}

function conversationFilePath(
  messages: OpenAIMessage[],
  sessionId: string,
  parentSessionId?: string,
): string {
  const convKey = deriveConversationKey(messages, sessionId, parentSessionId);
  return join(proxy!.runtimeConfig.conversationDiskDir, `${convKey}.json`);
}

function readPersistedConversationFile(path: string): PersistedConversationFile {
  return JSON.parse(readFileSync(path, "utf-8")) as PersistedConversationFile;
}

function writePersistedConversationFile(path: string, data: PersistedConversationFile): void {
  writeFileSync(path, JSON.stringify(data));
}

function sortedArchiveKeys(data: PersistedConversationFile): string[] {
  return Object.keys(data.checkpointArchive ?? {}).sort();
}

describe("conversation compaction flow", () => {
  // Core compaction behavior: primary history is archived, the summary lineage
  // becomes active, and restart still reloads the compacted checkpoint.
  test("preserves lineage and resumes from the compressed checkpoint", async () => {
    backend = await FakeCursorBackend.start();
    const runSnapshots: FakeRunRequestSnapshot[] = [];
    queueCheckpointRun(
      runSnapshots,
      701,
      BASELINE_ASSISTANT,
      [BASELINE_TURN],
      ["baseline-checkpoint"],
    );
    queueCheckpointRun(
      runSnapshots,
      702,
      COMPACTION_ASSISTANT,
      [SUMMARY_TURN],
      ["summary-checkpoint"],
    );
    queueTextOnlyRun(runSnapshots, 703, FOLLOW_UP_ASSISTANT);

    proxy = await startProxyHarness({
      runtimeConfig: {
        apiUrl: backend.apiUrl,
        agentUrl: backend.agentUrl,
      },
    });

    const baselineDriver = createDriver({
      sessionId: SESSION_ID,
      initialMessages: PRIMARY_SYSTEM_MESSAGES,
    });
    const baseline = await baselineDriver.runTurn(BASELINE_USER);

    const compactionDriver = createDriver({
      sessionId: SESSION_ID,
      ...COMPACTION_DRIVER_CONTEXT,
      initialMessages: [
        { role: "system", content: COMPACTION_SYSTEM },
        { role: "user", content: BASELINE_USER },
        { role: "assistant", content: BASELINE_ASSISTANT },
      ],
    });
    const compaction = await compactionDriver.runTurn(COMPACTION_USER);

    const followUpDriver = createDriver({
      sessionId: SESSION_ID,
      initialMessages: [
        { role: "system", content: PRIMARY_SYSTEM },
        { role: "user", content: SUMMARY_USER },
        { role: "assistant", content: SUMMARY_ASSISTANT },
      ],
    });
    const followUp = await followUpDriver.runTurn(FOLLOW_UP_USER);

    expect(baseline.assistantText).toBe(BASELINE_ASSISTANT);
    expect(compaction.assistantText).toBe(COMPACTION_ASSISTANT);
    expect(followUp.assistantText).toBe(FOLLOW_UP_ASSISTANT);

    expect(backend.runCount).toBe(3);
    expect(runSnapshots).toHaveLength(3);
    expect(runSnapshots[0]!.conversationId).not.toBe("");
    expect(runSnapshots[0]!.conversationId).toBe(runSnapshots[1]!.conversationId);
    expect(runSnapshots[1]!.conversationId).toBe(runSnapshots[2]!.conversationId);

    expect(runSnapshots[0]!.userText).toBe(BASELINE_USER);
    expect(runSnapshots[0]!.turns).toEqual([]);
    expect(runSnapshots[0]!.pendingToolCalls).toEqual([]);

    expect(runSnapshots[1]!.userText).toBe(COMPACTION_USER);
    expect(runSnapshots[1]!.turns).toEqual([BASELINE_TURN]);
    expect(runSnapshots[1]!.pendingToolCalls).toEqual([]);

    expect(runSnapshots[2]!.userText).toBe(FOLLOW_UP_USER);
    expect(runSnapshots[2]!.turns).toEqual([SUMMARY_TURN]);
    expect(runSnapshots[2]!.turns).not.toEqual([BASELINE_TURN]);
    expect(runSnapshots[2]!.pendingToolCalls).toEqual(["summary-checkpoint"]);
  }, 15_000);

  test("restores the compressed checkpoint after a proxy restart", async () => {
    backend = await FakeCursorBackend.start();
    const runSnapshots: FakeRunRequestSnapshot[] = [];
    queueCheckpointRun(
      runSnapshots,
      711,
      BASELINE_ASSISTANT,
      [BASELINE_TURN],
      ["baseline-checkpoint"],
    );
    queueCheckpointRun(
      runSnapshots,
      712,
      COMPACTION_ASSISTANT,
      [SUMMARY_TURN],
      ["summary-checkpoint"],
    );
    queueTextOnlyRun(runSnapshots, 713, FOLLOW_UP_ASSISTANT);

    proxy = await startProxyHarness({
      runtimeConfig: {
        apiUrl: backend.apiUrl,
        agentUrl: backend.agentUrl,
      },
    });

    const baselineDriver = createDriver({
      sessionId: SESSION_ID,
      initialMessages: PRIMARY_SYSTEM_MESSAGES,
    });
    const compactionDriver = createDriver({
      sessionId: SESSION_ID,
      ...COMPACTION_DRIVER_CONTEXT,
      initialMessages: [
        { role: "system", content: COMPACTION_SYSTEM },
        { role: "user", content: BASELINE_USER },
        { role: "assistant", content: BASELINE_ASSISTANT },
      ],
    });

    const baseline = await baselineDriver.runTurn(BASELINE_USER);
    const compaction = await compactionDriver.runTurn(COMPACTION_USER);

    await proxy.restart();

    const followUpDriver = createDriver({
      sessionId: SESSION_ID,
      initialMessages: [
        { role: "system", content: PRIMARY_SYSTEM },
        { role: "user", content: SUMMARY_USER },
        { role: "assistant", content: SUMMARY_ASSISTANT },
      ],
    });
    const followUp = await followUpDriver.runTurn(FOLLOW_UP_USER);

    expect(baseline.assistantText).toBe(BASELINE_ASSISTANT);
    expect(compaction.assistantText).toBe(COMPACTION_ASSISTANT);
    expect(followUp.assistantText).toBe(FOLLOW_UP_ASSISTANT);

    expect(backend.runCount).toBe(3);
    expect(runSnapshots).toHaveLength(3);
    expect(runSnapshots[0]!.conversationId).not.toBe("");
    expect(runSnapshots[0]!.conversationId).toBe(runSnapshots[1]!.conversationId);
    expect(runSnapshots[1]!.conversationId).toBe(runSnapshots[2]!.conversationId);

    expect(runSnapshots[2]!.userText).toBe(FOLLOW_UP_USER);
    expect(runSnapshots[2]!.turns).toEqual([SUMMARY_TURN]);
    expect(runSnapshots[2]!.turns).not.toEqual([BASELINE_TURN]);
    expect(runSnapshots[2]!.pendingToolCalls).toEqual(["summary-checkpoint"]);
  }, 15_000);

  // Tool-specific compaction behavior: compacted follow-ups must reuse the
  // completed tool result instead of reissuing the same execution.
  test("compaction after tool calls keeps the tool completed instead of replaying it", async () => {
    backend = await FakeCursorBackend.start();
    const runSnapshots: FakeRunRequestSnapshot[] = [];
    const observedToolResults: FakeMcpResultSnapshot[] = [];

    backend.enqueueRun(async (connection) => {
      runSnapshots.push(await connection.waitForRunRequest());
      await exchangeRequestContextOrThrow(connection, 721);
      connection.sendTextDelta(TOOL_ASSISTANT_PREFIX);
      connection.sendMcpToolCall(
        "echo_tool",
        { text: TOOL_ARG_TEXT },
        { toolCallId: TOOL_CALL_ID, execId: 722 },
      );
      connection.sendConversationCheckpoint([]);

      const mcpResult = await connection.waitForMcpResult(722);
      observedToolResults.push(mcpResult);
      await connection.waitForExecStreamClose(722);

      connection.sendTextDelta(TOOL_ASSISTANT_FINAL);
      connection.sendConversationCheckpoint([
        { userText: TOOL_USER, assistantText: TOOL_ASSISTANT_WITH_HISTORY },
      ]);
      connection.sendEndStreamOk();
    });

    backend.enqueueRun(async (connection) => {
      runSnapshots.push(await connection.waitForRunRequest());
      await exchangeRequestContextOrThrow(connection, 723);
      connection.sendTextDelta(TOOL_COMPACTION_ASSISTANT);
      connection.sendConversationCheckpoint([TOOL_SUMMARY_TURN], {
        pendingToolCalls: ["summary-tool-checkpoint"],
      });
      connection.sendEndStreamOk();
    });

    backend.enqueueRun(async (connection) => {
      const snapshot = await connection.waitForRunRequest();
      runSnapshots.push(snapshot);
      await exchangeRequestContextOrThrow(connection, 724);

      if (!matchesSnapshot(snapshot, [TOOL_SUMMARY_TURN], ["summary-tool-checkpoint"])) {
        connection.sendMcpToolCall(
          "echo_tool",
          { text: TOOL_ARG_TEXT },
          { toolCallId: TOOL_CALL_ID, execId: 725 },
        );
        connection.sendConversationCheckpoint([TOOL_SUMMARY_TURN], {
          pendingToolCalls: ["summary-tool-checkpoint"],
        });
        const replayedResult = await connection.waitForMcpResult(725);
        observedToolResults.push(replayedResult);
        await connection.waitForExecStreamClose(725);
        connection.sendTextDelta(unexpectedSnapshotMessage("Unexpected tool replay.", snapshot));
        connection.sendEndStreamOk();
        return;
      }

      connection.sendTextDelta(TOOL_FOLLOW_UP_ASSISTANT);
      connection.sendEndStreamOk();
    });

    proxy = await startProxyHarness({
      runtimeConfig: {
        apiUrl: backend.apiUrl,
        agentUrl: backend.agentUrl,
      },
    });

    let executorInvocations = 0;
    const executeEchoTool = (args: unknown) => {
      executorInvocations++;
      return `tool-result::${readEchoToolText(args)}`;
    };

    const baselineDriver = createDriver({
      sessionId: SESSION_ID,
      tools: createEchoTools(),
      toolExecutors: {
        echo_tool: executeEchoTool,
      },
      initialMessages: PRIMARY_SYSTEM_MESSAGES,
    });
    const baseline = await baselineDriver.runTurn(TOOL_USER);

    const compactionDriver = createDriver({
      sessionId: SESSION_ID,
      ...COMPACTION_DRIVER_CONTEXT,
      initialMessages: [
        { role: "system", content: COMPACTION_SYSTEM },
        ...baselineDriver.messages.filter((message) => message.role !== "system"),
      ],
    });
    const compaction = await compactionDriver.runTurn(COMPACTION_USER);

    const followUpDriver = createDriver({
      sessionId: SESSION_ID,
      tools: createEchoTools(),
      toolExecutors: {
        echo_tool: executeEchoTool,
      },
      initialMessages: [
        { role: "system", content: PRIMARY_SYSTEM },
        { role: "user", content: SUMMARY_USER },
        { role: "assistant", content: TOOL_SUMMARY_ASSISTANT },
      ],
    });
    const followUp = await followUpDriver.runTurn("Use the saved tool result.");

    // Streamed text omits the persisted tool-history block; that formatted
    // transcript is only stored in the checkpoint (`TOOL_ASSISTANT_WITH_HISTORY`).
    expect(baseline.assistantText).toBe(`${TOOL_ASSISTANT_PREFIX}${TOOL_ASSISTANT_FINAL}`);
    expect(baseline.toolExecutions).toHaveLength(1);
    expect(baseline.toolExecutions[0]!.toolCallId).toBe(TOOL_CALL_ID);
    expect(compaction.assistantText).toBe(TOOL_COMPACTION_ASSISTANT);
    expect(followUp.assistantText).toBe(TOOL_FOLLOW_UP_ASSISTANT);
    expect(followUp.toolExecutions).toHaveLength(0);

    expect(executorInvocations).toBe(1);
    expect(observedToolResults.map((result) => result.text)).toEqual([TOOL_RESULT]);

    expect(backend.runCount).toBe(3);
    expect(runSnapshots).toHaveLength(3);
    expect(runSnapshots[0]!.turns).toEqual([]);
    expect(runSnapshots[1]!.turns).toEqual([
      { userText: TOOL_USER, assistantText: TOOL_ASSISTANT_WITH_HISTORY },
    ]);
    expect(runSnapshots[2]!.turns).toEqual([TOOL_SUMMARY_TURN]);
    expect(runSnapshots[2]!.pendingToolCalls).toEqual(["summary-tool-checkpoint"]);
  }, 15_000);

  // Persistence hardening: bad archived checkpoints should be scrubbed and the
  // next request should rebuild from the caller-provided summary messages.
  test("malformed archived checkpoints are ignored and rebuilt from summary messages", async () => {
    backend = await FakeCursorBackend.start();
    const runSnapshots: FakeRunRequestSnapshot[] = [];
    queueCheckpointRun(
      runSnapshots,
      731,
      BASELINE_ASSISTANT,
      [BASELINE_TURN],
      ["baseline-checkpoint"],
    );
    queueCheckpointRun(
      runSnapshots,
      732,
      COMPACTION_ASSISTANT,
      [SUMMARY_TURN],
      ["summary-checkpoint"],
    );
    queueTextOnlyRun(runSnapshots, 733, RECOVERED_AFTER_INVALID_ARCHIVED_CHECKPOINT);

    proxy = await startProxyHarness({
      runtimeConfig: {
        apiUrl: backend.apiUrl,
        agentUrl: backend.agentUrl,
      },
    });

    const baselineDriver = createDriver({
      sessionId: SESSION_ID,
      initialMessages: PRIMARY_SYSTEM_MESSAGES,
    });
    const compactionDriver = createDriver({
      sessionId: SESSION_ID,
      ...COMPACTION_DRIVER_CONTEXT,
      initialMessages: [
        { role: "system", content: COMPACTION_SYSTEM },
        { role: "user", content: BASELINE_USER },
        { role: "assistant", content: BASELINE_ASSISTANT },
      ],
    });

    const baseline = await baselineDriver.runTurn(BASELINE_USER);
    const compaction = await compactionDriver.runTurn(COMPACTION_USER);

    const conversationFile = conversationFilePath(PRIMARY_SYSTEM_MESSAGES, SESSION_ID);
    const summaryFingerprint = turnsFingerprint([SUMMARY_TURN]);
    const persistedBefore = readPersistedConversationFile(conversationFile);
    expect(persistedBefore.checkpoint).not.toBeNull();
    expect(persistedBefore.checkpointArchive?.[summaryFingerprint]).toBeUndefined();

    writePersistedConversationFile(conversationFile, {
      ...persistedBefore,
      checkpoint: null,
      checkpointArchive: {
        ...(persistedBefore.checkpointArchive ?? {}),
        [summaryFingerprint]: INVALID_ARCHIVED_CHECKPOINT_BASE64,
      },
    });

    await proxy.restart();

    const followUpDriver = createDriver({
      sessionId: SESSION_ID,
      initialMessages: [
        { role: "system", content: PRIMARY_SYSTEM },
        { role: "user", content: SUMMARY_USER },
        { role: "assistant", content: SUMMARY_ASSISTANT },
      ],
    });
    const followUp = await followUpDriver.runTurn(FOLLOW_UP_USER);

    expect(baseline.assistantText).toBe(BASELINE_ASSISTANT);
    expect(compaction.assistantText).toBe(COMPACTION_ASSISTANT);
    expect(followUp.assistantText).toBe(RECOVERED_AFTER_INVALID_ARCHIVED_CHECKPOINT);

    expect(backend.runCount).toBe(3);
    expect(runSnapshots).toHaveLength(3);
    expect(runSnapshots[0]!.conversationId).not.toBe("");
    expect(runSnapshots[0]!.conversationId).toBe(runSnapshots[1]!.conversationId);
    expect(runSnapshots[1]!.conversationId).toBe(runSnapshots[2]!.conversationId);
    expect(runSnapshots[2]!.turns).toEqual([SUMMARY_TURN]);
    expect(runSnapshots[2]!.pendingToolCalls).toEqual([]);

    const persistedAfter = readPersistedConversationFile(conversationFile);
    expect(persistedAfter.checkpoint).toBeNull();
    expect(persistedAfter.checkpointArchive?.[summaryFingerprint]).toBeUndefined();
  }, 15_000);

  // Session scoping rules: anonymous and parent-scoped threads should not
  // bleed compaction state into the wrong lineage.
  test("anonymous compaction does not reuse the primary conversation state", async () => {
    backend = await FakeCursorBackend.start();
    const runSnapshots: FakeRunRequestSnapshot[] = [];
    queueCheckpointRun(
      runSnapshots,
      741,
      BASELINE_ASSISTANT,
      [BASELINE_TURN],
      ["baseline-checkpoint"],
    );
    queueCheckpointRun(
      runSnapshots,
      742,
      COMPACTION_ASSISTANT,
      [SUMMARY_TURN],
      ["summary-checkpoint"],
    );
    queueSnapshotExpectationRun(
      runSnapshots,
      743,
      [BASELINE_TURN],
      ["baseline-checkpoint"],
      ANONYMOUS_FOLLOW_UP_ASSISTANT,
      "Unexpected anonymous compaction leak.",
    );

    proxy = await startProxyHarness({
      runtimeConfig: {
        apiUrl: backend.apiUrl,
        agentUrl: backend.agentUrl,
      },
    });

    const baselineDriver = createDriver({
      sessionId: "",
      initialMessages: PRIMARY_SYSTEM_MESSAGES,
    });
    const compactionDriver = createDriver({
      sessionId: "",
      ...COMPACTION_DRIVER_CONTEXT,
      initialMessages: [
        { role: "system", content: COMPACTION_SYSTEM },
        { role: "user", content: BASELINE_USER },
        { role: "assistant", content: BASELINE_ASSISTANT },
      ],
    });
    const followUpDriver = createDriver({
      sessionId: "",
      initialMessages: [
        { role: "system", content: PRIMARY_SYSTEM },
        { role: "user", content: SUMMARY_USER },
        { role: "assistant", content: SUMMARY_ASSISTANT },
      ],
    });

    const baseline = await baselineDriver.runTurn(BASELINE_USER);
    const compaction = await compactionDriver.runTurn(COMPACTION_USER);
    const followUp = await followUpDriver.runTurn(FOLLOW_UP_USER);

    expect(baseline.assistantText).toBe(BASELINE_ASSISTANT);
    expect(compaction.assistantText).toBe(COMPACTION_ASSISTANT);
    expect(followUp.assistantText).toBe(ANONYMOUS_FOLLOW_UP_ASSISTANT);

    expect(backend.runCount).toBe(3);
    expect(runSnapshots).toHaveLength(3);
    expect(runSnapshots[0]!.conversationId).not.toBe("");
    expect(runSnapshots[0]!.conversationId).toBe(runSnapshots[2]!.conversationId);
    expect(runSnapshots[1]!.conversationId).not.toBe(runSnapshots[0]!.conversationId);
    expect(runSnapshots[1]!.turns).toEqual([BASELINE_TURN]);
    expect(runSnapshots[2]!.turns).toEqual([BASELINE_TURN]);
    expect(runSnapshots[2]!.turns).not.toEqual([SUMMARY_TURN]);
    expect(runSnapshots[2]!.pendingToolCalls).toEqual(["baseline-checkpoint"]);
  }, 15_000);

  // Archived lineage restore: older verbose fingerprints should still restore
  // after compaction archived them away.
  test("archived pre-compaction lineage can be restored by the old history fingerprint", async () => {
    backend = await FakeCursorBackend.start();
    const runSnapshots: FakeRunRequestSnapshot[] = [];
    queueCheckpointRun(
      runSnapshots,
      751,
      BASELINE_ASSISTANT,
      [BASELINE_TURN],
      ["baseline-checkpoint"],
    );
    queueCheckpointRun(
      runSnapshots,
      752,
      COMPACTION_ASSISTANT,
      [SUMMARY_TURN],
      ["summary-checkpoint"],
    );
    queueSnapshotExpectationRun(
      runSnapshots,
      753,
      [BASELINE_TURN],
      ["baseline-checkpoint"],
      ARCHIVED_RESTORE_ASSISTANT,
      "Failed to restore archived verbose history.",
    );

    proxy = await startProxyHarness({
      runtimeConfig: {
        apiUrl: backend.apiUrl,
        agentUrl: backend.agentUrl,
      },
    });

    const baselineDriver = createDriver({
      sessionId: SESSION_ID,
      initialMessages: PRIMARY_SYSTEM_MESSAGES,
    });
    const compactionDriver = createDriver({
      sessionId: SESSION_ID,
      ...COMPACTION_DRIVER_CONTEXT,
      initialMessages: [
        { role: "system", content: COMPACTION_SYSTEM },
        { role: "user", content: BASELINE_USER },
        { role: "assistant", content: BASELINE_ASSISTANT },
      ],
    });
    const archivedHistoryDriver = createDriver({
      sessionId: SESSION_ID,
      initialMessages: [
        { role: "system", content: PRIMARY_SYSTEM },
        { role: "user", content: BASELINE_USER },
        { role: "assistant", content: BASELINE_ASSISTANT },
      ],
    });

    const baseline = await baselineDriver.runTurn(BASELINE_USER);
    const compaction = await compactionDriver.runTurn(COMPACTION_USER);
    const archivedFollowUp = await archivedHistoryDriver.runTurn(ARCHIVED_FOLLOW_UP_USER);

    expect(baseline.assistantText).toBe(BASELINE_ASSISTANT);
    expect(compaction.assistantText).toBe(COMPACTION_ASSISTANT);
    expect(archivedFollowUp.assistantText).toBe(ARCHIVED_RESTORE_ASSISTANT);

    expect(backend.runCount).toBe(3);
    expect(runSnapshots).toHaveLength(3);
    expect(runSnapshots[0]!.conversationId).not.toBe("");
    expect(runSnapshots[0]!.conversationId).toBe(runSnapshots[1]!.conversationId);
    expect(runSnapshots[1]!.conversationId).toBe(runSnapshots[2]!.conversationId);
    expect(runSnapshots[1]!.turns).toEqual([BASELINE_TURN]);
    expect(runSnapshots[2]!.turns).toEqual([BASELINE_TURN]);
    expect(runSnapshots[2]!.turns).not.toEqual([SUMMARY_TURN]);
    expect(runSnapshots[2]!.pendingToolCalls).toEqual(["baseline-checkpoint"]);
  }, 15_000);

  test("parent session IDs isolate compaction state within the same session affinity", async () => {
    backend = await FakeCursorBackend.start();
    const runSnapshots: FakeRunRequestSnapshot[] = [];
    queueCheckpointRun(
      runSnapshots,
      761,
      BASELINE_ASSISTANT,
      [BASELINE_TURN],
      ["parent-a-checkpoint"],
    );
    queueCheckpointRun(
      runSnapshots,
      762,
      COMPACTION_ASSISTANT,
      [SUMMARY_TURN],
      ["parent-b-summary"],
    );
    queueSnapshotExpectationRun(
      runSnapshots,
      763,
      [BASELINE_TURN],
      ["parent-a-checkpoint"],
      PARENT_A_FOLLOW_UP_ASSISTANT,
      "Parent A state leaked.",
    );
    queueSnapshotExpectationRun(
      runSnapshots,
      764,
      [SUMMARY_TURN],
      ["parent-b-summary"],
      PARENT_B_FOLLOW_UP_ASSISTANT,
      "Parent B state leaked.",
    );

    proxy = await startProxyHarness({
      runtimeConfig: {
        apiUrl: backend.apiUrl,
        agentUrl: backend.agentUrl,
      },
    });

    const baselineDriver = createDriver({
      sessionId: SESSION_ID,
      parentSessionId: PARENT_A,
      initialMessages: PRIMARY_SYSTEM_MESSAGES,
    });
    const compactionDriver = createDriver({
      sessionId: SESSION_ID,
      parentSessionId: PARENT_B,
      ...COMPACTION_DRIVER_CONTEXT,
      initialMessages: [
        { role: "system", content: COMPACTION_SYSTEM },
        { role: "user", content: BASELINE_USER },
        { role: "assistant", content: BASELINE_ASSISTANT },
      ],
    });
    const followUpParentADriver = createDriver({
      sessionId: SESSION_ID,
      parentSessionId: PARENT_A,
      initialMessages: [
        { role: "system", content: PRIMARY_SYSTEM },
        { role: "user", content: BASELINE_USER },
        { role: "assistant", content: BASELINE_ASSISTANT },
      ],
    });
    const followUpParentBDriver = createDriver({
      sessionId: SESSION_ID,
      parentSessionId: PARENT_B,
      initialMessages: [
        { role: "system", content: PRIMARY_SYSTEM },
        { role: "user", content: SUMMARY_USER },
        { role: "assistant", content: SUMMARY_ASSISTANT },
      ],
    });

    const baseline = await baselineDriver.runTurn(BASELINE_USER);
    const compaction = await compactionDriver.runTurn(COMPACTION_USER);
    const followUpParentA = await followUpParentADriver.runTurn(FOLLOW_UP_USER);
    const followUpParentB = await followUpParentBDriver.runTurn(FOLLOW_UP_USER);

    expect(baseline.assistantText).toBe(BASELINE_ASSISTANT);
    expect(compaction.assistantText).toBe(COMPACTION_ASSISTANT);
    expect(followUpParentA.assistantText).toBe(PARENT_A_FOLLOW_UP_ASSISTANT);
    expect(followUpParentB.assistantText).toBe(PARENT_B_FOLLOW_UP_ASSISTANT);

    expect(backend.runCount).toBe(4);
    expect(runSnapshots).toHaveLength(4);
    expect(runSnapshots[0]!.conversationId).not.toBe("");
    expect(runSnapshots[0]!.conversationId).toBe(runSnapshots[2]!.conversationId);
    expect(runSnapshots[1]!.conversationId).toBe(runSnapshots[3]!.conversationId);
    expect(runSnapshots[0]!.conversationId).not.toBe(runSnapshots[1]!.conversationId);
    expect(runSnapshots[2]!.turns).toEqual([BASELINE_TURN]);
    expect(runSnapshots[2]!.pendingToolCalls).toEqual(["parent-a-checkpoint"]);
    expect(runSnapshots[3]!.turns).toEqual([SUMMARY_TURN]);
    expect(runSnapshots[3]!.pendingToolCalls).toEqual(["parent-b-summary"]);
  }, 15_000);

  test("multi-turn archived lineage remains restorable after compaction and restart", async () => {
    backend = await FakeCursorBackend.start();
    const runSnapshots: FakeRunRequestSnapshot[] = [];
    queueCheckpointRun(
      runSnapshots,
      771,
      BASELINE_ASSISTANT,
      [BASELINE_TURN],
      ["baseline-checkpoint"],
    );
    queueCheckpointRun(
      runSnapshots,
      772,
      SECOND_VERBOSE_ASSISTANT,
      [BASELINE_TURN, SECOND_VERBOSE_TURN],
      ["second-turn-checkpoint"],
    );
    queueCheckpointRun(
      runSnapshots,
      773,
      COMPACTION_ASSISTANT,
      [SUMMARY_TURN],
      ["summary-checkpoint"],
    );
    queueSnapshotExpectationRun(
      runSnapshots,
      774,
      [BASELINE_TURN, SECOND_VERBOSE_TURN],
      ["second-turn-checkpoint"],
      MULTI_ARCHIVE_RESTORE_ASSISTANT,
      "Failed to restore archived multi-turn history.",
    );

    proxy = await startProxyHarness({
      runtimeConfig: {
        apiUrl: backend.apiUrl,
        agentUrl: backend.agentUrl,
      },
    });

    const firstTurnDriver = createDriver({
      sessionId: SESSION_ID,
      initialMessages: PRIMARY_SYSTEM_MESSAGES,
    });
    const secondTurnDriver = createDriver({
      sessionId: SESSION_ID,
      initialMessages: [
        { role: "system", content: PRIMARY_SYSTEM },
        { role: "user", content: BASELINE_USER },
        { role: "assistant", content: BASELINE_ASSISTANT },
      ],
    });
    const compactionDriver = createDriver({
      sessionId: SESSION_ID,
      ...COMPACTION_DRIVER_CONTEXT,
      initialMessages: [
        { role: "system", content: COMPACTION_SYSTEM },
        { role: "user", content: BASELINE_USER },
        { role: "assistant", content: BASELINE_ASSISTANT },
        { role: "user", content: SECOND_VERBOSE_USER },
        { role: "assistant", content: SECOND_VERBOSE_ASSISTANT },
      ],
    });

    const first = await firstTurnDriver.runTurn(BASELINE_USER);
    const second = await secondTurnDriver.runTurn(SECOND_VERBOSE_USER);
    const compaction = await compactionDriver.runTurn(COMPACTION_USER);

    const conversationFile = conversationFilePath(PRIMARY_SYSTEM_MESSAGES, SESSION_ID);
    const firstFingerprint = turnsFingerprint([BASELINE_TURN]);
    const secondFingerprint = turnsFingerprint([BASELINE_TURN, SECOND_VERBOSE_TURN]);
    const persistedBeforeRestart = readPersistedConversationFile(conversationFile);
    expect(sortedArchiveKeys(persistedBeforeRestart)).toEqual(
      [firstFingerprint, secondFingerprint].sort(),
    );

    await proxy.restart();

    const archivedHistoryDriver = createDriver({
      sessionId: SESSION_ID,
      initialMessages: [
        { role: "system", content: PRIMARY_SYSTEM },
        { role: "user", content: BASELINE_USER },
        { role: "assistant", content: BASELINE_ASSISTANT },
        { role: "user", content: SECOND_VERBOSE_USER },
        { role: "assistant", content: SECOND_VERBOSE_ASSISTANT },
      ],
    });
    const archivedFollowUp = await archivedHistoryDriver.runTurn(ARCHIVED_FOLLOW_UP_USER);

    expect(first.assistantText).toBe(BASELINE_ASSISTANT);
    expect(second.assistantText).toBe(SECOND_VERBOSE_ASSISTANT);
    expect(compaction.assistantText).toBe(COMPACTION_ASSISTANT);
    expect(archivedFollowUp.assistantText).toBe(MULTI_ARCHIVE_RESTORE_ASSISTANT);

    expect(backend.runCount).toBe(4);
    expect(runSnapshots).toHaveLength(4);
    expect(runSnapshots[0]!.conversationId).not.toBe("");
    expect(runSnapshots[0]!.conversationId).toBe(runSnapshots[1]!.conversationId);
    expect(runSnapshots[1]!.conversationId).toBe(runSnapshots[2]!.conversationId);
    expect(runSnapshots[2]!.conversationId).toBe(runSnapshots[3]!.conversationId);
    expect(runSnapshots[3]!.turns).toEqual([BASELINE_TURN, SECOND_VERBOSE_TURN]);
    expect(runSnapshots[3]!.pendingToolCalls).toEqual(["second-turn-checkpoint"]);
  }, 15_000);
});
