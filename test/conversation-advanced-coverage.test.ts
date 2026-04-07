import { afterEach, describe, expect, test } from "bun:test";
import {
  createEchoTools,
  exchangeRequestContext,
  readEchoToolText,
} from "./support/conversation-test-helpers";
import {
  FakeCursorBackend,
  type FakeMcpResultSnapshot,
  type FakeRunConnection,
  type FakeRunRequestSnapshot,
} from "./support/fake-cursor-backend";
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

describe("advanced conversation coverage", () => {
  test("replays the same tool call without re-executing the frontend tool", async () => {
    backend = await FakeCursorBackend.start();
    const runSnapshots: FakeRunRequestSnapshot[] = [];
    const observedToolResults: FakeMcpResultSnapshot[] = [];

    backend.enqueueRun(async (connection) => {
      runSnapshots.push(await connection.waitForRunRequest());
      await exchangeRequestContextOrThrow(connection, 401);
      connection.sendMcpToolCall(
        "echo_tool",
        { text: "cache-me" },
        { toolCallId: "cached-tool-call", execId: 402 },
      );
      observedToolResults.push(await connection.waitForMcpResult(402));
      connection.resetStream();
      await connection.waitForClose(1_000);
    });

    backend.enqueueRun(async (connection) => {
      runSnapshots.push(await connection.waitForRunRequest());
      await exchangeRequestContextOrThrow(connection, 403);
      connection.sendMcpToolCall(
        "echo_tool",
        { text: "cache-me" },
        { toolCallId: "cached-tool-call", execId: 404 },
      );
      connection.sendConversationCheckpoint([]);
      const mcpResult = await connection.waitForMcpResult(404);
      observedToolResults.push(mcpResult);
      await connection.waitForExecStreamClose(404);
      connection.sendTextDelta(`Tool says: ${mcpResult.text}.`);
      connection.sendEndStreamOk();
    });

    proxy = await startProxyHarness({
      runtimeConfig: {
        apiUrl: backend.apiUrl,
        agentUrl: backend.agentUrl,
        thinkingTimeoutMs: 200,
        streamingTimeoutMs: 200,
        collectingTimeoutMs: 200,
      },
    });

    let executorInvocations = 0;
    const driver = new OpenAIConversationDriver({
      baseUrl: proxy.baseUrl,
      model: "test-model",
      sessionId: "at-most-once-tool-session",
      tools: createEchoTools(),
      maxRequestRetries: 2,
      toolExecutors: {
        echo_tool(args) {
          executorInvocations++;
          return `tool-result::${readEchoToolText(args)}`;
        },
      },
    });

    const turn = await driver.runTurn("please run the cached tool");

    expect(executorInvocations).toBe(1);
    expect(backend.runCount).toBe(2);
    expect(runSnapshots).toHaveLength(2);
    expect(runSnapshots[0]!.userText).toBe("please run the cached tool");
    expect(runSnapshots[1]!.userText).toBe("please run the cached tool");
    expect(runSnapshots[1]!.turns).toEqual([]);
    expect(turn.requests).toHaveLength(3);
    expect(turn.requests[0]!.finishReason).toBe("tool_calls");
    expect(turn.requests[1]!.finishReason).toBe("tool_calls");
    expect(turn.toolExecutions).toHaveLength(2);
    expect(turn.toolExecutions.map((execution) => execution.toolCallId)).toEqual([
      "cached-tool-call",
      "cached-tool-call",
    ]);
    expect(turn.assistantText).toBe("Tool says: tool-result::cache-me.");
    expect(observedToolResults.map((result) => result.text)).toEqual([
      "tool-result::cache-me",
      "tool-result::cache-me",
    ]);
  }, 15_000);

  test("recovers from a proxy restart by reusing the persisted checkpoint", async () => {
    backend = await FakeCursorBackend.start();
    const runSnapshots: FakeRunRequestSnapshot[] = [];

    backend.enqueueRun(async (connection) => {
      runSnapshots.push(await connection.waitForRunRequest());
      await exchangeRequestContextOrThrow(connection, 501);
      connection.sendTextDelta("Baseline turn complete.");
      connection.sendEndStreamOk();
    });

    backend.enqueueRun(async (connection) => {
      runSnapshots.push(await connection.waitForRunRequest());
      await exchangeRequestContextOrThrow(connection, 502);
      connection.sendConversationCheckpoint(
        [{ userText: "hello baseline", assistantText: "Baseline turn complete." }],
        { pendingToolCalls: ["restart-checkpoint-marker"] },
      );
      connection.resetStream();
      await connection.waitForClose(1_000);
    });

    backend.enqueueRun(async (connection) => {
      runSnapshots.push(await connection.waitForRunRequest());
      await exchangeRequestContextOrThrow(connection, 503);
      connection.sendTextDelta("Recovered after proxy restart.");
      connection.sendEndStreamOk();
    });

    proxy = await startProxyHarness({
      runtimeConfig: {
        apiUrl: backend.apiUrl,
        agentUrl: backend.agentUrl,
      },
    });

    const driver = new OpenAIConversationDriver({
      baseUrl: () => proxy!.baseUrl,
      model: "test-model",
      sessionId: "proxy-restart-recovery-session",
      maxRequestRetries: 2,
      onRetry: async () => {
        await proxy!.restart();
      },
    });

    const firstTurn = await driver.runTurn("hello baseline");
    const secondTurn = await driver.runTurn("continue after restart");

    expect(firstTurn.assistantText).toBe("Baseline turn complete.");
    expect(secondTurn.assistantText).toBe("Recovered after proxy restart.");
    expect(backend.runCount).toBe(3);
    expect(runSnapshots[2]!.userText).toBe("continue after restart");
    expect(runSnapshots[2]!.turns).toEqual([
      { userText: "hello baseline", assistantText: "Baseline turn complete." },
    ]);
    expect(runSnapshots[2]!.pendingToolCalls).toContain("restart-checkpoint-marker");
  }, 15_000);

  test("handles multiple tool batches over multiple resume cycles in one turn", async () => {
    backend = await FakeCursorBackend.start();
    const observedToolResults: FakeMcpResultSnapshot[] = [];

    backend.enqueueRun(async (connection) => {
      await connection.waitForRunRequest();
      await exchangeRequestContextOrThrow(connection, 601);

      connection.sendThinkingDelta("Planning the first tool batch.");
      connection.sendMcpToolCall(
        "echo_tool",
        { text: "alpha" },
        { toolCallId: "multi-alpha", execId: 611 },
      );
      connection.sendMcpToolCall(
        "echo_tool",
        { text: "beta" },
        { toolCallId: "multi-beta", execId: 612 },
      );
      connection.sendConversationCheckpoint([], { pendingToolCalls: ["phase-1"] });

      const alpha = await connection.waitForMcpResult(611);
      observedToolResults.push(alpha);
      await connection.waitForExecStreamClose(611);

      const beta = await connection.waitForMcpResult(612);
      observedToolResults.push(beta);
      await connection.waitForExecStreamClose(612);

      connection.sendThinkingDelta("Planning the second tool batch.");
      connection.sendMcpToolCall(
        "echo_tool",
        { text: "gamma" },
        { toolCallId: "multi-gamma", execId: 613 },
      );
      connection.sendConversationCheckpoint([], { pendingToolCalls: ["phase-2"] });

      const gamma = await connection.waitForMcpResult(613);
      observedToolResults.push(gamma);
      await connection.waitForExecStreamClose(613);

      connection.sendTextDelta(`Tools: ${alpha.text} | ${beta.text} | ${gamma.text}.`);
      connection.sendEndStreamOk();
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
      sessionId: "multi-resume-session",
      tools: createEchoTools(),
      toolExecutors: {
        echo_tool(args) {
          return `tool-result::${readEchoToolText(args)}`;
        },
      },
    });

    const turn = await driver.runTurn("use alpha and beta first, then gamma");

    expect(backend.runCount).toBe(1);
    expect(turn.requests).toHaveLength(3);
    expect(turn.requests[0]!.toolCalls.map((toolCall) => toolCall.id)).toEqual([
      "multi-alpha",
      "multi-beta",
    ]);
    expect(turn.requests[1]!.toolCalls.map((toolCall) => toolCall.id)).toEqual(["multi-gamma"]);
    expect(turn.toolExecutions.map((execution) => execution.result)).toEqual([
      "tool-result::alpha",
      "tool-result::beta",
      "tool-result::gamma",
    ]);
    expect(turn.reasoningText).toBe(
      "Planning the first tool batch.Planning the second tool batch.",
    );
    expect(turn.assistantText).toBe(
      "Tools: tool-result::alpha | tool-result::beta | tool-result::gamma.",
    );
    expect(observedToolResults.map((result) => result.text)).toEqual([
      "tool-result::alpha",
      "tool-result::beta",
      "tool-result::gamma",
    ]);
  }, 15_000);
});

async function exchangeRequestContextOrThrow(
  connection: FakeRunConnection,
  execMessageId: number,
): Promise<void> {
  const exchanged = await exchangeRequestContext(connection, execMessageId);
  if (!exchanged) throw new Error(`Request context exchange interrupted for ${execMessageId}`);
}
