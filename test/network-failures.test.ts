import { afterEach, describe, expect, test } from "bun:test";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { deriveConversationKey } from "../src/server";
import {
  FakeCursorBackend,
  type FakeRunConnection,
  type FakeRunRequestSnapshot,
} from "./support/fake-cursor-backend";
import { type ProxyHarness, startProxyHarness } from "./support/proxy-harness";

let backend: FakeCursorBackend | undefined;
let proxy: ProxyHarness | undefined;

function chatBody(overrides: Record<string, unknown> = {}) {
  return {
    model: "test-model",
    stream: true,
    messages: [{ role: "user", content: "hello from integration test" }],
    ...overrides,
  };
}

async function postStream(body: Record<string, unknown> = {}): Promise<Response> {
  if (!proxy) throw new Error("proxy not started");
  return fetch(`${proxy.baseUrl}/chat/completions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(chatBody(body)),
  });
}

afterEach(async () => {
  await proxy?.close();
  await backend?.close();
  proxy = undefined;
  backend = undefined;
});

describe("network failures and timeout integration", () => {
  test("thinking timeout auto-resumes against a fake Cursor backend", async () => {
    backend = await FakeCursorBackend.start();
    backend.enqueueRun(async (connection) => {
      await connection.waitForRunRequest();
      // Intentionally stall without sending output so the proxy hits thinking-timeout.
    });
    backend.enqueueRun(async (connection) => {
      await connection.waitForRunRequest();
      connection.sendTextDelta("Recovered after timeout");
      connection.sendEndStreamOk();
    });

    proxy = await startProxyHarness({
      runtimeConfig: {
        apiUrl: backend.apiUrl,
        agentUrl: backend.agentUrl,
        thinkingTimeoutMs: 50,
        streamingTimeoutMs: 50,
        collectingTimeoutMs: 50,
      },
    });

    const response = await postStream();
    expect(response.status).toBe(200);

    const body = await response.text();
    expect(backend.runCount).toBe(2);
    expect(body).toContain("Auto-resuming (attempt 1/5)");
    expect(body).toContain("Recovered after timeout");
  }, 10_000);

  test("streaming timeout after partial output also auto-resumes", async () => {
    backend = await FakeCursorBackend.start();
    backend.enqueueRun(async (connection) => {
      await connection.waitForRunRequest();
      connection.sendTextDelta("partial before stall");
      // Keep the upstream H2 stream open to force a streaming timeout.
    });
    backend.enqueueRun(async (connection) => {
      await connection.waitForRunRequest();
      connection.sendTextDelta("resumed tail");
      connection.sendEndStreamOk();
    });

    proxy = await startProxyHarness({
      runtimeConfig: {
        apiUrl: backend.apiUrl,
        agentUrl: backend.agentUrl,
        thinkingTimeoutMs: 200,
        streamingTimeoutMs: 50,
        collectingTimeoutMs: 50,
      },
    });

    const response = await postStream();
    expect(response.status).toBe(200);
    const body = await response.text();

    expect(backend.runCount).toBe(2);
    expect(body).toContain("partial before stall");
    expect(body).toContain("Auto-resuming (attempt 1/5)");
    expect(body).toContain("resumed tail");
  }, 10_000);

  test("resource_exhausted endStream triggers transparent auto-resume", async () => {
    backend = await FakeCursorBackend.start();
    backend.enqueueRun(async (connection) => {
      await connection.waitForRunRequest();
      connection.sendEndStreamError("resource_exhausted", "step boundary exceeded");
    });
    backend.enqueueRun(async (connection) => {
      await connection.waitForRunRequest();
      connection.sendTextDelta("Recovered after resource exhausted");
      connection.sendEndStreamOk();
    });

    proxy = await startProxyHarness({
      runtimeConfig: {
        apiUrl: backend.apiUrl,
        agentUrl: backend.agentUrl,
        thinkingTimeoutMs: 100,
        streamingTimeoutMs: 100,
        collectingTimeoutMs: 100,
      },
    });

    const response = await postStream();
    const body = await response.text();

    expect(backend.runCount).toBe(2);
    expect(body).not.toContain("Auto-resuming");
    expect(body).toContain("Recovered after resource exhausted");
  }, 10_000);

  test("blob_not_found retries rebuild from turns and then fully invalidates state", async () => {
    backend = await FakeCursorBackend.start();
    const runSnapshots: FakeRunRequestSnapshot[] = [];
    const baselineMessages = [{ role: "user" as const, content: "baseline turn" }];

    backend.enqueueRun(async (connection) => {
      runSnapshots.push(await connection.waitForRunRequest());
      connection.sendConversationCheckpoint([
        { userText: "baseline turn", assistantText: "Stored baseline." },
      ]);
      connection.sendTextDelta("Stored baseline.");
      connection.sendEndStreamOk();
    });
    backend.enqueueRun(async (connection) => {
      runSnapshots.push(await connection.waitForRunRequest());
      connection.sendEndStreamError("not_found", "Blob not found in blob store");
    });
    backend.enqueueRun(async (connection) => {
      runSnapshots.push(await connection.waitForRunRequest());
      connection.sendEndStreamError("not_found", "Blob not found again");
    });
    backend.enqueueRun(async (connection) => {
      runSnapshots.push(await connection.waitForRunRequest());
      connection.sendTextDelta("Recovered after blob retry.");
      connection.sendEndStreamOk();
    });

    proxy = await startProxyHarness({
      runtimeConfig: {
        apiUrl: backend.apiUrl,
        agentUrl: backend.agentUrl,
        thinkingTimeoutMs: 100,
        streamingTimeoutMs: 100,
        collectingTimeoutMs: 100,
      },
    });

    const baselineResponse = await postStream({ messages: baselineMessages });
    expect(baselineResponse.status).toBe(200);
    expect(await baselineResponse.text()).toContain("Stored baseline.");

    const followUpResponse = await postStream({
      messages: [
        ...baselineMessages,
        { role: "assistant", content: "Stored baseline." },
        { role: "user", content: "follow up after blob retry" },
      ],
    });
    expect(followUpResponse.status).toBe(200);
    const body = await followUpResponse.text();

    expect(backend.runCount).toBe(4);
    expect(body).toContain("Recovered after blob retry.");
    expect(body).not.toContain("[Error:");
    expect(runSnapshots[1]?.turns).toEqual([
      { userText: "baseline turn", assistantText: "Stored baseline." },
    ]);
    expect(runSnapshots[2]?.turns).toEqual([
      { userText: "baseline turn", assistantText: "Stored baseline." },
    ]);
    expect(runSnapshots[3]?.turns).toEqual([
      { userText: "baseline turn", assistantText: "Stored baseline." },
    ]);
  }, 10_000);

  test("corrupt persisted checkpoints are ignored and cleared before the request is rebuilt", async () => {
    backend = await FakeCursorBackend.start();
    const runSnapshots: FakeRunRequestSnapshot[] = [];
    const messages = [{ role: "user" as const, content: "recover from corrupt checkpoint" }];

    backend.enqueueRun(async (connection) => {
      runSnapshots.push(await connection.waitForRunRequest());
      connection.sendTextDelta("Recovered from corrupt checkpoint.");
      connection.sendEndStreamOk();
    });

    proxy = await startProxyHarness({
      runtimeConfig: {
        apiUrl: backend.apiUrl,
        agentUrl: backend.agentUrl,
      },
    });

    const convKey = deriveConversationKey(messages);
    const conversationFile = join(proxy.runtimeConfig.conversationDiskDir, `${convKey}.json`);
    writeFileSync(
      conversationFile,
      JSON.stringify({
        conversationId: "broken-conversation",
        checkpoint: Buffer.from([0xff]).toString("base64"),
        blobStore: {},
        savedMs: Date.now(),
      }),
    );

    const response = await postStream({ messages });
    expect(response.status).toBe(200);
    expect(await response.text()).toContain("Recovered from corrupt checkpoint.");
    expect(backend.runCount).toBe(1);
    expect(runSnapshots[0]?.userText).toBe("recover from corrupt checkpoint");
    expect(runSnapshots[0]?.turns).toEqual([]);

    if (existsSync(conversationFile)) {
      const persisted = JSON.parse(readFileSync(conversationFile, "utf-8")) as {
        checkpoint: string | null;
      };
      expect(persisted.checkpoint).toBeNull();
    }
  }, 10_000);

  test("client-side stream cancellation closes the upstream Cursor stream", async () => {
    backend = await FakeCursorBackend.start();
    let activeConnection: FakeRunConnection | undefined;
    backend.enqueueRun(async (connection) => {
      activeConnection = connection;
      await connection.waitForRunRequest();
      connection.sendTextDelta("hello from cursor");
      await connection.waitForClose(1_000);
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

    const response = await postStream();
    expect(response.body).toBeTruthy();

    const reader = response.body!.getReader();
    const firstChunk = await reader.read();
    const chunkText = new TextDecoder().decode(firstChunk.value);
    expect(chunkText).toContain("hello from cursor");

    await reader.cancel();
    expect(activeConnection).toBeTruthy();
    await activeConnection!.waitForClose(1_000);
  }, 10_000);
});
