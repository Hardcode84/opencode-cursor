import { afterEach, describe, expect, test } from "bun:test";
import { FakeCursorBackend } from "./support/fake-cursor-backend";
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
      await connection.waitForClientMessageCase("runRequest");
      // Intentionally stall without sending output so the proxy hits thinking-timeout.
    });
    backend.enqueueRun(async (connection) => {
      await connection.waitForClientMessageCase("runRequest");
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
      await connection.waitForClientMessageCase("runRequest");
      connection.sendTextDelta("partial before stall");
      // Keep the upstream H2 stream open to force a streaming timeout.
    });
    backend.enqueueRun(async (connection) => {
      await connection.waitForClientMessageCase("runRequest");
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
    const body = await response.text();

    expect(backend.runCount).toBe(2);
    expect(body).toContain("partial before stall");
    expect(body).toContain("Auto-resuming (attempt 1/5)");
    expect(body).toContain("resumed tail");
  }, 10_000);

  test("resource_exhausted endStream triggers transparent auto-resume", async () => {
    backend = await FakeCursorBackend.start();
    backend.enqueueRun(async (connection) => {
      await connection.waitForClientMessageCase("runRequest");
      connection.sendEndStreamError("resource_exhausted", "step boundary exceeded");
    });
    backend.enqueueRun(async (connection) => {
      await connection.waitForClientMessageCase("runRequest");
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

  test("client-side stream cancellation closes the upstream Cursor stream", async () => {
    backend = await FakeCursorBackend.start();
    backend.enqueueRun(async (connection) => {
      await connection.waitForClientMessageCase("runRequest");
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
    await backend.runConnections[0]!.waitForClose(1_000);
  }, 10_000);
});
