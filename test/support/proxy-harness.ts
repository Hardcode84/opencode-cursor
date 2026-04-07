import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { clearConversationStateCacheForTests } from "../../src/conversation-state";
import { type CursorRuntimeConfig, resolveRuntimeConfig } from "../../src/runtime-config";
import { startProxy, stopProxy } from "../../src/server";

let activeHarnessCount = 0;

export interface ProxyHarness {
  port: number;
  baseUrl: string;
  runtimeConfig: CursorRuntimeConfig;
  restart: (options?: {
    accessToken?: string;
    models?: ReadonlyArray<{ id: string; name: string }>;
    runtimeConfig?: Partial<CursorRuntimeConfig>;
  }) => Promise<void>;
  close: () => Promise<void>;
}

export async function startProxyHarness(
  options: {
    accessToken?: string;
    models?: ReadonlyArray<{ id: string; name: string }>;
    runtimeConfig?: Partial<CursorRuntimeConfig>;
  } = {},
): Promise<ProxyHarness> {
  if (activeHarnessCount > 0) {
    throw new Error("startProxyHarness only supports one active harness at a time");
  }
  activeHarnessCount++;
  clearConversationStateCacheForTests();
  const ownsConversationDiskDir = !options.runtimeConfig?.conversationDiskDir;
  const conversationDiskDir =
    options.runtimeConfig?.conversationDiskDir ??
    mkdtempSync(join(tmpdir(), "opencode-cursor-tests-"));
  let accessToken = options.accessToken ?? "fake-token";
  let models = options.models ?? [{ id: "test-model", name: "Test Model" }];
  let runtimeConfig = resolveRuntimeConfig({
    conversationDiskDir,
    ...options.runtimeConfig,
  });
  let closed = false;

  const harness: ProxyHarness = {
    port: 0,
    baseUrl: "",
    runtimeConfig,
    async restart(restartOptions = {}) {
      stopProxy();
      clearConversationStateCacheForTests();
      accessToken = restartOptions.accessToken ?? accessToken;
      models = restartOptions.models ?? models;
      runtimeConfig = resolveRuntimeConfig({
        ...runtimeConfig,
        ...restartOptions.runtimeConfig,
        conversationDiskDir,
      });
      const port = await startProxy(async () => accessToken, models, runtimeConfig);
      harness.port = port;
      harness.baseUrl = `http://localhost:${port}/v1`;
      harness.runtimeConfig = runtimeConfig;
    },
    async close() {
      if (closed) return;
      closed = true;
      stopProxy();
      clearConversationStateCacheForTests();
      if (ownsConversationDiskDir) {
        rmSync(conversationDiskDir, { recursive: true, force: true });
      }
      activeHarnessCount = Math.max(0, activeHarnessCount - 1);
    },
  };

  try {
    await harness.restart();
    return harness;
  } catch (error) {
    await harness.close();
    throw error;
  }
}
