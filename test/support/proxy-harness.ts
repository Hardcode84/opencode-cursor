import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { clearConversationStateCacheForTests } from "../../src/conversation-state";
import { type CursorRuntimeConfig, resolveRuntimeConfig } from "../../src/runtime-config";
import { startProxy, stopProxy } from "../../src/server";

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
  clearConversationStateCacheForTests();
  const managedConversationDiskDir = !options.runtimeConfig?.conversationDiskDir;
  const conversationDiskDir =
    options.runtimeConfig?.conversationDiskDir ??
    mkdtempSync(join(tmpdir(), "opencode-cursor-tests-"));
  let accessToken = options.accessToken ?? "fake-token";
  let models = options.models ?? [{ id: "test-model", name: "Test Model" }];
  let runtimeConfig = resolveRuntimeConfig({
    conversationDiskDir,
    ...options.runtimeConfig,
  });

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
      stopProxy();
      clearConversationStateCacheForTests();
      if (managedConversationDiskDir) {
        rmSync(conversationDiskDir, { recursive: true, force: true });
      }
    },
  };

  await harness.restart();
  return harness;
}
