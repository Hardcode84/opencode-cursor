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
  const conversationDiskDir = mkdtempSync(join(tmpdir(), "opencode-cursor-tests-"));
  const runtimeConfig = resolveRuntimeConfig({
    conversationDiskDir,
    ...options.runtimeConfig,
  });
  const port = await startProxy(
    async () => options.accessToken ?? "fake-token",
    options.models ?? [{ id: "test-model", name: "Test Model" }],
    runtimeConfig,
  );

  return {
    port,
    baseUrl: `http://localhost:${port}/v1`,
    runtimeConfig,
    async close() {
      stopProxy();
      clearConversationStateCacheForTests();
      rmSync(conversationDiskDir, { recursive: true, force: true });
    },
  };
}
