import { homedir } from "node:os";
import { join } from "node:path";

export interface CursorRuntimeConfig {
  apiUrl: string;
  agentUrl: string;
  loginUrl: string;
  pollUrl: string;
  refreshUrl: string;
  clientVersion: string;
  thinkingTimeoutMs: number;
  streamingTimeoutMs: number;
  collectingTimeoutMs: number;
  activeSessionTtlMs: number;
  flushedSessionMaxLifetimeMs: number;
  conversationTtlMs: number;
  conversationDiskTtlMs: number;
  conversationDiskDir: string;
}

export function defaultConversationDiskDir(): string {
  return join(
    process.env.XDG_DATA_HOME || join(homedir(), ".local", "share"),
    "opencode",
    "cursor-conversations",
  );
}

export function resolveRuntimeConfig(
  overrides: Partial<CursorRuntimeConfig> = {},
): CursorRuntimeConfig {
  return {
    apiUrl: process.env.CURSOR_API_URL ?? "https://api2.cursor.sh",
    agentUrl: process.env.CURSOR_AGENT_URL ?? "https://agentn.us.api5.cursor.sh",
    loginUrl: process.env.CURSOR_LOGIN_URL ?? "https://cursor.com/loginDeepControl",
    pollUrl: process.env.CURSOR_POLL_URL ?? "https://api2.cursor.sh/auth/poll",
    refreshUrl:
      process.env.CURSOR_REFRESH_URL ?? "https://api2.cursor.sh/auth/exchange_user_api_key",
    clientVersion: "cli-2026.03.30-a5d3e17",
    thinkingTimeoutMs: 30_000,
    streamingTimeoutMs: 15_000,
    collectingTimeoutMs: 30_000,
    activeSessionTtlMs: 5 * 60 * 1000,
    flushedSessionMaxLifetimeMs: 60 * 60 * 1000,
    conversationTtlMs: 30 * 60 * 1000,
    conversationDiskTtlMs: 24 * 60 * 60 * 1000,
    conversationDiskDir: defaultConversationDiskDir(),
    ...overrides,
  };
}
