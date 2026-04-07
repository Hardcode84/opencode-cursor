import { createHash } from "node:crypto";
import {
  mkdirSync,
  readdirSync,
  readFileSync,
  renameSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { join } from "node:path";
import { logDebug, logWarn } from "./logger";
import { type CursorRuntimeConfig, resolveRuntimeConfig } from "./runtime-config";

export interface StoredConversation {
  conversationId: string;
  checkpoint: Uint8Array | null;
  blobStore: Map<string, Uint8Array>;
  lastAccessMs: number;
  checkpointHistory: Map<string, Uint8Array>;
  checkpointArchive: Map<string, Uint8Array>;
}

interface ConversationCacheEntry {
  convKey: string;
  stored: StoredConversation;
  memoryTtlMs: number;
}

const conversationStates = new Map<string, ConversationCacheEntry>();

function conversationCacheKey(convKey: string, runtimeConfig: CursorRuntimeConfig): string {
  return `${runtimeConfig.conversationDiskDir}\0${convKey}`;
}

function ensureConversationDiskDir(runtimeConfig: CursorRuntimeConfig): void {
  try {
    mkdirSync(runtimeConfig.conversationDiskDir, { recursive: true });
  } catch (err) {
    logWarn("Failed to create conversation directory", {
      dir: runtimeConfig.conversationDiskDir,
      error: String(err),
    });
  }
}

function convDiskPath(convKey: string, runtimeConfig: CursorRuntimeConfig): string {
  return join(runtimeConfig.conversationDiskDir, `${convKey}.json`);
}

function evictStaleConversations(): void {
  const now = Date.now();
  for (const [key, entry] of conversationStates) {
    if (now - entry.stored.lastAccessMs > entry.memoryTtlMs) {
      conversationStates.delete(key);
    }
  }
}

// --- Disk persistence for conversation state across process restarts ---

interface SerializedConversation {
  conversationId: string;
  checkpoint: string | null; // base64
  blobStore: Record<string, string>; // hex key → base64 value
  savedMs: number;
  checkpointHistory?: Record<string, string>; // fingerprint → base64 checkpoint
  checkpointArchive?: Record<string, string>; // archived fingerprint → base64 checkpoint
}

function serializeByteMap(map: Map<string, Uint8Array>): Record<string, string> {
  return Object.fromEntries([...map].map(([k, v]) => [k, Buffer.from(v).toString("base64")]));
}

function deserializeByteMap(obj: Record<string, string> | undefined): Map<string, Uint8Array> {
  return new Map(
    Object.entries(obj ?? {}).map(([k, v]) => [k, new Uint8Array(Buffer.from(v, "base64"))]),
  );
}

function writeConversationFileAtomically(path: string, data: SerializedConversation): void {
  const tempPath = `${path}.${process.pid}.${Date.now()}.tmp`;
  try {
    writeFileSync(tempPath, JSON.stringify(data));
    renameSync(tempPath, path);
  } catch (error) {
    try {
      unlinkSync(tempPath);
    } catch {
      /* ignore cleanup failure */
    }
    throw error;
  }
}

export function persistConversation(
  convKey: string,
  stored: StoredConversation,
  runtimeConfig?: Partial<CursorRuntimeConfig>,
): void {
  const config = resolveRuntimeConfig(runtimeConfig);
  ensureConversationDiskDir(config);
  const path = convDiskPath(convKey, config);
  const data: SerializedConversation = {
    conversationId: stored.conversationId,
    checkpoint: stored.checkpoint ? Buffer.from(stored.checkpoint).toString("base64") : null,
    blobStore: serializeByteMap(stored.blobStore),
    savedMs: Date.now(),
    checkpointHistory: serializeByteMap(stored.checkpointHistory),
    checkpointArchive: serializeByteMap(stored.checkpointArchive),
  };
  try {
    writeConversationFileAtomically(path, data);
  } catch (err) {
    logWarn("Failed to persist conversation to disk", { convKey, error: String(err) });
  }
}

function loadConversation(
  convKey: string,
  runtimeConfig: CursorRuntimeConfig,
): StoredConversation | null {
  ensureConversationDiskDir(runtimeConfig);
  try {
    const raw: SerializedConversation = JSON.parse(
      readFileSync(convDiskPath(convKey, runtimeConfig), "utf-8"),
    );
    if (Date.now() - raw.savedMs > runtimeConfig.conversationDiskTtlMs) {
      try {
        unlinkSync(convDiskPath(convKey, runtimeConfig));
      } catch {}
      return null;
    }
    return {
      conversationId: raw.conversationId,
      checkpoint: raw.checkpoint ? new Uint8Array(Buffer.from(raw.checkpoint, "base64")) : null,
      blobStore: deserializeByteMap(raw.blobStore),
      lastAccessMs: Date.now(),
      checkpointHistory: deserializeByteMap(raw.checkpointHistory),
      checkpointArchive: deserializeByteMap(raw.checkpointArchive),
    };
  } catch (err) {
    logDebug("Failed to load conversation from disk", { convKey, error: String(err) });
    try {
      unlinkSync(convDiskPath(convKey, runtimeConfig));
    } catch {
      /* ignore cleanup failure */
    }
    return null;
  }
}

function evictStaleDiskConversations(runtimeConfig: CursorRuntimeConfig): void {
  ensureConversationDiskDir(runtimeConfig);
  try {
    const now = Date.now();
    for (const name of readdirSync(runtimeConfig.conversationDiskDir)) {
      if (!name.endsWith(".json")) continue;
      const full = join(runtimeConfig.conversationDiskDir, name);
      try {
        if (now - statSync(full).mtimeMs > runtimeConfig.conversationDiskTtlMs) unlinkSync(full);
      } catch {}
    }
  } catch {}
}

/** Deterministic UUID derived from convKey so Cursor's server-side conversation
 *  persists across proxy restarts. Uses the first 128 bits of SHA-256, formatted
 *  as a v4-shaped UUID. */
export function deterministicConversationId(convKey: string): string {
  const hex = createHash("sha256").update(`cursor-conv-id:${convKey}`).digest("hex").slice(0, 32);
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    `4${hex.slice(13, 16)}`,
    `${(0x8 | (parseInt(hex[16], 16) & 0x3)).toString(16)}${hex.slice(17, 20)}`,
    hex.slice(20, 32),
  ].join("-");
}

export function resolveConversationState(
  convKey: string,
  runtimeConfig?: Partial<CursorRuntimeConfig>,
): StoredConversation {
  const config = resolveRuntimeConfig(runtimeConfig);
  const key = conversationCacheKey(convKey, config);
  let entry = conversationStates.get(key);
  if (!entry) {
    entry = {
      convKey,
      memoryTtlMs: config.conversationTtlMs,
      stored: loadConversation(convKey, config) ?? {
        conversationId: deterministicConversationId(convKey),
        checkpoint: null,
        blobStore: new Map(),
        lastAccessMs: Date.now(),
        checkpointHistory: new Map(),
        checkpointArchive: new Map(),
      },
    };
    conversationStates.set(key, entry);
  }
  entry.memoryTtlMs = config.conversationTtlMs;
  entry.stored.lastAccessMs = Date.now();
  evictStaleConversations();
  evictStaleDiskConversations(config);
  return entry.stored;
}

export function getConversationState(
  convKey: string,
  runtimeConfig?: Partial<CursorRuntimeConfig>,
): StoredConversation | undefined {
  const config = resolveRuntimeConfig(runtimeConfig);
  return conversationStates.get(conversationCacheKey(convKey, config))?.stored;
}

export function invalidateConversationState(
  convKey: string,
  runtimeConfig?: Partial<CursorRuntimeConfig>,
): void {
  const config = resolveRuntimeConfig(runtimeConfig);
  conversationStates.delete(conversationCacheKey(convKey, config));
  try {
    unlinkSync(convDiskPath(convKey, config));
  } catch {}
}

/** @internal Test-only. */
export function clearConversationStateCacheForTests(): void {
  conversationStates.clear();
}

export type Turn = { userText: string; assistantText: string };

export function turnsFingerprint(turns: Turn[]): string {
  if (turns.length === 0) return "";
  const h = createHash("md5");
  for (const t of turns) {
    h.update(t.userText);
    h.update("\0");
    h.update(t.assistantText);
    h.update("\0");
  }
  return `${turns.length}:${h.digest("hex").slice(0, 12)}`;
}
