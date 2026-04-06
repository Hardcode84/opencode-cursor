import { createHash } from "node:crypto";
import { mkdirSync, readdirSync, readFileSync, statSync, unlinkSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { logDebug, logWarn } from "./logger";

export interface StoredConversation {
  conversationId: string;
  checkpoint: Uint8Array | null;
  blobStore: Map<string, Uint8Array>;
  lastAccessMs: number;
  checkpointHistory: Map<string, Uint8Array>;
}

const conversationStates = new Map<string, StoredConversation>();
const CONVERSATION_TTL_MS = 30 * 60 * 1000; // 30 minutes

function evictStaleConversations(): void {
  const now = Date.now();
  for (const [key, stored] of conversationStates) {
    if (now - stored.lastAccessMs > CONVERSATION_TTL_MS) {
      conversationStates.delete(key);
      try {
        unlinkSync(convDiskPath(key));
      } catch {}
    }
  }
}

// --- Disk persistence for conversation state across process restarts ---

const CONV_DISK_DIR = join(
  process.env.XDG_DATA_HOME || join(homedir(), ".local", "share"),
  "opencode",
  "cursor-conversations",
);
try {
  mkdirSync(CONV_DISK_DIR, { recursive: true });
} catch (err) {
  logWarn("Failed to create conversation directory", { dir: CONV_DISK_DIR, error: String(err) });
}

const CONV_DISK_TTL_MS = 24 * 60 * 60 * 1000; // 24h on-disk TTL

function convDiskPath(convKey: string): string {
  return join(CONV_DISK_DIR, `${convKey}.json`);
}

interface SerializedConversation {
  conversationId: string;
  checkpoint: string | null; // base64
  blobStore: Record<string, string>; // hex key → base64 value
  savedMs: number;
  checkpointHistory?: Record<string, string>; // fingerprint → base64 checkpoint
}

export function persistConversation(convKey: string, stored: StoredConversation): void {
  const data: SerializedConversation = {
    conversationId: stored.conversationId,
    checkpoint: stored.checkpoint ? Buffer.from(stored.checkpoint).toString("base64") : null,
    blobStore: Object.fromEntries(
      [...stored.blobStore].map(([k, v]) => [k, Buffer.from(v).toString("base64")]),
    ),
    savedMs: Date.now(),
    checkpointHistory: Object.fromEntries(
      [...stored.checkpointHistory].map(([fp, cp]) => [fp, Buffer.from(cp).toString("base64")]),
    ),
  };
  try {
    writeFileSync(convDiskPath(convKey), JSON.stringify(data));
  } catch (err) {
    logWarn("Failed to persist conversation to disk", { convKey, error: String(err) });
  }
}

function loadConversation(convKey: string): StoredConversation | null {
  try {
    const raw: SerializedConversation = JSON.parse(readFileSync(convDiskPath(convKey), "utf-8"));
    if (Date.now() - raw.savedMs > CONV_DISK_TTL_MS) {
      try {
        unlinkSync(convDiskPath(convKey));
      } catch {}
      return null;
    }
    return {
      conversationId: raw.conversationId,
      checkpoint: raw.checkpoint ? new Uint8Array(Buffer.from(raw.checkpoint, "base64")) : null,
      blobStore: new Map(
        Object.entries(raw.blobStore).map(([k, v]) => [
          k,
          new Uint8Array(Buffer.from(v, "base64")),
        ]),
      ),
      lastAccessMs: Date.now(),
      checkpointHistory: new Map(
        Object.entries(raw.checkpointHistory ?? {}).map(([fp, cp]) => [
          fp,
          new Uint8Array(Buffer.from(cp, "base64")),
        ]),
      ),
    };
  } catch (err) {
    logDebug("Failed to load conversation from disk", { convKey, error: String(err) });
    return null;
  }
}

function evictStaleDiskConversations(): void {
  try {
    const now = Date.now();
    for (const name of readdirSync(CONV_DISK_DIR)) {
      if (!name.endsWith(".json")) continue;
      const full = join(CONV_DISK_DIR, name);
      try {
        if (now - statSync(full).mtimeMs > CONV_DISK_TTL_MS) unlinkSync(full);
      } catch {}
    }
  } catch {}
}

/** Deterministic UUID derived from convKey so Cursor's server-side conversation
 *  persists across proxy restarts. Formats 16 bytes of SHA-256 as a v4-shaped UUID. */
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

export function resolveConversationState(convKey: string): StoredConversation {
  let stored = conversationStates.get(convKey);
  if (!stored) {
    stored = loadConversation(convKey) ?? {
      conversationId: deterministicConversationId(convKey),
      checkpoint: null,
      blobStore: new Map(),
      lastAccessMs: Date.now(),
      checkpointHistory: new Map(),
    };
    conversationStates.set(convKey, stored);
  }
  stored.lastAccessMs = Date.now();
  evictStaleConversations();
  evictStaleDiskConversations();
  return stored;
}

export function getConversationState(convKey: string): StoredConversation | undefined {
  return conversationStates.get(convKey);
}

export function invalidateConversationState(convKey: string): void {
  conversationStates.delete(convKey);
  try {
    unlinkSync(convDiskPath(convKey));
  } catch {}
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
