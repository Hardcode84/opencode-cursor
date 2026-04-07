import { describe, expect, test } from "bun:test";
import { type StoredConversation, turnsFingerprint } from "../src/conversation-state";
import type { OpenAIMessage } from "../src/openai-messages";
import {
  autoResumeAttemptLimit,
  deriveConversationKey,
  prepareStoredConversationForRequest,
  resourceExhaustedBackoffDelayMs,
} from "../src/server";

function createStoredConversation(): StoredConversation {
  return {
    conversationId: "conv-1",
    checkpoint: null,
    blobStore: new Map(),
    lastAccessMs: Date.now(),
    checkpointHistory: new Map(),
    checkpointArchive: new Map(),
  };
}

describe("deriveConversationKey", () => {
  test("session affinity keeps compaction on the same stored conversation", () => {
    const buildMessages: OpenAIMessage[] = [
      { role: "system", content: "You are the build agent." },
      { role: "user", content: "hello" },
    ];
    const compactionMessages: OpenAIMessage[] = [
      { role: "system", content: "You summarize conversations." },
      { role: "user", content: "hello" },
    ];

    expect(deriveConversationKey(buildMessages, "session-1")).toBe(
      deriveConversationKey(compactionMessages, "session-1"),
    );
  });

  test("system prompt still scopes anonymous conversations", () => {
    const keyA = deriveConversationKey([
      { role: "system", content: "Prompt A" },
      { role: "user", content: "hello" },
    ]);
    const keyB = deriveConversationKey([
      { role: "system", content: "Prompt B" },
      { role: "user", content: "hello" },
    ]);

    expect(keyA).not.toBe(keyB);
  });
});

describe("prepareStoredConversationForRequest", () => {
  test("compaction archives stale checkpoints and keeps blobs for restore", () => {
    const stored = createStoredConversation();
    const currentTurns = [{ userText: "Q2", assistantText: "A2" }];
    const currentFingerprint = turnsFingerprint(currentTurns);
    const oldTurns = [{ userText: "Q1", assistantText: "A1" }];
    const oldFingerprint = turnsFingerprint(oldTurns);
    const currentCheckpoint = new Uint8Array([1, 2, 3]);
    const historicCheckpoint = new Uint8Array([4, 5, 6]);

    stored.checkpoint = currentCheckpoint;
    stored.blobStore.set("blob-1", new Uint8Array([7, 8, 9]));
    stored.checkpointHistory.set(oldFingerprint, historicCheckpoint);

    const result = prepareStoredConversationForRequest(stored, currentTurns, "compaction");

    expect(result).toEqual({ checkpoint: null, didReset: true });
    expect(stored.checkpoint).toBeNull();
    expect(stored.blobStore.size).toBe(1);
    expect(stored.checkpointHistory.size).toBe(0);
    expect(stored.checkpointArchive.get(oldFingerprint)).toBe(historicCheckpoint);
    expect(stored.checkpointArchive.get(currentFingerprint)).toBe(currentCheckpoint);
  });

  test("normal requests restore a matching historical checkpoint", () => {
    const stored = createStoredConversation();
    const currentCheckpoint = new Uint8Array([1, 2, 3]);
    const historicCheckpoint = new Uint8Array([4, 5, 6]);
    const turns = [{ userText: "Q1", assistantText: "A1" }];

    stored.checkpoint = currentCheckpoint;
    stored.checkpointHistory.set(turnsFingerprint(turns), historicCheckpoint);

    const result = prepareStoredConversationForRequest(stored, turns);

    expect(result.didReset).toBe(false);
    expect(result.checkpoint).toBe(historicCheckpoint);
    expect(stored.checkpoint).toBe(historicCheckpoint);
  });

  test("normal requests restore a matching archived checkpoint", () => {
    const stored = createStoredConversation();
    const archivedCheckpoint = new Uint8Array([4, 5, 6]);
    const turns = [{ userText: "Q1", assistantText: "A1" }];

    stored.checkpointArchive.set(turnsFingerprint(turns), archivedCheckpoint);

    const result = prepareStoredConversationForRequest(stored, turns);

    expect(result.didReset).toBe(false);
    expect(result.checkpoint).toBe(archivedCheckpoint);
    expect(stored.checkpoint).toBe(archivedCheckpoint);
  });

  test("current history wins over archived restore for the same fingerprint", () => {
    const stored = createStoredConversation();
    const currentCheckpoint = new Uint8Array([1, 2, 3]);
    const archivedCheckpoint = new Uint8Array([4, 5, 6]);
    const turns = [{ userText: "Q1", assistantText: "A1" }];
    const fp = turnsFingerprint(turns);

    stored.checkpointHistory.set(fp, currentCheckpoint);
    stored.checkpointArchive.set(fp, archivedCheckpoint);

    const result = prepareStoredConversationForRequest(stored, turns);

    expect(result.didReset).toBe(false);
    expect(result.checkpoint).toBe(currentCheckpoint);
    expect(stored.checkpoint).toBe(currentCheckpoint);
  });

  test("compaction with empty turns still archives the current checkpoint", () => {
    const stored = createStoredConversation();
    const currentCheckpoint = new Uint8Array([10, 20, 30]);
    stored.checkpoint = currentCheckpoint;

    const result = prepareStoredConversationForRequest(stored, [], "compaction");

    expect(result).toEqual({ checkpoint: null, didReset: true });
    expect(stored.checkpoint).toBeNull();
    expect(stored.checkpointArchive.get("__current__")).toBe(currentCheckpoint);
  });

  test("normal requests snapshot the current checkpoint into history", () => {
    const stored = createStoredConversation();
    const currentCheckpoint = new Uint8Array([1, 2, 3]);
    const turns = [{ userText: "Q1", assistantText: "A1" }];
    const fp = turnsFingerprint(turns);

    stored.checkpoint = currentCheckpoint;

    const result = prepareStoredConversationForRequest(stored, turns);

    expect(result.didReset).toBe(false);
    expect(result.checkpoint).toBe(currentCheckpoint);
    expect(stored.checkpointHistory.get(fp)).toBe(currentCheckpoint);
  });
});

describe("resourceExhaustedBackoffDelayMs", () => {
  test("grows exponentially and caps at the configured maximum", () => {
    const runtimeConfig = {
      resourceExhaustedRetryDelayMs: 250,
      resourceExhaustedRetryMaxDelayMs: 1_000,
    };

    expect(resourceExhaustedBackoffDelayMs(0, runtimeConfig)).toBe(0);
    expect(resourceExhaustedBackoffDelayMs(1, runtimeConfig)).toBe(250);
    expect(resourceExhaustedBackoffDelayMs(2, runtimeConfig)).toBe(500);
    expect(resourceExhaustedBackoffDelayMs(3, runtimeConfig)).toBe(1_000);
    expect(resourceExhaustedBackoffDelayMs(4, runtimeConfig)).toBe(1_000);
  });
});

describe("autoResumeAttemptLimit", () => {
  test("keeps timeout retries at 5 and resource_exhausted retries at 10", () => {
    expect(autoResumeAttemptLimit("timeout")).toBe(5);
    expect(autoResumeAttemptLimit("resource_exhausted")).toBe(10);
    expect(autoResumeAttemptLimit("blob_not_found")).toBe(0);
    expect(autoResumeAttemptLimit(undefined)).toBe(0);
  });
});
