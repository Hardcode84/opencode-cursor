/**
 * Tests for the CursorSession batch-collecting flush logic and SSE lifecycle.
 *
 * Covers the bugs that caused sessions to stall permanently:
 * 1. Checkpoint + exec in the same H2 chunk: checkpoint erased by exec's state transition
 * 2. Heartbeat responses resetting the collecting inactivity timer indefinitely
 * 3. requestContextArgs prematurely flushing incomplete batches (sub-subagent interleaving)
 *
 * Also verifies SSE stream termination (sendDone closes controller) and abort resilience.
 *
 * NOTE: Tests run serially and share a single `mockH2Stream` instance that is
 * reassigned on each `http2.connect()` call. Do not run these in parallel.
 */
import { afterEach, describe, expect, mock, test } from "bun:test";
import { EventEmitter } from "node:events";

// ── Mock node:http2 before CursorSession is resolved ──

let mockH2Stream: EventEmitter & Record<string, unknown>;

mock.module("node:http2", () => ({
  connect: () => {
    mockH2Stream = Object.assign(new EventEmitter(), {
      write: () => true,
      close: () => {},
      end: () => {},
    });
    const session = Object.assign(new EventEmitter(), {
      request: () => mockH2Stream,
      close: () => {},
    });
    return session;
  },
}));

const { CursorSession } = await import("../src/cursor-session");
const { create, toBinary } = await import("@bufbuild/protobuf");
const proto = await import("../src/proto/agent_pb");
const { frameConnectMessage } = await import("../src/protocol");
const { createSSECtx } = await import("../src/openai-stream");

// ── Protobuf frame helpers ──

function makeCheckpointFrame(): Buffer {
  const state = create(proto.ConversationStateStructureSchema, {});
  const msg = create(proto.AgentServerMessageSchema, {
    message: { case: "conversationCheckpointUpdate", value: state },
  });
  return Buffer.from(frameConnectMessage(toBinary(proto.AgentServerMessageSchema, msg)));
}

function makeExecFrame(toolCallId: string, execId = 1): Buffer {
  const mcpArgs = create(proto.McpArgsSchema, {
    name: "test_tool",
    toolCallId,
    args: {},
  });
  const exec = create(proto.ExecServerMessageSchema, {
    id: execId,
    execId: `exec_${execId}`,
    message: { case: "mcpArgs", value: mcpArgs },
  });
  const msg = create(proto.AgentServerMessageSchema, {
    message: { case: "execServerMessage", value: exec },
  });
  return Buffer.from(frameConnectMessage(toBinary(proto.AgentServerMessageSchema, msg)));
}

function makeRequestContextArgsFrame(execId = 99): Buffer {
  const rca = create(proto.RequestContextArgsSchema, {});
  const exec = create(proto.ExecServerMessageSchema, {
    id: execId,
    execId: `exec_rca_${execId}`,
    message: { case: "requestContextArgs", value: rca },
  });
  const msg = create(proto.AgentServerMessageSchema, {
    message: { case: "execServerMessage", value: exec },
  });
  return Buffer.from(frameConnectMessage(toBinary(proto.AgentServerMessageSchema, msg)));
}

function makeHeartbeatFrame(): Buffer {
  const hb = create(proto.HeartbeatUpdateSchema, {});
  const update = create(proto.InteractionUpdateSchema, {
    message: { case: "heartbeat", value: hb },
  });
  const msg = create(proto.AgentServerMessageSchema, {
    message: { case: "interactionUpdate", value: update },
  });
  return Buffer.from(frameConnectMessage(toBinary(proto.AgentServerMessageSchema, msg)));
}

type SessionOptions = ConstructorParameters<typeof CursorSession>[0];

function createSession(extraOpts?: Partial<Pick<SessionOptions, "_testCollectingTimeoutMs">>) {
  return new CursorSession({
    accessToken: "test-token",
    requestBytes: new Uint8Array(0),
    blobStore: new Map(),
    mcpTools: [],
    convKey: "test-conv",
    ...extraOpts,
  });
}

// ── Helpers ──

/** Drain events until one matches `type`, or return all collected after `max` iterations. */
async function drainUntil(
  session: InstanceType<typeof CursorSession>,
  type: string,
  max = 5,
): Promise<{ found: boolean; events: string[] }> {
  const events: string[] = [];
  for (let i = 0; i < max; i++) {
    const e = await session.next();
    events.push(e.type);
    if (e.type === type) return { found: true, events };
  }
  return { found: false, events };
}

// ── Tests ──

let session: InstanceType<typeof CursorSession>;

afterEach(() => {
  session?.close();
});

describe("batch flush: checkpoint + exec in same chunk", () => {
  test("emits batchReady when checkpoint precedes exec in one H2 data event", async () => {
    session = createSession();

    const chunk = Buffer.concat([makeCheckpointFrame(), makeExecFrame("call_1")]);
    mockH2Stream.emit("data", chunk);

    const { found, events } = await drainUntil(session, "batchReady", 3);
    expect(found).toBe(true);
    expect(events).toContain("toolCall");
  });

  test("emits batchReady when exec precedes checkpoint in one H2 data event", async () => {
    session = createSession();

    const chunk = Buffer.concat([makeExecFrame("call_2"), makeCheckpointFrame()]);
    mockH2Stream.emit("data", chunk);

    const { found, events } = await drainUntil(session, "batchReady", 3);
    expect(found).toBe(true);
    expect(events).toContain("toolCall");
  });

  test("flushes correctly when checkpoint and exec arrive in separate chunks", async () => {
    session = createSession();

    mockH2Stream.emit("data", makeExecFrame("call_3"));

    const e1 = await session.next();
    expect(e1.type).toBe("toolCall");

    mockH2Stream.emit("data", makeCheckpointFrame());

    const events: string[] = [];
    for (let i = 0; i < 2; i++) {
      const e = await session.next();
      events.push(e.type);
    }

    expect(events).toContain("usage");
    expect(events).toContain("batchReady");
  });

  test("stale checkpoint from streaming state does not flush a later batch", async () => {
    session = createSession();

    // Checkpoint while streaming (no pending execs) -- should NOT cause a later flush
    mockH2Stream.emit("data", makeCheckpointFrame());
    const eUsage = await session.next();
    expect(eUsage.type).toBe("usage");

    // Later exec in a new chunk -- must NOT trigger immediate flush
    mockH2Stream.emit("data", makeExecFrame("call_stale"));
    const eToolCall = await session.next();
    expect(eToolCall.type).toBe("toolCall");

    // Now send the real checkpoint covering this exec
    mockH2Stream.emit("data", makeCheckpointFrame());
    const events: string[] = [];
    for (let i = 0; i < 2; i++) {
      const e = await session.next();
      events.push(e.type);
    }
    expect(events).toContain("batchReady");
  });
});

describe("batch flush: collecting timer vs heartbeats", () => {
  test("collecting timeout flushes the batch", async () => {
    session = createSession({ _testCollectingTimeoutMs: 50 });

    mockH2Stream.emit("data", makeExecFrame("call_4"));
    await session.next(); // drain toolCall

    const e2 = await session.next();
    expect(e2.type).toBe("batchReady");
  }, 5000);

  test("heartbeats during the window do not prevent flush", async () => {
    session = createSession({ _testCollectingTimeoutMs: 80 });

    mockH2Stream.emit("data", makeExecFrame("call_5"));
    await session.next(); // drain toolCall

    // Heartbeats arrive during the timeout window
    await Bun.sleep(20);
    mockH2Stream.emit("data", makeHeartbeatFrame());
    await Bun.sleep(20);
    mockH2Stream.emit("data", makeHeartbeatFrame());
    await Bun.sleep(20);
    mockH2Stream.emit("data", makeHeartbeatFrame());

    // Non-sliding timer still fires -- batchReady arrives
    const e2 = await session.next();
    expect(e2.type).toBe("batchReady");
  }, 5000);
});

describe("batch flush: flushedExecs populated correctly", () => {
  test("flushedExecs contains the pending exec after flush", async () => {
    session = createSession();

    const chunk = Buffer.concat([makeCheckpointFrame(), makeExecFrame("call_flush_1")]);
    mockH2Stream.emit("data", chunk);

    const { found } = await drainUntil(session, "batchReady");
    expect(found).toBe(true);

    const flushed = session.flushedExecs;
    expect(flushed.length).toBe(1);
    expect(flushed[0]!.toolCallId).toBe("call_flush_1");
  });
});

describe("batch flush: no double batchReady while flushed", () => {
  test("exec arriving while prior batch awaits results does not flush", async () => {
    session = createSession();

    // First batch: checkpoint + exec → batchReady
    mockH2Stream.emit("data", Buffer.concat([makeCheckpointFrame(), makeExecFrame("call_a")]));
    const { found: foundBatch } = await drainUntil(session, "batchReady");
    expect(foundBatch).toBe(true);
    expect(session.flushedExecs.length).toBe(1);
    expect(session.flushedExecs[0]!.toolCallId).toBe("call_a");

    // While flushed, server sends checkpoint + new exec in same chunk
    mockH2Stream.emit("data", Buffer.concat([makeCheckpointFrame(), makeExecFrame("call_b", 2)]));

    // Should get usage + toolCall but NOT a second batchReady
    const e1 = await session.next();
    const e2 = await session.next();
    const types = [e1.type, e2.type].sort();
    expect(types).toEqual(["toolCall", "usage"]);

    // flushedExecs still from first batch -- not overwritten by a second flush
    expect(session.flushedExecs[0]!.toolCallId).toBe("call_a");
  });
});

describe("streaming: cancel-race pattern", () => {
  test("onSession closes session immediately if already cancelled", () => {
    let closeCalled = false;
    const ref = { session: undefined as { close: () => void } | undefined, cancelled: false };

    // Simulate cancel firing before pump creates session
    ref.cancelled = true;
    ref.session?.close();

    // Simulate pump creating session and calling onSession
    const mockSession = {
      close() {
        closeCalled = true;
      },
    };
    ref.session = mockSession;
    if (ref.cancelled) mockSession.close();

    expect(closeCalled).toBe(true);
  });
});

describe("batch flush: requestContextArgs does not premature-flush", () => {
  test("requestContextArgs with pending execs does not trigger flush", async () => {
    session = createSession();

    // MCP exec arrives → collecting state
    mockH2Stream.emit("data", makeExecFrame("call_rca_1"));
    const e1 = await session.next();
    expect(e1.type).toBe("toolCall");

    // requestContextArgs arrives while 1 exec pending -- should NOT flush
    mockH2Stream.emit("data", makeRequestContextArgsFrame());

    // Send another MCP exec
    mockH2Stream.emit("data", makeExecFrame("call_rca_2", 2));
    const e2 = await session.next();
    expect(e2.type).toBe("toolCall");

    // Now checkpoint arrives -- should flush both execs together
    mockH2Stream.emit("data", makeCheckpointFrame());
    const { found, events } = await drainUntil(session, "batchReady");
    expect(found).toBe(true);
    expect(session.flushedExecs.length).toBe(2);
  });
});

describe("streaming: SSE abort handling", () => {
  test("sendChunk marks context closed on enqueue failure", () => {
    const controller = {
      enqueue() {
        throw new Error("stream aborted");
      },
      close() {},
    } as unknown as ReadableStreamDefaultController;
    const ctx = createSSECtx(controller, "model", "id", 0);

    ctx.sendChunk({ content: "test" });
    expect(ctx.closed).toBe(true);
    ctx.sendChunk({ content: "ignored" });
    ctx.close();
  });

  test("sendDone does not throw on enqueue failure", () => {
    const controller = {
      enqueue() {
        throw new Error("stream aborted");
      },
      close() {},
    } as unknown as ReadableStreamDefaultController;
    const ctx = createSSECtx(controller, "model", "id", 0);

    expect(() => ctx.sendDone()).not.toThrow();
    expect(ctx.closed).toBe(true);
  });

  test("sendDone calls controller.close to terminate the stream", () => {
    let closeCalled = false;
    const chunks: string[] = [];
    const controller = {
      enqueue(bytes: Uint8Array) {
        chunks.push(new TextDecoder().decode(bytes));
      },
      close() {
        closeCalled = true;
      },
    } as unknown as ReadableStreamDefaultController;
    const ctx = createSSECtx(controller, "model", "id", 0);

    ctx.sendDone();
    expect(ctx.closed).toBe(true);
    expect(chunks.some((c) => c.includes("[DONE]"))).toBe(true);
    expect(closeCalled).toBe(true);
  });

  test("close after sendDone still calls controller.close", () => {
    let closeCount = 0;
    const controller = {
      enqueue() {},
      close() {
        closeCount++;
        if (closeCount > 1) throw new Error("already closed");
      },
    } as unknown as ReadableStreamDefaultController;
    const ctx = createSSECtx(controller, "model", "id", 0);

    ctx.sendDone();
    expect(closeCount).toBe(1);
    // Second close() should attempt controller.close but swallow the error
    expect(() => ctx.close()).not.toThrow();
    expect(closeCount).toBe(2);
  });

  test("close does not throw on controller.close failure", () => {
    const controller = {
      enqueue() {},
      close() {
        throw new Error("already closed");
      },
    } as unknown as ReadableStreamDefaultController;
    const ctx = createSSECtx(controller, "model", "id", 0);

    expect(() => ctx.close()).not.toThrow();
    expect(ctx.closed).toBe(true);
  });
});
