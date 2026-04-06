import { randomUUID } from "node:crypto";
import type { CursorSession, RetryHint, SessionEvent } from "./cursor-session";
import { logDebug } from "./logger";
import { createThinkingTagFilter } from "./thinking-filter";

export const SSE_HEADERS = {
  "Content-Type": "text/event-stream",
  "Cache-Control": "no-cache",
  Connection: "keep-alive",
} as const;

export interface SSECtx {
  sendChunk(delta: Record<string, unknown>, finishReason?: string | null): void;
  sendUsage(usage: OpenAIUsage): void;
  sendDone(): void;
  close(): void;
  readonly closed: boolean;
}

const SSE_KEEPALIVE_MS = 15_000;

interface OpenAIUsage {
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
}

function sanitizeTokenCount(value: number): number {
  return Number.isFinite(value) ? Math.max(0, Math.trunc(value)) : 0;
}

function buildUsage(completionTokens: number, totalTokens: number): OpenAIUsage | null {
  const completion = sanitizeTokenCount(completionTokens);
  const reportedTotal = sanitizeTokenCount(totalTokens);
  if (completion === 0 && reportedTotal === 0) return null;
  if (reportedTotal === 0) return null;
  const total = Math.max(completion, reportedTotal);
  return {
    prompt_tokens: Math.max(0, total - completion),
    completion_tokens: completion,
    total_tokens: total,
  };
}

function pickBetterUsage(
  current: OpenAIUsage | null,
  candidate: OpenAIUsage | null,
): OpenAIUsage | null {
  if (!candidate) return current;
  if (!current) return candidate;
  if (candidate.total_tokens > current.total_tokens) return candidate;
  if (candidate.total_tokens === current.total_tokens) {
    if (candidate.completion_tokens > current.completion_tokens) return candidate;
    if (
      candidate.completion_tokens === current.completion_tokens &&
      candidate.prompt_tokens > current.prompt_tokens
    ) {
      return candidate;
    }
  }
  return current;
}

export function createSSECtx(
  controller: ReadableStreamDefaultController,
  modelId: string,
  completionId: string,
  created: number,
): SSECtx {
  const encoder = new TextEncoder();
  let closed = false;

  // stopKeepalive is defined after keepaliveTimer below; closure resolves at call time.
  const markClosed = () => {
    if (closed) return false;
    closed = true;
    stopKeepalive();
    return true;
  };

  const safeEnqueue = (bytes: Uint8Array) => {
    if (closed) return;
    try {
      controller.enqueue(bytes);
    } catch {
      markClosed(); // stream aborted by client -- stop all further writes
    }
  };

  const sendRaw = (data: object) => {
    safeEnqueue(encoder.encode(`data: ${JSON.stringify(data)}\n\n`));
  };

  const keepaliveTimer = setInterval(() => {
    safeEnqueue(encoder.encode(": keep-alive\n\n"));
  }, SSE_KEEPALIVE_MS);
  if (typeof keepaliveTimer === "object" && "unref" in keepaliveTimer) {
    keepaliveTimer.unref();
  }

  const stopKeepalive = () => clearInterval(keepaliveTimer);

  return {
    sendChunk(delta, finishReason = null) {
      sendRaw({
        id: completionId,
        object: "chat.completion.chunk",
        created,
        model: modelId,
        choices: [{ index: 0, delta, finish_reason: finishReason }],
      });
    },
    sendUsage(usage) {
      sendRaw({
        id: completionId,
        object: "chat.completion.chunk",
        created,
        model: modelId,
        choices: [],
        usage,
      });
    },
    sendDone() {
      if (!markClosed()) return;
      try {
        controller.enqueue(encoder.encode("data: [DONE]\n\n"));
        controller.close();
      } catch {
        /* stream already aborted */
      }
    },
    // Idempotent teardown: always attempts controller.close() even if
    // sendDone already closed it, so finally-blocks can call unconditionally.
    close() {
      markClosed();
      try {
        controller.close();
      } catch {
        /* already closed or aborted */
      }
    },
    get closed() {
      return closed;
    },
  };
}

export type PumpResult =
  | { outcome: "done" }
  | { outcome: "batchReady" }
  | { outcome: "retry"; retryHint: RetryHint; error: string };

/**
 * Drain events from a CursorSession and write them as SSE chunks.
 * Returns when the session emits batchReady (tool_calls pause) or done.
 * Also returns early with 'done' if the SSE context is already closed
 * (e.g. client disconnect). For retryable errors, returns 'retry' without
 * writing stop/DONE -- the caller can create a new session and call
 * pumpSession again on the same ctx.
 */
// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: event loop with thinking/native branching
export async function pumpSession(session: CursorSession, ctx: SSECtx): Promise<PumpResult> {
  const tagFilter = createThinkingTagFilter();
  let hasNativeThinking = false;
  let toolCallIndex = 0;
  let bestUsage: OpenAIUsage | null = null;

  const sendUsageIfBetter = (completionTokens: number, totalTokens: number) => {
    const nextUsage = pickBetterUsage(bestUsage, buildUsage(completionTokens, totalTokens));
    if (!nextUsage || nextUsage === bestUsage) return;
    ctx.sendUsage(nextUsage);
    bestUsage = nextUsage;
  };

  while (true) {
    const event: SessionEvent = await session.next();
    if (ctx.closed) {
      logDebug("pumpSession: ctx already closed, dropping event", { eventType: event.type });
      return { outcome: "done" };
    }

    switch (event.type) {
      case "text":
        if (event.isThinking) {
          hasNativeThinking = true;
          ctx.sendChunk({ reasoning_content: event.text });
        } else if (hasNativeThinking) {
          ctx.sendChunk({ content: event.text });
        } else {
          const { content, reasoning } = tagFilter.process(event.text);
          if (reasoning) ctx.sendChunk({ reasoning_content: reasoning });
          if (content) ctx.sendChunk({ content });
        }
        break;

      case "toolCall":
        ctx.sendChunk({
          tool_calls: [
            {
              index: toolCallIndex++,
              id: event.exec.toolCallId,
              type: "function",
              function: {
                name: event.exec.toolName,
                arguments: event.exec.decodedArgs,
              },
            },
          ],
        });
        break;

      case "batchReady": {
        logDebug("pumpSession: batchReady, sending finish_reason=tool_calls");
        const flushed = tagFilter.flush();
        if (flushed.reasoning) ctx.sendChunk({ reasoning_content: flushed.reasoning });
        if (flushed.content) ctx.sendChunk({ content: flushed.content });
        sendUsageIfBetter(session.outputTokens, session.totalTokens);
        ctx.sendChunk({}, "tool_calls");
        ctx.sendDone();
        return { outcome: "batchReady" };
      }

      case "usage":
        sendUsageIfBetter(event.outputTokens, event.totalTokens);
        break;

      case "done": {
        const flushed = tagFilter.flush();
        if (flushed.reasoning) ctx.sendChunk({ reasoning_content: flushed.reasoning });
        if (flushed.content) ctx.sendChunk({ content: flushed.content });

        if (event.retryHint) {
          return { outcome: "retry", retryHint: event.retryHint, error: event.error || "" };
        }

        sendUsageIfBetter(session.outputTokens, session.totalTokens);

        if (event.error) {
          ctx.sendChunk({ content: `\n[Error: ${event.error}]` });
        }
        ctx.sendChunk({}, "stop");
        ctx.sendDone();
        return { outcome: "done" };
      }
    }
  }
}

export async function collectNonStreamingResponse(
  session: CursorSession,
  modelId: string,
): Promise<Response> {
  const tagFilter = createThinkingTagFilter();
  let text = "";
  let usage: OpenAIUsage | null = null;
  while (true) {
    const event = await session.next();
    if (event.type === "text" && !event.isThinking) {
      const { content } = tagFilter.process(event.text);
      text += content;
    } else if (event.type === "usage") {
      usage = pickBetterUsage(usage, buildUsage(event.outputTokens, event.totalTokens));
    } else if (event.type === "done") {
      text += tagFilter.flush().content;
      break;
    }
  }
  usage = pickBetterUsage(usage, buildUsage(session.outputTokens, session.totalTokens));
  session.close();

  return new Response(
    JSON.stringify({
      id: `chatcmpl-${randomUUID().replace(/-/g, "").slice(0, 28)}`,
      object: "chat.completion",
      created: Math.floor(Date.now() / 1000),
      model: modelId,
      choices: [{ index: 0, message: { role: "assistant", content: text }, finish_reason: "stop" }],
      ...(usage ? { usage } : {}),
    }),
    { headers: { "Content-Type": "application/json" } },
  );
}
