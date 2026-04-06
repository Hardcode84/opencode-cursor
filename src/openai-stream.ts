import { randomUUID } from "node:crypto";
import type { CursorSession, RetryHint, SessionEvent } from "./cursor-session";
import { createThinkingTagFilter } from "./thinking-filter";

export const SSE_HEADERS = {
  "Content-Type": "text/event-stream",
  "Cache-Control": "no-cache",
  Connection: "keep-alive",
} as const;

export interface SSECtx {
  sendChunk(delta: Record<string, unknown>, finishReason?: string | null): void;
  sendUsage(completion: number, total: number): void;
  sendDone(): void;
  close(): void;
  readonly closed: boolean;
}

export function createSSECtx(
  controller: ReadableStreamDefaultController,
  modelId: string,
  completionId: string,
  created: number,
): SSECtx {
  const encoder = new TextEncoder();
  let closed = false;

  const sendRaw = (data: object) => {
    if (closed) return;
    controller.enqueue(encoder.encode(`data: ${JSON.stringify(data)}\n\n`));
  };

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
    sendUsage(completion, total) {
      sendRaw({
        id: completionId,
        object: "chat.completion.chunk",
        created,
        model: modelId,
        choices: [],
        usage: { prompt_tokens: 0, completion_tokens: completion, total_tokens: total },
      });
    },
    sendDone() {
      if (closed) return;
      controller.enqueue(encoder.encode("data: [DONE]\n\n"));
    },
    close() {
      if (closed) return;
      closed = true;
      controller.close();
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
 * For retryable errors, returns 'retry' without writing stop/DONE — the
 * caller can create a new session and call pumpSession again on the same ctx.
 */
// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: event loop with thinking/native branching
export async function pumpSession(session: CursorSession, ctx: SSECtx): Promise<PumpResult> {
  const tagFilter = createThinkingTagFilter();
  let hasNativeThinking = false;
  let toolCallIndex = 0;

  while (true) {
    const event: SessionEvent = await session.next();
    if (ctx.closed) return { outcome: "done" };

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
        const flushed = tagFilter.flush();
        if (flushed.reasoning) ctx.sendChunk({ reasoning_content: flushed.reasoning });
        if (flushed.content) ctx.sendChunk({ content: flushed.content });
        ctx.sendChunk({}, "tool_calls");
        ctx.sendDone();
        return { outcome: "batchReady" };
      }

      case "usage":
        ctx.sendUsage(event.outputTokens, event.totalTokens || event.outputTokens);
        break;

      case "done": {
        const flushed = tagFilter.flush();
        if (flushed.reasoning) ctx.sendChunk({ reasoning_content: flushed.reasoning });
        if (flushed.content) ctx.sendChunk({ content: flushed.content });

        if (event.retryHint) {
          return { outcome: "retry", retryHint: event.retryHint, error: event.error || "" };
        }

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
  while (true) {
    const event = await session.next();
    if (event.type === "text" && !event.isThinking) {
      const { content } = tagFilter.process(event.text);
      text += content;
    } else if (event.type === "done") {
      text += tagFilter.flush().content;
      break;
    }
  }
  session.close();

  return new Response(
    JSON.stringify({
      id: `chatcmpl-${randomUUID().replace(/-/g, "").slice(0, 28)}`,
      object: "chat.completion",
      created: Math.floor(Date.now() / 1000),
      model: modelId,
      choices: [{ index: 0, message: { role: "assistant", content: text }, finish_reason: "stop" }],
      usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
    }),
    { headers: { "Content-Type": "application/json" } },
  );
}
