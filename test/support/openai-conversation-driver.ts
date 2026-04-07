import type { OpenAIMessage, OpenAIToolCall, OpenAIToolDef } from "../../src/openai-messages";

export interface ConversationDriverToolExecution {
  toolCallId: string;
  toolName: string;
  args: unknown;
  result: string;
}

export type ConversationDriverEvent =
  | { kind: "reasoning"; text: string }
  | { kind: "content"; text: string }
  | { kind: "toolCall"; toolCall: OpenAIToolCall }
  | {
      kind: "usage";
      usage: { prompt_tokens: number; completion_tokens: number; total_tokens: number };
    }
  | { kind: "finish"; reason: string | null };

export interface ConversationDriverRequestTrace {
  reasoning: string;
  content: string;
  toolCalls: OpenAIToolCall[];
  finishReason: string | null;
  events: ConversationDriverEvent[];
}

export interface ConversationDriverTurnTrace {
  userText: string;
  requests: ConversationDriverRequestTrace[];
  toolExecutions: ConversationDriverToolExecution[];
  assistantText: string;
  reasoningText: string;
}

type ToolExecutor = (args: unknown, toolCall: OpenAIToolCall) => string | Promise<string>;
type ResponseValidator = (
  messages: ReadonlyArray<OpenAIMessage>,
  trace: ConversationDriverRequestTrace,
) => string | null;
type RetryHook = (attempt: number, error: Error) => void | Promise<void>;

export interface NormalizedConversationMessage {
  role: OpenAIMessage["role"];
  content: string | null;
  toolCallId?: string;
  toolCalls?: Array<{ id: string; name: string; arguments: string }>;
}

export class OpenAIConversationDriver {
  readonly messages: OpenAIMessage[];
  private readonly toolResultCache = new Map<string, string>();

  constructor(
    private readonly options: {
      baseUrl: string | (() => string);
      model: string;
      sessionId: string;
      tools?: OpenAIToolDef[];
      toolExecutors?: Record<string, ToolExecutor>;
      initialMessages?: OpenAIMessage[];
      maxRequestRetries?: number;
      responseValidator?: ResponseValidator;
      onRetry?: RetryHook;
    },
  ) {
    this.messages = [...(options.initialMessages ?? [])];
  }

  async runTurn(userText: string): Promise<ConversationDriverTurnTrace> {
    this.messages.push({ role: "user", content: userText });
    const requests: ConversationDriverRequestTrace[] = [];
    const toolExecutions: ConversationDriverToolExecution[] = [];
    let assistantText = "";
    let reasoningText = "";

    for (;;) {
      const requestTrace = await this.postCurrentConversation();
      requests.push(requestTrace);
      assistantText += requestTrace.content;
      reasoningText += requestTrace.reasoning;

      if (requestTrace.finishReason === "tool_calls") {
        if (requestTrace.toolCalls.length === 0) {
          throw new Error("Received finish_reason=tool_calls without tool calls");
        }
        this.messages.push({
          role: "assistant",
          content: requestTrace.content || null,
          tool_calls: requestTrace.toolCalls,
        });

        for (const toolCall of requestTrace.toolCalls) {
          const args = parseToolCallArguments(toolCall);
          const result = await this.resolveToolResult(toolCall, args);
          toolExecutions.push({
            toolCallId: toolCall.id,
            toolName: toolCall.function.name,
            args,
            result,
          });
          this.messages.push({
            role: "tool",
            tool_call_id: toolCall.id,
            content: result,
          });
        }
        continue;
      }

      if (requestTrace.finishReason && requestTrace.finishReason !== "stop") {
        throw new Error(`Unexpected finish reason: ${requestTrace.finishReason}`);
      }

      this.messages.push({
        role: "assistant",
        content: requestTrace.content || null,
      });

      return {
        userText,
        requests,
        toolExecutions,
        assistantText,
        reasoningText,
      };
    }
  }

  private async postCurrentConversation(): Promise<ConversationDriverRequestTrace> {
    const maxRetries = this.options.maxRequestRetries ?? 0;
    let attempt = 0;
    for (;;) {
      try {
        return await this.postCurrentConversationOnce();
      } catch (error) {
        if (!(error instanceof RetryableConversationRequestError) || attempt >= maxRetries) {
          throw error;
        }
        await this.options.onRetry?.(attempt + 1, error);
        attempt++;
      }
    }
  }

  private async resolveToolResult(toolCall: OpenAIToolCall, args: unknown): Promise<string> {
    const cached = this.toolResultCache.get(toolCall.id);
    if (cached !== undefined) return cached;
    const executor = this.options.toolExecutors?.[toolCall.function.name];
    if (!executor) {
      throw new Error(`No tool executor registered for ${toolCall.function.name}`);
    }
    const result = await executor(args, toolCall);
    this.toolResultCache.set(toolCall.id, result);
    return result;
  }

  private async postCurrentConversationOnce(): Promise<ConversationDriverRequestTrace> {
    const response = await fetch(`${this.resolveBaseUrl()}/chat/completions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-session-affinity": this.options.sessionId,
      },
      body: JSON.stringify({
        model: this.options.model,
        stream: true,
        messages: this.messages,
        tools: this.options.tools ?? [],
      }),
    });

    if (!response.ok) {
      throw new Error(`Chat completion failed with status ${response.status}`);
    }
    if (!response.body) {
      throw new RetryableConversationRequestError("Streaming response body missing");
    }

    const trace = await readSSE(response.body);
    if (isRetryableFailureTrace(trace)) {
      throw new RetryableConversationRequestError(trace.content.trim());
    }
    const validatorMessage = this.options.responseValidator?.(this.messages, trace);
    if (validatorMessage) {
      throw new RetryableConversationRequestError(validatorMessage);
    }
    return trace;
  }

  private resolveBaseUrl(): string {
    return typeof this.options.baseUrl === "function"
      ? this.options.baseUrl()
      : this.options.baseUrl;
  }
}

async function readSSE(
  stream: ReadableStream<Uint8Array>,
): Promise<ConversationDriverRequestTrace> {
  try {
    const reader = stream.getReader();
    const decoder = new TextDecoder();
    let pending = "";
    const trace = createEmptyTrace();

    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      pending += decoder.decode(value, { stream: true });
      const blocks = drainSseBlocks(pending);
      pending = blocks.pending;
      for (const payload of blocks.payloads) {
        if (payload === "[DONE]") {
          ensureTerminalTrace(trace);
          return trace;
        }
        applyPayload(trace, payload);
      }
    }
  } catch (error) {
    throw new RetryableConversationRequestError(
      error instanceof Error ? error.message : String(error),
    );
  }

  throw new RetryableConversationRequestError("Streaming response ended before [DONE]");
}

function extractDataPayload(block: string): string | null {
  const payloads = block
    .split("\n")
    .map((line) => line.replace(/\r$/, ""))
    .filter((line) => line.startsWith("data: "))
    .map((line) => line.slice("data: ".length));
  if (payloads.length === 0) return null;
  return payloads.join("\n");
}

function createEmptyTrace(): ConversationDriverRequestTrace {
  return {
    reasoning: "",
    content: "",
    toolCalls: [],
    finishReason: null,
    events: [],
  };
}

function drainSseBlocks(input: string): { payloads: string[]; pending: string } {
  let pending = input;
  const payloads: string[] = [];
  for (;;) {
    const boundary = pending.indexOf("\n\n");
    if (boundary === -1) break;
    const block = pending.slice(0, boundary);
    pending = pending.slice(boundary + 2);
    const payload = extractDataPayload(block);
    if (payload) payloads.push(payload);
  }
  return { payloads, pending };
}

function applyPayload(trace: ConversationDriverRequestTrace, payload: string): void {
  const parsed = JSON.parse(payload) as {
    choices?: Array<{
      delta?: {
        reasoning_content?: string;
        content?: string;
        tool_calls?: Array<{
          index?: number;
          id?: string;
          type?: "function";
          function?: { name?: string; arguments?: string };
        }>;
      };
      finish_reason?: string | null;
    }>;
    usage?: { prompt_tokens: number; completion_tokens: number; total_tokens: number };
  };

  if (parsed.usage) {
    trace.events.push({ kind: "usage", usage: parsed.usage });
  }

  for (const choice of parsed.choices ?? []) {
    applyChoice(trace, choice);
  }
}

function applyChoice(
  trace: ConversationDriverRequestTrace,
  choice: {
    delta?: {
      reasoning_content?: string;
      content?: string;
      tool_calls?: Array<{
        index?: number;
        id?: string;
        type?: "function";
        function?: { name?: string; arguments?: string };
      }>;
    };
    finish_reason?: string | null;
  },
): void {
  const delta = choice.delta ?? {};
  appendReasoning(trace, delta.reasoning_content);
  appendContent(trace, delta.content);
  appendToolCalls(trace, delta.tool_calls ?? []);
  if (choice.finish_reason !== undefined) {
    trace.finishReason = choice.finish_reason;
    trace.events.push({ kind: "finish", reason: trace.finishReason });
  }
}

function appendReasoning(trace: ConversationDriverRequestTrace, text: string | undefined): void {
  if (!text) return;
  trace.reasoning += text;
  trace.events.push({ kind: "reasoning", text });
}

function appendContent(trace: ConversationDriverRequestTrace, text: string | undefined): void {
  if (!text) return;
  trace.content += text;
  trace.events.push({ kind: "content", text });
}

function appendToolCalls(
  trace: ConversationDriverRequestTrace,
  toolCalls: Array<{
    index?: number;
    id?: string;
    type?: "function";
    function?: { name?: string; arguments?: string };
  }>,
): void {
  for (const toolCall of toolCalls) {
    const index = resolveToolCallIndex(trace, toolCall);
    const existing = trace.toolCalls[index];
    const merged: OpenAIToolCall = {
      id: typeof toolCall.id === "string" ? toolCall.id : (existing?.id ?? ""),
      type: "function",
      function: {
        name:
          typeof toolCall.function?.name === "string"
            ? toolCall.function.name
            : (existing?.function.name ?? "unknown"),
        arguments:
          typeof toolCall.function?.arguments === "string"
            ? `${existing?.function.arguments ?? ""}${toolCall.function.arguments}`
            : (existing?.function.arguments ?? ""),
      },
    };
    if (index === trace.toolCalls.length) {
      trace.toolCalls.push(merged);
    } else {
      trace.toolCalls[index] = merged;
    }
    trace.events.push({ kind: "toolCall", toolCall: merged });
  }
}

function resolveToolCallIndex(
  trace: ConversationDriverRequestTrace,
  toolCall: {
    index?: number;
    id?: string;
  },
): number {
  if (
    Number.isInteger(toolCall.index) &&
    toolCall.index !== undefined &&
    toolCall.index >= 0 &&
    toolCall.index <= trace.toolCalls.length
  ) {
    return toolCall.index;
  }
  if (typeof toolCall.id === "string") {
    const existingIndex = trace.toolCalls.findIndex((candidate) => candidate.id === toolCall.id);
    if (existingIndex >= 0) return existingIndex;
  }
  return trace.toolCalls.length;
}

function parseToolCallArguments(toolCall: OpenAIToolCall): unknown {
  try {
    return toolCall.function.arguments ? JSON.parse(toolCall.function.arguments) : {};
  } catch (error) {
    throw new Error(
      `Tool call ${toolCall.id || toolCall.function.name} had invalid JSON arguments: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

function ensureTerminalTrace(trace: ConversationDriverRequestTrace): void {
  if (trace.finishReason === "stop" || trace.finishReason === "tool_calls") return;
  throw new RetryableConversationRequestError(
    `Streaming response missing terminal finish reason, got ${trace.finishReason ?? "null"}`,
  );
}

function isRetryableFailureTrace(trace: ConversationDriverRequestTrace): boolean {
  if (trace.finishReason !== "stop") return false;
  const trimmedContent = trace.content.trim();
  if (/^\[Error: .+\]$/s.test(trimmedContent)) return true;
  return (
    trimmedContent.length === 0 && trace.reasoning.length === 0 && trace.toolCalls.length === 0
  );
}

export function normalizeConversationMessages(
  messages: OpenAIMessage[],
): NormalizedConversationMessage[] {
  return messages.map((message) => ({
    role: message.role,
    content:
      typeof message.content === "string"
        ? message.content
        : message.content
          ? JSON.stringify(message.content)
          : null,
    ...(message.tool_call_id ? { toolCallId: message.tool_call_id } : {}),
    ...(message.tool_calls
      ? {
          toolCalls: message.tool_calls.map((toolCall) => ({
            id: toolCall.id,
            name: toolCall.function.name,
            arguments: toolCall.function.arguments,
          })),
        }
      : {}),
  }));
}

class RetryableConversationRequestError extends Error {}
