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
  | { kind: "tool_call"; toolCall: OpenAIToolCall }
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

export class OpenAIConversationDriver {
  readonly messages: OpenAIMessage[];

  constructor(
    private readonly options: {
      baseUrl: string;
      model: string;
      sessionId: string;
      tools?: OpenAIToolDef[];
      toolExecutors?: Record<string, ToolExecutor>;
      initialMessages?: OpenAIMessage[];
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
          const executor = this.options.toolExecutors?.[toolCall.function.name];
          if (!executor) {
            throw new Error(`No tool executor registered for ${toolCall.function.name}`);
          }
          const args = toolCall.function.arguments ? JSON.parse(toolCall.function.arguments) : {};
          const result = await executor(args, toolCall);
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
    const response = await fetch(`${this.options.baseUrl}/chat/completions`, {
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
      throw new Error("Streaming response body missing");
    }

    return readSSE(response.body);
  }
}

async function readSSE(
  stream: ReadableStream<Uint8Array>,
): Promise<ConversationDriverRequestTrace> {
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
      if (payload === "[DONE]") return trace;
      applyPayload(trace, payload);
    }
  }

  return trace;
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
        tool_calls?: OpenAIToolCall[];
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
      tool_calls?: OpenAIToolCall[];
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

function appendToolCalls(trace: ConversationDriverRequestTrace, toolCalls: OpenAIToolCall[]): void {
  for (const toolCall of toolCalls) {
    trace.toolCalls.push(toolCall);
    trace.events.push({ kind: "tool_call", toolCall });
  }
}
