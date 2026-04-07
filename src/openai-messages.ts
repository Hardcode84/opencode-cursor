/**
 * Pure, side-effect-free utilities for parsing OpenAI-shaped chat messages.
 * Extracted from server.ts so that tests (and future consumers) do not
 * depend on the HTTP orchestration module graph.
 */

export interface OpenAIToolCall {
  id: string;
  type: "function";
  function: { name: string; arguments: string };
}

export interface ContentPart {
  type: string;
  text?: string;
}

export interface OpenAIMessage {
  role: "system" | "user" | "assistant" | "tool";
  content: string | null | ContentPart[];
  tool_call_id?: string;
  tool_calls?: OpenAIToolCall[];
}

export interface OpenAIToolDef {
  type: "function";
  function: {
    name: string;
    description?: string;
    parameters?: Record<string, unknown>;
  };
}

export interface ToolResultInfo {
  toolCallId: string;
  content: string;
}

export interface ParsedMessages {
  systemPrompt: string;
  userText: string;
  turns: Array<{ userText: string; assistantText: string }>;
  toolResults: ToolResultInfo[];
}

function normalizeToolCall(raw: unknown): OpenAIToolCall | null {
  if (!raw || typeof raw !== "object") return null;
  const call = raw as {
    id?: unknown;
    function?: { name?: unknown; arguments?: unknown };
  };
  return {
    id: typeof call.id === "string" ? call.id : "",
    type: "function",
    function: {
      name: typeof call.function?.name === "string" ? call.function.name : "unknown",
      arguments: typeof call.function?.arguments === "string" ? call.function.arguments : "",
    },
  };
}

function normalizeToolCalls(raw: unknown): OpenAIToolCall[] {
  if (!Array.isArray(raw)) return [];
  return raw.flatMap((call) => {
    const normalized = normalizeToolCall(call);
    return normalized ? [normalized] : [];
  });
}

export function textContent(content: OpenAIMessage["content"]): string {
  if (content == null) return "";
  if (typeof content === "string") return content;
  return content
    .filter(
      (part): part is ContentPart =>
        !!part &&
        typeof part === "object" &&
        part.type === "text" &&
        typeof part.text === "string" &&
        part.text.length > 0,
    )
    .map((part) => part.text)
    .join("\n");
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: OpenAI message parsing with role/tool interleaving
export function parseMessages(messages: OpenAIMessage[]): ParsedMessages {
  let systemPrompt = "You are a helpful assistant.";
  const pairs: Array<{ userText: string; assistantText: string }> = [];
  const toolResults: ToolResultInfo[] = [];

  const systemParts = messages
    .filter((m) => m.role === "system")
    .map((m) => textContent(m.content));
  if (systemParts.length > 0) systemPrompt = systemParts.join("\n");

  const nonSystem = messages.filter((m) => m.role !== "system");
  let pendingUser = "";
  let pendingAssistant = "";
  const pendingToolCalls: OpenAIToolCall[] = [];

  for (const msg of nonSystem) {
    if (msg.role === "tool") {
      const toolId = msg.tool_call_id ?? "";
      const call = pendingToolCalls.find((tc) => tc.id === toolId);
      const toolContent = textContent(msg.content);
      if (call) {
        const argsPreview =
          call.function.arguments.length > 200
            ? `${call.function.arguments.slice(0, 200)}...`
            : call.function.arguments;
        const resultPreview =
          toolContent.length > 20000
            ? `${toolContent.slice(0, 20000)}\n...[truncated from ${toolContent.length} chars]`
            : toolContent;
        pendingAssistant += `\n[Tool ${call.function.name}(${argsPreview})]\n${resultPreview}\n`;
      }
      toolResults.push({ toolCallId: toolId, content: toolContent });
    } else if (msg.role === "user") {
      if (pendingUser) {
        pairs.push({ userText: pendingUser, assistantText: pendingAssistant });
        pendingAssistant = "";
        pendingToolCalls.length = 0;
      }
      pendingUser = textContent(msg.content);
    } else if (msg.role === "assistant") {
      const text = textContent(msg.content);
      if (text) pendingAssistant += text;
      const normalizedToolCalls = normalizeToolCalls(msg.tool_calls);
      if (normalizedToolCalls.length > 0) pendingToolCalls.push(...normalizedToolCalls);
      if (pendingUser && normalizedToolCalls.length === 0) {
        pairs.push({ userText: pendingUser, assistantText: pendingAssistant });
        pendingUser = "";
        pendingAssistant = "";
        pendingToolCalls.length = 0;
      }
    }
  }

  let lastUserText = "";
  if (pendingUser) {
    lastUserText = pendingUser;
  } else if (pairs.length > 0 && toolResults.length === 0) {
    const last = pairs.pop();
    if (last) lastUserText = last.userText;
  }

  return { systemPrompt, userText: lastUserText, turns: pairs, toolResults };
}

export function selectToolsForChoice(tools: OpenAIToolDef[], toolChoice: unknown): OpenAIToolDef[] {
  if (!tools.length) return [];
  if (
    toolChoice === undefined ||
    toolChoice === null ||
    toolChoice === "auto" ||
    toolChoice === "required"
  )
    return tools;
  if (toolChoice === "none") return [];
  if (typeof toolChoice === "object" && toolChoice !== null) {
    const choice = toolChoice as { type?: unknown; function?: { name?: unknown } };
    if (choice.type === "function" && typeof choice.function?.name === "string") {
      return tools.filter((t) => t.function.name === choice.function!.name);
    }
  }
  return tools;
}
