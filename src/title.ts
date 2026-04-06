import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { callCursorUnaryRpc } from "./cursor-session";
import { logInfo } from "./logger";
import { textContent } from "./openai-messages";
import { SSE_HEADERS } from "./openai-stream";
import { NameAgentRequestSchema, NameAgentResponseSchema } from "./proto/agent_pb";

const TITLE_REQUEST_MARKER = "Generate a title for this conversation:";

interface TitleCheckMessage {
  role: string;
  content: string | null | Array<{ type: string; text?: string }>;
  tool_calls?: unknown[];
}

interface TitleCheckBody {
  tools?: unknown[];
  messages: TitleCheckMessage[];
}

export function detectTitleRequest(body: TitleCheckBody): boolean {
  if ((body.tools?.length ?? 0) > 0) return false;
  const firstUser = body.messages.find((m) => m.role === "user");
  return !!firstUser && textContent(firstUser.content).trim() === TITLE_REQUEST_MARKER;
}

export function buildTitleSourceText(messages: TitleCheckMessage[]): string {
  return messages
    .filter((m) => m.role !== "system")
    .map((m) => {
      const text = textContent(m.content).trim();
      return text === TITLE_REQUEST_MARKER ? "" : text;
    })
    .filter(Boolean)
    .join("\n")
    .trim();
}

function finalizeTitle(value: string): string {
  return value
    .replace(/^#{1,6}\s*/, "")
    .replace(/[.!?,:;]+$/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 80)
    .trim();
}

function deriveFallbackTitle(text: string): string {
  const cleaned = text
    .replace(/<[^>]+>/g, " ")
    .replace(/\[[^\]]+\]/g, " ")
    .replace(/[^\p{L}\p{N}''\u2019\-\s]+/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!cleaned) return "";
  const words = cleaned.split(" ").filter(Boolean).slice(0, 6);
  return finalizeTitle(words.map((w) => w[0]!.toUpperCase() + w.slice(1)).join(" "));
}

export async function handleTitleGenerationRequest(
  sourceText: string,
  accessToken: string,
  modelId: string,
  stream: boolean,
): Promise<Response> {
  let title: string;
  try {
    const requestBody = toBinary(
      NameAgentRequestSchema,
      create(NameAgentRequestSchema, { userMessage: sourceText }),
    );
    const response = await callCursorUnaryRpc({
      accessToken,
      rpcPath: "/agent.v1.AgentService/NameAgent",
      requestBody,
      timeoutMs: 5_000,
    });
    if (response.timedOut || response.exitCode !== 0) {
      title = deriveFallbackTitle(sourceText);
    } else {
      let payload = response.body;
      if (payload.length > 5 && payload[0] === 0x00) payload = payload.slice(5);
      const decoded = fromBinary(NameAgentResponseSchema, payload);
      title = finalizeTitle(decoded.name) || deriveFallbackTitle(sourceText);
    }
  } catch {
    title = deriveFallbackTitle(sourceText);
  }
  title = title || "Untitled Session";
  logInfo("title generated", { title });

  const completionId = `chatcmpl-${crypto.randomUUID().replace(/-/g, "").slice(0, 28)}`;
  const created = Math.floor(Date.now() / 1000);
  const usage = { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 };

  if (stream) {
    const chunks = `${[
      {
        id: completionId,
        object: "chat.completion.chunk",
        created,
        model: modelId,
        choices: [{ index: 0, delta: { content: title }, finish_reason: null }],
      },
      {
        id: completionId,
        object: "chat.completion.chunk",
        created,
        model: modelId,
        choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
      },
      {
        id: completionId,
        object: "chat.completion.chunk",
        created,
        model: modelId,
        choices: [],
        usage,
      },
    ]
      .map((c) => `data: ${JSON.stringify(c)}\n\n`)
      .join("")}data: [DONE]\n\n`;
    return new Response(chunks, { headers: SSE_HEADERS });
  }

  return new Response(
    JSON.stringify({
      id: completionId,
      object: "chat.completion",
      created,
      model: modelId,
      choices: [
        { index: 0, message: { role: "assistant", content: title }, finish_reason: "stop" },
      ],
      usage,
    }),
    { headers: { "Content-Type": "application/json" } },
  );
}
