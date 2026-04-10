import type { OpenAIMessage, OpenAIToolDef } from "../../src/openai-messages";
import type { FakeRunConnection } from "./fake-cursor-backend";

export const ECHO_TOOL_NAME = "echo_tool";

export function createEchoToolDefinition(): OpenAIToolDef {
  return {
    type: "function",
    function: {
      name: ECHO_TOOL_NAME,
      description: "Echo text back to the caller",
      parameters: {
        type: "object",
        properties: {
          text: { type: "string" },
        },
        required: ["text"],
      },
    },
  };
}

export function createEchoTools(): OpenAIToolDef[] {
  return [createEchoToolDefinition()];
}

export function readEchoToolText(args: unknown): string {
  return typeof args === "object" && args && "text" in args ? String(args.text) : "";
}

export function formatEchoToolHistory(text: string, result: string): string {
  return `\n[Tool ${ECHO_TOOL_NAME}(${JSON.stringify({ text })})]\n${result}\n`;
}

export function findLatestUserText(messages: ReadonlyArray<OpenAIMessage>): string | null {
  for (let index = messages.length - 1; index >= 0; index--) {
    const message = messages[index]!;
    if (message.role !== "user") continue;
    return typeof message.content === "string" ? message.content : JSON.stringify(message.content);
  }
  return null;
}

export async function exchangeRequestContext(
  connection: FakeRunConnection,
  execMessageId: number,
): Promise<boolean> {
  connection.sendRequestContextArgs(execMessageId);
  if (connection.interrupted) return false;
  await connection.waitForRequestContextResult(execMessageId);
  return !connection.interrupted;
}

export async function exchangeRequestContextOrThrow(
  connection: FakeRunConnection,
  execMessageId: number,
): Promise<void> {
  const exchanged = await exchangeRequestContext(connection, execMessageId);
  if (!exchanged) throw new Error(`Request context exchange interrupted for ${execMessageId}`);
}
