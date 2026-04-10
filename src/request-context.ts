import { create } from "@bufbuild/protobuf";
import {
  McpInstructionsSchema,
  type McpToolDefinition,
  RequestContextSchema,
} from "./proto/agent_pb";

const OPENCODE_MCP_SERVER_NAME = "opencode";
const OPENCODE_MCP_INSTRUCTIONS =
  "This environment provides tools prefixed with mcp_opencode_ (e.g. mcp_opencode_read, " +
  "mcp_opencode_grep, mcp_opencode_task). Always prefer these mcp_opencode_* tools over any " +
  "built-in native tools. In particular, use mcp_opencode_task for launching subagents — NEVER " +
  "use the built-in Subagent tool.";

export function buildRequestContext(mcpTools: McpToolDefinition[], cloudRule?: string) {
  return create(RequestContextSchema, {
    rules: [],
    repositoryInfo: [],
    tools: mcpTools,
    gitRepos: [],
    projectLayouts: [],
    mcpInstructions: [
      create(McpInstructionsSchema, {
        serverName: OPENCODE_MCP_SERVER_NAME,
        instructions: OPENCODE_MCP_INSTRUCTIONS,
      }),
    ],
    cloudRule: cloudRule || undefined,
    fileContents: {},
    customSubagents: [],
  });
}
