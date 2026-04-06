/**
 * Cursor model discovery via GetUsableModels.
 * Uses the H2 bridge for transport. Falls back to a hardcoded list
 * when discovery fails.
 */
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { z } from "zod";
import { callCursorUnaryRpc } from "./cursor-session";
import {
  GetUsableModelsRequestSchema,
  GetUsableModelsResponseSchema,
} from "./proto/agent_pb";

// TODO: switch to aiserver.v1.AvailableModels which returns per-model
// context_token_limit and context_token_limit_for_max_mode fields.
// agent.v1.GetUsableModels lacks context window info entirely.
const GET_USABLE_MODELS_PATH = "/agent.v1.AgentService/GetUsableModels";

const DEFAULT_CONTEXT_WINDOW = 200_000;
const DEFAULT_MAX_TOKENS = 64_000;

const MODEL_LIMITS: Record<string, { context?: number; maxTokens?: number }> = {
  // Claude — 1M variants
  "claude-4-sonnet-1m":         { context: 1_000_000 },
  "claude-4.5-opus":            { context: 200_000, maxTokens: 128_000 },
  "claude-4.6-opus":            { context: 200_000, maxTokens: 128_000 },
  "claude-4.6-opus-fast":       { context: 200_000, maxTokens: 128_000 },
  "claude-4.6-opus-high":       { context: 200_000, maxTokens: 128_000 },
  // GPT — larger contexts
  "gpt-5.2":                    { context: 400_000, maxTokens: 128_000 },
  "gpt-5.2-codex":              { context: 400_000, maxTokens: 128_000 },
  "gpt-5.3-codex":              { context: 400_000, maxTokens: 128_000 },
  "gpt-5.4":                    { context: 272_000, maxTokens: 128_000 },
  "gpt-5.4-medium":             { context: 272_000, maxTokens: 128_000 },
  // Gemini — 1M+
  "gemini-3-pro":               { context: 1_000_000 },
  "gemini-3.1-pro":             { context: 1_000_000 },
  "gemini-3-flash":             { context: 1_000_000 },
  "gemini-2.5-flash":           { context: 1_000_000 },
};

const CursorModelDetailsSchema = z.object({
  modelId: z.string(),
  displayName: z.string().optional().catch(undefined),
  displayNameShort: z.string().optional().catch(undefined),
  displayModelId: z.string().optional().catch(undefined),
  aliases: z
    .array(z.unknown())
    .optional()
    .catch([])
    .transform((aliases) =>
      (aliases ?? []).filter(
        (alias: unknown): alias is string => typeof alias === "string",
      ),
    ),
  thinkingDetails: z.unknown().optional(),
  maxMode: z.boolean().optional().catch(undefined),
});

type CursorModelDetails = z.infer<typeof CursorModelDetailsSchema>;

export interface CursorModel {
  id: string;
  name: string;
  reasoning: boolean;
  contextWindow: number;
  maxTokens: number;
}

const FALLBACK_MODELS: CursorModel[] = [
  // Composer models
  { id: "composer-1", name: "Composer 1", reasoning: true, contextWindow: 200_000, maxTokens: 64_000 },
  { id: "composer-1.5", name: "Composer 1.5", reasoning: true, contextWindow: 200_000, maxTokens: 64_000 },
  // Claude models
  { id: "claude-4.6-opus-high", name: "Claude 4.6 Opus", reasoning: true, contextWindow: 200_000, maxTokens: 128_000 },
  { id: "claude-4.6-sonnet-medium", name: "Claude 4.6 Sonnet", reasoning: true, contextWindow: 200_000, maxTokens: 64_000 },
  { id: "claude-4.5-sonnet", name: "Claude 4.5 Sonnet", reasoning: true, contextWindow: 200_000, maxTokens: 64_000 },
  // GPT models
  { id: "gpt-5.4-medium", name: "GPT-5.4", reasoning: true, contextWindow: 272_000, maxTokens: 128_000 },
  { id: "gpt-5.2", name: "GPT-5.2", reasoning: true, contextWindow: 400_000, maxTokens: 128_000 },
  { id: "gpt-5.2-codex", name: "GPT-5.2 Codex", reasoning: true, contextWindow: 400_000, maxTokens: 128_000 },
  { id: "gpt-5.3-codex", name: "GPT-5.3 Codex", reasoning: true, contextWindow: 400_000, maxTokens: 128_000 },
  { id: "gpt-5.3-codex-spark-preview", name: "GPT-5.3 Codex Spark", reasoning: true, contextWindow: 128_000, maxTokens: 128_000 },
  // Other models
  { id: "gemini-3.1-pro", name: "Gemini 3.1 Pro", reasoning: true, contextWindow: 1_000_000, maxTokens: 64_000 },
  { id: "grok-code-fast-1", name: "Grok Code Fast 1", reasoning: false, contextWindow: 128_000, maxTokens: 64_000 },
];

async function fetchCursorUsableModels(
  apiKey: string,
): Promise<CursorModel[] | null> {
  try {
    const requestPayload = create(GetUsableModelsRequestSchema, {});
    const requestBody = toBinary(GetUsableModelsRequestSchema, requestPayload);

    const response = await callCursorUnaryRpc({
      accessToken: apiKey,
      rpcPath: GET_USABLE_MODELS_PATH,
      requestBody,
    });

    if (response.timedOut || response.exitCode !== 0 || response.body.length === 0) {
      return null;
    }

    const decoded = decodeGetUsableModelsResponse(response.body);
    if (!decoded) return null;

    const models = normalizeCursorModels(decoded.models);
    return models.length > 0 ? models : null;
  } catch {
    return null;
  }
}

let cachedModels: CursorModel[] | null = null;

export async function getCursorModels(
  apiKey: string,
): Promise<CursorModel[]> {
  if (cachedModels) return cachedModels;
  const discovered = await fetchCursorUsableModels(apiKey);
  cachedModels = discovered && discovered.length > 0 ? discovered : FALLBACK_MODELS;
  return cachedModels;
}

/** @internal Test-only. */
export function clearModelCache(): void {
  cachedModels = null;
}

function decodeGetUsableModelsResponse(payload: Uint8Array): {
  models: readonly unknown[];
} | null {
  try {
    return fromBinary(GetUsableModelsResponseSchema, payload);
  } catch {
    const framedBody = decodeConnectUnaryBody(payload);
    if (!framedBody) return null;
    try {
      return fromBinary(GetUsableModelsResponseSchema, framedBody);
    } catch {
      return null;
    }
  }
}

function decodeConnectUnaryBody(payload: Uint8Array): Uint8Array | null {
  if (payload.length < 5) return null;

  let offset = 0;
  while (offset + 5 <= payload.length) {
    const flags = payload[offset]!;
    const view = new DataView(
      payload.buffer,
      payload.byteOffset + offset,
      payload.byteLength - offset,
    );
    const messageLength = view.getUint32(1, false);
    const frameEnd = offset + 5 + messageLength;
    if (frameEnd > payload.length) return null;

    // Compression flag
    if ((flags & 0b0000_0001) !== 0) return null;

    // End-of-stream flag — skip trailer frames
    if ((flags & 0b0000_0010) === 0) {
      return payload.subarray(offset + 5, frameEnd);
    }

    offset = frameEnd;
  }

  return null;
}

function normalizeCursorModels(
  models: readonly unknown[],
): CursorModel[] {
  if (models.length === 0) return [];

  const byId = new Map<string, CursorModel>();
  for (const model of models) {
    const normalized = normalizeSingleModel(model);
    if (normalized) byId.set(normalized.id, normalized);
  }
  return [...byId.values()].sort((a, b) => a.id.localeCompare(b.id));
}

function normalizeSingleModel(model: unknown): CursorModel | null {
  const parsed = CursorModelDetailsSchema.safeParse(model);
  if (!parsed.success) return null;

  const details = parsed.data;
  const id = details.modelId.trim();
  if (!id) return null;

  const limits = resolveModelLimits(id, details.maxMode);
  return {
    id,
    name: pickDisplayName(details, id),
    reasoning: Boolean(details.thinkingDetails),
    contextWindow: limits.context,
    maxTokens: limits.maxTokens,
  };
}

function resolveModelLimits(modelId: string, maxMode?: boolean): { context: number; maxTokens: number } {
  const isMax = maxMode || /-max(?:-|$)/.test(modelId);
  const exact = MODEL_LIMITS[modelId];
  if (exact) {
    let context = exact.context ?? DEFAULT_CONTEXT_WINDOW;
    if (isMax) context = Math.max(context, 1_000_000);
    return { context, maxTokens: exact.maxTokens ?? DEFAULT_MAX_TOKENS };
  }
  // Strip suffixes like "-max-thinking", "-thinking", "-max" and retry
  const base = modelId.replace(/-(max-thinking|thinking|max|high|medium|low|fast|xhigh)$/g, "");
  if (base !== modelId) {
    const baseLimits = MODEL_LIMITS[base];
    if (baseLimits) {
      let context = baseLimits.context ?? DEFAULT_CONTEXT_WINDOW;
      if (isMax) context = Math.max(context, 1_000_000);
      return { context, maxTokens: baseLimits.maxTokens ?? DEFAULT_MAX_TOKENS };
    }
  }
  return { context: isMax ? 1_000_000 : DEFAULT_CONTEXT_WINDOW, maxTokens: DEFAULT_MAX_TOKENS };
}

function pickDisplayName(model: CursorModelDetails, fallbackId: string): string {
  const candidates = [
    model.displayName,
    model.displayNameShort,
    model.displayModelId,
    ...model.aliases,
    fallbackId,
  ];
  for (const candidate of candidates) {
    if (typeof candidate !== "string") continue;
    const trimmed = candidate.trim();
    if (trimmed) return trimmed;
  }
  return fallbackId;
}
