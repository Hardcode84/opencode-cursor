/**
 * Cursor model discovery.
 *
 * 1. AvailableModels RPC → model list + capabilities.
 * 2. GetEffectiveTokenLimit RPC (parallel) → per-model context window.
 * 3. Fallback: GetUsableModels + hardcoded MODEL_LIMITS.
 */
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { callCursorUnaryRpc } from "./cursor-session";
import { logDebug, logWarn } from "./logger";
import {
  GetUsableModelsRequestSchema,
  type GetUsableModelsResponse,
  GetUsableModelsResponseSchema,
} from "./proto/agent_pb";
import {
  AvailableModelsRequestSchema,
  type AvailableModelsResponse,
  type AvailableModelsResponse_AvailableModel,
  AvailableModelsResponseSchema,
} from "./proto/aiserver_pb";
import { decodeConnectUnaryBody } from "./protocol";
import { type CursorRuntimeConfig, resolveRuntimeConfig } from "./runtime-config";

const AVAILABLE_MODELS_PATH = "/aiserver.v1.AiService/AvailableModels";
const GET_USABLE_MODELS_PATH = "/agent.v1.AgentService/GetUsableModels";
const GET_EFFECTIVE_TOKEN_LIMIT_PATH = "/aiserver.v1.AiService/GetEffectiveTokenLimit";

const DEFAULT_CONTEXT_WINDOW = 200_000;
const DEFAULT_MAX_TOKENS = 64_000;
const AVAILABLE_MODELS_TIMEOUT_MS = 8_000;
const TOKEN_LIMIT_TIMEOUT_MS = 5_000;
const TOKEN_LIMIT_CONCURRENCY = 12;

/** Last-resort hardcoded fallback (values from GetEffectiveTokenLimit). */
const MODEL_LIMITS: Record<string, { context?: number; maxTokens?: number }> = {
  "claude-4-sonnet": { context: 1_000_000 },
  "claude-4-sonnet-1m": { context: 1_000_000 },
  "claude-4.5-haiku": { context: 200_000 },
  "claude-4.5-opus": { context: 1_000_000 },
  "claude-4.5-sonnet": { context: 1_000_000 },
  "claude-4.6-opus": { context: 1_000_000 },
  "claude-4.6-sonnet": { context: 1_000_000 },
  "composer-1.5": { context: 1_000_000 },
  "composer-2": { context: 200_000 },
  "gemini-2.5-flash": { context: 1_000_000 },
  "gemini-3-flash": { context: 1_000_000 },
  "gemini-3.1-pro": { context: 1_000_000 },
  "gpt-5.1": { context: 272_000 },
  "gpt-5.1-codex-max": { context: 272_000 },
  "gpt-5.1-codex-mini": { context: 272_000 },
  "gpt-5.2": { context: 272_000 },
  "gpt-5.2-codex": { context: 272_000 },
  "gpt-5.3-codex": { context: 272_000 },
  "gpt-5.3-codex-spark-preview": { context: 128_000 },
  "gpt-5.4": { context: 922_000 },
  "gpt-5.4-mini": { context: 272_000 },
  "gpt-5.4-nano": { context: 272_000 },
};

export interface CursorModel {
  id: string;
  name: string;
  reasoning: boolean;
  contextWindow: number;
  maxTokens: number;
}

const FALLBACK_MODELS: CursorModel[] = [
  {
    id: "composer-1",
    name: "Composer 1",
    reasoning: true,
    contextWindow: 200_000,
    maxTokens: 64_000,
  },
  {
    id: "composer-1.5",
    name: "Composer 1.5",
    reasoning: true,
    contextWindow: 1_000_000,
    maxTokens: 64_000,
  },
  {
    id: "claude-4.6-opus-high",
    name: "Claude 4.6 Opus",
    reasoning: true,
    contextWindow: 1_000_000,
    maxTokens: 128_000,
  },
  {
    id: "claude-4.6-sonnet-medium",
    name: "Claude 4.6 Sonnet",
    reasoning: true,
    contextWindow: 1_000_000,
    maxTokens: 64_000,
  },
  {
    id: "claude-4.5-sonnet",
    name: "Claude 4.5 Sonnet",
    reasoning: true,
    contextWindow: 1_000_000,
    maxTokens: 64_000,
  },
  {
    id: "gpt-5.4-medium",
    name: "GPT-5.4",
    reasoning: true,
    contextWindow: 922_000,
    maxTokens: 128_000,
  },
  {
    id: "gpt-5.2",
    name: "GPT-5.2",
    reasoning: true,
    contextWindow: 272_000,
    maxTokens: 128_000,
  },
  {
    id: "gemini-3.1-pro",
    name: "Gemini 3.1 Pro",
    reasoning: true,
    contextWindow: 1_000_000,
    maxTokens: 64_000,
  },
];

// ---------------------------------------------------------------------------
// GetEffectiveTokenLimit RPC — manual wire encoding
// ---------------------------------------------------------------------------

/** Encode a protobuf varint. */
export function encodeVarint(value: number): Uint8Array {
  const bytes: number[] = [];
  let v = value >>> 0;
  while (v > 0x7f) {
    bytes.push((v & 0x7f) | 0x80);
    v >>>= 7;
  }
  bytes.push(v);
  return new Uint8Array(bytes);
}

/**
 * Encode GetEffectiveTokenLimitRequest.
 * Wire layout: field 1 (model_details message) → field 1 (model_id string).
 * The server only needs model_id; extra ModelDetails fields are rejected.
 */
export function encodeTokenLimitRequest(modelId: string): Uint8Array {
  const id = new TextEncoder().encode(modelId);
  const idLen = encodeVarint(id.length);
  const inner = new Uint8Array(1 + idLen.length + id.length);
  inner[0] = 0x0a; // field 1, wire type 2
  inner.set(idLen, 1);
  inner.set(id, 1 + idLen.length);

  const innerLen = encodeVarint(inner.length);
  const outer = new Uint8Array(1 + innerLen.length + inner.length);
  outer[0] = 0x0a; // field 1, wire type 2
  outer.set(innerLen, 1);
  outer.set(inner, 1 + innerLen.length);
  return outer;
}

/** Decode varint starting at offset. Returns [value, newOffset]. */
function readVarint(buf: Uint8Array, off: number): [number, number] {
  let val = 0;
  let shift = 0;
  while (off < buf.length) {
    const b = buf[off++]!;
    val |= (b & 0x7f) << shift;
    shift += 7;
    if ((b & 0x80) === 0) break;
  }
  return [val >>> 0, off];
}

/**
 * Decode GetEffectiveTokenLimitResponse.
 * Handles both raw protobuf and Connect-framed responses.
 */
export function decodeTokenLimitResponse(body: Uint8Array): number | null {
  if (body.length === 0) return null;
  if (body[0] === 0x7b) return null; // '{' = JSON error

  const parsed = parseTokenLimitField(body);
  if (parsed !== null) return parsed;

  // Try Connect framing unwrap
  const unframed = decodeConnectUnaryBody(body);
  if (unframed) return parseTokenLimitField(unframed);

  return null;
}

function parseTokenLimitField(buf: Uint8Array): number | null {
  if (buf.length < 2) return null;
  const tag = buf[0]!;
  if (tag >> 3 !== 1 || (tag & 7) !== 0) return null;
  const [val] = readVarint(buf, 1);
  return val > 0 ? val : null;
}

async function fetchTokenLimit(
  apiKey: string,
  modelId: string,
  runtimeConfig?: Partial<CursorRuntimeConfig>,
): Promise<number | null> {
  try {
    const response = await callCursorUnaryRpc({
      accessToken: apiKey,
      rpcPath: GET_EFFECTIVE_TOKEN_LIMIT_PATH,
      requestBody: encodeTokenLimitRequest(modelId),
      timeoutMs: TOKEN_LIMIT_TIMEOUT_MS,
      runtimeConfig,
    });
    if (response.timedOut || response.exitCode !== 0) {
      logDebug("[models] GetEffectiveTokenLimit failed", {
        model: modelId,
        timedOut: response.timedOut,
        exitCode: response.exitCode,
      });
      return null;
    }
    return decodeTokenLimitResponse(response.body);
  } catch (err) {
    logDebug("[models] GetEffectiveTokenLimit error", {
      model: modelId,
      error: err instanceof Error ? err.message : String(err),
    });
    return null;
  }
}

/** Fetch token limits for multiple models in parallel with bounded concurrency. */
async function fetchTokenLimits(
  apiKey: string,
  modelIds: string[],
  runtimeConfig?: Partial<CursorRuntimeConfig>,
): Promise<Map<string, number>> {
  const limits = new Map<string, number>();
  const queue = [...modelIds];
  let idx = 0;

  async function worker() {
    while (idx < queue.length) {
      const i = idx++;
      const modelId = queue[i]!;
      const limit = await fetchTokenLimit(apiKey, modelId, runtimeConfig);
      if (limit !== null) limits.set(modelId, limit);
    }
  }

  const workers = Array.from({ length: Math.min(TOKEN_LIMIT_CONCURRENCY, queue.length) }, () =>
    worker(),
  );
  await Promise.all(workers);
  return limits;
}

// ---------------------------------------------------------------------------
// Primary path: AvailableModels + GetEffectiveTokenLimit
// ---------------------------------------------------------------------------

async function fetchAvailableModels(
  apiKey: string,
  runtimeConfig?: Partial<CursorRuntimeConfig>,
): Promise<CursorModel[] | null> {
  try {
    const req = create(AvailableModelsRequestSchema, {
      includeLongContextModels: true,
      includeHiddenModels: true,
    });

    const response = await callCursorUnaryRpc({
      accessToken: apiKey,
      rpcPath: AVAILABLE_MODELS_PATH,
      requestBody: toBinary(AvailableModelsRequestSchema, req),
      timeoutMs: AVAILABLE_MODELS_TIMEOUT_MS,
      runtimeConfig,
    });

    if (response.timedOut || response.exitCode !== 0 || response.body.length === 0) {
      logWarn("[models] AvailableModels RPC failed", {
        timedOut: response.timedOut,
        exitCode: response.exitCode,
        bodyLen: response.body.length,
      });
      return null;
    }

    const decoded = decodeConnectResponse<AvailableModelsResponse>(
      AvailableModelsResponseSchema,
      response.body,
    );
    if (!decoded || decoded.models.length === 0) {
      logWarn("[models] AvailableModels returned empty");
      return null;
    }

    const modelIds = decoded.models.map((m) => m.name).filter(Boolean);
    const tokenLimits = await fetchTokenLimits(apiKey, modelIds, runtimeConfig);

    logDebug("[models] GetEffectiveTokenLimit results", {
      requested: modelIds.length,
      resolved: tokenLimits.size,
    });

    const models = decoded.models
      .map((m) => normalizeAvailableModel(m, tokenLimits))
      .filter((m): m is CursorModel => m !== null);

    logDebug("[models] AvailableModels discovery complete", {
      total: decoded.models.length,
      usable: models.length,
    });
    return models.length > 0 ? models : null;
  } catch (err) {
    logWarn("[models] AvailableModels RPC error", {
      error: err instanceof Error ? err.message : String(err),
    });
    return null;
  }
}

function normalizeAvailableModel(
  m: AvailableModelsResponse_AvailableModel,
  tokenLimits: Map<string, number>,
): CursorModel | null {
  const id = m.name?.trim();
  if (!id) return null;

  const serverLimit = tokenLimits.get(id);
  const context = serverLimit ?? fallbackContext(id);

  return {
    id,
    name: m.clientDisplayName?.trim() || id,
    reasoning: m.supportsThinking === true,
    contextWindow: context,
    maxTokens: DEFAULT_MAX_TOKENS,
  };
}

function fallbackContext(modelId: string): number {
  const exact = MODEL_LIMITS[modelId];
  if (exact?.context) return exact.context;

  const base = modelId.replace(
    /-(max-thinking|thinking|max|high|medium|low|fast|xhigh|none)$/g,
    "",
  );
  if (base !== modelId) {
    const baseLimits = MODEL_LIMITS[base];
    if (baseLimits?.context) return baseLimits.context;
  }

  return DEFAULT_CONTEXT_WINDOW;
}

// ---------------------------------------------------------------------------
// Fallback: agent.v1.GetUsableModels (no context window info)
// ---------------------------------------------------------------------------

async function fetchGetUsableModels(
  apiKey: string,
  runtimeConfig?: Partial<CursorRuntimeConfig>,
): Promise<CursorModel[] | null> {
  try {
    const requestBody = toBinary(
      GetUsableModelsRequestSchema,
      create(GetUsableModelsRequestSchema, {}),
    );
    const response = await callCursorUnaryRpc({
      accessToken: apiKey,
      rpcPath: GET_USABLE_MODELS_PATH,
      requestBody,
      runtimeConfig,
    });

    if (response.timedOut || response.exitCode !== 0 || response.body.length === 0) return null;

    const decoded = decodeConnectResponse<GetUsableModelsResponse>(
      GetUsableModelsResponseSchema,
      response.body,
    );
    if (!decoded) return null;

    const models = normalizeLegacyModels(decoded.models);
    return models.length > 0 ? models : null;
  } catch {
    return null;
  }
}

function normalizeLegacyModels(models: readonly unknown[]): CursorModel[] {
  const byId = new Map<string, CursorModel>();
  for (const model of models) {
    const m = model as Record<string, unknown>;
    const id = (typeof m.modelId === "string" ? m.modelId : "").trim();
    if (!id) continue;

    const ctx = fallbackContext(id);
    const exactLimits = MODEL_LIMITS[id];

    byId.set(id, {
      id,
      name: pickLegacyName(m, id),
      reasoning: Boolean(m.thinkingDetails),
      contextWindow: ctx,
      maxTokens: exactLimits?.maxTokens ?? DEFAULT_MAX_TOKENS,
    });
  }
  return [...byId.values()].sort((a, b) => a.id.localeCompare(b.id));
}

function pickLegacyName(model: Record<string, unknown>, fallbackId: string): string {
  for (const key of ["displayName", "displayNameShort", "displayModelId"]) {
    const v = model[key];
    if (typeof v === "string" && v.trim()) return v.trim();
  }
  return fallbackId;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export interface ModelDiscoveryResult {
  models: CursorModel[];
  source: "available_models" | "get_usable_models" | "fallback";
}

const cachedResults = new Map<string, ModelDiscoveryResult>();

export async function getCursorModels(
  apiKey: string,
  runtimeConfig?: Partial<CursorRuntimeConfig>,
): Promise<ModelDiscoveryResult> {
  const config = resolveRuntimeConfig(runtimeConfig);
  const cacheKey = config.apiUrl;
  const cachedResult = cachedResults.get(cacheKey);
  if (cachedResult) return cachedResult;

  const available = await fetchAvailableModels(apiKey, config);
  if (available && available.length > 0) {
    const result = { models: available, source: "available_models" as const };
    cachedResults.set(cacheKey, result);
    logDebug("[models] Using AvailableModels", { count: available.length });
    return result;
  }

  const usable = await fetchGetUsableModels(apiKey, config);
  if (usable && usable.length > 0) {
    const result = { models: usable, source: "get_usable_models" as const };
    cachedResults.set(cacheKey, result);
    logWarn("[models] Fell back to GetUsableModels", { count: usable.length });
    return result;
  }

  const result = { models: FALLBACK_MODELS, source: "fallback" as const };
  cachedResults.set(cacheKey, result);
  logWarn("[models] Using hardcoded fallback models");
  return result;
}

/** @internal Test-only. */
export function clearModelCache(): void {
  cachedResults.clear();
}

// ---------------------------------------------------------------------------
// Connect-protocol decode helpers
// ---------------------------------------------------------------------------

function decodeConnectResponse<T>(
  schema: Parameters<typeof fromBinary>[0],
  payload: Uint8Array,
): T | null {
  try {
    return fromBinary(schema, payload) as T;
  } catch {
    const framedBody = decodeConnectUnaryBody(payload);
    if (!framedBody) return null;
    try {
      return fromBinary(schema, framedBody) as T;
    } catch {
      return null;
    }
  }
}
