const TRUTHY_MAX_MODE_VALUES = new Set(["1", "true", "yes", "on"]);
const FALSY_MAX_MODE_VALUES = new Set(["0", "false", "no", "off"]);
const MAX_MODE_OPTION_KEYS = ["maxMode", "max_mode"] as const;

export const CURSOR_MAX_MODE_HEADER = "x-cursor-max-mode";

export function parseCursorMaxModeValue(value: unknown): boolean | undefined {
  if (typeof value === "boolean") return value;
  if (typeof value !== "string") return undefined;

  const normalized = value.trim().toLowerCase();
  if (TRUTHY_MAX_MODE_VALUES.has(normalized)) return true;
  if (FALSY_MAX_MODE_VALUES.has(normalized)) return false;
  return undefined;
}

export function resolveCursorMaxModeOption(
  modelOptions?: Record<string, unknown>,
  providerOptions?: Record<string, unknown>,
): boolean | undefined {
  return readMaxModeOption(modelOptions) ?? readMaxModeOption(providerOptions);
}

export function resolveEffectiveCursorMaxMode(override?: boolean): boolean {
  return override ?? true;
}

function readMaxModeOption(options?: Record<string, unknown>): boolean | undefined {
  for (const key of MAX_MODE_OPTION_KEYS) {
    const parsed = parseCursorMaxModeValue(options?.[key]);
    if (parsed !== undefined) return parsed;
  }
  return undefined;
}
