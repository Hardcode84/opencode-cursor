const PRETTY_NAME_OVERRIDES: Record<string, string> = {
  "composer-1": "Composer 1",
  "composer-1.5": "Composer 1.5",
  "composer-2": "Composer 2",
};

const RAW_NAME_PATTERN = /^[a-z0-9][a-z0-9._-]*$/;

export function prettyCursorModelName(modelId: string): string {
  const normalizedId = modelId.trim().toLowerCase();
  if (!normalizedId) return modelId;
  const overridden = PRETTY_NAME_OVERRIDES[normalizedId];
  if (overridden) return overridden;

  const parts = normalizedId.split("-").filter(Boolean);
  if (parts.length === 0) return modelId;

  switch (parts[0]) {
    case "claude":
      return formatClaudeName(parts.slice(1));
    case "gpt":
      return formatGptName(parts.slice(1));
    case "gemini":
      return formatPrefixedName("Gemini", parts.slice(1));
    case "composer":
      return formatPrefixedName("Composer", parts.slice(1));
    default:
      return parts.map(formatToken).join(" ");
  }
}

export function resolveCursorModelName(modelId: string, discoveredName?: string): string {
  const preferredName = discoveredName?.trim();
  if (preferredName && !looksLikeRawModelName(preferredName, modelId)) {
    return preferredName;
  }
  return prettyCursorModelName(modelId);
}

function looksLikeRawModelName(name: string, modelId: string): boolean {
  const normalizedName = name.trim();
  if (!normalizedName) return true;
  if (normalizedName.toLowerCase() === modelId.trim().toLowerCase()) return true;
  return RAW_NAME_PATTERN.test(normalizedName);
}

function formatClaudeName(parts: string[]): string {
  const [version, family, ...rest] = parts;
  return ["Claude", family ? formatToken(family) : "", version ?? "", ...rest.map(formatToken)]
    .filter(Boolean)
    .join(" ");
}

function formatGptName(parts: string[]): string {
  const [version, ...rest] = parts;
  return [`GPT${version ? `-${version}` : ""}`, ...rest.map(formatToken)].filter(Boolean).join(" ");
}

function formatPrefixedName(prefix: string, parts: string[]): string {
  return [prefix, ...parts.map(formatToken)].join(" ");
}

function formatToken(token: string): string {
  if (/^\d+(\.\d+)?$/.test(token)) return token;
  if (/^\d+m$/.test(token)) return `${token.slice(0, -1)}M`;
  if (token === "xhigh") return "XHigh";
  if (token === "gpt") return "GPT";
  return token.charAt(0).toUpperCase() + token.slice(1);
}
