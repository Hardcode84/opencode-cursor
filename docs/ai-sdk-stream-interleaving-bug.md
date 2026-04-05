# AI SDK Stream Interleaving Bug

## Summary

`@ai-sdk/openai-compatible` (Vercel AI SDK) has a bug in its stream
parser that breaks reasoning/text interleaving. When a model alternates
between thinking and text output (thinking → text → thinking → text),
all text after the first reasoning block merges into a single text part.

## Root Cause

File: `packages/openai-compatible/src/chat/openai-compatible-chat-language-model.ts`
in [vercel/ai](https://github.com/vercel/ai) (around lines 406–664 on `main`).

Three issues:

1. **Hardcoded block IDs** — reasoning segments always use `"reasoning-0"`,
   text segments always use `"txt-0"`. IDs are never incremented, so the
   consumer cannot distinguish separate blocks.

2. **Missing `text-end` on modality switch** — when reasoning starts while
   `isActiveText` is true, no `text-end` is emitted and the flag stays set.
   The consumer sees reasoning deltas interleaved into an "open" text block.

3. **No fresh `text-start` after reasoning** — post-reasoning text reuses the
   stale `txt-0` ID without a new `text-start`, so the consumer merges it
   with the original text block. This is the direct cause of the "everything
   after reasoning merges" symptom.

The `flush()` handler at end-of-stream also uses the literal IDs
(`"reasoning-0"` / `"txt-0"`) instead of tracking which segment is current.

## Affected Repos

| Repo | File | Notes |
|------|------|-------|
| [vercel/ai](https://github.com/vercel/ai) | `packages/openai-compatible/src/chat/openai-compatible-chat-language-model.ts` | Canonical source of the bug |
| [anomalyco/opencode](https://github.com/anomalyco/opencode) | `packages/opencode/src/provider/sdk/copilot/chat/openai-compatible-chat-language-model.ts` | Vendored copy for GitHub Copilot; has the same bug plus an additional issue where `reasoning-end` is gated on `!isActiveText`, which can fail to close reasoning when text was already active |

## Workaround

Our plugin ships `src/sdk-wrapper.ts`, which wraps `createOpenAICompatible`
and pipes `doStream` output through a `TransformStream` that:

- Assigns monotonic IDs to each segment (`reasoning-${n++}`, `txt-${m++}`)
- Emits `text-end` before `reasoning-start` when text is active
- Emits a fresh `text-start` with a new ID when text resumes after reasoning
- Routes `*-delta` and `*-end` events to the correct active segment ID

The wrapper is deployed alongside the main plugin and loaded via the `npm`
field in each model's `api` config (`file://…/opencode-cursor-sdk.js`).

## Proper Fix

The fix in `vercel/ai` should:

1. Use monotonic counters (or `generateId()`, already imported for tools) for
   segment IDs — same ID for the `*-start`, `*-delta`, and `*-end` of one
   segment, new ID for the next segment.

2. On reasoning start: if `isActiveText`, enqueue `text-end` with the current
   text ID, set `isActiveText = false`.

3. On text start after reasoning: always open a new `text-start` with a fresh
   ID (the existing code already ends reasoning before text; the missing piece
   is closing/reopening text).

4. In `flush()`: use the tracked current IDs, not literals.

Once upstream ships the fix and OpenCode bumps its `@ai-sdk/openai-compatible`
dependency, `sdk-wrapper.ts` can be removed.
