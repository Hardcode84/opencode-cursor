/**
 * Wrapper around @ai-sdk/openai-compatible that fixes a stream interleaving bug.
 *
 * The bundled SDK hardcodes block IDs ("txt-0", "reasoning-0") and never resets
 * its `isActiveText` flag, so all text after the first reasoning block merges
 * into a single text part. This wrapper pipes doStream's output through a fixup
 * transform that gives each text/reasoning segment a unique ID and properly
 * closes/reopens blocks on transitions.
 *
 * Loaded via `npm: "file://.../sdk-wrapper.js"` so OpenCode imports this instead
 * of the bundled provider.
 */

import {
  createOpenAICompatible,
  type OpenAICompatibleProviderSettings,
} from "@ai-sdk/openai-compatible";

type StreamPart = {
  type: string;
  id?: string;
  delta?: string;
  [key: string]: unknown;
};

export function fixInterleavingTransform(): TransformStream<StreamPart, StreamPart> {
  let textCount = 0;
  let reasoningCount = 0;
  let activeTextId: string | null = null;
  let activeReasoningId: string | null = null;

  return new TransformStream({
    transform(chunk: StreamPart, controller) {
      switch (chunk.type) {
        case "reasoning-start":
          if (activeTextId) {
            controller.enqueue({ type: "text-end", id: activeTextId });
            activeTextId = null;
          }
          activeReasoningId = `reasoning-${reasoningCount++}`;
          controller.enqueue({ ...chunk, id: activeReasoningId });
          break;

        case "reasoning-delta":
          controller.enqueue({ ...chunk, id: activeReasoningId ?? chunk.id });
          break;

        case "reasoning-end":
          // The upstream SDK can emit a duplicate reasoning-end during flush even
          // after we already closed the wrapped segment. Ignore that stale close
          // instead of leaking the raw "reasoning-0" id downstream.
          if (!activeReasoningId) break;
          controller.enqueue({ ...chunk, id: activeReasoningId });
          activeReasoningId = null;
          break;

        case "text-start":
          activeTextId = `txt-${textCount++}`;
          controller.enqueue({ ...chunk, id: activeTextId });
          break;

        case "text-delta":
          if (!activeTextId) {
            activeTextId = `txt-${textCount++}`;
            controller.enqueue({ type: "text-start", id: activeTextId });
          }
          controller.enqueue({ ...chunk, id: activeTextId });
          break;

        case "text-end":
          // The upstream SDK keeps its internal text flag open across some
          // reasoning transitions, so flush() can emit a stale txt-0 close after
          // we have already closed the wrapped text block. Drop it.
          if (!activeTextId) break;
          controller.enqueue({ ...chunk, id: activeTextId });
          activeTextId = null;
          break;

        default:
          controller.enqueue(chunk);
      }
    },
  });
}

export function createCursorCompatible(options: OpenAICompatibleProviderSettings) {
  const sdk = createOpenAICompatible(options);

  function wrapLanguageModel(modelId: string) {
    const model = sdk.languageModel(modelId);
    const origDoStream = model.doStream.bind(model);

    model.doStream = async (opts: any) => {
      const result = await origDoStream(opts);
      return {
        ...result,
        stream: (result.stream as ReadableStream<StreamPart>).pipeThrough(
          fixInterleavingTransform(),
        ) as any,
      };
    };

    return model;
  }

  const provider: any = (modelId: string) => wrapLanguageModel(modelId);
  provider.languageModel = wrapLanguageModel;
  provider.chat = wrapLanguageModel;

  const s = sdk as any;
  if (s.completion) provider.completion = s.completion.bind(sdk);
  if (s.textEmbeddingModel) provider.textEmbeddingModel = s.textEmbeddingModel.bind(sdk);
  if (s.textEmbedding) provider.textEmbedding = s.textEmbedding.bind(sdk);
  if (s.image) provider.image = s.image.bind(sdk);

  return provider;
}
