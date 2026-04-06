export function frameConnectMessage(data: Uint8Array, flags = 0): Buffer {
  const header = Buffer.alloc(5);
  header[0] = flags;
  header.writeUInt32BE(data.length, 1);
  return Buffer.concat([header, Buffer.from(data)]);
}

export const CONNECT_END_STREAM_FLAG = 0b00000010;

const MAX_FRAME_SIZE = 32 * 1024 * 1024; // 32 MiB

export function createConnectFrameParser(
  onMessage: (bytes: Uint8Array) => void,
  onEndStream: (bytes: Uint8Array) => void,
): (incoming: Buffer) => void {
  let pending = Buffer.alloc(0);
  return (incoming: Buffer) => {
    pending = Buffer.concat([pending, incoming]);
    while (pending.length >= 5) {
      const flags = pending[0]!;
      const msgLen = pending.readUInt32BE(1);
      if (msgLen > MAX_FRAME_SIZE) {
        pending = Buffer.alloc(0);
        onEndStream(
          new TextEncoder().encode(
            JSON.stringify({
              error: { code: "frame_too_large", message: `Frame size ${msgLen} exceeds limit` },
            }),
          ),
        );
        return;
      }
      if (pending.length < 5 + msgLen) break;
      const messageBytes = pending.subarray(5, 5 + msgLen);
      pending = pending.subarray(5 + msgLen);
      if (flags & CONNECT_END_STREAM_FLAG) {
        onEndStream(messageBytes);
      } else {
        onMessage(messageBytes);
      }
    }
  };
}

export function parseConnectEndStream(data: Uint8Array): Error | null {
  try {
    const payload = JSON.parse(new TextDecoder().decode(data));
    const error = payload?.error;
    if (error) {
      const code = error.code ?? "unknown";
      const message = error.message ?? "Unknown error";
      return new Error(`Connect error ${code}: ${message}`);
    }
    return null;
  } catch {
    return new Error("Failed to parse Connect end stream");
  }
}
