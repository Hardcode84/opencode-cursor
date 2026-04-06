/** Connect protocol frame: [1-byte flags][4-byte BE length][payload] */
export function frameConnectMessage(data: Uint8Array, flags = 0): Buffer {
  const frame = Buffer.alloc(5 + data.length);
  frame[0] = flags;
  frame.writeUInt32BE(data.length, 1);
  frame.set(data, 5);
  return frame;
}

export const CONNECT_END_STREAM_FLAG = 0b00000010;
