import { describe, expect, test } from "bun:test";
import {
  CONNECT_END_STREAM_FLAG,
  createConnectFrameParser,
  frameConnectMessage,
  parseConnectEndStream,
} from "../src/protocol";

describe("frameConnectMessage", () => {
  test("creates 5-byte header + payload", () => {
    const data = new Uint8Array([1, 2, 3]);
    const frame = frameConnectMessage(data);
    expect(frame.length).toBe(8);
    expect(frame[0]).toBe(0);
    expect(frame.readUInt32BE(1)).toBe(3);
    expect(Buffer.from(frame.subarray(5))).toEqual(Buffer.from([1, 2, 3]));
  });

  test("encodes custom flags", () => {
    const frame = frameConnectMessage(new Uint8Array([42]), CONNECT_END_STREAM_FLAG);
    expect(frame[0]).toBe(CONNECT_END_STREAM_FLAG);
    expect(frame.readUInt32BE(1)).toBe(1);
    expect(frame[5]).toBe(42);
  });

  test("handles empty payload", () => {
    const frame = frameConnectMessage(new Uint8Array(0));
    expect(frame.length).toBe(5);
    expect(frame.readUInt32BE(1)).toBe(0);
  });
});

describe("createConnectFrameParser", () => {
  test("parses single complete frame", () => {
    const messages: Uint8Array[] = [];
    const endStreams: Uint8Array[] = [];
    const parse = createConnectFrameParser(
      (b) => messages.push(new Uint8Array(b)),
      (b) => endStreams.push(new Uint8Array(b)),
    );

    parse(frameConnectMessage(new Uint8Array([10, 20, 30])));

    expect(messages).toHaveLength(1);
    expect(endStreams).toHaveLength(0);
    expect(Buffer.from(messages[0]!)).toEqual(Buffer.from([10, 20, 30]));
  });

  test("parses multiple frames in one chunk", () => {
    const messages: Uint8Array[] = [];
    const parse = createConnectFrameParser(
      (b) => messages.push(new Uint8Array(b)),
      () => {},
    );

    const combined = Buffer.concat([
      frameConnectMessage(new Uint8Array([1])),
      frameConnectMessage(new Uint8Array([2])),
      frameConnectMessage(new Uint8Array([3])),
    ]);
    parse(combined);

    expect(messages).toHaveLength(3);
    expect(messages[0]![0]).toBe(1);
    expect(messages[1]![0]).toBe(2);
    expect(messages[2]![0]).toBe(3);
  });

  test("reassembles split frames across chunks", () => {
    const messages: Uint8Array[] = [];
    const parse = createConnectFrameParser(
      (b) => messages.push(new Uint8Array(b)),
      () => {},
    );

    const full = frameConnectMessage(new Uint8Array([1, 2, 3, 4, 5]));
    parse(Buffer.from(full.subarray(0, 3)));
    expect(messages).toHaveLength(0);

    parse(Buffer.from(full.subarray(3)));
    expect(messages).toHaveLength(1);
    expect(Buffer.from(messages[0]!)).toEqual(Buffer.from([1, 2, 3, 4, 5]));
  });

  test("handles header split across chunks", () => {
    const messages: Uint8Array[] = [];
    const parse = createConnectFrameParser(
      (b) => messages.push(new Uint8Array(b)),
      () => {},
    );

    const full = frameConnectMessage(new Uint8Array([99]));
    // split inside the 5-byte header
    parse(Buffer.from(full.subarray(0, 2)));
    parse(Buffer.from(full.subarray(2)));
    expect(messages).toHaveLength(1);
    expect(messages[0]![0]).toBe(99);
  });

  test("routes endStream frames separately", () => {
    const messages: Uint8Array[] = [];
    const endStreams: Uint8Array[] = [];
    const parse = createConnectFrameParser(
      (b) => messages.push(new Uint8Array(b)),
      (b) => endStreams.push(new Uint8Array(b)),
    );

    parse(frameConnectMessage(new Uint8Array([1])));
    parse(frameConnectMessage(new Uint8Array([2]), CONNECT_END_STREAM_FLAG));

    expect(messages).toHaveLength(1);
    expect(endStreams).toHaveLength(1);
    expect(endStreams[0]![0]).toBe(2);
  });

  test("rejects oversized frames", () => {
    const endStreams: Uint8Array[] = [];
    const parse = createConnectFrameParser(
      () => {},
      (b) => endStreams.push(new Uint8Array(b)),
    );

    const header = Buffer.alloc(5);
    header[0] = 0;
    header.writeUInt32BE(33 * 1024 * 1024, 1); // 33 MiB > 32 MiB limit
    parse(header);

    expect(endStreams).toHaveLength(1);
    const error = JSON.parse(new TextDecoder().decode(endStreams[0]!));
    expect(error.error.code).toBe("frame_too_large");
  });

  test("recovers state after oversized frame rejection", () => {
    const messages: Uint8Array[] = [];
    const endStreams: Uint8Array[] = [];
    const parse = createConnectFrameParser(
      (b) => messages.push(new Uint8Array(b)),
      (b) => endStreams.push(new Uint8Array(b)),
    );

    // Send oversized header
    const bad = Buffer.alloc(5);
    bad.writeUInt32BE(33 * 1024 * 1024, 1);
    parse(bad);
    expect(endStreams).toHaveLength(1);

    // Parser should still work for subsequent valid frames
    parse(frameConnectMessage(new Uint8Array([77])));
    expect(messages).toHaveLength(1);
    expect(messages[0]![0]).toBe(77);
  });
});

describe("parseConnectEndStream", () => {
  test("extracts error with code and message", () => {
    const data = new TextEncoder().encode(
      JSON.stringify({ error: { code: "resource_exhausted", message: "too many" } }),
    );
    const err = parseConnectEndStream(data);
    expect(err).not.toBeNull();
    expect(err!.message).toContain("resource_exhausted");
    expect(err!.message).toContain("too many");
  });

  test("returns null for clean close (no error field)", () => {
    const data = new TextEncoder().encode(JSON.stringify({}));
    expect(parseConnectEndStream(data)).toBeNull();
  });

  test("returns error for empty error object", () => {
    const data = new TextEncoder().encode(JSON.stringify({ error: {} }));
    const err = parseConnectEndStream(data);
    expect(err).not.toBeNull();
    expect(err!.message).toContain("unknown");
  });

  test("returns error for unparseable data", () => {
    const data = new TextEncoder().encode("not json {{{");
    const err = parseConnectEndStream(data);
    expect(err).not.toBeNull();
    expect(err!.message).toContain("Failed to parse");
  });

  test("returns null for null error field", () => {
    const data = new TextEncoder().encode(JSON.stringify({ error: null }));
    expect(parseConnectEndStream(data)).toBeNull();
  });
});
