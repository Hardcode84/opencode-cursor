import { describe, expect, test } from "bun:test";
import { decodeTokenLimitResponse, encodeTokenLimitRequest, encodeVarint } from "../src/models";
import { frameConnectMessage } from "../src/protocol";

describe("encodeVarint", () => {
  test("encodes single-byte values (0–127)", () => {
    expect(encodeVarint(0)).toEqual(new Uint8Array([0]));
    expect(encodeVarint(1)).toEqual(new Uint8Array([1]));
    expect(encodeVarint(127)).toEqual(new Uint8Array([127]));
  });

  test("encodes two-byte values (128–16383)", () => {
    expect(encodeVarint(128)).toEqual(new Uint8Array([0x80, 0x01]));
    expect(encodeVarint(300)).toEqual(new Uint8Array([0xac, 0x02]));
    expect(encodeVarint(16383)).toEqual(new Uint8Array([0xff, 0x7f]));
  });

  test("encodes larger values", () => {
    expect(encodeVarint(16384)).toEqual(new Uint8Array([0x80, 0x80, 0x01]));
  });
});

describe("encodeTokenLimitRequest", () => {
  test("encodes short model ID", () => {
    const result = encodeTokenLimitRequest("gpt-5.4");
    // outer: tag 0x0a + varint(innerLen) + inner
    // inner: tag 0x0a + varint(idLen) + id bytes
    expect(result[0]).toBe(0x0a);
    const id = new TextEncoder().encode("gpt-5.4");
    expect(result).toContain(id[0]!);
    expect(result.length).toBe(2 + 2 + id.length); // 2 outer header + 2 inner header + id
  });

  test("round-trips through protobuf structure", () => {
    const result = encodeTokenLimitRequest("claude-4.6-sonnet");
    expect(result[0]).toBe(0x0a); // field 1 LEN
    const innerStart = 2; // tag + 1-byte length (model_id < 128 bytes)
    expect(result[innerStart]).toBe(0x0a); // inner field 1 LEN
  });

  test("handles model IDs longer than 127 bytes", () => {
    const longId = "a".repeat(200);
    const result = encodeTokenLimitRequest(longId);
    expect(result[0]).toBe(0x0a);
    // The id is 200 bytes so varint length is 2 bytes
    const idBytes = new TextEncoder().encode(longId);
    const innerTagLen = 1 + encodeVarint(idBytes.length).length + idBytes.length;
    const outerTagLen = 1 + encodeVarint(innerTagLen).length + innerTagLen;
    expect(result.length).toBe(outerTagLen);
  });

  test("encodes empty model ID", () => {
    const result = encodeTokenLimitRequest("");
    expect(result[0]).toBe(0x0a);
    expect(result.length).toBe(4); // outer(tag+len) + inner(tag+len) + 0 bytes
  });
});

describe("decodeTokenLimitResponse", () => {
  test("decodes small varint (token_limit < 128)", () => {
    // field 1 varint, value 1000 → 0x08 + varint(1000)
    const buf = new Uint8Array([0x08, 0xe8, 0x07]);
    expect(decodeTokenLimitResponse(buf)).toBe(1000);
  });

  test("decodes large token limit", () => {
    // 1_000_000 = 64 + (4 << 7) + (61 << 14) → varint [0xC0, 0x84, 0x3D]
    const buf = new Uint8Array([0x08, 0xc0, 0x84, 0x3d]);
    expect(decodeTokenLimitResponse(buf)).toBe(1_000_000);
  });

  test("decodes 200_000", () => {
    // 200_000 = 0x30D40 → varint [0xC0, 0x9A, 0x0C]
    const buf = new Uint8Array([0x08, 0xc0, 0x9a, 0x0c]);
    expect(decodeTokenLimitResponse(buf)).toBe(200_000);
  });

  test("returns null for empty body", () => {
    expect(decodeTokenLimitResponse(new Uint8Array(0))).toBeNull();
  });

  test("returns null for JSON error body", () => {
    const json = new TextEncoder().encode('{"code":"internal"}');
    expect(decodeTokenLimitResponse(json)).toBeNull();
  });

  test("returns null for wrong field tag", () => {
    // field 2 varint instead of field 1
    const buf = new Uint8Array([0x10, 0x0a]);
    expect(decodeTokenLimitResponse(buf)).toBeNull();
  });

  test("returns null for zero value", () => {
    const buf = new Uint8Array([0x08, 0x00]);
    expect(decodeTokenLimitResponse(buf)).toBeNull();
  });

  test("decodes Connect-framed response", () => {
    const raw = new Uint8Array([0x08, 0xc0, 0x84, 0x3d]); // 1_000_000
    const framed = frameConnectMessage(raw);
    expect(decodeTokenLimitResponse(new Uint8Array(framed))).toBe(1_000_000);
  });

  test("returns null for truncated body", () => {
    expect(decodeTokenLimitResponse(new Uint8Array([0x08]))).toBeNull();
  });
});
