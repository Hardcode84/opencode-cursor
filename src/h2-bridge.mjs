#!/usr/bin/env node
/**
 * HTTP/2 bidirectional pipe for Cursor gRPC (Connect protocol).
 *
 * Bun's node:http2 is broken. This Node script acts as a transparent
 * HTTP/2 proxy: it opens a single bidirectional stream and ferries
 * raw bytes between the parent process (via stdin/stdout) and Cursor.
 *
 * Protocol (length-prefixed framing over stdin/stdout):
 *   [4 bytes big-endian length][payload]
 *
 * First message on stdin is JSON config:
 *   { "accessToken": "...", "url": "...", "path": "...", "unary": false }
 *
 * When unary=true, the bridge uses application/proto (raw protobuf) instead
 * of application/connect+proto (Connect streaming). The single stdin message
 * is written as the request body and the stream is ended immediately.
 * After config, subsequent stdin messages are raw bytes to write to the H2 stream.
 * H2 response data is written to stdout using the same length-prefixed framing.
 */
import http2 from "node:http2";
import crypto from "node:crypto";

const CURSOR_CLIENT_VERSION = "cli-2026.03.30-a5d3e17";
const HEARTBEAT_INTERVAL_MS = 5_000;

// Pre-built Connect frame for AgentClientMessage { clientHeartbeat: {} }
// Protobuf: field 7 (tag 0x3a), length-delimited, 0 bytes → [0x3a, 0x00]
// Connect envelope: [flags=0x00][length BE32 = 2][0x3a, 0x00]
const HEARTBEAT_FRAME = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x02, 0x3a, 0x00]);

/** Write one length-prefixed message to stdout. */
function writeMessage(data) {
  const lenBuf = Buffer.alloc(4);
  lenBuf.writeUInt32BE(data.length, 0);
  process.stdout.write(lenBuf);
  process.stdout.write(data);
}

// --- Buffered stdin reader ---

let stdinBuf = Buffer.alloc(0);
let stdinResolve = null;
let stdinEnded = false;

process.stdin.on("data", (chunk) => {
  stdinBuf = Buffer.concat([stdinBuf, chunk]);
  if (stdinResolve) {
    const r = stdinResolve;
    stdinResolve = null;
    r();
  }
});

process.stdin.on("end", () => {
  stdinEnded = true;
  if (stdinResolve) {
    const r = stdinResolve;
    stdinResolve = null;
    r();
  }
});

function waitForData() {
  return new Promise((resolve) => { stdinResolve = resolve; });
}

async function readExact(n) {
  while (stdinBuf.length < n) {
    if (stdinEnded) return null;
    await waitForData();
  }
  const result = stdinBuf.subarray(0, n);
  stdinBuf = stdinBuf.subarray(n);
  return Buffer.from(result);
}

async function readMessage() {
  const lenBuf = await readExact(4);
  if (!lenBuf) return null;
  const len = lenBuf.readUInt32BE(0);
  if (len === 0) return Buffer.alloc(0);
  return readExact(len);
}

// --- Main ---

const configBuf = await readMessage();
if (!configBuf) process.exit(1);

const config = JSON.parse(configBuf.toString("utf8"));
const { accessToken, url, path: rpcPath, unary } = config;

const baseUrl = url || "https://api2.cursor.sh";
const isApi2 = baseUrl.includes("api2.cursor.sh");
const connectUrl = isApi2
  ? baseUrl.replace("api2.cursor.sh", "api2direct.cursor.sh")
  : baseUrl;
const isDirect = isApi2;

process.stderr.write(`[bridge] connecting to ${connectUrl}\n`);
const client = http2.connect(connectUrl);

let timeout = setTimeout(() => killBridge("initial connect timeout 30s"), 30_000);

function resetTimeout() {
  clearTimeout(timeout);
  timeout = setTimeout(() => killBridge("inactivity timeout 120s"), 120_000);
}

function killBridge(reason) {
  process.stderr.write(`[bridge] kill: ${reason}\n`);
  clearTimeout(timeout);
  clearInterval(heartbeatTimer);
  client.destroy();
  process.exit(1);
}

client.on("error", (err) => {
  process.stderr.write(`[bridge] h2 client error: ${err?.message ?? err}\n`);
  clearTimeout(timeout);
  clearInterval(heartbeatTimer);
  process.exit(1);
});

const requestId = crypto.randomUUID();
const traceId = crypto.randomBytes(16).toString("hex");
const spanId = crypto.randomBytes(8).toString("hex");
const traceparent = `00-${traceId}-${spanId}-01`;

const headers = {
  ":method": "POST",
  ":path": rpcPath || "/agent.v1.AgentService/Run",
  "content-type": unary ? "application/proto" : "application/connect+proto",
  "user-agent": "connect-es/1.6.1",
  authorization: `Bearer ${accessToken}`,
  "x-ghost-mode": "true",
  "x-cursor-client-version": CURSOR_CLIENT_VERSION,
  "x-cursor-client-type": "cli",
  "x-request-id": requestId,
  "x-original-request-id": requestId,
  "traceparent": traceparent,
  "backend-traceparent": traceparent,
};
if (isDirect) {
  headers[":authority"] = "api2.cursor.sh";
}
if (!unary) {
  headers["connect-protocol-version"] = "1";
}
const h2Stream = client.request(headers);

// --- Client heartbeat: send every 5s to keep the server connection alive ---
let heartbeatTimer;
if (!unary) {
  heartbeatTimer = setInterval(() => {
    if (!h2Stream.closed && !h2Stream.destroyed) {
      h2Stream.write(HEARTBEAT_FRAME);
    }
  }, HEARTBEAT_INTERVAL_MS);
} else {
  heartbeatTimer = undefined;
}

// Forward H2 response data → stdout (length-prefixed)
h2Stream.on("data", (chunk) => {
  resetTimeout();
  writeMessage(chunk);
});

h2Stream.on("end", () => {
  process.stderr.write("[bridge] stream ended by server\n");
  clearTimeout(timeout);
  clearInterval(heartbeatTimer);
  client.close();
  setTimeout(() => process.exit(0), 100);
});

h2Stream.on("error", (err) => {
  process.stderr.write(`[bridge] stream error: ${err?.message ?? err}\n`);
  clearTimeout(timeout);
  clearInterval(heartbeatTimer);
  client.close();
  process.exit(1);
});

h2Stream.on("close", () => {
  process.stderr.write("[bridge] stream closed\n");
});

// Forward stdin → H2 stream (after config message)
if (unary) {
  const body = await readMessage();
  if (body && body.length > 0 && !h2Stream.closed && !h2Stream.destroyed) {
    h2Stream.end(body);
  } else {
    h2Stream.end();
  }
} else {
  (async () => {
    while (true) {
      const msg = await readMessage();
      if (!msg || msg.length === 0) break;
      if (!h2Stream.closed && !h2Stream.destroyed) {
        resetTimeout();
        h2Stream.write(msg);
      }
    }
    if (!h2Stream.closed && !h2Stream.destroyed) {
      h2Stream.end();
    }
  })();
}
