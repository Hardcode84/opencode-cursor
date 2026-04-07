#!/usr/bin/env bun
/**
 * Transparent TLS MITM — captures raw bytes between Cursor and its backend.
 * No HTTP/2 parsing, no protobuf decoding. Just raw relay + capture.
 * Decode offline with: bun tools/mitm-decode.ts /tmp/mitm-captures/0001-c2s.bin
 *
 * Setup:
 *   1. bun tools/mitm-proxy.ts --gen-certs
 *   2. sudo -- sh -c 'echo "127.0.0.1 api5.cursor.sh" >> /etc/hosts'
 *   3. sudo bun tools/mitm-proxy.ts
 *   4. NODE_EXTRA_CA_CERTS=$PWD/tools/certs/ca.pem cursor .
 *   5. Use Cursor, then remove hosts entry.
 *
 * Output per TLS connection:
 *   /tmp/mitm-captures/NNNN-c2s.bin  — client→server raw bytes (after TLS)
 *   /tmp/mitm-captures/NNNN-s2c.bin  — server→client raw bytes (after TLS)
 *
 * Env vars:
 *   MITM_PORT       — listen port (default: 443)
 *   MITM_TARGET     — upstream host (default: api5.cursor.sh)
 *   MITM_TARGET_IP  — skip DNS, connect to this IP
 *   MITM_CAPTURES   — capture dir (default: /tmp/mitm-captures)
 */

import tls from "node:tls";
import net from "node:net";
import { existsSync, mkdirSync, readFileSync, writeFileSync, createWriteStream } from "node:fs";
import { execSync } from "node:child_process";
import { resolve, join } from "node:path";

const TARGET_HOST = process.env.MITM_TARGET || "api5.cursor.sh";
const LISTEN_PORT = parseInt(process.env.MITM_PORT || "443");
const CERT_DIR = join(resolve(import.meta.dir), "certs");
const CAPTURE_DIR = process.env.MITM_CAPTURES || "/tmp/mitm-captures";

// ─── Logging (always works — direct fd write) ────────────────────────────────

function log(msg: string) {
  const ts = new Date().toISOString().slice(11, 23);
  process.stderr.write(`[${ts}] ${msg}\n`);
}

// ─── TLS Certificates ───────────────────────────────────────────────────────

function ensureCerts(): { key: Buffer; cert: Buffer } {
  const caKey = join(CERT_DIR, "ca.key");
  const caCert = join(CERT_DIR, "ca.pem");
  const serverKey = join(CERT_DIR, "server.key");
  const serverCert = join(CERT_DIR, "server.pem");

  if (existsSync(serverCert) && existsSync(serverKey) && existsSync(caCert)) {
    log("Using existing certs");
    return { key: readFileSync(serverKey), cert: readFileSync(serverCert) };
  }

  mkdirSync(CERT_DIR, { recursive: true });
  log("Generating certificates…");

  execSync(`openssl genrsa -out "${caKey}" 2048 2>/dev/null`);
  execSync(`openssl req -x509 -new -nodes -key "${caKey}" -sha256 -days 825 -out "${caCert}" -subj "/CN=Cursor MITM CA"`);
  execSync(`openssl genrsa -out "${serverKey}" 2048 2>/dev/null`);

  const csrPath = join(CERT_DIR, "server.csr");
  execSync(`openssl req -new -key "${serverKey}" -out "${csrPath}" -subj "/CN=${TARGET_HOST}"`);

  writeFileSync(join(CERT_DIR, "server.ext"), [
    "authorityKeyIdentifier=keyid,issuer",
    "basicConstraints=CA:FALSE",
    "keyUsage=digitalSignature,nonRepudiation,keyEncipherment,dataEncipherment",
    `subjectAltName=DNS:${TARGET_HOST},DNS:*.api5.cursor.sh,DNS:*.cursor.sh`,
  ].join("\n"));

  execSync(`openssl x509 -req -in "${csrPath}" -CA "${caCert}" -CAkey "${caKey}" -CAcreateserial -out "${serverCert}" -days 825 -sha256 -extfile "${join(CERT_DIR, "server.ext")}" 2>/dev/null`);

  log(`Certs generated in ${CERT_DIR}`);
  return { key: readFileSync(serverKey), cert: readFileSync(serverCert) };
}

// ─── DNS ─────────────────────────────────────────────────────────────────────

async function resolveTargetIp(): Promise<string> {
  if (process.env.MITM_TARGET_IP) return process.env.MITM_TARGET_IP;

  // Always use DoH since /etc/hosts likely points to localhost
  log("Resolving via DoH (Cloudflare)…");
  const resp = await fetch(
    `https://1.1.1.1/dns-query?name=${TARGET_HOST}&type=A`,
    { headers: { Accept: "application/dns-json" } },
  );
  const json = (await resp.json()) as { Answer?: { type: number; data: string }[] };
  const ip = json.Answer?.find((a) => a.type === 1)?.data;
  if (ip) return ip;

  // Fallback to system DNS
  const { promises: dns } = await import("node:dns");
  const addrs = await dns.resolve4(TARGET_HOST);
  if (addrs.length && !addrs[0]!.startsWith("127.")) return addrs[0]!;

  throw new Error(`Cannot resolve ${TARGET_HOST}`);
}

// ─── Capture transform ──────────────────────────────────────────────────────

function captureTransform(sink: import("node:fs").WriteStream): Transform {
  return new Transform({
    transform(chunk, _enc, cb) {
      sink.write(chunk);
      this.push(chunk);
      cb();
    },
  });
}

// ─── Main ────────────────────────────────────────────────────────────────────

let seq = 0;

async function main() {
  if (process.argv.includes("--gen-certs")) {
    ensureCerts();
    console.log(`\nCA cert: ${join(CERT_DIR, "ca.pem")}`);
    console.log(`Launch Cursor with:\n  NODE_EXTRA_CA_CERTS=${join(CERT_DIR, "ca.pem")} cursor .\n`);
    process.exit(0);
  }

  const certs = ensureCerts();
  const targetIp = await resolveTargetIp();
  log(`Resolved ${TARGET_HOST} → ${targetIp}`);

  // Verify upstream is reachable
  await new Promise<void>((resolve, reject) => {
    const sock = net.connect(443, targetIp, () => {
      log(`Upstream reachable at ${targetIp}:443`);
      sock.destroy();
      resolve();
    });
    sock.on("error", (e) => reject(new Error(`Upstream unreachable: ${e.message}`)));
    sock.setTimeout(5000, () => { sock.destroy(); reject(new Error("Upstream TCP timeout")); });
  });

  mkdirSync(CAPTURE_DIR, { recursive: true });

  const server = tls.createServer(
    {
      key: certs.key,
      cert: certs.cert,
      ALPNProtocols: ["h2", "http/1.1"],
    },
    (clientSocket) => {
      const id = String(++seq).padStart(4, "0");
      const alpn = clientSocket.alpnProtocol || "h2";
      log(`[${id}] new TLS conn, ALPN=${alpn}`);

      const c2sSink = createWriteStream(join(CAPTURE_DIR, `${id}-c2s.bin`));
      const s2cSink = createWriteStream(join(CAPTURE_DIR, `${id}-s2c.bin`));
      let c2sBytes = 0, s2cBytes = 0;
      const t0 = Date.now();

      // Buffer client data immediately — don't lose the H2 preface
      const pendingToUpstream: Buffer[] = [];
      let upReady = false;
      let upSocket: tls.TLSSocket | null = null;
      let clientEnded = false;

      clientSocket.on("data", (chunk: Buffer) => {
        c2sBytes += chunk.length;
        c2sSink.write(chunk);
        if (upReady && upSocket) {
          upSocket.write(chunk);
        } else {
          pendingToUpstream.push(Buffer.from(chunk));
        }
      });

      clientSocket.on("end", () => {
        clientEnded = true;
        if (upReady && upSocket) upSocket.end();
      });

      // Connect upstream
      upSocket = tls.connect(
        {
          host: targetIp,
          port: 443,
          servername: TARGET_HOST,
          ALPNProtocols: [alpn],
        },
        () => {
          log(`[${id}] upstream TLS OK, ALPN=${upSocket!.alpnProtocol}`);
          upReady = true;

          // Drain buffered client data (includes H2 preface)
          for (const buf of pendingToUpstream) upSocket!.write(buf);
          pendingToUpstream.length = 0;
          if (clientEnded) upSocket!.end();

          // Upstream → client (+ capture)
          upSocket!.on("data", (chunk: Buffer) => {
            s2cBytes += chunk.length;
            s2cSink.write(chunk);
            clientSocket.write(chunk);
          });
        },
      );

      const cleanup = (reason: string) => {
        c2sSink.end();
        s2cSink.end();
        const elapsed = Date.now() - t0;
        log(`[${id}] closed (${reason}) — ${c2sBytes}B c2s, ${s2cBytes}B s2c, ${elapsed}ms`);
        if (!clientSocket.destroyed) clientSocket.destroy();
        if (upSocket && !upSocket.destroyed) upSocket.destroy();
      };

      clientSocket.on("error", (e) => log(`[${id}] client err: ${e.message}`));
      upSocket.on("error", (e) => log(`[${id}] upstream err: ${e.message}`));
      clientSocket.on("close", () => cleanup("client closed"));
      upSocket.on("close", () => cleanup("upstream closed"));
    },
  );

  server.listen(LISTEN_PORT, () => {
    log(`Listening on :${LISTEN_PORT} → ${TARGET_HOST} (${targetIp})`);
    log(`Captures → ${CAPTURE_DIR}/`);
    console.log(`\nSetup:`);
    console.log(`  sudo -- sh -c 'echo "127.0.0.1 ${TARGET_HOST}" >> /etc/hosts'`);
    console.log(`  NODE_EXTRA_CA_CERTS=${join(CERT_DIR, "ca.pem")} cursor .`);
  });

  server.on("error", (e: any) => {
    if (e.code === "EACCES") {
      console.error(`Port ${LISTEN_PORT} needs root. Use: sudo bun tools/mitm-proxy.ts`);
    }
    log(`server err: ${e.message}`);
  });
}

main().catch((e) => { console.error(e); process.exit(1); });
