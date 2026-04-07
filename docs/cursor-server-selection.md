# Cursor Agent Server Selection Logic

Analysis of `cursor-always-local/dist/main.js` (build `224838f`, March 2025).

## Server URLs — Role-Based, Not Failover

The agent does not do `api2` → `api2direct` failover. Different services get
different hosts based on credential config from `getCursorCreds()`:

| Credential Field             | Purpose                      | Example Hosts         |
|------------------------------|------------------------------|-----------------------|
| `backendUrl`                 | Default/legacy               | `api2.cursor.sh`      |
| `repoBackendUrl`             | Repo indexing                | `repo42.cursor.sh`    |
| `geoCppBackendUrl`           | Geo-routed autocomplete      | `gcpp.cursor.sh`      |
| `cppConfigBackendUrl`        | Autocomplete config          | `api4.cursor.sh`      |
| `cmdkBackendUrl`             | Cmd-K                        | `api3.cursor.sh`      |
| `telemBackendUrl`            | Telemetry                    | `api3.cursor.sh`      |
| `bcProxyUrl`                 | Background composer proxy    | varies                |
| `agentBackendUrlPrivacy`     | Agent (privacy mode)         | varies                |
| `agentBackendUrlNonPrivacy`  | Agent (non-privacy)          | varies                |

The agent URL is privacy-aware:
`getPrivacyMode() ? agentBackendUrlPrivacy : agentBackendUrlNonPrivacy`,
falling back to `backendUrl`.

All hostnames found in the bundle:

- `api2.cursor.sh`, `api3.cursor.sh`, `api4.cursor.sh`, `api5.cursor.sh`
- `a.cursor.sh`, `gcpp.cursor.sh`, `repo42.cursor.sh`
- `*.cursorvm.com` (us1, us1p, us3–us6, dev, eval, train variants)
- `cursor.com`

**`api2direct.cursor.sh` does not appear anywhere in the agent code.**

## HTTP/2 vs HTTP/1.1 — Protocol Downgrade, Not Host Failover

A global flag `s` controls whether HTTP/2 is allowed:

- Server config `http2Config` can be `FORCE_ALL_DISABLED` / `FORCE_BIDI_DISABLED`
- Workspace setting `cursor.debug.disableHttp2` can disable it
- If ALPN negotiation fails, the agent **auto-disables HTTP/2** in workspace settings

Per-transport HTTP/2 usage when allowed:

| Transport                    | Base URL          | useHttp2   |
|------------------------------|-------------------|------------|
| `_backendTransport` (legacy) | `backendUrl`      | false      |
| `http2RepoTransport`         | `repoBackendUrl`  | true       |
| `geoCppTransport`            | `geoCppBackendUrl` | host has `.cursor.sh` and not `api2` |
| `cmdkTransport`, `telemTransport` | cmdk/telem   | host contains `api3.cursor` |
| `agenticComposerTransport`, `agentBidiTransport` | agent URL | true |

When HTTP/2 is disabled but the configured host is HTTP/2-only (`api3`, `api4`,
`gcpp`, `api5`), the URL is **rewritten to `api2.cursor.sh`**:

```
api3.cursor.sh  → api2.cursor.sh
api4.cursor.sh  → api2.cursor.sh
*.gcpp.cursor.sh → api2.cursor.sh
*.api5.cursor.sh → api2.cursor.sh
```

This is `replaceBaseUrlWithApi2()` in module 8670 — the only "failover" logic.

## HTTP/2 Connection Establishment

- Normal RPCs: `@connectrpc/connect-node` transport with `httpVersion: "2"` and
  a session manager for connection pooling.
- Debug ping: explicit `http2.connect(url, { protocol: "https:", ALPNProtocols: ["h2"] })`.
- Health check: `POST /aiserver.v1.HealthService/Unary` with empty proto body.

## Headers

| Header                       | Value                          |
|------------------------------|--------------------------------|
| `Authorization`              | `Bearer <token>`               |
| `x-ghost-mode`               | ghost mode state               |
| `x-cursor-client-version`    | version string                 |
| `x-cursor-client-type`       | `"ide"`                        |
| `x-cursor-streaming`         | `"true"` on AI transports      |
| `x-cursor-checksum`          | derived checksum               |
| `x-session-id`               | session tracking               |
| `x-cursor-client-os`         | optional platform info         |
| `x-cursor-client-arch`       | optional platform info         |
| `x-cursor-client-os-version` | optional platform info         |
| `x-cursor-client-device-type`| `"desktop"`                    |
| `x-cursor-canary`            | when applicable                |
| `x-cursor-config-version`    | server config version          |
| `x-new-onboarding-completed` | snippet learning eligibility   |

No explicit `:authority` header — Connect/Node derives it from the request URL.

## RPC Paths

Standard Connect pattern: `/<package>.<Service>/<Method>`

- `agent.v1.AgentService/Run` (bidi streaming) + `RunSSE` / `RunPoll` fallbacks
- `agent.v1.AgentService/NameAgent` (unary)
- `aiserver.v1.HealthService/Unary` (health check)
- `aiserver.v1.AiService/AvailableModels` (model discovery)
- `aiserver.v1.AiService/GetEffectiveTokenLimit` (context limits)

The transport layer maps bidi methods to parallel SSE and poll method descriptors
for fallback (e.g. `Run` → `RunSSE` / `RunPoll`).

## Background Composer — Server-Assigned Routing

Background composer uses a separate routing mechanism:

1. Client calls `BackgroundComposerService/GetCursorServerUrl` with a `bcId`
2. Server returns `host`, `port`, `connection_token`, and **routing headers**
3. Client connects via TCP/TLS to the assigned host with those headers
4. URL is cached; cache is invalidated after 3 resolve attempts

The response proto:

```protobuf
message GetCursorServerUrlResponse {
  string host = 1;
  int32 port = 2;
  string connection_token = 3;
  repeated Header headers = 4; // "headers to send to the proxy for routing"
}
```

TLS is used when `port === 443`; plain TCP otherwise.

## Environment Variables

**No `CURSOR_API_URL` or `CURSOR_AGENT_URL`** in the agent code. URLs come
entirely from `getCursorCreds()` (Cursor host API) and hardcoded defaults
(`api2.cursor.sh` for debug tools, `api3.cursor.sh` for metrics).

## Implications for Our Proxy

Our proxy now uses `api2.cursor.sh` directly for API calls and as the default
agent host. That matches the original upstream fork more closely, but it still
does not mirror the credential-driven routing from `getCursorCreds()`. The
`api3/4/5/gcpp → api2` rewrite when HTTP/2 is unavailable is the only
"failover" logic that exists in the real agent.

Our `CURSOR_API_URL` / `CURSOR_AGENT_URL` env overrides are proxy-specific
and have no counterpart in the Cursor agent.
