# OTLP authentication support: audit findings

Date: 2026-09-05. Status: **investigation only — nothing decided, nothing built.**

Audit of what the OpenTelemetry specification defines for exporter
authentication, and where `asyncio-otlp-client` stands against it.

## What the spec actually defines

The OTLP exporter spec defines no authentication *concept*. It defines three
transport-security knobs, and bearer tokens / API keys are just headers by
convention:

1. **Headers** — arbitrary key-value pairs attached to every request.
2. **Server certificate trust** — a CA file used to verify the server.
3. **mTLS** — a client key and certificate chain.

Plus **Insecure**, a gRPC-only toggle for whether the channel uses TLS at all.

There is no OAuth flow, no credential provider, and no token-refresh hook
anywhere in the spec.

## Conformance

| Spec option | Env vars | Status |
| --- | --- | --- |
| Headers | `OTEL_EXPORTER_OTLP_HEADERS` | Supported |
| Certificate file | `OTEL_EXPORTER_OTLP_CERTIFICATE` | Supported |
| Client key file | `OTEL_EXPORTER_OTLP_CLIENT_KEY` | Supported |
| Client certificate file | `OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE` | Supported |
| Insecure | `OTEL_EXPORTER_OTLP_INSECURE` | Supported (fixed 2026-09-05) |
| Per-signal `HEADERS` / `TIMEOUT` / `COMPRESSION` | `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_*` | Supported (2026-09-05) |
| Per-signal `PROTOCOL` / `INSECURE` / TLS settings | `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_*` | Rejected by design |
| Dynamic credentials (beyond spec) | — | Supported (2026-09-05) |

Headers reach both wire formats correctly: HTTP merges them into the request
headers (`transport/http.py`), gRPC passes them as per-call metadata
(`transport/grpc.py`). TLS material is loaded off the event loop on both sides
(`_build_ssl_context` / `_read_credentials`), which is right.

Header parsing (`config.py:_parse_headers`) matches the spec's W3C Baggage
`key1=value1,key2=value2` form and percent-decodes both halves.

## Findings, ranked

### 1. Scheme-less gRPC endpoints silently fall back to plaintext — FIXED 2026-09-05

`_target()` in `transport/grpc.py` infers transport security as
`parsed.scheme != "https"`. Verified behaviour:

    'otel.example.com:4317'          -> plaintext=True
    'http://otel.example.com:4317'   -> plaintext=True
    'https://otel.example.com:4317'  -> plaintext=False

The bare `host:port` form is the most common way to write a gRPC endpoint, and
we send it in the clear — including any `authorization` metadata, which is
exactly the case where cleartext matters most.

The spec says Insecure defaults to `false`, and:

> This option only applies to OTLP/gRPC when an endpoint is provided without
> the `http` or `https` scheme.

So for a scheme-less endpoint the spec-correct default is **TLS**, not
plaintext. Our handling of explicit `http://` and `https://` schemes is
correct and does take precedence, as the spec requires; only the scheme-less
case is wrong.

**Resolved 2026-09-05.** `_target()` now takes the `insecure` setting: an
explicit `http`/`https` scheme decides transport security and takes precedence,
and only a scheme-less endpoint consults `insecure`, which defaults to false.
`OTEL_EXPORTER_OTLP_INSECURE` is parsed per the spec Boolean rules. The
paragraphs below describe the original defect.

No test covered a scheme-less endpoint. `tests/test_transport_grpc.py` only
exercises `insecure_skip_verify`, which is a different knob (skip *verification*
on an already-TLS channel) and should not be confused with spec `Insecure`
(use no TLS at all). We have the second-order knob and are missing the
first-order one.

### 2. No per-signal configuration variants — RESOLVED 2026-09-05

**Resolved 2026-09-05.** Headers, timeout and compression are now per-signal,
replacing rather than merging with the general value. Protocol, `insecure` and
the TLS settings stay general-only and their per-signal forms raise — see
"Intentional deviations" below. The paragraphs below describe the original gap.

Every option in the table has `_TRACES_`, `_METRICS_`, `_LOGS_` forms in the
spec. `OTLPConfig.from_env()` reads only the base form plus per-signal
*endpoints*. Per-signal headers are the practically useful case — different
API keys per signal, e.g. a vendor that meters traces and metrics separately.

Note the transports differ here: per-signal headers are meaningful over gRPC
(metadata is per call), but per-signal certificates are not (one channel, one
credential set). gRPC already rejects per-signal endpoints for the same reason.

### 3. Headers are frozen at construction time — RESOLVED 2026-09-05

**Resolved 2026-09-05** by `docs/superpowers/specs/2026-09-05-credential-providers-design.md`.
`OTLPClient.create(..., credentials=...)` takes a provider awaited per export
attempt, with `BearerToken`, `BasicAuth` and `OAuth2ClientCredentials` helpers.
Unlike every other item in this audit, this is beyond-spec work rather than
conformance: the specification defines no authentication concept, so there is
nothing here to conform to. The paragraphs below describe the original gap.

`OTLPConfig` is a frozen dataclass; transports read `self._config.headers` at
send time. A rotating bearer token cannot be refreshed without tearing down and
rebuilding the client.

This is a known upstream sore point rather than a defect on our side —
opentelemetry-java#4590 and opentelemetry-dotnet#2504 track it, and the
Collector routes around it with authenticator extensions (`bearertokenauth`,
`oauth2clientauth`, `basicauth`, `oidc`). It is the one area where an
async-native library could offer something the spec does not: an awaitable
credential provider consulted per request, with helpers for bearer, basic, and
OAuth2 client-credentials with cached refresh.

## Adjacent deviations noticed during the audit

Not auth, but found while reading the same code path:

- **gRPC default endpoint.** ~~`from_env()` defaults to `http://localhost:4318`
  regardless of protocol. The spec's gRPC default is `http://localhost:4317`.
  With `OTEL_EXPORTER_OTLP_PROTOCOL=grpc` and no endpoint set, we target the
  HTTP port.~~ **Fixed 2026-09-05** — `from_env()` now selects the default
  endpoint from the parsed protocol via `_DEFAULT_ENDPOINTS`.
- **Per-signal `PROTOCOL`** is rejected by design rather than unimplemented —
  see finding 2 and "Intentional deviations" below.

## Intentional deviations

Recorded so a later audit does not reopen them as defects.

### Default protocol is `http/json`, not `http/protobuf`

The spec's footnote [4] says the default protocol SHOULD be `http/protobuf`.
This client defaults to `http/json` deliberately, and should keep doing so.

The core install depends on `aiohttp` alone — not even `opentelemetry-api`.
The primary consumer is a Home Assistant integration, and HA ships neither
`protobuf` nor `grpcio`, so the manifest requirement is one line with no
extras. `http/protobuf` would pull in `opentelemetry-proto`, which means wheels
with musllinux tags and the HA wheel builder.

This is not a soft preference. `tests/test_core_only.py` enforces the
dependency-free core with two independent guards (an AST scan for module-level
imports of the extras, and a subprocess asserting they never reach
`sys.modules`). And the deviation is load-bearing rather than cosmetic: because
`_build_encoder()` raises `OTLPConfigError` when the protobuf extra is absent,
a spec-conformant `http/protobuf` default would make plain
`OTLPConfig(endpoint=...)` fail on a core-only install — the library's main use
case would not work out of the box.

The spec says SHOULD, not MUST, which permits deviation where the implications
are understood; they are, and they are documented here and in the README. The
requirement is also addressed to SDKs, and this package is scoped as a client,
not an SDK.

Users who want protobuf on the wire set `protocol` explicitly and install the
matching extra, which the README covers.

### Per-signal protocol and TLS settings are rejected, not honoured

One `OTLPClient` holds one encoder and one transport, both fixed at `create()`.
`PROTOCOL` selects both, and `INSECURE`, `CERTIFICATE`, `CLIENT_KEY` and
`CLIENT_CERTIFICATE` configure the connection, so none can vary per signal
within a client.

Per-signal certificates were considered for OTLP/HTTP alone, where aiohttp
accepts `ssl=` per request. They were rejected: nine more config fields, and a
field that silently behaves differently depending on the protocol. One clear
rule — per-request options vary per signal, connection-level options do not —
beats partial conformance with an asymmetry.

`from_env()` raises on these rather than ignoring them, because a silently
dropped per-signal protocol sends that signal in the wrong wire format and a
silently dropped certificate is a security surprise.

## Sources

- OTLP Exporter specification:
  <https://opentelemetry.io/docs/specs/otel/protocol/exporter/>
- Raw spec text:
  <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/exporter.md>
- opentelemetry-java#4590 — improved auth support on OTLP exporters
- opentelemetry-dotnet#2504 — OTLP HttpExporter bearer authentication
