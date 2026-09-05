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
| Per-signal variants of all five | `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_*` | **Missing** |

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

### 2. No per-signal configuration variants

Every option in the table has `_TRACES_`, `_METRICS_`, `_LOGS_` forms in the
spec. `OTLPConfig.from_env()` reads only the base form plus per-signal
*endpoints*. Per-signal headers are the practically useful case — different
API keys per signal, e.g. a vendor that meters traces and metrics separately.

Note the transports differ here: per-signal headers are meaningful over gRPC
(metadata is per call), but per-signal certificates are not (one channel, one
credential set). gRPC already rejects per-signal endpoints for the same reason.

### 3. Headers are frozen at construction time

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

Deliberately not designed here. Worth its own brainstorm if we pursue it.

## Adjacent deviations noticed during the audit

Not auth, but found while reading the same code path:

- **gRPC default endpoint.** ~~`from_env()` defaults to `http://localhost:4318`
  regardless of protocol. The spec's gRPC default is `http://localhost:4317`.
  With `OTEL_EXPORTER_OTLP_PROTOCOL=grpc` and no endpoint set, we target the
  HTTP port.~~ **Fixed 2026-09-05** — `from_env()` now selects the default
  endpoint from the parsed protocol via `_DEFAULT_ENDPOINTS`.
- **Default protocol.** Ours is `http/json`; the spec says SHOULD be
  `http/protobuf`. Plausibly a deliberate choice to keep the core install
  dependency-free — worth recording as intentional if so.
- **Per-signal `TIMEOUT`, `COMPRESSION`, `PROTOCOL`** are also unimplemented,
  same gap as finding 2.

## Sources

- OTLP Exporter specification:
  <https://opentelemetry.io/docs/specs/otel/protocol/exporter/>
- Raw spec text:
  <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/exporter.md>
- opentelemetry-java#4590 — improved auth support on OTLP exporters
- opentelemetry-dotnet#2504 — OTLP HttpExporter bearer authentication
