# Per-signal exporter configuration — Design

**Date:** 2026-09-05
**Status:** Approved, ready for implementation planning
**Extends:** `docs/superpowers/specs/2026-09-04-asyncio-otlp-client-design.md`
**Follows from:** `docs/auth-audit.md`, finding 2

## Problem

The OTLP exporter spec opens its configuration section with:

> The following configuration options MUST be available to configure the OTLP
> exporter. Each configuration option MUST be overridable by a signal specific
> option.

Every option therefore has `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_*` forms
alongside the general one. `OTLPConfig.from_env()` reads only the general form,
plus per-signal *endpoints*. Everything else — headers, timeout, compression,
protocol, and the TLS settings — is general-only.

The gap with a concrete use case is per-signal **headers**: a vendor that meters
signals separately issues a different API key per signal, and today that cannot
be expressed in one client.

## Decisions

### 1. Only per-request options become per-signal

One `OTLPClient` holds one `Encoder` and one `Transport`, both fixed at
`create()`. That splits the spec's options in two:

| Option | Per-signal? | Why |
| --- | --- | --- |
| `HEADERS` | **Yes** | Per request on HTTP, per call metadata on gRPC |
| `TIMEOUT` | **Yes** | Per request / per call on both |
| `COMPRESSION` | **Yes** | Per request / per call on both |
| `ENDPOINT` | Already | HTTP yes; gRPC rejects (one channel, one host) |
| `PROTOCOL` | **No** | Selects both the encoder and the transport |
| `INSECURE` | **No** | gRPC channel-level |
| `CERTIFICATE` | **No** | gRPC channel-level |
| `CLIENT_KEY` | **No** | gRPC channel-level |
| `CLIENT_CERTIFICATE` | **No** | gRPC channel-level |

The rule to teach: **per-request options can vary per signal; connection-level
options cannot — use one client per signal.**

Per-signal certificates were considered and rejected. They are technically
possible over HTTP, since aiohttp accepts `ssl=` per request, but they would add
nine config fields and make a single field behave differently depending on
protocol. One clear rule beats partial conformance with an asymmetry.

### 2. A per-signal value replaces the general one

The spec says options are *overridable*, and an option's value is the whole
value — so for headers the per-signal map replaces the general map rather than
merging with it. This matches other SDKs, so configuration ported from another
SDK behaves the same way here.

```
OTEL_EXPORTER_OTLP_HEADERS=api-key=secret
OTEL_EXPORTER_OTLP_TRACES_HEADERS=x-tenant=acme

traces  -> {x-tenant: acme}      # api-key is NOT sent
metrics -> {api-key: secret}
logs    -> {api-key: secret}
```

This is a footgun — a per-signal header silently drops shared credentials for
that signal — so the README must state it explicitly.

### 3. Forbidden per-signal variables raise

`from_env()` raises `OTLPConfigError` when it sees any of the fifteen names from
the "No" rows above, naming the variable and pointing at separate `OTLPClient`
instances.

Raising rather than ignoring because both silent outcomes are bad: ignoring
`OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=grpc` would send traces in the wrong wire
format, and ignoring a per-signal certificate is a security surprise. It is also
consistent with how `from_env()` already treats an unknown protocol or
compression.

The accepted cost: an environment that sets these for a different tool will now
fail fast rather than starting. `from_env()` is opt-in, never called implicitly,
so this cannot fire without the caller asking for it.

### 4. Flat fields, mirroring the existing endpoint trio

`metrics_endpoint` / `logs_endpoint` / `traces_endpoint` already establish the
shape; the new options follow it rather than introducing a second mechanism.

The alternative — a `SignalOverrides` sub-dataclass keyed by `SignalKind` —
scales better but breaks the three public `*_endpoint` fields and adds an
exported type. Its advantage is scaling to more per-signal options, which
decision 1 has deliberately foreclosed: the set is closed at nine fields.

## Config changes

```python
metrics_headers: Mapping[str, str] | None = field(default=None, hash=False)
logs_headers: Mapping[str, str] | None = field(default=None, hash=False)
traces_headers: Mapping[str, str] | None = field(default=None, hash=False)

metrics_timeout: float | None = None
logs_timeout: float | None = None
traces_timeout: float | None = None

metrics_compression: Compression | None = None
logs_compression: Compression | None = None
traces_compression: Compression | None = None
```

`hash=False` on the header fields for the same reason the general `headers`
field carries it: the dataclass is frozen, and a `Mapping` is not hashable.

`None` and an empty mapping mean different things and both are reachable:

- `None` — not configured; fall back to the general value.
- `{}` — configured as empty; under replace semantics, send no headers for this
  signal. Set by `OTEL_EXPORTER_OTLP_TRACES_HEADERS=""`.

A bare `Mapping` type could not express both, which is why the fields are
`| None`.

## Resolution

Three resolvers join `endpoint_for()`, each following the same shape: take the
override for the signal, return it when set, otherwise the general value.

```python
def headers_for(self, kind: SignalKind) -> Mapping[str, str]: ...
def timeout_for(self, kind: SignalKind) -> float: ...
def compression_for(self, kind: SignalKind) -> Compression: ...
```

`SignalKind.PROFILES` resolves to the general value throughout, as it does for
`endpoint_for()` today.

## Env parsing

Nine new lookups. Headers read through `src.get(name)` so an absent variable
gives `None` while an empty one gives an empty mapping, preserving the
distinction above.

`_parse_timeout(raw, name)` and `_parse_compression(raw, name)` are factored out
of the existing general-form parsing and used by all four call sites each,
rather than repeating the millisecond conversion and enum error handling four
times. This is a tidy inside code already being edited, not a general refactor.

## Endpoint root-path conformance

Folded in because it lives in `endpoint_for()`, which this work already touches.
The spec requires:

> For the per-signal variables the URL MUST be used as-is without any
> modification. The only exception is that if an URL contains no path part, the
> root path `/` MUST be used.

`endpoint_for()` returns per-signal overrides verbatim, so
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://collector:4318` yields a pathless
URL. Append `/` when a per-signal override has no path. The general-endpoint
branch is unaffected, since it already appends a signal path.

## Transport changes

`HTTPTransport` builds a `ClientTimeout` per signal once in `__init__` instead of
holding a single one, and `send()` reads `headers_for`, `compression_for` and the
per-signal timeout. `GRPCTransport.send()` reads the same three — all per call on
a gRPC channel, so the channel and its credentials are untouched.

The existing gRPC rejection of per-signal *endpoints* stays exactly as it is.

## Testing

- **Resolution** — general fallback; override replaces; an empty override
  replaces with empty; signals resolve independently; `PROFILES` falls back.
- **Env parsing** — all nine variables; timeouts in milliseconds; invalid values
  raise; an empty headers variable yields an empty mapping, not `None`.
- **Rejections** — one case per forbidden variable, asserting the message names
  the variable.
- **Endpoint root path** — a pathless per-signal override gains `/`; one that
  already has a path is untouched.
- **Transports** — per-signal headers, compression and timeout reach the wire on
  both, via the existing `Recorder` and `EchoHandler` fixtures.

Every existing test must stay green. Several assert general-header behavior and
would catch a resolution bug that always returned the override.

## Documentation

README gains a per-signal section stating replace semantics with the
dropped-credential footgun called out, and listing which options cannot vary per
signal. `docs/auth-audit.md` finding 2 is marked resolved, with the certificate
decision recorded under "Intentional deviations".

## Out of scope

- Per-signal certificates, `insecure`, and protocol — decision 1.
- Per-signal endpoints over gRPC — already rejected, unchanged.
- Dynamic credential providers — the remaining audit item, its own design.
