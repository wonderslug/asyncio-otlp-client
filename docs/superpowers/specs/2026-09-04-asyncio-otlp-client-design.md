# asyncio-otlp-client — Design

**Date:** 2026-09-04
**Status:** Approved, ready for implementation planning

## Problem

There is no async OTLP client for Python. The official `opentelemetry-python`
OTLP exporters (1.44.0) are synchronous: the HTTP exporter is built on
`requests`, the gRPC exporter on a blocking `grpcio` channel. Async exporter
support has been an open, unassigned feature request since April 2023
(open-telemetry/opentelemetry-python#3273, preceded by #2314 in 2021), with no
implementation in the tree.

Third-party projects cover fragments. `otelmini` is a minimal SDK with
HTTP+JSON transport and all three signals, but it is synchronous and
single-maintainer. `otlp-json` was a dependency-free spans-to-JSON encoder with
no transport; it was archived in April 2026 and covered traces only. `logfire`
wraps the official synchronous exporter and targets a vendor backend.

The immediate consumer is a Home Assistant custom or embedded component. HA
core (2026.10 dev) requires Python >=3.14.2 and ships `aiohttp==3.14.3` and
`orjson`, but ships **neither `protobuf` nor `grpcio`** — neither appears in
core dependencies or `requirements_all.txt`. Custom integration dependencies
must publish wheels with musllinux tags. Using the official SDK inside HA means
introducing protobuf (and, for gRPC, a native `grpcio` wheel) plus running
blocking exports through `async_add_executor_job` and the SDK's own background
threads.

The OTLP specification provides the opening: OTLP/HTTP defines JSON as a
first-class encoding (`Content-Type: application/json`) across the same
endpoints. A pure-Python, protobuf-free, all-signals async client is therefore
feasible, and fills a real ecosystem gap rather than only a local convenience.

## Scope

`asyncio-otlp-client` is an OTLP **client**: it owns the data model, encoding,
transport, retry, and batching. It does not provide a `Tracer`, `Meter`, or
`Logger` API, does not aggregate metrics, does not manage temporality state,
and does not instrument anything.

Signal build order follows consumer priority: **metrics, then logs, then
traces**. Profiles are deferred but designed for.

### Decisions

1. **JSON core; protobuf and gRPC as extras.** The core install is pure Python
   over `aiohttp` with OTLP/HTTP JSON encoding and no native dependencies.
   `pip install asyncio-otlp-client[protobuf]` adds binary protobuf over HTTP.
   `[grpc]` adds the gRPC transport. The public API is identical across all
   three; encoder and transport are swappable.
2. **Transport and encoding only.** Callers construct data points and call
   `await client.export_metrics(...)`.
3. **Direct export plus an optional batcher.** A stateless `export_*` coroutine
   performs one round trip. An opt-in `BatchProcessor` owns a bounded queue and
   a background flush task, exposed as an async context manager.
4. **Bounded in-memory queues, drop oldest.** Spec-compliant retry with
   exponential backoff. No disk spooling; telemetry is best-effort and nothing
   survives a restart.
5. **Explicit configuration; environment variables opt-in.** A frozen
   `OTLPConfig` is the only source of settings. `OTLPConfig.from_env()` parses
   the standard `OTEL_EXPORTER_OTLP_*` variables when a caller asks for it.

### Assumptions

- **Profiles are deferred.** Every signal is `dataclass tree -> encoder ->
  transport -> path`. Profiles get a defined `SignalKind` carrying the
  `/v1development/profiles` path and a stub encoder raising
  `NotImplementedError`. Stabilization requires one encoder module, not a
  refactor.
- **Python >=3.12.** HA needs 3.14.2+; 3.12 keeps the library usable elsewhere
  at no cost. `py.typed` is shipped.
- **Zero Home Assistant coupling.** Nothing in the package imports
  `homeassistant`. HA guidance lives in documentation.
- **Core runtime dependency is `aiohttp` alone** — not even
  `opentelemetry-api`. HA already ships `aiohttp`, so the marginal footprint
  inside HA is zero new packages.

## Architecture

```
src/otlp_client/
  config.py         OTLPConfig (frozen), Protocol/Compression enums, from_env()
  model/            Resource, Scope, AnyValue; metrics.py, logs.py, traces.py
  encoding/
    json.py         hand-written OTLP/JSON encoder (core, no dependencies)
    protobuf.py     [protobuf] extra, via opentelemetry-proto
  transport/
    base.py         Transport protocol, ExportOutcome union
    http.py         aiohttp; JSON or protobuf body
    grpc.py         [grpc] extra, via grpcio.aio
  retry.py          backoff policy, Retry-After, retryable classification
  processor.py      BatchProcessor: bounded deques, flush task, async CM
  client.py         OTLPClient: export_metrics / export_logs / export_traces
  errors.py         OTLPError hierarchy
```

`Transport` and `Encoder` are `Protocol`s. `OTLPClient` holds one of each and
knows nothing about aiohttp, grpcio, JSON, or protobuf. This is what makes the
extras genuinely optional and lets every test run against an in-memory fake
transport with no network.

## Data model

Frozen, slotted dataclasses mirroring the OTLP proto tree exactly:
`ResourceMetrics -> ScopeMetrics -> Metric -> {Gauge | Sum | Histogram |
ExponentialHistogram | Summary} -> NumberDataPoint`, with equivalents for logs
(`ResourceLogs -> ScopeLogs -> LogRecord`) and traces (`ResourceSpans ->
ScopeSpans -> Span`).

Attributes are `Mapping[str, AnyValue]` where
`AnyValue = str | bool | int | float | bytes | Sequence[AnyValue] |
Mapping[str, AnyValue]`, normalized at encode time.

Because building that tree by hand for a single reading is five levels of
nesting, `model/` also ships thin constructors for common cases:

```python
from otlp_client import gauge

await client.export_metrics(
    resource=hass_resource,
    metrics=[gauge("home.temperature", 21.5, unit="Cel",
                   attributes={"entity_id": "sensor.living_room"})],
)
```

The dataclasses remain the interface; helpers are sugar over them.

## Client API

Each signal has two entry points. The convenience form builds the envelope from
a resource and scope bound at client construction:

```python
async def export_metrics(
    self,
    metrics: Sequence[Metric],
    *,
    resource: Resource | None = None,   # defaults to config.resource
    scope: InstrumentationScope | None = None,
) -> ExportOutcome: ...
```

The raw form takes a fully built envelope for callers that need multiple
resources or scopes in one request:

```python
async def export_resource_metrics(
    self, data: Sequence[ResourceMetrics]
) -> ExportOutcome: ...
```

`export_logs`/`export_resource_logs` and `export_traces`/`export_resource_spans`
mirror this exactly. `BatchProcessor.submit_metrics(metrics: Sequence[Metric])`
accepts the convenience form only; its resource and scope are bound when the
processor is constructed, since queued records must be envelope-independent
until flush.

## Encoding

### OTLP/JSON (core)

Verified against the OTLP specification:

- Keys are lowerCamelCase.
- **All 64-bit fields are decimal strings**: `timeUnixNano`,
  `startTimeUnixNano`, `asInt`, histogram `count` and `bucketCounts`.
- **`traceId` and `spanId` are case-insensitive hex strings, not base64.** This
  is the one documented deviation from the protobuf-JSON mapping. Every other
  bytes field remains base64.
- **Enums are encoded as integers.** The spec requires senders to use integer
  values and forbids enum name strings (`severityNumber`,
  `aggregationTemporality`, `spanKind`, `statusCode`).
- Default and empty fields are omitted to reduce payload size; receivers treat
  absent fields as defaults.
- Serialization uses the stdlib `json` module, with an automatic `orjson` fast
  path when `orjson` is importable. `orjson` is never a declared dependency; it
  is free inside HA, which already ships it.

### OTLP/protobuf (extra)

Maps the same dataclasses onto `opentelemetry-proto`'s generated classes. No
vendored `.proto` files and no codegen step in this repository.

### Responses

Responses are decoded, not merely status-checked. An HTTP 200 may carry
`partialSuccess` with `rejectedDataPoints` and an error message. Per
specification this **must not** be retried, so it surfaces as a distinct
`PartialSuccess` outcome for the caller to log. Each encoder parses its own
response format.

## Transport

```python
class Transport(Protocol):
    async def send(self, kind: SignalKind, payload: bytes) -> ExportOutcome: ...
    async def aclose(self) -> None: ...
```

`ExportOutcome = Success | PartialSuccess(rejected, message) |
Retryable(retry_after) | Permanent(status, message)`. Transports classify;
`retry.py` decides. Neither knows the other's wire format.

### HTTP

- **The `ClientSession` is injected, not created.** HA forbids integrations
  from creating their own sessions; they must use
  `async_get_clientsession(hass)`. `HTTPTransport(session=...)` accepts one and
  creates (and then owns) a session only when none is supplied.
- **The `SSLContext` is built off-loop.** `ssl.create_default_context()` and
  loading a CA bundle or client certificate perform blocking file reads, which
  HA flags in the event loop. TLS setup happens once inside `asyncio.to_thread`
  during async construction.
- **gzip is offloaded above a size threshold.** Compression is optional per
  config; payloads over 32 KB are compressed via `asyncio.to_thread` so a large
  flush cannot stall the event loop on low-powered hardware. When used, the
  request carries `Content-Encoding: gzip`.
- Endpoint handling follows the specification's asymmetry: a base endpoint has
  the signal path (`/v1/metrics`, `/v1/logs`, `/v1/traces`) appended, while a
  per-signal endpoint is used verbatim.
- `Content-Type` is `application/json` or `application/x-protobuf` per encoder.

### gRPC

Uses `grpcio.aio` channels against the standard `Export` methods.
**The `[grpc]` extra requires `[protobuf]`** — OTLP over gRPC has no JSON
encoding, so the extra pulls both. `UNAVAILABLE`, `DEADLINE_EXCEEDED`, and
`RESOURCE_EXHAUSTED` map to `Retryable`; all other statuses to `Permanent`.

## Retry

Exponential backoff with full jitter, honoring `Retry-After` in both
delay-seconds and HTTP-date forms. Defaults, all configurable on
`OTLPConfig`: `initial_backoff=1.0s`, `max_backoff=30.0s`,
`backoff_multiplier=1.5`, `max_elapsed=90.0s`. The retryable set is
exactly HTTP 429, 502, 503, and 504, plus connection errors and timeouts. All
other 4xx responses are permanent and drop. Partial success is never retried.
The retry loop is cancellation-safe: cancellation during a backoff sleep
propagates rather than being swallowed.

## Batch processor

One instance serves all signals, holding a bounded deque per signal and a
single flush task, exposed as an async context manager:

```python
async with BatchProcessor(
    client, max_batch=512, flush_interval=5.0, max_queue=2048,
) as proc:
    proc.submit_metrics([...])   # non-blocking; returns False if dropped
```

- Flush triggers: batch size reached, interval elapsed, or explicit
  `await proc.flush()`.
- `max_queue` bounds each signal's deque independently (default 2048
  records per signal). On overflow the oldest entries are dropped and a
  counter is incremented.
- While the collector is unreachable the flush task applies the same backoff
  policy rather than hot-looping, and queues keep absorbing until they wrap.
- `proc.stats` exposes `submitted`, `exported`, `dropped`,
  `consecutive_failures`, and `last_error`, so a consumer can surface client
  health as its own entity.
- `__aexit__` makes one bounded final flush attempt, then cancels cleanly.

## Error handling contract

- `proc.submit_*()` **never raises and never blocks**; it returns `False` when
  the record was dropped. A state-change listener has nowhere to handle an
  exception.
- `await client.export_*()` **raises** on permanent failure and otherwise
  returns `Success` or `PartialSuccess`.
- All exceptions derive from `OTLPError`, split into `OTLPTransportError`,
  `OTLPPermanentError`, and `OTLPConfigError`. A missing extra raises
  `OTLPConfigError` naming the exact `pip install` command.

## Testing

### Encoder oracle

The hand-written JSON encoder is the highest-risk component, so it is not
validated by hand-written golden files alone. `opentelemetry-proto` is a **dev**
dependency providing a canonical schema:

```
model tree --> json encoder    --> json_format.Parse(...) --+
           \-> protobuf encoder ------------------------- --+-> assert equal
```

If the JSON parses into a real `ExportMetricsServiceRequest` and the resulting
message equals what the protobuf encoder produces from the same input, both
encoders are validated against the official schema and against each other.
Driven by Hypothesis over generated model trees, this catches int-as-string
slips, base64-versus-hex identifier bugs, and enum-name regressions.

### Determinism

Retry and processor tests inject a fake clock and a fake `sleep`; no test waits
on wall time, and backoff schedules are asserted as sequences. Transport tests
run against `aiohttp.test_utils` and an in-process `grpc.aio` server. An
in-memory fake transport covers the client and processor. The default suite is
entirely offline.

### Integration

A separate, marked, opt-in suite runs against a real `otel-collector` in Docker
writing through a file exporter. It is the only check that proves a real
collector accepts the produced bytes, so it runs in CI but not on every local
`pytest` invocation.

### Optional-extra enforcement

`encoding/protobuf.py` and `transport/grpc.py` are lazily imported behind
factory functions and never at package import time. CI includes a job with only
`aiohttp` installed that imports the package and exercises the full JSON/HTTP
path, so an accidental top-level `import grpc` fails the build rather than a
user's installation.

Implementation follows TDD throughout.

## Home Assistant integration

No HA-specific code lives in this package. The properties that matter follow
from the decisions above:

- The core install is pure Python over a dependency HA already ships, so the
  published wheel is `py3-none-any` and installs on every HA architecture with
  no involvement from the HA wheel builder, no musllinux tags, and no native
  compilation.
- The manifest requirement is a single line:
  `"requirements": ["asyncio-otlp-client==x.y.z"]`, with no extras.

Documentation carries a worked example: `async_get_clientsession(hass)` passed
into the client, the processor started with `entry.async_create_background_task`
in `async_setup_entry`, and stopped in `async_unload_entry`.

## Tooling

`src` layout, `uv` with `hatchling`, `ruff`, `mypy --strict`, `pytest` with
`pytest-asyncio`. CI matrix covers Python 3.12, 3.13, and 3.14, plus the
core-only import job described above.

## Out of scope

- Instrument APIs (`Tracer`, `Meter`, `Logger`), aggregation, and temporality
  state. A future SDK layer may sit on top of the dataclass boundary as a
  separate project.
- Disk spooling and restart durability.
- Automatic instrumentation of any library.
- Profiles encoding, until the signal stabilizes.
