# asyncio-otlp-client

A pure-asyncio OpenTelemetry OTLP client for Python. Exports metrics, logs, and
traces over OTLP/HTTP with **no native dependencies** — the core install needs
only `aiohttp`.

The official `opentelemetry-python` OTLP exporters are synchronous (`requests`
for HTTP, a blocking channel for gRPC) and always require protobuf. This library
exists for asyncio applications that want neither.

## Install

```bash
pip install asyncio-otlp-client              # JSON over HTTP; pure Python
pip install 'asyncio-otlp-client[protobuf]'  # binary protobuf over HTTP
pip install 'asyncio-otlp-client[grpc]'      # OTLP over gRPC (implies protobuf)
```

The JSON encoder is pure Python and always available. The protobuf encoder
(used for `http/protobuf` and required for `grpc`) is imported lazily and only
needs to be installed if you use it. Both encoders agree on the wire: they omit
`resource`, `scope`, and `status` from the payload entirely when those carry no
information, rather than sending an empty object.

## Use

```python
import time

from aiohttp import ClientSession
from otlp_client import OTLPClient, OTLPConfig, Resource, gauge

config = OTLPConfig(
    endpoint="http://localhost:4318",
    resource=Resource(attributes={"service.name": "my-app"}),
)

async with ClientSession() as session:
    client = await OTLPClient.create(config, session=session)
    await client.export_metrics(
        [gauge("temperature", 21.5, unit="Cel", time_unix_nano=time.time_ns())]
    )
    await client.aclose()
```

`export_*` performs one round trip, with retries. It returns `Success` or
`PartialSuccess`, and raises `OTLPPermanentError` or `OTLPTransportError` on
failure.

## Batching

For fire-and-forget submission, wrap the client in a `BatchProcessor`. It owns a
bounded queue per signal and a background flush task.

```python
from otlp_client import BatchProcessor

async with BatchProcessor(client, max_batch=512, flush_interval=5.0) as proc:
    proc.submit_metrics([gauge("temperature", 21.5, time_unix_nano=time.time_ns())])
```

`submit_*` never blocks and never raises; it returns `False` if a record was
dropped. Check `proc.stats` for `submitted`, `exported`, `dropped`,
`consecutive_failures`, and `last_error`.

`dropped` counts every record that never reached the collector, regardless of
cause: the bounded queue evicts the oldest record on overflow, and a batch that
fails to export (a collector outage, for example) is discarded rather than
requeued — telemetry here is best-effort with no on-disk durability, so a
failed batch is a loss, not a retry candidate. `last_error` tells you which
kind of loss happened last.

You don't have to poll `stats` to notice an outage: the first export failure in
a run logs at `WARNING`, and consecutive failures after that log at `DEBUG` so
a persistent outage doesn't spam your logs for as long as it lasts.

## Configuration

`OTLPConfig` is the only source of settings. To read the standard environment
variables instead, opt in explicitly:

```python
config = OTLPConfig.from_env()  # OTEL_EXPORTER_OTLP_*
```

TLS is configured via `certificate_file` (a CA to trust), `client_certificate_file`
/ `client_key_file` (mutual TLS), and `insecure_skip_verify` (skip certificate
verification). `insecure_skip_verify` works over HTTPS. It does **not** work
with `protocol=OTLPProtocol.GRPC` against a TLS endpoint — grpcio offers no API
to disable certificate verification — and `OTLPClient.create()` raises
`OTLPConfigError` there rather than silently connecting with verification
still on.

## Scope

This is a client, not an SDK. It owns the data model, encoding, transport,
retry, and batching. It does not provide `Tracer`/`Meter`/`Logger` APIs, does
not aggregate metrics, and does not instrument anything — you construct data
points and hand them over.

The profiles signal is defined as a seam (`SignalKind.PROFILES` carries its
`/v1development/profiles` path) but is not encoded yet; calling `export_*` for
it raises `NotImplementedError`, matching its still-in-development status
upstream.

## Home Assistant

See [docs/home-assistant.md](docs/home-assistant.md). The core install is pure
Python and publishes a `py3-none-any` wheel, so it installs on every Home
Assistant architecture with no wheel-builder involvement.
