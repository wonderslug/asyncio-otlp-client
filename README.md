# asyncio-otlp-client

[![CI](https://github.com/wonderslug/asyncio-otlp-client/actions/workflows/ci.yml/badge.svg)](https://github.com/wonderslug/asyncio-otlp-client/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/asyncio-otlp-client.svg)](https://pypi.org/project/asyncio-otlp-client/)
[![Python versions](https://img.shields.io/pypi/pyversions/asyncio-otlp-client.svg)](https://pypi.org/project/asyncio-otlp-client/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE.md)

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

## Trace context and exemplars

Spans and span links carry W3C `trace_state` and `flags`; build the flags
with the helper rather than by hand:

```python
from otlp_client import span, span_flags

s = span(
    "handle_request",
    trace_id=trace_id,
    span_id=span_id,
    start_time_unix_nano=start,
    end_time_unix_nano=end,
    trace_state="vendor=abc",
    flags=span_flags(sampled=True, is_remote=False),
)
```

`is_remote=None` (the default) means "unknown", which is distinct from
`is_remote=False`: `None` leaves both context-related bits clear, while
`False` sets the "is-remote is known" bit without setting "is-remote" itself.

Exemplars link a metric data point to the trace that produced it, so a spike
is traceable back to the request behind it:

```python
from otlp_client import Exemplar, NumberDataPoint

point = NumberDataPoint(
    time_unix_nano=now,
    value=1.5,
    exemplars=[Exemplar(time_unix_nano=now, value=1.5, trace_id=trace_id, span_id=span_id)],
)
```

A data point can also record that no measurement was taken, which is
different from measuring zero. The `gauge()`/`sum_()` helpers don't expose
`flags`, so set it on the dataclass directly:

```python
from otlp_client import data_point_flags

point = NumberDataPoint(time_unix_nano=now, value=0, flags=data_point_flags(no_recorded_value=True))
```

## Configuration

`OTLPConfig` is the only source of settings. To read the standard environment
variables instead, opt in explicitly:

```python
config = OTLPConfig.from_env()  # OTEL_EXPORTER_OTLP_*
```

TLS is configured via `certificate_file` (a CA to trust) and
`client_certificate_file` / `client_key_file` (mutual TLS).

Two separate settings control transport security, and they are easy to confuse:

- **`insecure`** decides whether TLS is used *at all*. It applies only to gRPC
  endpoints written without a scheme, and defaults to `False`.
- **`insecure_skip_verify`** keeps TLS but stops verifying the server's
  certificate.

For gRPC, an explicit scheme always wins over `insecure`: `https://host:4317`
is TLS and `http://host:4317` is plaintext, whatever `insecure` says. Only a
scheme-less `host:4317` consults `insecure`. OTLP/HTTP ignores `insecure`
entirely and always uses the scheme in the endpoint.

> **Behaviour change.** Before 0.3.0 a scheme-less gRPC endpoint such as
> `collector.local:4317` connected in **plaintext**. It now uses TLS, matching
> the OTLP spec, whose `insecure` default is `false`. If you relied on the old
> behaviour, either write the scheme explicitly (`http://collector.local:4317`)
> or set `insecure=True` / `OTEL_EXPORTER_OTLP_INSECURE=true`.

`insecure_skip_verify` works over HTTPS. It does **not** work
with `protocol=OTLPProtocol.GRPC` against a TLS endpoint — grpcio offers no API
to disable certificate verification — and `OTLPClient.create()` raises
`OTLPConfigError` there rather than silently connecting with verification
still on.

## Scope

This is a client, not an SDK. It owns the data model, encoding, transport,
retry, and batching. It does not provide `Tracer`/`Meter`/`Logger` APIs, does
not aggregate metrics, and does not instrument anything — you construct data
points and hand them over.

The profiles signal is defined as a seam — `SignalKind.PROFILES` exists and
carries its `/v1development/profiles` path — but no encoder implements it and
no public export method accepts it yet: there is no `export_profiles` on
`OTLPClient`, and `submit_*`/`export_*` cover metrics, logs, and traces only.
The signal remains in development upstream.

## Home Assistant

See [docs/home-assistant.md](docs/home-assistant.md). The core install is pure
Python and publishes a `py3-none-any` wheel, so it installs on every Home
Assistant architecture with no wheel-builder involvement.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). `make dev` sets up the environment,
`make lint` and `make test` are what CI runs.

## License

MIT — see [LICENSE.md](LICENSE.md).
