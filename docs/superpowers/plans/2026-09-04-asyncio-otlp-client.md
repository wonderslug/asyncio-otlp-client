# asyncio-otlp-client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a pure-asyncio OTLP client for Python that exports metrics, logs, and traces over OTLP/HTTP with JSON encoding using only `aiohttp`, with binary protobuf and gRPC available as optional extras.

**Architecture:** Frozen dataclasses mirror the OTLP proto tree. An `Encoder` turns them into bytes and a `Transport` ships those bytes, both behind one-method `Protocol`s so the client core never imports `protobuf`, `grpcio`, or even `aiohttp` directly. A retry engine sits between the client and the transport; an optional `BatchProcessor` adds bounded queueing and a background flush task.

**Tech Stack:** Python 3.12+, `aiohttp`, `uv` + `hatchling`, `pytest` + `pytest-asyncio`, `hypothesis`, `ruff`, `mypy --strict`. Dev-only: `opentelemetry-proto` (encoder oracle).

**Spec:** `docs/superpowers/specs/2026-09-04-asyncio-otlp-client-design.md`

## Global Constraints

These apply to every task. Violating any of them is grounds for rejecting a task.

- **Core runtime dependency is `aiohttp` alone.** Not `opentelemetry-api`, not `opentelemetry-proto`, not `protobuf`, not `orjson`.
- **`protobuf` and `grpcio` are optional extras**, imported lazily inside factory functions — never at module import time in any module reachable from `import otlp_client`.
- **`opentelemetry-proto` is a dev dependency only**, used by tests as an encoding oracle.
- **Nothing imports `homeassistant`.** Ever. HA guidance lives in documentation only.
- **Python floor is 3.12.** CI matrix: 3.12, 3.13, 3.14.
- **Package name is `otlp_client`; distribution name is `asyncio-otlp-client`.**
- **All dataclasses are `frozen=True, slots=True`** unless a task says otherwise.
- **`mypy --strict` and `ruff check` must pass** before every commit.
- **No test may sleep on wall-clock time.** Retry and processor tests inject a fake clock and fake sleep.
- **OTLP/JSON wire rules, non-negotiable:** lowerCamelCase keys; all 64-bit fields (`timeUnixNano`, `startTimeUnixNano`, `asInt`, histogram `count`, `bucketCounts`) as decimal **strings**; `traceId`/`spanId` as **hex** strings, all other bytes base64; enums as **integers**, never name strings; default/empty fields omitted.
- **Partial success is never retried.**

## File Structure

| File | Responsibility |
|---|---|
| `src/otlp_client/__init__.py` | Public exports only; no logic |
| `src/otlp_client/errors.py` | `OTLPError` hierarchy |
| `src/otlp_client/outcomes.py` | `Success`/`PartialSuccess`/`Retryable`/`Permanent` union, shared by encoding and transport |
| `src/otlp_client/signals.py` | `SignalKind` enum + HTTP path table (includes the profiles seam) |
| `src/otlp_client/config.py` | `OTLPConfig`, `OTLPProtocol`, `Compression`, `from_env()` |
| `src/otlp_client/model/common.py` | `AnyValue`, `Resource`, `InstrumentationScope` |
| `src/otlp_client/model/metrics.py` | Metric tree + `gauge()`/`sum_()` helpers |
| `src/otlp_client/model/logs.py` | Log tree + `log_record()` helper |
| `src/otlp_client/model/traces.py` | Span tree + `span()` helper |
| `src/otlp_client/encoding/base.py` | `Encoder` protocol |
| `src/otlp_client/encoding/primitives.py` | `u64`, `hex_id`, `encode_any_value`, `encode_attributes`, `omit_empty` |
| `src/otlp_client/encoding/json.py` | JSON encoder for all signals + response decode |
| `src/otlp_client/encoding/protobuf.py` | `[protobuf]` extra; lazy |
| `src/otlp_client/transport/base.py` | `Transport` protocol |
| `src/otlp_client/transport/http.py` | aiohttp transport |
| `src/otlp_client/transport/grpc.py` | `[grpc]` extra; lazy |
| `src/otlp_client/retry.py` | `RetryPolicy`, `with_retry`, `parse_retry_after` |
| `src/otlp_client/client.py` | `OTLPClient` |
| `src/otlp_client/processor.py` | `BatchProcessor`, `ProcessorStats` |
| `tests/support/fakes.py` | `FakeTransport`, `FakeClock` |

---

### Task 1: Project scaffolding and common model types

**Files:**
- Create: `pyproject.toml`, `src/otlp_client/__init__.py`, `src/otlp_client/py.typed`, `src/otlp_client/model/__init__.py`, `src/otlp_client/model/common.py`
- Test: `tests/test_model_common.py`

**Interfaces:**
- Consumes: nothing
- Produces: `AnyValue` type alias; `Resource(attributes, dropped_attributes_count)`; `InstrumentationScope(name, version, attributes)`; both frozen+slots dataclasses

- [ ] **Step 1: Create the project skeleton**

```bash
mkdir -p src/otlp_client/model tests
touch src/otlp_client/py.typed src/otlp_client/model/__init__.py
```

Create `pyproject.toml`:

```toml
[project]
name = "asyncio-otlp-client"
version = "0.1.0"
description = "Pure-asyncio OpenTelemetry OTLP client"
requires-python = ">=3.12"
dependencies = ["aiohttp>=3.9"]

[project.optional-dependencies]
protobuf = ["opentelemetry-proto>=1.20"]
grpc = ["opentelemetry-proto>=1.20", "grpcio>=1.63"]

[dependency-groups]
dev = [
  "pytest>=8", "pytest-asyncio>=0.24", "hypothesis>=6",
  "mypy>=1.11", "ruff>=0.6", "opentelemetry-proto>=1.20",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/otlp_client"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
pythonpath = ["."]
markers = ["integration: requires a live otel-collector"]
addopts = "-m 'not integration'"

[tool.mypy]
strict = true
files = ["src", "tests"]

[tool.ruff]
line-length = 100
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_model_common.py`:

```python
import dataclasses
import pytest
from otlp_client.model.common import InstrumentationScope, Resource


def test_resource_holds_attributes():
    r = Resource(attributes={"service.name": "hass", "port": 8123})
    assert r.attributes["service.name"] == "hass"
    assert r.dropped_attributes_count == 0


def test_resource_is_frozen():
    r = Resource(attributes={})
    with pytest.raises(dataclasses.FrozenInstanceError):
        r.dropped_attributes_count = 5  # type: ignore[misc]


def test_scope_defaults():
    s = InstrumentationScope(name="otlp_client")
    assert s.name == "otlp_client"
    assert s.version is None
    assert s.attributes == {}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/test_model_common.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'otlp_client.model.common'`

- [ ] **Step 4: Write the implementation**

Create `src/otlp_client/model/common.py`:

```python
"""Types shared by every OTLP signal."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType

type AnyValue = (
    str | bool | int | float | bytes | Sequence["AnyValue"] | Mapping[str, "AnyValue"]
)

_EMPTY: Mapping[str, AnyValue] = MappingProxyType({})


@dataclass(frozen=True, slots=True)
class Resource:
    """The entity producing telemetry."""

    attributes: Mapping[str, AnyValue] = field(default=_EMPTY)
    dropped_attributes_count: int = 0


@dataclass(frozen=True, slots=True)
class InstrumentationScope:
    """The library or component that emitted a signal."""

    name: str
    version: str | None = None
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY)
```

- [ ] **Step 5: Run tests and lint**

Run: `uv run pytest tests/test_model_common.py -v && uv run mypy && uv run ruff check`
Expected: 3 passed, no mypy errors, no ruff errors

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml src/otlp_client tests/test_model_common.py
git commit -m "feat: project scaffolding and common model types"
```

---

### Task 2: Metrics model and construction helpers

**Files:**
- Create: `src/otlp_client/model/metrics.py`
- Test: `tests/test_model_metrics.py`

**Interfaces:**
- Consumes: `Resource`, `InstrumentationScope`, `AnyValue` from `model/common.py`
- Produces: `AggregationTemporality` (IntEnum: `UNSPECIFIED=0, DELTA=1, CUMULATIVE=2`); `NumberDataPoint(attributes, time_unix_nano, value, start_time_unix_nano)`; `HistogramDataPoint(attributes, time_unix_nano, count, sum, bucket_counts, explicit_bounds, start_time_unix_nano)`; `Gauge(data_points)`; `Sum(data_points, aggregation_temporality, is_monotonic)`; `Histogram(data_points, aggregation_temporality)`; `Metric(name, data, description, unit)`; `ScopeMetrics(scope, metrics)`; `ResourceMetrics(resource, scope_metrics)`; helpers `gauge(name, value, *, unit, attributes, time_unix_nano) -> Metric` and `sum_(name, value, *, unit, attributes, time_unix_nano, is_monotonic, temporality) -> Metric`

- [ ] **Step 1: Write the failing test**

Create `tests/test_model_metrics.py`:

```python
from otlp_client.model.metrics import (
    AggregationTemporality,
    Gauge,
    Metric,
    NumberDataPoint,
    Sum,
    gauge,
    sum_,
)


def test_gauge_helper_builds_single_point_metric():
    m = gauge("home.temperature", 21.5, unit="Cel",
              attributes={"entity_id": "sensor.living_room"}, time_unix_nano=1700000000000000000)
    assert isinstance(m, Metric)
    assert m.name == "home.temperature"
    assert m.unit == "Cel"
    assert isinstance(m.data, Gauge)
    (point,) = m.data.data_points
    assert point.value == 21.5
    assert point.attributes == {"entity_id": "sensor.living_room"}
    assert point.time_unix_nano == 1700000000000000000


def test_sum_helper_defaults_to_monotonic_cumulative():
    m = sum_("home.energy", 42, time_unix_nano=1)
    assert isinstance(m.data, Sum)
    assert m.data.is_monotonic is True
    assert m.data.aggregation_temporality is AggregationTemporality.CUMULATIVE


def test_number_data_point_preserves_int_vs_float():
    assert NumberDataPoint(time_unix_nano=1, value=3).value == 3
    assert isinstance(NumberDataPoint(time_unix_nano=1, value=3.0).value, float)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_model_metrics.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'otlp_client.model.metrics'`

- [ ] **Step 3: Write the implementation**

Create `src/otlp_client/model/metrics.py`:

```python
"""The OTLP metrics data model."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import IntEnum
from types import MappingProxyType

from otlp_client.model.common import AnyValue, InstrumentationScope, Resource

_EMPTY: Mapping[str, AnyValue] = MappingProxyType({})


class AggregationTemporality(IntEnum):
    UNSPECIFIED = 0
    DELTA = 1
    CUMULATIVE = 2


@dataclass(frozen=True, slots=True)
class NumberDataPoint:
    time_unix_nano: int
    value: int | float
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY)
    start_time_unix_nano: int | None = None


@dataclass(frozen=True, slots=True)
class HistogramDataPoint:
    time_unix_nano: int
    count: int
    bucket_counts: Sequence[int]
    explicit_bounds: Sequence[float]
    sum: float | None = None
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY)
    start_time_unix_nano: int | None = None


@dataclass(frozen=True, slots=True)
class Gauge:
    data_points: Sequence[NumberDataPoint]


@dataclass(frozen=True, slots=True)
class Sum:
    data_points: Sequence[NumberDataPoint]
    aggregation_temporality: AggregationTemporality = AggregationTemporality.CUMULATIVE
    is_monotonic: bool = True


@dataclass(frozen=True, slots=True)
class Histogram:
    data_points: Sequence[HistogramDataPoint]
    aggregation_temporality: AggregationTemporality = AggregationTemporality.CUMULATIVE


type MetricData = Gauge | Sum | Histogram


@dataclass(frozen=True, slots=True)
class Metric:
    name: str
    data: MetricData
    description: str = ""
    unit: str = ""


@dataclass(frozen=True, slots=True)
class ScopeMetrics:
    scope: InstrumentationScope
    metrics: Sequence[Metric]


@dataclass(frozen=True, slots=True)
class ResourceMetrics:
    resource: Resource
    scope_metrics: Sequence[ScopeMetrics]


def gauge(
    name: str,
    value: int | float,
    *,
    time_unix_nano: int,
    unit: str = "",
    description: str = "",
    attributes: Mapping[str, AnyValue] | None = None,
) -> Metric:
    """Build a single-point gauge metric."""
    point = NumberDataPoint(
        time_unix_nano=time_unix_nano, value=value, attributes=attributes or _EMPTY
    )
    return Metric(name=name, data=Gauge(data_points=(point,)), unit=unit, description=description)


def sum_(
    name: str,
    value: int | float,
    *,
    time_unix_nano: int,
    start_time_unix_nano: int | None = None,
    unit: str = "",
    description: str = "",
    attributes: Mapping[str, AnyValue] | None = None,
    is_monotonic: bool = True,
    temporality: AggregationTemporality = AggregationTemporality.CUMULATIVE,
) -> Metric:
    """Build a single-point sum metric."""
    point = NumberDataPoint(
        time_unix_nano=time_unix_nano,
        value=value,
        attributes=attributes or _EMPTY,
        start_time_unix_nano=start_time_unix_nano,
    )
    data = Sum(
        data_points=(point,), aggregation_temporality=temporality, is_monotonic=is_monotonic
    )
    return Metric(name=name, data=data, unit=unit, description=description)
```

- [ ] **Step 4: Run tests and lint**

Run: `uv run pytest tests/test_model_metrics.py -v && uv run mypy && uv run ruff check`
Expected: 3 passed, clean

- [ ] **Step 5: Commit**

```bash
git add src/otlp_client/model/metrics.py tests/test_model_metrics.py
git commit -m "feat: metrics data model and construction helpers"
```

---

### Task 3: Signal kinds, errors, and configuration

**Files:**
- Create: `src/otlp_client/signals.py`, `src/otlp_client/errors.py`, `src/otlp_client/config.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Consumes: `Resource` from `model/common.py`
- Produces: `SignalKind` (StrEnum: `METRICS/LOGS/TRACES/PROFILES`) and `http_path(kind) -> str`; `OTLPError`, `OTLPTransportError`, `OTLPPermanentError`, `OTLPConfigError`; `OTLPProtocol` (StrEnum `HTTP_JSON="http/json"`, `HTTP_PROTOBUF="http/protobuf"`, `GRPC="grpc"`); `Compression` (StrEnum `NONE`, `GZIP`); `OTLPConfig` frozen dataclass with `endpoint_for(kind) -> str`

**Note on the profiles seam:** `SignalKind.PROFILES` exists and carries its `/v1development/profiles` path from day one. No encoder implements it; the JSON encoder raises `NotImplementedError` for it in Task 5. This is the seam the spec calls for — do not remove it.

- [ ] **Step 1: Write the failing test**

Create `tests/test_config.py`:

```python
import pytest
from otlp_client.config import Compression, OTLPConfig, OTLPProtocol
from otlp_client.errors import OTLPConfigError
from otlp_client.signals import SignalKind, http_path


def test_http_paths_cover_every_signal():
    assert http_path(SignalKind.METRICS) == "/v1/metrics"
    assert http_path(SignalKind.LOGS) == "/v1/logs"
    assert http_path(SignalKind.TRACES) == "/v1/traces"
    assert http_path(SignalKind.PROFILES) == "/v1development/profiles"


def test_base_endpoint_gets_signal_path_appended():
    cfg = OTLPConfig(endpoint="https://collector.local:4318")
    assert cfg.endpoint_for(SignalKind.METRICS) == "https://collector.local:4318/v1/metrics"


def test_trailing_slash_on_base_endpoint_does_not_double_up():
    cfg = OTLPConfig(endpoint="https://collector.local:4318/")
    assert cfg.endpoint_for(SignalKind.LOGS) == "https://collector.local:4318/v1/logs"


def test_per_signal_endpoint_is_used_verbatim():
    cfg = OTLPConfig(
        endpoint="https://collector.local:4318",
        metrics_endpoint="https://elsewhere.example/ingest",
    )
    assert cfg.endpoint_for(SignalKind.METRICS) == "https://elsewhere.example/ingest"
    assert cfg.endpoint_for(SignalKind.TRACES) == "https://collector.local:4318/v1/traces"


def test_defaults_match_spec():
    cfg = OTLPConfig(endpoint="http://localhost:4318")
    assert cfg.protocol is OTLPProtocol.HTTP_JSON
    assert cfg.compression is Compression.NONE
    assert cfg.timeout == 10.0
    assert cfg.gzip_threshold == 32 * 1024
    assert (cfg.initial_backoff, cfg.max_backoff) == (1.0, 30.0)
    assert (cfg.backoff_multiplier, cfg.max_elapsed) == (1.5, 90.0)


def test_empty_endpoint_is_rejected():
    with pytest.raises(OTLPConfigError, match="endpoint"):
        OTLPConfig(endpoint="")


def test_non_positive_timeout_is_rejected():
    with pytest.raises(OTLPConfigError, match="timeout"):
        OTLPConfig(endpoint="http://localhost:4318", timeout=0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_config.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'otlp_client.config'`

- [ ] **Step 3: Write signals and errors**

Create `src/otlp_client/signals.py`:

```python
"""Signal identity and the OTLP/HTTP path table."""

from __future__ import annotations

from enum import StrEnum


class SignalKind(StrEnum):
    METRICS = "metrics"
    LOGS = "logs"
    TRACES = "traces"
    PROFILES = "profiles"


_PATHS: dict[SignalKind, str] = {
    SignalKind.METRICS: "/v1/metrics",
    SignalKind.LOGS: "/v1/logs",
    SignalKind.TRACES: "/v1/traces",
    SignalKind.PROFILES: "/v1development/profiles",
}


def http_path(kind: SignalKind) -> str:
    """Return the OTLP/HTTP path for a signal."""
    return _PATHS[kind]
```

Create `src/otlp_client/errors.py`:

```python
"""Exception hierarchy."""

from __future__ import annotations


class OTLPError(Exception):
    """Base for every error raised by this library."""


class OTLPConfigError(OTLPError):
    """Invalid configuration, or a missing optional extra."""


class OTLPTransportError(OTLPError):
    """The export could not be delivered after exhausting retries."""


class OTLPPermanentError(OTLPError):
    """The collector rejected the export and retrying cannot help."""
```

- [ ] **Step 4: Write the config module**

Create `src/otlp_client/config.py`:

```python
"""Client configuration."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType

from otlp_client.errors import OTLPConfigError
from otlp_client.model.common import Resource
from otlp_client.signals import SignalKind, http_path

_NO_HEADERS: Mapping[str, str] = MappingProxyType({})


class OTLPProtocol(StrEnum):
    HTTP_JSON = "http/json"
    HTTP_PROTOBUF = "http/protobuf"
    GRPC = "grpc"


class Compression(StrEnum):
    NONE = "none"
    GZIP = "gzip"


@dataclass(frozen=True, slots=True)
class OTLPConfig:
    """Every knob the client reads. The only source of settings."""

    endpoint: str
    protocol: OTLPProtocol = OTLPProtocol.HTTP_JSON
    headers: Mapping[str, str] = field(default=_NO_HEADERS)
    timeout: float = 10.0
    compression: Compression = Compression.NONE
    gzip_threshold: int = 32 * 1024
    resource: Resource | None = None

    metrics_endpoint: str | None = None
    logs_endpoint: str | None = None
    traces_endpoint: str | None = None

    certificate_file: str | None = None
    client_certificate_file: str | None = None
    client_key_file: str | None = None
    insecure_skip_verify: bool = False

    initial_backoff: float = 1.0
    max_backoff: float = 30.0
    backoff_multiplier: float = 1.5
    max_elapsed: float = 90.0

    def __post_init__(self) -> None:
        if not self.endpoint:
            raise OTLPConfigError("endpoint must be a non-empty URL")
        if self.timeout <= 0:
            raise OTLPConfigError("timeout must be greater than zero")
        if self.max_elapsed <= 0:
            raise OTLPConfigError("max_elapsed must be greater than zero")

    def endpoint_for(self, kind: SignalKind) -> str:
        """Resolve the URL for a signal.

        A per-signal endpoint is used verbatim; the base endpoint gets the
        signal path appended. This asymmetry is required by the OTLP spec.
        """
        override = {
            SignalKind.METRICS: self.metrics_endpoint,
            SignalKind.LOGS: self.logs_endpoint,
            SignalKind.TRACES: self.traces_endpoint,
            SignalKind.PROFILES: None,
        }[kind]
        if override:
            return override
        return self.endpoint.rstrip("/") + http_path(kind)
```

- [ ] **Step 5: Run tests and lint**

Run: `uv run pytest tests/test_config.py -v && uv run mypy && uv run ruff check`
Expected: 7 passed, clean

- [ ] **Step 6: Commit**

```bash
git add src/otlp_client/signals.py src/otlp_client/errors.py src/otlp_client/config.py tests/test_config.py
git commit -m "feat: signal kinds, error hierarchy, and OTLPConfig"
```

---

### Task 4: Environment variable configuration

**Files:**
- Modify: `src/otlp_client/config.py` (add `from_env` classmethod)
- Test: `tests/test_config_from_env.py`

**Interfaces:**
- Consumes: `OTLPConfig` from Task 3
- Produces: `OTLPConfig.from_env(env: Mapping[str, str] | None = None) -> OTLPConfig`

**Why `env` is a parameter:** tests must not mutate `os.environ`, and injecting the mapping keeps `from_env` a pure function. It defaults to `os.environ` for real callers.

- [ ] **Step 1: Write the failing test**

Create `tests/test_config_from_env.py`:

```python
import pytest
from otlp_client.config import Compression, OTLPConfig, OTLPProtocol
from otlp_client.errors import OTLPConfigError
from otlp_client.signals import SignalKind


def test_reads_base_endpoint_and_protocol():
    cfg = OTLPConfig.from_env({
        "OTEL_EXPORTER_OTLP_ENDPOINT": "https://collector.local:4318",
        "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
    })
    assert cfg.endpoint == "https://collector.local:4318"
    assert cfg.protocol is OTLPProtocol.HTTP_PROTOBUF


def test_per_signal_endpoint_override():
    cfg = OTLPConfig.from_env({
        "OTEL_EXPORTER_OTLP_ENDPOINT": "https://base.local:4318",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "https://metrics.local/ingest",
    })
    assert cfg.endpoint_for(SignalKind.METRICS) == "https://metrics.local/ingest"
    assert cfg.endpoint_for(SignalKind.LOGS) == "https://base.local:4318/v1/logs"


def test_headers_are_comma_separated_key_value_pairs():
    cfg = OTLPConfig.from_env({
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
        "OTEL_EXPORTER_OTLP_HEADERS": "api-key=secret,x-tenant=home",
    })
    assert cfg.headers == {"api-key": "secret", "x-tenant": "home"}


def test_header_values_are_url_decoded_and_whitespace_stripped():
    cfg = OTLPConfig.from_env({
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
        "OTEL_EXPORTER_OTLP_HEADERS": " authorization = Bearer%20abc ",
    })
    assert cfg.headers == {"authorization": "Bearer abc"}


def test_timeout_env_var_is_milliseconds():
    cfg = OTLPConfig.from_env({
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
        "OTEL_EXPORTER_OTLP_TIMEOUT": "2500",
    })
    assert cfg.timeout == 2.5


def test_compression_and_certificate():
    cfg = OTLPConfig.from_env({
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
        "OTEL_EXPORTER_OTLP_COMPRESSION": "gzip",
        "OTEL_EXPORTER_OTLP_CERTIFICATE": "/etc/ssl/ca.pem",
    })
    assert cfg.compression is Compression.GZIP
    assert cfg.certificate_file == "/etc/ssl/ca.pem"


def test_missing_endpoint_defaults_to_localhost():
    assert OTLPConfig.from_env({}).endpoint == "http://localhost:4318"


def test_unknown_protocol_is_rejected():
    with pytest.raises(OTLPConfigError, match="protocol"):
        OTLPConfig.from_env({"OTEL_EXPORTER_OTLP_PROTOCOL": "carrier-pigeon"})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_config_from_env.py -v`
Expected: FAIL with `AttributeError: type object 'OTLPConfig' has no attribute 'from_env'`

- [ ] **Step 3: Implement `from_env`**

Add to the imports at the top of `src/otlp_client/config.py`:

```python
import os
from urllib.parse import unquote
```

Add this method to `OTLPConfig`, immediately before `endpoint_for`:

```python
    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> OTLPConfig:
        """Build a config from the standard OTEL_EXPORTER_OTLP_* variables.

        Never called implicitly. Callers opt in so that ambient environment
        cannot silently change client behaviour.
        """
        src = os.environ if env is None else env

        raw_protocol = src.get("OTEL_EXPORTER_OTLP_PROTOCOL", OTLPProtocol.HTTP_JSON.value)
        try:
            protocol = OTLPProtocol(raw_protocol)
        except ValueError as exc:
            raise OTLPConfigError(f"unknown protocol {raw_protocol!r}") from exc

        raw_compression = src.get("OTEL_EXPORTER_OTLP_COMPRESSION", Compression.NONE.value)
        try:
            compression = Compression(raw_compression)
        except ValueError as exc:
            raise OTLPConfigError(f"unknown compression {raw_compression!r}") from exc

        timeout_ms = src.get("OTEL_EXPORTER_OTLP_TIMEOUT")
        try:
            timeout = float(timeout_ms) / 1000.0 if timeout_ms else 10.0
        except ValueError as exc:
            raise OTLPConfigError(f"invalid timeout {timeout_ms!r}") from exc

        return cls(
            endpoint=src.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"),
            protocol=protocol,
            headers=_parse_headers(src.get("OTEL_EXPORTER_OTLP_HEADERS", "")),
            timeout=timeout,
            compression=compression,
            metrics_endpoint=src.get("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"),
            logs_endpoint=src.get("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"),
            traces_endpoint=src.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
            certificate_file=src.get("OTEL_EXPORTER_OTLP_CERTIFICATE"),
            client_certificate_file=src.get("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE"),
            client_key_file=src.get("OTEL_EXPORTER_OTLP_CLIENT_KEY"),
        )
```

Add this module-level function at the bottom of `config.py`:

```python
def _parse_headers(raw: str) -> Mapping[str, str]:
    """Parse the OTEL_EXPORTER_OTLP_HEADERS `k=v,k=v` form."""
    headers: dict[str, str] = {}
    for pair in raw.split(","):
        if not pair.strip():
            continue
        key, sep, value = pair.partition("=")
        if not sep:
            raise OTLPConfigError(f"malformed header entry {pair!r}, expected key=value")
        headers[unquote(key.strip())] = unquote(value.strip())
    return MappingProxyType(headers)
```

- [ ] **Step 4: Run tests and lint**

Run: `uv run pytest tests/test_config_from_env.py -v && uv run mypy && uv run ruff check`
Expected: 8 passed, clean

- [ ] **Step 5: Commit**

```bash
git add src/otlp_client/config.py tests/test_config_from_env.py
git commit -m "feat: opt-in OTEL_EXPORTER_OTLP_* environment configuration"
```

---

### Task 5: Export outcomes, encoder protocol, and JSON primitives

**Files:**
- Create: `src/otlp_client/outcomes.py`, `src/otlp_client/encoding/__init__.py`, `src/otlp_client/encoding/base.py`, `src/otlp_client/encoding/primitives.py`
- Test: `tests/test_encoding_primitives.py`

**Interfaces:**
- Consumes: `AnyValue` from `model/common.py`, `SignalKind` from `signals.py`
- Produces: `Success()`, `PartialSuccess(rejected, message)`, `Retryable(status, message, retry_after)`, `Permanent(status, message)`, `type ExportOutcome`; `Encoder` protocol with `content_type: str`, `encode(kind, data) -> bytes`, `decode_response(kind, body) -> PartialSuccess | None`; primitives `u64(int) -> str`, `hex_id(bytes) -> str`, `b64(bytes) -> str`, `encode_any_value(AnyValue) -> dict`, `encode_attributes(Mapping) -> list[dict]`, `omit_empty(dict) -> dict`

- [ ] **Step 1: Write the failing test**

Create `tests/test_encoding_primitives.py`:

```python
from otlp_client.encoding.primitives import (
    b64,
    encode_any_value,
    encode_attributes,
    hex_id,
    omit_empty,
    u64,
)


def test_u64_renders_as_decimal_string():
    assert u64(1700000000000000000) == "1700000000000000000"
    assert u64(0) == "0"


def test_hex_id_is_lowercase_hex_not_base64():
    assert hex_id(bytes.fromhex("0102030405060708")) == "0102030405060708"


def test_other_bytes_use_base64():
    assert b64(b"\x01\x02") == "AQI="


def test_bool_encodes_as_bool_not_int():
    # bool is a subclass of int; checking int first would silently corrupt this.
    assert encode_any_value(True) == {"boolValue": True}
    assert encode_any_value(1) == {"intValue": "1"}


def test_int_value_is_a_string_and_double_is_a_number():
    assert encode_any_value(7) == {"intValue": "7"}
    assert encode_any_value(7.5) == {"doubleValue": 7.5}


def test_string_and_bytes_values():
    assert encode_any_value("hi") == {"stringValue": "hi"}
    assert encode_any_value(b"\x01\x02") == {"bytesValue": "AQI="}


def test_array_and_kvlist_values():
    assert encode_any_value([1, "a"]) == {
        "arrayValue": {"values": [{"intValue": "1"}, {"stringValue": "a"}]}
    }
    assert encode_any_value({"k": 2}) == {
        "kvlistValue": {"values": [{"key": "k", "value": {"intValue": "2"}}]}
    }


def test_encode_attributes_produces_key_value_list():
    assert encode_attributes({"service.name": "hass"}) == [
        {"key": "service.name", "value": {"stringValue": "hass"}}
    ]


def test_encode_attributes_of_empty_mapping_is_empty_list():
    assert encode_attributes({}) == []


def test_omit_empty_drops_none_and_empty_containers_but_keeps_zero():
    assert omit_empty({"a": None, "b": "", "c": [], "d": {}, "e": 0, "f": "x"}) == {
        "e": 0,
        "f": "x",
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_encoding_primitives.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'otlp_client.encoding'`

- [ ] **Step 3: Write the outcome types**

Create `src/otlp_client/outcomes.py`:

```python
"""The result of one export attempt.

Transports classify a response into one of these; `retry.py` decides what to
do about it. Neither knows the other's wire format.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Success:
    """Everything was accepted."""


@dataclass(frozen=True, slots=True)
class PartialSuccess:
    """Some records were rejected. Per the OTLP spec this is never retried."""

    rejected: int
    message: str = ""


@dataclass(frozen=True, slots=True)
class Retryable:
    """A transient failure. Safe to send again after a backoff."""

    status: int | None = None
    message: str = ""
    retry_after: float | None = None


@dataclass(frozen=True, slots=True)
class Permanent:
    """The collector will never accept this payload. Drop it."""

    status: int | None = None
    message: str = ""


type ExportOutcome = Success | PartialSuccess | Retryable | Permanent
```

- [ ] **Step 4: Write the encoder protocol**

Create `src/otlp_client/encoding/__init__.py` (empty file) and `src/otlp_client/encoding/base.py`:

```python
"""The encoder seam."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from otlp_client.outcomes import PartialSuccess
from otlp_client.signals import SignalKind


class Encoder(Protocol):
    """Turns model dataclasses into request bytes and reads responses back."""

    @property
    def content_type(self) -> str:
        """The Content-Type this encoder produces."""

    def encode(self, kind: SignalKind, data: Sequence[Any]) -> bytes:
        """Encode a sequence of Resource-level envelopes into a request body."""

    def decode_response(self, kind: SignalKind, body: bytes) -> PartialSuccess | None:
        """Return a PartialSuccess if the response reports one, else None."""
```

- [ ] **Step 5: Write the primitives**

Create `src/otlp_client/encoding/primitives.py`:

```python
"""OTLP/JSON primitive encoding rules.

These are the rules most hand-written OTLP JSON gets wrong:
64-bit fields are decimal strings, trace and span ids are hex rather than
base64, and enums are integers.
"""

from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from typing import Any

from otlp_client.model.common import AnyValue


def u64(value: int) -> str:
    """Render a 64-bit field as the decimal string the spec requires."""
    return str(value)


def hex_id(value: bytes) -> str:
    """Render a traceId or spanId as hex. These are never base64."""
    return value.hex()


def b64(value: bytes) -> str:
    """Render any other bytes field as base64."""
    return base64.b64encode(value).decode("ascii")


def encode_any_value(value: AnyValue) -> dict[str, Any]:
    """Encode one AnyValue.

    `bool` is checked before `int` deliberately: `bool` is a subclass of `int`,
    so the reverse order would encode `True` as `{"intValue": "1"}`.
    """
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": u64(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, str):
        return {"stringValue": value}
    if isinstance(value, bytes):
        return {"bytesValue": b64(value)}
    if isinstance(value, Mapping):
        return {"kvlistValue": {"values": encode_attributes(value)}}
    if isinstance(value, Sequence):
        return {"arrayValue": {"values": [encode_any_value(v) for v in value]}}
    raise TypeError(f"unsupported attribute value type: {type(value)!r}")


def encode_attributes(attributes: Mapping[str, AnyValue]) -> list[dict[str, Any]]:
    """Encode an attribute mapping as an OTLP KeyValue list."""
    return [{"key": key, "value": encode_any_value(value)} for key, value in attributes.items()]


def omit_empty(obj: dict[str, Any]) -> dict[str, Any]:
    """Drop absent fields to keep payloads small.

    Removes None, empty strings, and empty containers. Zero and False are kept
    so that an explicitly-zero measurement is never confused with an absent one.
    """
    return {k: v for k, v in obj.items() if v is not None and v != "" and v != [] and v != {}}
```

- [ ] **Step 6: Run tests and lint**

Run: `uv run pytest tests/test_encoding_primitives.py -v && uv run mypy && uv run ruff check`
Expected: 10 passed, clean

- [ ] **Step 7: Commit**

```bash
git add src/otlp_client/outcomes.py src/otlp_client/encoding tests/test_encoding_primitives.py
git commit -m "feat: export outcomes, encoder protocol, and JSON primitives"
```

---

### Task 6: JSON encoder for metrics

**Files:**
- Create: `src/otlp_client/encoding/json.py`
- Test: `tests/test_encoding_json_metrics.py`

**Interfaces:**
- Consumes: primitives from Task 5, metrics model from Task 2, `SignalKind`, `PartialSuccess`
- Produces: `JSONEncoder()` implementing `Encoder`; `content_type == "application/json"`; `encode(SignalKind.METRICS, Sequence[ResourceMetrics]) -> bytes`; `decode_response(kind, body) -> PartialSuccess | None`; raises `NotImplementedError` for `SignalKind.PROFILES`

- [ ] **Step 1: Write the failing test**

Create `tests/test_encoding_json_metrics.py`:

```python
import json

import pytest
from otlp_client.encoding.json import JSONEncoder
from otlp_client.model.common import InstrumentationScope, Resource
from otlp_client.model.metrics import (
    AggregationTemporality,
    Histogram,
    HistogramDataPoint,
    Metric,
    ResourceMetrics,
    ScopeMetrics,
    gauge,
    sum_,
)
from otlp_client.signals import SignalKind


def encode_one(metric):
    payload = JSONEncoder().encode(
        SignalKind.METRICS,
        [ResourceMetrics(
            resource=Resource(attributes={"service.name": "hass"}),
            scope_metrics=[ScopeMetrics(
                scope=InstrumentationScope(name="otlp_client", version="0.1.0"),
                metrics=[metric],
            )],
        )],
    )
    return json.loads(payload)


def test_envelope_shape_and_resource_attributes():
    doc = encode_one(gauge("t", 21.5, time_unix_nano=1700000000000000000))
    (rm,) = doc["resourceMetrics"]
    assert rm["resource"]["attributes"] == [
        {"key": "service.name", "value": {"stringValue": "hass"}}
    ]
    (sm,) = rm["scopeMetrics"]
    assert sm["scope"] == {"name": "otlp_client", "version": "0.1.0"}


def test_gauge_double_point_uses_asDouble_and_string_timestamp():
    doc = encode_one(gauge("t", 21.5, unit="Cel", time_unix_nano=1700000000000000000))
    metric = doc["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]
    assert metric["name"] == "t"
    assert metric["unit"] == "Cel"
    (point,) = metric["gauge"]["dataPoints"]
    assert point["asDouble"] == 21.5
    assert point["timeUnixNano"] == "1700000000000000000"
    assert isinstance(point["timeUnixNano"], str)


def test_integer_point_uses_asInt_as_a_string():
    doc = encode_one(sum_("e", 42, time_unix_nano=5))
    (point,) = doc["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]["sum"]["dataPoints"]
    assert point["asInt"] == "42"


def test_sum_enum_is_an_integer_not_a_name():
    doc = encode_one(sum_("e", 1, time_unix_nano=5))
    data = doc["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]["sum"]
    assert data["aggregationTemporality"] == 2
    assert data["isMonotonic"] is True


def test_histogram_count_and_bucket_counts_are_strings():
    point = HistogramDataPoint(
        time_unix_nano=9, count=3, sum=6.0,
        bucket_counts=[1, 2], explicit_bounds=[10.0],
    )
    doc = encode_one(Metric(name="h", data=Histogram(
        data_points=[point], aggregation_temporality=AggregationTemporality.DELTA)))
    hist = doc["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]["histogram"]
    assert hist["aggregationTemporality"] == 1
    (p,) = hist["dataPoints"]
    assert p["count"] == "3"
    assert p["bucketCounts"] == ["1", "2"]
    assert p["explicitBounds"] == [10.0]
    assert p["sum"] == 6.0


def test_empty_fields_are_omitted():
    doc = encode_one(gauge("t", 1.0, time_unix_nano=1))
    metric = doc["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]
    assert "description" not in metric
    assert "unit" not in metric
    point = metric["gauge"]["dataPoints"][0]
    assert "attributes" not in point
    assert "startTimeUnixNano" not in point


def test_content_type():
    assert JSONEncoder().content_type == "application/json"


def test_decode_partial_success():
    body = json.dumps(
        {"partialSuccess": {"rejectedDataPoints": "7", "errorMessage": "bad unit"}}
    ).encode()
    result = JSONEncoder().decode_response(SignalKind.METRICS, body)
    assert result is not None
    assert result.rejected == 7
    assert result.message == "bad unit"


def test_decode_full_success_returns_none():
    assert JSONEncoder().decode_response(SignalKind.METRICS, b"{}") is None
    assert JSONEncoder().decode_response(SignalKind.METRICS, b"") is None


def test_profiles_is_a_defined_seam_that_is_not_yet_implemented():
    with pytest.raises(NotImplementedError, match="profiles"):
        JSONEncoder().encode(SignalKind.PROFILES, [])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_encoding_json_metrics.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'otlp_client.encoding.json'`

- [ ] **Step 3: Write the encoder**

Create `src/otlp_client/encoding/json.py`:

```python
"""OTLP/JSON encoding. The core encoder: pure Python, no dependencies."""

from __future__ import annotations

import json as _stdlib_json
from collections.abc import Sequence
from typing import Any

from otlp_client.encoding.primitives import encode_attributes, omit_empty, u64
from otlp_client.model.common import InstrumentationScope, Resource
from otlp_client.model.metrics import (
    Gauge,
    Histogram,
    HistogramDataPoint,
    Metric,
    NumberDataPoint,
    ResourceMetrics,
    ScopeMetrics,
    Sum,
)
from otlp_client.outcomes import PartialSuccess
from otlp_client.signals import SignalKind

try:  # pragma: no cover - exercised by whichever path is installed
    import orjson

    def _dumps(doc: dict[str, Any]) -> bytes:
        """Serialize with orjson when available.

        orjson is never a declared dependency; Home Assistant already ships it,
        so this is a free speedup there and a no-op everywhere else.
        """
        return orjson.dumps(doc)

except ImportError:  # pragma: no cover

    def _dumps(doc: dict[str, Any]) -> bytes:
        return _stdlib_json.dumps(doc, separators=(",", ":")).encode("utf-8")


def _encode_resource(resource: Resource) -> dict[str, Any]:
    return omit_empty(
        {
            "attributes": encode_attributes(resource.attributes),
            "droppedAttributesCount": resource.dropped_attributes_count or None,
        }
    )


def _encode_scope(scope: InstrumentationScope) -> dict[str, Any]:
    return omit_empty(
        {
            "name": scope.name,
            "version": scope.version,
            "attributes": encode_attributes(scope.attributes),
        }
    )


def _encode_number_point(point: NumberDataPoint) -> dict[str, Any]:
    # bool before int: bool subclasses int, and a bool is not a valid metric value.
    if isinstance(point.value, bool):
        raise TypeError("metric data point values must be int or float, not bool")
    value_field = (
        {"asInt": u64(point.value)}
        if isinstance(point.value, int)
        else {"asDouble": point.value}
    )
    return omit_empty(
        {
            "attributes": encode_attributes(point.attributes),
            "startTimeUnixNano": u64(point.start_time_unix_nano)
            if point.start_time_unix_nano is not None
            else None,
            "timeUnixNano": u64(point.time_unix_nano),
            **value_field,
        }
    )


def _encode_histogram_point(point: HistogramDataPoint) -> dict[str, Any]:
    return omit_empty(
        {
            "attributes": encode_attributes(point.attributes),
            "startTimeUnixNano": u64(point.start_time_unix_nano)
            if point.start_time_unix_nano is not None
            else None,
            "timeUnixNano": u64(point.time_unix_nano),
            "count": u64(point.count),
            "sum": point.sum,
            "bucketCounts": [u64(c) for c in point.bucket_counts],
            "explicitBounds": list(point.explicit_bounds),
        }
    )


def _encode_metric(metric: Metric) -> dict[str, Any]:
    data = metric.data
    if isinstance(data, Gauge):
        body = {"gauge": {"dataPoints": [_encode_number_point(p) for p in data.data_points]}}
    elif isinstance(data, Sum):
        body = {
            "sum": {
                "dataPoints": [_encode_number_point(p) for p in data.data_points],
                "aggregationTemporality": int(data.aggregation_temporality),
                "isMonotonic": data.is_monotonic,
            }
        }
    elif isinstance(data, Histogram):
        body = {
            "histogram": {
                "dataPoints": [_encode_histogram_point(p) for p in data.data_points],
                "aggregationTemporality": int(data.aggregation_temporality),
            }
        }
    else:  # pragma: no cover - exhaustive over MetricData
        raise TypeError(f"unsupported metric data type: {type(data)!r}")

    return omit_empty(
        {"name": metric.name, "description": metric.description, "unit": metric.unit, **body}
    )


def _encode_scope_metrics(scope_metrics: ScopeMetrics) -> dict[str, Any]:
    return omit_empty(
        {
            "scope": _encode_scope(scope_metrics.scope),
            "metrics": [_encode_metric(m) for m in scope_metrics.metrics],
        }
    )


def _encode_resource_metrics(data: Sequence[ResourceMetrics]) -> dict[str, Any]:
    return {
        "resourceMetrics": [
            omit_empty(
                {
                    "resource": _encode_resource(rm.resource),
                    "scopeMetrics": [_encode_scope_metrics(sm) for sm in rm.scope_metrics],
                }
            )
            for rm in data
        ]
    }


class JSONEncoder:
    """Encodes the model tree as OTLP/JSON."""

    @property
    def content_type(self) -> str:
        return "application/json"

    def encode(self, kind: SignalKind, data: Sequence[Any]) -> bytes:
        if kind is SignalKind.METRICS:
            return _dumps(_encode_resource_metrics(data))
        if kind is SignalKind.PROFILES:
            raise NotImplementedError(
                "the profiles signal is still in development and is not encoded yet"
            )
        raise NotImplementedError(f"no encoder registered for {kind}")

    def decode_response(self, kind: SignalKind, body: bytes) -> PartialSuccess | None:
        """Read a partialSuccess block if the collector reported one."""
        if not body:
            return None
        try:
            doc = _stdlib_json.loads(body)
        except ValueError:
            return None
        if not isinstance(doc, dict):
            return None
        partial = doc.get("partialSuccess")
        if not isinstance(partial, dict) or not partial:
            return None
        rejected_key = {
            SignalKind.METRICS: "rejectedDataPoints",
            SignalKind.LOGS: "rejectedLogRecords",
            SignalKind.TRACES: "rejectedSpans",
            SignalKind.PROFILES: "rejectedProfiles",
        }[kind]
        return PartialSuccess(
            rejected=int(partial.get(rejected_key, 0)),
            message=str(partial.get("errorMessage", "")),
        )
```

- [ ] **Step 4: Run tests and lint**

Run: `uv run pytest tests/test_encoding_json_metrics.py -v && uv run mypy && uv run ruff check`
Expected: 10 passed, clean

- [ ] **Step 5: Commit**

```bash
git add src/otlp_client/encoding/json.py tests/test_encoding_json_metrics.py
git commit -m "feat: OTLP/JSON encoder for metrics"
```

---

### Task 7: Transport protocol and test doubles

**Files:**
- Create: `src/otlp_client/transport/__init__.py`, `src/otlp_client/transport/base.py`, `tests/__init__.py`, `tests/support/__init__.py`, `tests/support/fakes.py`
- Test: `tests/test_fakes.py`

**Interfaces:**
- Consumes: `SignalKind`, `ExportOutcome` types
- Produces: `Transport` protocol with `async send(kind, payload: bytes) -> ExportOutcome` and `async aclose() -> None`; `FakeTransport(outcomes=...)` recording `.sent: list[tuple[SignalKind, bytes]]` with `.closed: bool`; `FakeClock(start=0.0)` with `.monotonic() -> float`, `async .sleep(seconds)`, `.slept: list[float]`

**Design note:** the client encodes and hands the transport bytes; the transport holds a reference to the same `Encoder` purely to read `content_type` and to call `decode_response`. This keeps gzip and TLS concerns inside the transport while the encoder stays free of network knowledge.

- [ ] **Step 1: Write the failing test**

Create `tests/test_fakes.py`:

```python
from otlp_client.outcomes import Permanent, Success
from otlp_client.signals import SignalKind
from tests.support.fakes import FakeClock, FakeTransport


async def test_fake_transport_records_and_replays_outcomes():
    transport = FakeTransport(outcomes=[Success(), Permanent(status=400, message="nope")])
    assert isinstance(await transport.send(SignalKind.METRICS, b"one"), Success)
    second = await transport.send(SignalKind.LOGS, b"two")
    assert isinstance(second, Permanent)
    assert transport.sent == [(SignalKind.METRICS, b"one"), (SignalKind.LOGS, b"two")]


async def test_fake_transport_repeats_its_last_outcome():
    transport = FakeTransport(outcomes=[Success()])
    await transport.send(SignalKind.METRICS, b"a")
    assert isinstance(await transport.send(SignalKind.METRICS, b"b"), Success)


async def test_fake_transport_close():
    transport = FakeTransport()
    await transport.aclose()
    assert transport.closed is True


async def test_fake_clock_advances_only_when_slept():
    clock = FakeClock()
    assert clock.monotonic() == 0.0
    await clock.sleep(2.5)
    await clock.sleep(1.0)
    assert clock.monotonic() == 3.5
    assert clock.slept == [2.5, 1.0]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_fakes.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'tests.support'`

- [ ] **Step 3: Write the transport protocol**

Create `src/otlp_client/transport/__init__.py` (empty) and `src/otlp_client/transport/base.py`:

```python
"""The transport seam."""

from __future__ import annotations

from typing import Protocol

from otlp_client.outcomes import ExportOutcome
from otlp_client.signals import SignalKind


class Transport(Protocol):
    """Ships already-encoded bytes and classifies the response."""

    async def send(self, kind: SignalKind, payload: bytes) -> ExportOutcome:
        """Deliver one encoded batch. Never retries; classification only."""

    async def aclose(self) -> None:
        """Release any resources this transport owns."""
```

- [ ] **Step 4: Write the fakes**

Create `tests/__init__.py` and `tests/support/__init__.py` (both empty) and `tests/support/fakes.py`:

```python
"""Test doubles shared across the suite."""

from __future__ import annotations

from collections.abc import Sequence

from otlp_client.outcomes import ExportOutcome, Success
from otlp_client.signals import SignalKind


class FakeTransport:
    """An in-memory Transport. Records what was sent, replays scripted outcomes.

    Once the script is exhausted the last outcome repeats, so a test that only
    cares about a steady state does not have to enumerate every attempt.
    """

    def __init__(self, outcomes: Sequence[ExportOutcome] | None = None) -> None:
        self._outcomes = list(outcomes) if outcomes else [Success()]
        self._index = 0
        self.sent: list[tuple[SignalKind, bytes]] = []
        self.closed = False

    async def send(self, kind: SignalKind, payload: bytes) -> ExportOutcome:
        self.sent.append((kind, payload))
        outcome = self._outcomes[min(self._index, len(self._outcomes) - 1)]
        self._index += 1
        return outcome

    async def aclose(self) -> None:
        self.closed = True


class FakeClock:
    """A monotonic clock that only moves when something sleeps on it.

    Lets retry and flush schedules be asserted exactly, with no wall-clock wait.
    """

    def __init__(self, start: float = 0.0) -> None:
        self._now = start
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self._now

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self._now += seconds
```

- [ ] **Step 5: Run tests and lint**

Run: `uv run pytest tests/test_fakes.py -v && uv run mypy && uv run ruff check`
Expected: 4 passed, clean

- [ ] **Step 6: Commit**

```bash
git add src/otlp_client/transport tests/support tests/test_fakes.py
git commit -m "feat: transport protocol and shared test doubles"
```

---

### Task 8: Retry engine

**Files:**
- Create: `src/otlp_client/retry.py`
- Test: `tests/test_retry.py`

**Interfaces:**
- Consumes: outcomes from Task 5, `OTLPConfig` from Task 3, `FakeClock` from Task 7
- Produces: `RetryPolicy(initial_backoff, max_backoff, multiplier, max_elapsed)` with `RetryPolicy.from_config(cfg) -> RetryPolicy`; `parse_retry_after(value: str, *, now_wall: float) -> float | None`; `async with_retry(op, policy, *, sleep, monotonic, jitter) -> ExportOutcome`

**Semantics:** `with_retry` calls `op()` repeatedly while it returns `Retryable` and the elapsed time stays under `max_elapsed`. It returns the first non-`Retryable` outcome, or the last `Retryable` when the budget is exhausted (the client converts that into `OTLPTransportError`). `PartialSuccess` is returned immediately and never retried.

- [ ] **Step 1: Write the failing test**

Create `tests/test_retry.py`:

```python
from datetime import UTC, datetime

import pytest
from otlp_client.config import OTLPConfig
from otlp_client.outcomes import ExportOutcome, PartialSuccess, Permanent, Retryable, Success
from otlp_client.retry import RetryPolicy, parse_retry_after, with_retry
from tests.support.fakes import FakeClock

POLICY = RetryPolicy(initial_backoff=1.0, max_backoff=30.0, multiplier=2.0, max_elapsed=90.0)


def scripted(*outcomes: ExportOutcome):
    """An op that returns each outcome in turn, repeating the last."""
    calls: list[int] = []

    async def op() -> ExportOutcome:
        result = outcomes[min(len(calls), len(outcomes) - 1)]
        calls.append(1)
        return result

    return op, calls


async def test_success_on_first_attempt_does_not_sleep():
    clock = FakeClock()
    op, calls = scripted(Success())
    result = await with_retry(op, POLICY, sleep=clock.sleep, monotonic=clock.monotonic,
                              jitter=lambda: 1.0)
    assert isinstance(result, Success)
    assert len(calls) == 1
    assert clock.slept == []


async def test_retries_until_success_with_exponential_backoff():
    clock = FakeClock()
    op, calls = scripted(Retryable(status=503), Retryable(status=503), Success())
    result = await with_retry(op, POLICY, sleep=clock.sleep, monotonic=clock.monotonic,
                              jitter=lambda: 1.0)
    assert isinstance(result, Success)
    assert len(calls) == 3
    assert clock.slept == [1.0, 2.0]


async def test_full_jitter_scales_each_delay():
    clock = FakeClock()
    op, _ = scripted(Retryable(), Retryable(), Success())
    await with_retry(op, POLICY, sleep=clock.sleep, monotonic=clock.monotonic,
                     jitter=lambda: 0.5)
    assert clock.slept == [0.5, 1.0]


async def test_backoff_is_capped_at_max_backoff():
    clock = FakeClock()
    policy = RetryPolicy(initial_backoff=10.0, max_backoff=15.0, multiplier=10.0,
                         max_elapsed=1000.0)
    op, _ = scripted(Retryable(), Retryable(), Retryable(), Success())
    await with_retry(op, policy, sleep=clock.sleep, monotonic=clock.monotonic,
                     jitter=lambda: 1.0)
    assert clock.slept == [10.0, 15.0, 15.0]


async def test_retry_after_overrides_computed_backoff():
    clock = FakeClock()
    op, _ = scripted(Retryable(status=429, retry_after=7.0), Success())
    await with_retry(op, POLICY, sleep=clock.sleep, monotonic=clock.monotonic,
                     jitter=lambda: 1.0)
    assert clock.slept == [7.0]


async def test_permanent_is_returned_immediately():
    clock = FakeClock()
    op, calls = scripted(Permanent(status=400, message="bad"))
    result = await with_retry(op, POLICY, sleep=clock.sleep, monotonic=clock.monotonic,
                              jitter=lambda: 1.0)
    assert isinstance(result, Permanent)
    assert len(calls) == 1


async def test_partial_success_is_never_retried():
    clock = FakeClock()
    op, calls = scripted(PartialSuccess(rejected=3), Success())
    result = await with_retry(op, POLICY, sleep=clock.sleep, monotonic=clock.monotonic,
                              jitter=lambda: 1.0)
    assert isinstance(result, PartialSuccess)
    assert len(calls) == 1
    assert clock.slept == []


async def test_budget_exhaustion_returns_the_last_retryable():
    clock = FakeClock()
    policy = RetryPolicy(initial_backoff=1.0, max_backoff=1.0, multiplier=1.0, max_elapsed=3.0)
    op, calls = scripted(Retryable(status=503, message="down"))
    result = await with_retry(op, policy, sleep=clock.sleep, monotonic=clock.monotonic,
                              jitter=lambda: 1.0)
    assert isinstance(result, Retryable)
    assert result.message == "down"
    assert clock.monotonic() <= 3.0 + 1.0
    assert len(calls) >= 2


def test_parse_retry_after_delay_seconds():
    assert parse_retry_after("120", now_wall=0.0) == 120.0


def test_parse_retry_after_http_date():
    when = datetime(2026, 9, 4, 12, 0, 30, tzinfo=UTC)
    now = datetime(2026, 9, 4, 12, 0, 0, tzinfo=UTC).timestamp()
    header = "Fri, 04 Sep 2026 12:00:30 GMT"
    assert parse_retry_after(header, now_wall=now) == pytest.approx(30.0, abs=1.0)
    assert when.timestamp() > now


def test_parse_retry_after_past_date_clamps_to_zero():
    now = datetime(2026, 9, 4, 12, 0, 0, tzinfo=UTC).timestamp()
    assert parse_retry_after("Fri, 04 Sep 2026 11:59:00 GMT", now_wall=now) == 0.0


def test_parse_retry_after_garbage_returns_none():
    assert parse_retry_after("soon please", now_wall=0.0) is None


def test_policy_from_config_uses_config_values():
    cfg = OTLPConfig(endpoint="http://localhost:4318", initial_backoff=2.0, max_backoff=8.0,
                     backoff_multiplier=3.0, max_elapsed=40.0)
    policy = RetryPolicy.from_config(cfg)
    assert (policy.initial_backoff, policy.max_backoff) == (2.0, 8.0)
    assert (policy.multiplier, policy.max_elapsed) == (3.0, 40.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_retry.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'otlp_client.retry'`

- [ ] **Step 3: Write the retry engine**

Create `src/otlp_client/retry.py`:

```python
"""Retry policy and the retry loop.

Retryable statuses are exactly 429, 502, 503 and 504 plus connection errors and
timeouts; transports classify those. Everything else is permanent. Partial
success is never retried, per the OTLP spec.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from email.utils import parsedate_to_datetime

from otlp_client.config import OTLPConfig
from otlp_client.outcomes import ExportOutcome, Retryable

RETRYABLE_STATUSES = frozenset({429, 502, 503, 504})


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    initial_backoff: float = 1.0
    max_backoff: float = 30.0
    multiplier: float = 1.5
    max_elapsed: float = 90.0

    @classmethod
    def from_config(cls, config: OTLPConfig) -> RetryPolicy:
        return cls(
            initial_backoff=config.initial_backoff,
            max_backoff=config.max_backoff,
            multiplier=config.backoff_multiplier,
            max_elapsed=config.max_elapsed,
        )


def parse_retry_after(value: str, *, now_wall: float) -> float | None:
    """Parse a Retry-After header in either delay-seconds or HTTP-date form.

    Returns seconds to wait, never negative, or None if unparseable.
    """
    text = value.strip()
    if not text:
        return None
    try:
        return max(0.0, float(text))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    return max(0.0, when.timestamp() - now_wall)


async def with_retry(
    op: Callable[[], Awaitable[ExportOutcome]],
    policy: RetryPolicy,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    jitter: Callable[[], float] = random.random,
) -> ExportOutcome:
    """Run `op` until it succeeds, fails permanently, or the budget runs out.

    Uses full jitter: each delay is `jitter() * capped_backoff`. Cancellation
    during a backoff sleep propagates rather than being swallowed.
    """
    started = monotonic()
    attempt = 0
    while True:
        outcome = await op()
        if not isinstance(outcome, Retryable):
            return outcome

        elapsed = monotonic() - started
        if elapsed >= policy.max_elapsed:
            return outcome

        capped = min(policy.max_backoff, policy.initial_backoff * policy.multiplier**attempt)
        delay = outcome.retry_after if outcome.retry_after is not None else jitter() * capped
        remaining = policy.max_elapsed - elapsed
        await sleep(min(delay, remaining))
        attempt += 1
```

- [ ] **Step 4: Run tests and lint**

Run: `uv run pytest tests/test_retry.py -v && uv run mypy && uv run ruff check`
Expected: 13 passed, clean

- [ ] **Step 5: Commit**

```bash
git add src/otlp_client/retry.py tests/test_retry.py
git commit -m "feat: retry engine with full jitter and Retry-After support"
```

---

### Task 9: HTTP transport

**Files:**
- Create: `src/otlp_client/transport/http.py`
- Test: `tests/test_transport_http.py`

**Interfaces:**
- Consumes: `OTLPConfig`, `Encoder`, outcomes, `parse_retry_after`, `RETRYABLE_STATUSES`
- Produces: `HTTPTransport(config, encoder, *, session, owns_session=False, ssl_context=None)`; `async HTTPTransport.create(config, encoder, *, session=None) -> HTTPTransport`; implements `Transport`

**Three constraints from the spec that this task exists to satisfy:**
1. The `ClientSession` is **injected**. Home Assistant forbids integrations from creating their own; `create()` only builds one when the caller passes none, and then sets `owns_session=True` so `aclose()` cleans it up.
2. The `SSLContext` is built **off-loop** inside `asyncio.to_thread`, because loading a CA bundle or client certificate does blocking file I/O.
3. gzip above `config.gzip_threshold` is compressed **off-loop** so a large flush cannot stall the event loop.

- [ ] **Step 1: Write the failing test**

Create `tests/test_transport_http.py`:

```python
import gzip
import json

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer
from otlp_client.config import Compression, OTLPConfig
from otlp_client.encoding.json import JSONEncoder
from otlp_client.outcomes import PartialSuccess, Permanent, Retryable, Success
from otlp_client.signals import SignalKind
from otlp_client.transport.http import HTTPTransport


class Recorder:
    def __init__(self, status=200, body=b"{}", headers=None):
        self.status, self.body, self.headers = status, body, headers or {}
        self.requests: list[tuple[str, dict[str, str], bytes]] = []

    async def handle(self, request: web.Request) -> web.Response:
        self.requests.append((request.path, dict(request.headers), await request.read()))
        return web.Response(status=self.status, body=self.body, headers=self.headers)


@pytest.fixture
async def server_factory():
    servers: list[TestServer] = []
    sessions: list[ClientSession] = []

    async def make(recorder: Recorder):
        app = web.Application()
        app.router.add_route("POST", "/{tail:.*}", recorder.handle)
        server = TestServer(app)
        await server.start_server()
        servers.append(server)
        session = ClientSession()
        sessions.append(session)
        return str(server.make_url("")).rstrip("/"), session

    yield make
    for s in sessions:
        await s.close()
    for srv in servers:
        await srv.close()


async def test_posts_to_the_signal_path_with_content_type(server_factory):
    rec = Recorder()
    base, session = await server_factory(rec)
    transport = HTTPTransport(OTLPConfig(endpoint=base), JSONEncoder(), session=session)
    result = await transport.send(SignalKind.METRICS, b'{"resourceMetrics":[]}')
    assert isinstance(result, Success)
    path, headers, body = rec.requests[0]
    assert path == "/v1/metrics"
    assert headers["Content-Type"] == "application/json"
    assert body == b'{"resourceMetrics":[]}'


async def test_custom_headers_are_sent(server_factory):
    rec = Recorder()
    base, session = await server_factory(rec)
    cfg = OTLPConfig(endpoint=base, headers={"api-key": "secret"})
    transport = HTTPTransport(cfg, JSONEncoder(), session=session)
    await transport.send(SignalKind.LOGS, b"{}")
    assert rec.requests[0][1]["api-key"] == "secret"
    assert rec.requests[0][0] == "/v1/logs"


async def test_gzip_sets_content_encoding_and_compresses(server_factory):
    rec = Recorder()
    base, session = await server_factory(rec)
    cfg = OTLPConfig(endpoint=base, compression=Compression.GZIP, gzip_threshold=0)
    transport = HTTPTransport(cfg, JSONEncoder(), session=session)
    await transport.send(SignalKind.METRICS, b'{"a":1}')
    _, headers, body = rec.requests[0]
    assert headers["Content-Encoding"] == "gzip"
    assert gzip.decompress(body) == b'{"a":1}'


async def test_partial_success_is_surfaced(server_factory):
    body = json.dumps({"partialSuccess": {"rejectedDataPoints": "2", "errorMessage": "x"}})
    rec = Recorder(body=body.encode())
    base, session = await server_factory(rec)
    transport = HTTPTransport(OTLPConfig(endpoint=base), JSONEncoder(), session=session)
    result = await transport.send(SignalKind.METRICS, b"{}")
    assert isinstance(result, PartialSuccess)
    assert result.rejected == 2


@pytest.mark.parametrize("status", [429, 502, 503, 504])
async def test_retryable_statuses(server_factory, status):
    rec = Recorder(status=status, body=b"busy")
    base, session = await server_factory(rec)
    transport = HTTPTransport(OTLPConfig(endpoint=base), JSONEncoder(), session=session)
    result = await transport.send(SignalKind.METRICS, b"{}")
    assert isinstance(result, Retryable)
    assert result.status == status


async def test_retry_after_header_is_parsed(server_factory):
    rec = Recorder(status=503, headers={"Retry-After": "12"})
    base, session = await server_factory(rec)
    transport = HTTPTransport(OTLPConfig(endpoint=base), JSONEncoder(), session=session)
    result = await transport.send(SignalKind.METRICS, b"{}")
    assert isinstance(result, Retryable)
    assert result.retry_after == 12.0


@pytest.mark.parametrize("status", [400, 401, 404, 422])
async def test_client_errors_are_permanent(server_factory, status):
    rec = Recorder(status=status, body=b"nope")
    base, session = await server_factory(rec)
    transport = HTTPTransport(OTLPConfig(endpoint=base), JSONEncoder(), session=session)
    result = await transport.send(SignalKind.METRICS, b"{}")
    assert isinstance(result, Permanent)
    assert result.status == status


async def test_connection_failure_is_retryable():
    async with ClientSession() as session:
        cfg = OTLPConfig(endpoint="http://127.0.0.1:1", timeout=1.0)
        transport = HTTPTransport(cfg, JSONEncoder(), session=session)
        result = await transport.send(SignalKind.METRICS, b"{}")
        assert isinstance(result, Retryable)


async def test_injected_session_is_not_closed(server_factory):
    rec = Recorder()
    base, session = await server_factory(rec)
    transport = HTTPTransport(OTLPConfig(endpoint=base), JSONEncoder(), session=session)
    await transport.aclose()
    assert session.closed is False


async def test_created_session_is_owned_and_closed():
    transport = await HTTPTransport.create(
        OTLPConfig(endpoint="http://localhost:4318"), JSONEncoder()
    )
    session = transport._session
    await transport.aclose()
    assert session.closed is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_transport_http.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'otlp_client.transport.http'`

- [ ] **Step 3: Write the transport**

Create `src/otlp_client/transport/http.py`:

```python
"""OTLP/HTTP transport built on aiohttp."""

from __future__ import annotations

import asyncio
import gzip
import ssl
import time

import aiohttp

from otlp_client.config import Compression, OTLPConfig
from otlp_client.encoding.base import Encoder
from otlp_client.outcomes import ExportOutcome, Permanent, Retryable, Success
from otlp_client.retry import RETRYABLE_STATUSES, parse_retry_after
from otlp_client.signals import SignalKind


def _build_ssl_context(config: OTLPConfig) -> ssl.SSLContext | None:
    """Build the TLS context. Blocking: only call via asyncio.to_thread."""
    if not (
        config.certificate_file
        or config.client_certificate_file
        or config.insecure_skip_verify
    ):
        return None
    context = ssl.create_default_context(cafile=config.certificate_file)
    if config.insecure_skip_verify:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    if config.client_certificate_file:
        context.load_cert_chain(config.client_certificate_file, config.client_key_file)
    return context


class HTTPTransport:
    """Ships encoded bytes over OTLP/HTTP and classifies the response."""

    def __init__(
        self,
        config: OTLPConfig,
        encoder: Encoder,
        *,
        session: aiohttp.ClientSession,
        owns_session: bool = False,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self._config = config
        self._encoder = encoder
        self._session = session
        self._owns_session = owns_session
        self._ssl = ssl_context
        self._timeout = aiohttp.ClientTimeout(total=config.timeout)

    @classmethod
    async def create(
        cls,
        config: OTLPConfig,
        encoder: Encoder,
        *,
        session: aiohttp.ClientSession | None = None,
    ) -> HTTPTransport:
        """Build a transport, doing all blocking setup off the event loop.

        Pass `session` whenever one exists. Home Assistant integrations must
        pass `async_get_clientsession(hass)` rather than letting this create one.
        """
        ssl_context = await asyncio.to_thread(_build_ssl_context, config)
        owns = session is None
        return cls(
            config,
            encoder,
            session=session or aiohttp.ClientSession(),
            owns_session=owns,
            ssl_context=ssl_context,
        )

    async def _compress(self, payload: bytes) -> bytes:
        """gzip the payload, off-loop when it is large enough to matter."""
        if len(payload) >= self._config.gzip_threshold:
            return await asyncio.to_thread(gzip.compress, payload)
        return gzip.compress(payload)

    async def send(self, kind: SignalKind, payload: bytes) -> ExportOutcome:
        headers = {**self._config.headers, "Content-Type": self._encoder.content_type}
        body = payload
        if self._config.compression is Compression.GZIP:
            body = await self._compress(payload)
            headers["Content-Encoding"] = "gzip"

        try:
            async with self._session.post(
                self._config.endpoint_for(kind),
                data=body,
                headers=headers,
                timeout=self._timeout,
                ssl=self._ssl,
            ) as response:
                raw = await response.read()
                if 200 <= response.status < 300:
                    partial = self._encoder.decode_response(kind, raw)
                    return partial if partial is not None else Success()
                message = raw.decode("utf-8", "replace")[:512]
                if response.status in RETRYABLE_STATUSES:
                    header = response.headers.get("Retry-After", "")
                    return Retryable(
                        status=response.status,
                        message=message,
                        retry_after=parse_retry_after(header, now_wall=time.time())
                        if header
                        else None,
                    )
                return Permanent(status=response.status, message=message)
        except (aiohttp.ClientError, TimeoutError) as exc:
            return Retryable(message=f"{type(exc).__name__}: {exc}")

    async def aclose(self) -> None:
        """Close the session only if this transport created it."""
        if self._owns_session:
            await self._session.close()
```

- [ ] **Step 4: Run tests and lint**

Run: `uv run pytest tests/test_transport_http.py -v && uv run mypy && uv run ruff check`
Expected: 16 passed (parametrized cases included), clean

- [ ] **Step 5: Commit**

```bash
git add src/otlp_client/transport/http.py tests/test_transport_http.py
git commit -m "feat: aiohttp OTLP/HTTP transport with injected session and off-loop TLS"
```

---

### Task 10: OTLPClient

**Files:**
- Create: `src/otlp_client/client.py`
- Modify: `src/otlp_client/__init__.py`
- Test: `tests/test_client.py`

**Interfaces:**
- Consumes: everything from Tasks 1-9
- Produces: `OTLPClient(config, *, transport, encoder, scope=None, policy=None)`; `async OTLPClient.create(config, *, session=None, scope=None) -> OTLPClient`; `async export_metrics(metrics, *, resource=None, scope=None) -> Success | PartialSuccess`; `async export_resource_metrics(data) -> Success | PartialSuccess`; `async aclose()`; async context manager support; module `__version__`

**Error contract:** `Permanent` raises `OTLPPermanentError`. An exhausted `Retryable` raises `OTLPTransportError`. `Success` and `PartialSuccess` are returned.

- [ ] **Step 1: Write the failing test**

Create `tests/test_client.py`:

```python
import json

import pytest
from otlp_client.client import OTLPClient
from otlp_client.config import OTLPConfig
from otlp_client.encoding.json import JSONEncoder
from otlp_client.errors import OTLPPermanentError, OTLPTransportError
from otlp_client.model.common import InstrumentationScope, Resource
from otlp_client.model.metrics import ResourceMetrics, ScopeMetrics, gauge
from otlp_client.outcomes import PartialSuccess, Permanent, Retryable, Success
from otlp_client.retry import RetryPolicy
from otlp_client.signals import SignalKind
from tests.support.fakes import FakeClock, FakeTransport

CONFIG = OTLPConfig(
    endpoint="http://localhost:4318", resource=Resource(attributes={"service.name": "hass"})
)


def make_client(transport, **kwargs):
    return OTLPClient(CONFIG, transport=transport, encoder=JSONEncoder(), **kwargs)


async def test_export_metrics_wraps_metrics_in_config_resource():
    transport = FakeTransport()
    client = make_client(transport)
    result = await client.export_metrics([gauge("t", 21.5, time_unix_nano=1)])
    assert isinstance(result, Success)
    (kind, payload) = transport.sent[0]
    assert kind is SignalKind.METRICS
    doc = json.loads(payload)
    (rm,) = doc["resourceMetrics"]
    assert rm["resource"]["attributes"][0]["value"]["stringValue"] == "hass"
    assert rm["scopeMetrics"][0]["metrics"][0]["name"] == "t"


async def test_explicit_resource_overrides_config_resource():
    transport = FakeTransport()
    client = make_client(transport)
    await client.export_metrics(
        [gauge("t", 1.0, time_unix_nano=1)], resource=Resource(attributes={"host": "pi"})
    )
    doc = json.loads(transport.sent[0][1])
    assert doc["resourceMetrics"][0]["resource"]["attributes"][0]["key"] == "host"


async def test_default_scope_names_this_library():
    transport = FakeTransport()
    client = make_client(transport)
    await client.export_metrics([gauge("t", 1.0, time_unix_nano=1)])
    doc = json.loads(transport.sent[0][1])
    assert doc["resourceMetrics"][0]["scopeMetrics"][0]["scope"]["name"] == "otlp_client"


async def test_export_resource_metrics_passes_the_envelope_through():
    transport = FakeTransport()
    client = make_client(transport)
    envelope = ResourceMetrics(
        resource=Resource(attributes={"a": "b"}),
        scope_metrics=[ScopeMetrics(
            scope=InstrumentationScope(name="custom"),
            metrics=[gauge("t", 1.0, time_unix_nano=1)],
        )],
    )
    await client.export_resource_metrics([envelope])
    doc = json.loads(transport.sent[0][1])
    assert doc["resourceMetrics"][0]["scopeMetrics"][0]["scope"]["name"] == "custom"


async def test_partial_success_is_returned_not_raised():
    transport = FakeTransport(outcomes=[PartialSuccess(rejected=2, message="bad")])
    result = await make_client(transport).export_metrics([gauge("t", 1.0, time_unix_nano=1)])
    assert isinstance(result, PartialSuccess)
    assert result.rejected == 2


async def test_permanent_failure_raises():
    transport = FakeTransport(outcomes=[Permanent(status=400, message="bad request")])
    with pytest.raises(OTLPPermanentError, match="400"):
        await make_client(transport).export_metrics([gauge("t", 1.0, time_unix_nano=1)])


async def test_exhausted_retries_raise_transport_error():
    clock = FakeClock()
    transport = FakeTransport(outcomes=[Retryable(status=503, message="down")])
    client = make_client(
        transport,
        policy=RetryPolicy(initial_backoff=1.0, max_backoff=1.0, multiplier=1.0, max_elapsed=2.0),
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )
    with pytest.raises(OTLPTransportError, match="down"):
        await client.export_metrics([gauge("t", 1.0, time_unix_nano=1)])
    assert len(transport.sent) >= 2


async def test_retry_then_success_returns_success():
    clock = FakeClock()
    transport = FakeTransport(outcomes=[Retryable(status=503), Success()])
    client = make_client(transport, sleep=clock.sleep, monotonic=clock.monotonic)
    assert isinstance(await client.export_metrics([gauge("t", 1.0, time_unix_nano=1)]), Success)
    assert len(transport.sent) == 2


async def test_empty_metrics_list_does_not_hit_the_transport():
    transport = FakeTransport()
    assert isinstance(await make_client(transport).export_metrics([]), Success)
    assert transport.sent == []


async def test_context_manager_closes_the_transport():
    transport = FakeTransport()
    async with make_client(transport):
        pass
    assert transport.closed is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_client.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'otlp_client.client'`

- [ ] **Step 3: Write the client**

Create `src/otlp_client/client.py`:

```python
"""The OTLP client: encode, ship, retry, classify."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from types import TracebackType

from otlp_client.config import OTLPConfig, OTLPProtocol
from otlp_client.encoding.base import Encoder
from otlp_client.errors import OTLPConfigError, OTLPPermanentError, OTLPTransportError
from otlp_client.model.common import InstrumentationScope, Resource
from otlp_client.model.metrics import Metric, ResourceMetrics, ScopeMetrics
from otlp_client.outcomes import ExportOutcome, PartialSuccess, Permanent, Retryable, Success
from otlp_client.retry import RetryPolicy, with_retry
from otlp_client.signals import SignalKind
from otlp_client.transport.base import Transport

__version__ = "0.1.0"

DEFAULT_SCOPE = InstrumentationScope(name="otlp_client", version=__version__)
_EMPTY_RESOURCE = Resource()


def _build_encoder(config: OTLPConfig) -> Encoder:
    """Pick an encoder, importing optional extras only when they are asked for."""
    if config.protocol is OTLPProtocol.HTTP_JSON:
        from otlp_client.encoding.json import JSONEncoder

        return JSONEncoder()
    from otlp_client.encoding.protobuf import build_protobuf_encoder

    return build_protobuf_encoder()


class OTLPClient:
    """Exports OTLP signals. One round trip per call, with retries."""

    def __init__(
        self,
        config: OTLPConfig,
        *,
        transport: Transport,
        encoder: Encoder,
        scope: InstrumentationScope | None = None,
        policy: RetryPolicy | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._config = config
        self._transport = transport
        self._encoder = encoder
        self._scope = scope or DEFAULT_SCOPE
        self._policy = policy or RetryPolicy.from_config(config)
        self._sleep = sleep
        self._monotonic = monotonic

    @classmethod
    async def create(
        cls,
        config: OTLPConfig,
        *,
        session: object | None = None,
        scope: InstrumentationScope | None = None,
    ) -> OTLPClient:
        """Build a client for the configured protocol.

        `session` is an `aiohttp.ClientSession` for the HTTP protocols. Home
        Assistant integrations must pass `async_get_clientsession(hass)`.
        """
        encoder = _build_encoder(config)
        if config.protocol is OTLPProtocol.GRPC:
            from otlp_client.transport.grpc import GRPCTransport

            transport: Transport = await GRPCTransport.create(config, encoder)
        else:
            import aiohttp

            from otlp_client.transport.http import HTTPTransport

            if session is not None and not isinstance(session, aiohttp.ClientSession):
                raise OTLPConfigError("session must be an aiohttp.ClientSession")
            transport = await HTTPTransport.create(config, encoder, session=session)
        return cls(config, transport=transport, encoder=encoder, scope=scope)

    @property
    def resource(self) -> Resource:
        return self._config.resource or _EMPTY_RESOURCE

    async def _export(self, kind: SignalKind, data: Sequence[object]) -> Success | PartialSuccess:
        if not data:
            return Success()
        payload = self._encoder.encode(kind, data)

        async def attempt() -> ExportOutcome:
            return await self._transport.send(kind, payload)

        outcome = await with_retry(
            attempt,
            self._policy,
            sleep=self._sleep,
            monotonic=self._monotonic,
        )
        if isinstance(outcome, Permanent):
            raise OTLPPermanentError(f"collector rejected {kind} (status {outcome.status}): "
                                     f"{outcome.message}")
        if isinstance(outcome, Retryable):
            raise OTLPTransportError(f"could not deliver {kind} after retries: {outcome.message}")
        return outcome

    async def export_metrics(
        self,
        metrics: Sequence[Metric],
        *,
        resource: Resource | None = None,
        scope: InstrumentationScope | None = None,
    ) -> Success | PartialSuccess:
        """Export metrics, wrapping them in the client's resource and scope."""
        if not metrics:
            return Success()
        envelope = ResourceMetrics(
            resource=resource or self.resource,
            scope_metrics=[ScopeMetrics(scope=scope or self._scope, metrics=list(metrics))],
        )
        return await self.export_resource_metrics([envelope])

    async def export_resource_metrics(
        self, data: Sequence[ResourceMetrics]
    ) -> Success | PartialSuccess:
        """Export fully built metric envelopes."""
        return await self._export(SignalKind.METRICS, data)

    async def aclose(self) -> None:
        await self._transport.aclose()

    async def __aenter__(self) -> OTLPClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()
```

- [ ] **Step 4: Write the public exports**

Replace `src/otlp_client/__init__.py` with:

```python
"""A pure-asyncio OpenTelemetry OTLP client."""

from otlp_client.client import OTLPClient, __version__
from otlp_client.config import Compression, OTLPConfig, OTLPProtocol
from otlp_client.errors import (
    OTLPConfigError,
    OTLPError,
    OTLPPermanentError,
    OTLPTransportError,
)
from otlp_client.model.common import AnyValue, InstrumentationScope, Resource
from otlp_client.model.metrics import (
    AggregationTemporality,
    Gauge,
    Histogram,
    HistogramDataPoint,
    Metric,
    NumberDataPoint,
    ResourceMetrics,
    ScopeMetrics,
    Sum,
    gauge,
    sum_,
)
from otlp_client.outcomes import ExportOutcome, PartialSuccess, Permanent, Retryable, Success
from otlp_client.signals import SignalKind

__all__ = [
    "AggregationTemporality",
    "AnyValue",
    "Compression",
    "ExportOutcome",
    "Gauge",
    "Histogram",
    "HistogramDataPoint",
    "InstrumentationScope",
    "Metric",
    "NumberDataPoint",
    "OTLPClient",
    "OTLPConfig",
    "OTLPConfigError",
    "OTLPError",
    "OTLPPermanentError",
    "OTLPProtocol",
    "OTLPTransportError",
    "PartialSuccess",
    "Permanent",
    "Resource",
    "ResourceMetrics",
    "Retryable",
    "ScopeMetrics",
    "SignalKind",
    "Success",
    "Sum",
    "__version__",
    "gauge",
    "sum_",
]
```

- [ ] **Step 5: Run tests and lint**

Run: `uv run pytest tests/test_client.py -v && uv run mypy && uv run ruff check`
Expected: 10 passed, clean

- [ ] **Step 6: Commit**

```bash
git add src/otlp_client/client.py src/otlp_client/__init__.py tests/test_client.py
git commit -m "feat: OTLPClient with retry integration and error contract"
```

---

### Task 11: BatchProcessor

**Files:**
- Create: `src/otlp_client/processor.py`
- Modify: `src/otlp_client/__init__.py` (export `BatchProcessor`, `ProcessorStats`)
- Test: `tests/test_processor.py`

**Interfaces:**
- Consumes: `OTLPClient`, `Metric`, `Resource`, `InstrumentationScope`
- Produces: `ProcessorStats(submitted, exported, dropped, consecutive_failures, last_error)`; `BatchProcessor(client, *, max_batch=512, flush_interval=5.0, max_queue=2048, resource=None, scope=None)`; `submit_metrics(metrics) -> bool`; `async flush()`; `stats` property; async context manager

**Why no fake clock here:** the flush task's backoff comes from the client's retry engine, which already has its own budget. Tests drive flushes by reaching `max_batch` or by calling `flush()` directly, and wait on an `asyncio.Event` the processor sets after each flush — so nothing waits on wall-clock time.

- [ ] **Step 1: Write the failing test**

Create `tests/test_processor.py`:

```python
import asyncio

import pytest
from otlp_client.client import OTLPClient
from otlp_client.config import OTLPConfig
from otlp_client.encoding.json import JSONEncoder
from otlp_client.model.common import Resource
from otlp_client.model.metrics import gauge
from otlp_client.outcomes import Permanent, Success
from otlp_client.processor import BatchProcessor
from tests.support.fakes import FakeTransport

CONFIG = OTLPConfig(endpoint="http://localhost:4318", resource=Resource(attributes={"a": "b"}))


def make_client(transport):
    return OTLPClient(CONFIG, transport=transport, encoder=JSONEncoder())


def one(n=1):
    return [gauge("t", float(n), time_unix_nano=n)]


async def test_submit_is_non_blocking_and_queues():
    proc = BatchProcessor(make_client(FakeTransport()), flush_interval=3600.0)
    assert proc.submit_metrics(one()) is True
    assert proc.stats.submitted == 1
    assert proc.stats.exported == 0


async def test_explicit_flush_exports_everything_queued():
    transport = FakeTransport()
    proc = BatchProcessor(make_client(transport), flush_interval=3600.0)
    proc.submit_metrics(one(1))
    proc.submit_metrics(one(2))
    await proc.flush()
    assert len(transport.sent) == 1
    assert proc.stats.exported == 2
    assert proc.stats.submitted == 2


async def test_flush_with_empty_queue_is_a_no_op():
    transport = FakeTransport()
    proc = BatchProcessor(make_client(transport), flush_interval=3600.0)
    await proc.flush()
    assert transport.sent == []


async def test_queue_overflow_drops_oldest_and_counts():
    transport = FakeTransport()
    proc = BatchProcessor(make_client(transport), max_queue=2, flush_interval=3600.0)
    assert proc.submit_metrics(one(1)) is True
    assert proc.submit_metrics(one(2)) is True
    assert proc.submit_metrics(one(3)) is False
    assert proc.stats.dropped == 1
    await proc.flush()
    # The oldest record was evicted; the two newest survived.
    assert proc.stats.exported == 2


async def test_export_failure_records_error_and_does_not_raise():
    transport = FakeTransport(outcomes=[Permanent(status=400, message="bad")])
    proc = BatchProcessor(make_client(transport), flush_interval=3600.0)
    proc.submit_metrics(one())
    await proc.flush()
    assert proc.stats.consecutive_failures == 1
    assert proc.stats.last_error is not None
    assert "400" in proc.stats.last_error


async def test_consecutive_failures_reset_after_a_success():
    transport = FakeTransport(outcomes=[Permanent(status=400), Success()])
    proc = BatchProcessor(make_client(transport), flush_interval=3600.0)
    proc.submit_metrics(one(1))
    await proc.flush()
    assert proc.stats.consecutive_failures == 1
    proc.submit_metrics(one(2))
    await proc.flush()
    assert proc.stats.consecutive_failures == 0


async def test_reaching_max_batch_triggers_a_background_flush():
    transport = FakeTransport()
    async with BatchProcessor(
        make_client(transport), max_batch=2, flush_interval=3600.0
    ) as proc:
        proc.submit_metrics(one(1))
        proc.submit_metrics(one(2))
        async with asyncio.timeout(5):
            await proc.flushed.wait()
    assert len(transport.sent) == 1
    assert proc.stats.exported == 2


async def test_context_manager_drains_on_exit():
    transport = FakeTransport()
    async with BatchProcessor(make_client(transport), flush_interval=3600.0) as proc:
        proc.submit_metrics(one())
    assert len(transport.sent) == 1
    assert proc.stats.exported == 1


async def test_submitting_after_close_is_rejected():
    transport = FakeTransport()
    async with BatchProcessor(make_client(transport), flush_interval=3600.0) as proc:
        pass
    assert proc.submit_metrics(one()) is False


async def test_flush_task_is_cancelled_on_exit():
    transport = FakeTransport()
    async with BatchProcessor(make_client(transport), flush_interval=3600.0) as proc:
        task = proc._task
    assert task is not None
    assert task.done()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_processor.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'otlp_client.processor'`

- [ ] **Step 3: Write the processor**

Create `src/otlp_client/processor.py`:

```python
"""Bounded queueing and background flushing on top of OTLPClient."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Any, cast

from otlp_client.client import OTLPClient
from otlp_client.errors import OTLPError
from otlp_client.model.common import InstrumentationScope, Resource
from otlp_client.model.metrics import Metric
from otlp_client.outcomes import PartialSuccess
from otlp_client.signals import SignalKind

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ProcessorStats:
    """A snapshot of processor health, safe to surface in a UI."""

    submitted: int = 0
    exported: int = 0
    dropped: int = 0
    consecutive_failures: int = 0
    last_error: str | None = None


class BatchProcessor:
    """Queues records and flushes them on size, interval, or demand.

    `submit_*` never blocks and never raises: a caller such as a state-change
    listener has nowhere to handle an exception. Overflow drops the oldest
    record and increments `stats.dropped`.

    Backoff on a failing collector comes from the client's retry engine, which
    bounds each flush by `max_elapsed`, so the flush loop cannot hot-loop.
    """

    def __init__(
        self,
        client: OTLPClient,
        *,
        max_batch: int = 512,
        flush_interval: float = 5.0,
        max_queue: int = 2048,
        resource: Resource | None = None,
        scope: InstrumentationScope | None = None,
    ) -> None:
        self._client = client
        self._max_batch = max_batch
        self._flush_interval = flush_interval
        self._resource = resource
        self._scope = scope
        self._max_queue = max_queue
        # One bounded queue per signal. Tasks 12 and 13 add LOGS and TRACES.
        self._queues: dict[SignalKind, deque[Any]] = {
            SignalKind.METRICS: deque(maxlen=max_queue),
        }
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._closed = False
        self._lock = asyncio.Lock()
        self.flushed = asyncio.Event()
        self._submitted = 0
        self._exported = 0
        self._dropped = 0
        self._consecutive_failures = 0
        self._last_error: str | None = None

    @property
    def stats(self) -> ProcessorStats:
        return ProcessorStats(
            submitted=self._submitted,
            exported=self._exported,
            dropped=self._dropped,
            consecutive_failures=self._consecutive_failures,
            last_error=self._last_error,
        )

    def _submit(self, kind: SignalKind, records: Sequence[Any]) -> bool:
        """Queue records for one signal. Never blocks, never raises."""
        if self._closed:
            return False
        queue = self._queues[kind]
        accepted = True
        for record in records:
            if queue.maxlen is not None and len(queue) == queue.maxlen:
                self._dropped += 1
                accepted = False
            queue.append(record)
            self._submitted += 1
        if len(queue) >= self._max_batch:
            self._wake.set()
        return accepted

    def submit_metrics(self, metrics: Sequence[Metric]) -> bool:
        """Queue metrics. Returns False if anything was dropped or we are closed."""
        return self._submit(SignalKind.METRICS, metrics)

    async def _export_batch(self, kind: SignalKind, batch: Sequence[Any]) -> PartialSuccess | None:
        """Dispatch one drained batch to the right client method.

        Tasks 12 and 13 add the LOGS and TRACES branches.
        """
        if kind is SignalKind.METRICS:
            result = await self._client.export_metrics(
                cast("Sequence[Metric]", batch), resource=self._resource, scope=self._scope
            )
        else:  # pragma: no cover - unreachable until later signals are added
            raise NotImplementedError(f"no processor branch for {kind}")
        return result if isinstance(result, PartialSuccess) else None

    async def flush(self) -> None:
        """Export everything queued, for every signal.

        Never raises; failures land in stats so a caller can surface health.
        """
        async with self._lock:
            for kind, queue in self._queues.items():
                batch = list(queue)
                queue.clear()
                if not batch:
                    continue
                try:
                    partial = await self._export_batch(kind, batch)
                except OTLPError as exc:
                    self._consecutive_failures += 1
                    self._last_error = str(exc)
                    _LOGGER.debug("OTLP %s export failed: %s", kind, exc)
                    continue
                self._consecutive_failures = 0
                self._last_error = None
                exported = len(batch)
                if partial is not None:
                    exported -= partial.rejected
                    self._last_error = f"partial success: {partial.message}"
                self._exported += max(0, exported)

    async def _run(self) -> None:
        while True:
            try:
                async with asyncio.timeout(self._flush_interval):
                    await self._wake.wait()
            except TimeoutError:
                pass
            self._wake.clear()
            await self.flush()
            self.flushed.set()

    async def __aenter__(self) -> BatchProcessor:
        self._task = asyncio.create_task(self._run())
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Stop accepting work, make one final flush, then cancel the task."""
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self.flush()
```

- [ ] **Step 4: Export the new names**

In `src/otlp_client/__init__.py`, add the import and the two `__all__` entries (keeping `__all__` alphabetically sorted):

```python
from otlp_client.processor import BatchProcessor, ProcessorStats
```

- [ ] **Step 5: Run tests and lint**

Run: `uv run pytest tests/test_processor.py -v && uv run mypy && uv run ruff check`
Expected: 10 passed, clean

- [ ] **Step 6: Commit**

```bash
git add src/otlp_client/processor.py src/otlp_client/__init__.py tests/test_processor.py
git commit -m "feat: BatchProcessor with bounded queues and drop-oldest overflow"
```

---

### Task 12: Logs signal, end to end

**Files:**
- Create: `src/otlp_client/model/logs.py`
- Modify: `src/otlp_client/encoding/json.py`, `src/otlp_client/client.py`, `src/otlp_client/processor.py`, `src/otlp_client/__init__.py`
- Test: `tests/test_logs.py`

**Interfaces:**
- Consumes: `AnyValue`, `Resource`, `InstrumentationScope`, primitives, `OTLPClient`, `BatchProcessor`
- Produces: `SeverityNumber` IntEnum; `LogRecord(time_unix_nano, body, observed_time_unix_nano, severity_number, severity_text, attributes, trace_id, span_id, flags)`; `ScopeLogs(scope, log_records)`; `ResourceLogs(resource, scope_logs)`; helper `log_record(body, *, time_unix_nano, severity, attributes, trace_id, span_id) -> LogRecord`; `OTLPClient.export_logs(records, *, resource, scope)` and `export_resource_logs(data)`; `BatchProcessor.submit_logs(records)`

- [ ] **Step 1: Write the failing test**

Create `tests/test_logs.py`:

```python
import json

from otlp_client.client import OTLPClient
from otlp_client.config import OTLPConfig
from otlp_client.encoding.json import JSONEncoder
from otlp_client.model.common import Resource
from otlp_client.model.logs import SeverityNumber, log_record
from otlp_client.outcomes import Success
from otlp_client.processor import BatchProcessor
from otlp_client.signals import SignalKind
from tests.support.fakes import FakeTransport

CONFIG = OTLPConfig(endpoint="http://localhost:4318", resource=Resource(attributes={"a": "b"}))


def make_client(transport):
    return OTLPClient(CONFIG, transport=transport, encoder=JSONEncoder())


def test_log_record_helper_defaults_observed_time_to_time():
    rec = log_record("boot complete", time_unix_nano=42, severity=SeverityNumber.INFO)
    assert rec.time_unix_nano == 42
    assert rec.observed_time_unix_nano == 42
    assert rec.severity_number is SeverityNumber.INFO
    assert rec.severity_text == "INFO"


async def test_export_logs_envelope_and_field_encoding():
    transport = FakeTransport()
    result = await make_client(transport).export_logs(
        [log_record("hello", time_unix_nano=7, severity=SeverityNumber.WARN,
                    attributes={"logger": "hass.core"})]
    )
    assert isinstance(result, Success)
    kind, payload = transport.sent[0]
    assert kind is SignalKind.LOGS
    doc = json.loads(payload)
    (rl,) = doc["resourceLogs"]
    (sl,) = rl["scopeLogs"]
    (record,) = sl["logRecords"]
    assert record["timeUnixNano"] == "7"
    assert record["observedTimeUnixNano"] == "7"
    assert record["severityNumber"] == 13
    assert record["severityText"] == "WARN"
    assert record["body"] == {"stringValue": "hello"}
    assert record["attributes"] == [
        {"key": "logger", "value": {"stringValue": "hass.core"}}
    ]


async def test_severity_number_is_an_integer_not_a_name():
    transport = FakeTransport()
    await make_client(transport).export_logs(
        [log_record("x", time_unix_nano=1, severity=SeverityNumber.ERROR)]
    )
    record = json.loads(transport.sent[0][1])["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]
    assert record["severityNumber"] == 17
    assert not isinstance(record["severityNumber"], str)


async def test_trace_and_span_ids_are_hex_not_base64():
    transport = FakeTransport()
    await make_client(transport).export_logs([
        log_record("x", time_unix_nano=1,
                   trace_id=bytes.fromhex("0102030405060708090a0b0c0d0e0f10"),
                   span_id=bytes.fromhex("1112131415161718"))
    ])
    record = json.loads(transport.sent[0][1])["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]
    assert record["traceId"] == "0102030405060708090a0b0c0d0e0f10"
    assert record["spanId"] == "1112131415161718"


async def test_structured_body_is_encoded_as_any_value():
    transport = FakeTransport()
    await make_client(transport).export_logs(
        [log_record({"event": "state_changed", "count": 3}, time_unix_nano=1)]
    )
    record = json.loads(transport.sent[0][1])["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]
    assert record["body"] == {"kvlistValue": {"values": [
        {"key": "event", "value": {"stringValue": "state_changed"}},
        {"key": "count", "value": {"intValue": "3"}},
    ]}}


async def test_processor_queues_and_flushes_logs():
    transport = FakeTransport()
    proc = BatchProcessor(make_client(transport), flush_interval=3600.0)
    assert proc.submit_logs([log_record("a", time_unix_nano=1)]) is True
    await proc.flush()
    assert transport.sent[0][0] is SignalKind.LOGS
    assert proc.stats.exported == 1


async def test_processor_keeps_metrics_and_logs_in_separate_queues():
    from otlp_client.model.metrics import gauge

    transport = FakeTransport()
    proc = BatchProcessor(make_client(transport), flush_interval=3600.0)
    proc.submit_metrics([gauge("t", 1.0, time_unix_nano=1)])
    proc.submit_logs([log_record("a", time_unix_nano=1)])
    await proc.flush()
    assert {kind for kind, _ in transport.sent} == {SignalKind.METRICS, SignalKind.LOGS}
    assert proc.stats.exported == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_logs.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'otlp_client.model.logs'`

- [ ] **Step 3: Write the logs model**

Create `src/otlp_client/model/logs.py`:

```python
"""The OTLP logs data model."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import IntEnum
from types import MappingProxyType

from otlp_client.model.common import AnyValue, InstrumentationScope, Resource

_EMPTY: Mapping[str, AnyValue] = MappingProxyType({})


class SeverityNumber(IntEnum):
    UNSPECIFIED = 0
    TRACE = 1
    DEBUG = 5
    INFO = 9
    WARN = 13
    ERROR = 17
    FATAL = 21


@dataclass(frozen=True, slots=True)
class LogRecord:
    time_unix_nano: int
    body: AnyValue
    observed_time_unix_nano: int
    severity_number: SeverityNumber = SeverityNumber.UNSPECIFIED
    severity_text: str = ""
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY)
    trace_id: bytes | None = None
    span_id: bytes | None = None
    flags: int = 0


@dataclass(frozen=True, slots=True)
class ScopeLogs:
    scope: InstrumentationScope
    log_records: Sequence[LogRecord]


@dataclass(frozen=True, slots=True)
class ResourceLogs:
    resource: Resource
    scope_logs: Sequence[ScopeLogs]


def log_record(
    body: AnyValue,
    *,
    time_unix_nano: int,
    observed_time_unix_nano: int | None = None,
    severity: SeverityNumber = SeverityNumber.UNSPECIFIED,
    severity_text: str | None = None,
    attributes: Mapping[str, AnyValue] | None = None,
    trace_id: bytes | None = None,
    span_id: bytes | None = None,
) -> LogRecord:
    """Build a log record.

    `observed_time_unix_nano` defaults to `time_unix_nano`, and `severity_text`
    defaults to the severity's name, which is what collectors display.
    """
    return LogRecord(
        time_unix_nano=time_unix_nano,
        observed_time_unix_nano=(
            time_unix_nano if observed_time_unix_nano is None else observed_time_unix_nano
        ),
        body=body,
        severity_number=severity,
        severity_text=(
            severity_text
            if severity_text is not None
            else ("" if severity is SeverityNumber.UNSPECIFIED else severity.name)
        ),
        attributes=attributes or _EMPTY,
        trace_id=trace_id,
        span_id=span_id,
    )
```

- [ ] **Step 4: Extend the JSON encoder**

In `src/otlp_client/encoding/json.py`, **extend the existing primitives import**
(do not add a second import from the same module, ruff rejects that) and add the
logs model import:

```python
from otlp_client.encoding.primitives import (
    encode_any_value,
    encode_attributes,
    hex_id,
    omit_empty,
    u64,
)
from otlp_client.model.logs import LogRecord, ResourceLogs, ScopeLogs
```

Add these functions above the `JSONEncoder` class:

```python
def _encode_log_record(record: LogRecord) -> dict[str, Any]:
    return omit_empty(
        {
            "timeUnixNano": u64(record.time_unix_nano),
            "observedTimeUnixNano": u64(record.observed_time_unix_nano),
            "severityNumber": int(record.severity_number) or None,
            "severityText": record.severity_text,
            "body": encode_any_value(record.body),
            "attributes": encode_attributes(record.attributes),
            # traceId and spanId are hex, never base64. This is the one
            # documented deviation from the protobuf-JSON mapping.
            "traceId": hex_id(record.trace_id) if record.trace_id else None,
            "spanId": hex_id(record.span_id) if record.span_id else None,
            "flags": record.flags or None,
        }
    )


def _encode_resource_logs(data: Sequence[ResourceLogs]) -> dict[str, Any]:
    return {
        "resourceLogs": [
            omit_empty(
                {
                    "resource": _encode_resource(rl.resource),
                    "scopeLogs": [
                        omit_empty(
                            {
                                "scope": _encode_scope(sl.scope),
                                "logRecords": [_encode_log_record(r) for r in sl.log_records],
                            }
                        )
                        for sl in rl.scope_logs
                    ],
                }
            )
            for rl in data
        ]
    }
```

In `JSONEncoder.encode`, add the logs branch immediately after the metrics branch:

```python
        if kind is SignalKind.LOGS:
            return _dumps(_encode_resource_logs(data))
```

- [ ] **Step 5: Extend the client**

In `src/otlp_client/client.py`, add to the imports:

```python
from otlp_client.model.logs import LogRecord, ResourceLogs, ScopeLogs
```

Add these methods to `OTLPClient`, after `export_resource_metrics`:

```python
    async def export_logs(
        self,
        records: Sequence[LogRecord],
        *,
        resource: Resource | None = None,
        scope: InstrumentationScope | None = None,
    ) -> Success | PartialSuccess:
        """Export log records, wrapping them in the client's resource and scope."""
        if not records:
            return Success()
        envelope = ResourceLogs(
            resource=resource or self.resource,
            scope_logs=[ScopeLogs(scope=scope or self._scope, log_records=list(records))],
        )
        return await self.export_resource_logs([envelope])

    async def export_resource_logs(
        self, data: Sequence[ResourceLogs]
    ) -> Success | PartialSuccess:
        """Export fully built log envelopes."""
        return await self._export(SignalKind.LOGS, data)
```

- [ ] **Step 6: Extend the processor**

In `src/otlp_client/processor.py`, add the import:

```python
from otlp_client.model.logs import LogRecord
```

Add `SignalKind.LOGS` to the queue dict in `__init__`:

```python
        self._queues: dict[SignalKind, deque[Any]] = {
            SignalKind.METRICS: deque(maxlen=max_queue),
            SignalKind.LOGS: deque(maxlen=max_queue),
        }
```

Add the submit method after `submit_metrics`:

```python
    def submit_logs(self, records: Sequence[LogRecord]) -> bool:
        """Queue log records. Returns False if anything was dropped or we are closed."""
        return self._submit(SignalKind.LOGS, records)
```

Add the dispatch branch in `_export_batch`, between the metrics branch and the `else`:

```python
        elif kind is SignalKind.LOGS:
            result = await self._client.export_logs(
                cast("Sequence[LogRecord]", batch), resource=self._resource, scope=self._scope
            )
```

- [ ] **Step 7: Export the new names**

In `src/otlp_client/__init__.py`, add the import and the corresponding `__all__` entries, keeping `__all__` alphabetically sorted:

```python
from otlp_client.model.logs import LogRecord, ResourceLogs, ScopeLogs, SeverityNumber, log_record
```

- [ ] **Step 8: Run the full suite and lint**

Run: `uv run pytest -v && uv run mypy && uv run ruff check`
Expected: all tests pass including the 7 new ones, clean

- [ ] **Step 9: Commit**

```bash
git add src/otlp_client tests/test_logs.py
git commit -m "feat: logs signal end to end"
```

---

### Task 13: Traces signal, end to end

**Files:**
- Create: `src/otlp_client/model/traces.py`
- Modify: `src/otlp_client/encoding/json.py`, `src/otlp_client/client.py`, `src/otlp_client/processor.py`, `src/otlp_client/__init__.py`
- Test: `tests/test_traces.py`

**Interfaces:**
- Consumes: as Task 12
- Produces: `SpanKind` and `StatusCode` IntEnums; `Status(code, message)`; `SpanEvent(time_unix_nano, name, attributes)`; `SpanLink(trace_id, span_id, attributes)`; `Span(trace_id, span_id, name, start_time_unix_nano, end_time_unix_nano, kind, parent_span_id, attributes, events, links, status)`; `ScopeSpans(scope, spans)`; `ResourceSpans(resource, scope_spans)`; helper `span(name, *, trace_id, span_id, start_time_unix_nano, end_time_unix_nano, ...) -> Span`; `OTLPClient.export_traces(spans, ...)` and `export_resource_spans(data)`; `BatchProcessor.submit_traces(spans)`

- [ ] **Step 1: Write the failing test**

Create `tests/test_traces.py`:

```python
import json

from otlp_client.client import OTLPClient
from otlp_client.config import OTLPConfig
from otlp_client.encoding.json import JSONEncoder
from otlp_client.model.common import Resource
from otlp_client.model.traces import SpanKind, StatusCode, span
from otlp_client.outcomes import Success
from otlp_client.processor import BatchProcessor
from otlp_client.signals import SignalKind
from tests.support.fakes import FakeTransport

TRACE_ID = bytes.fromhex("0102030405060708090a0b0c0d0e0f10")
SPAN_ID = bytes.fromhex("1112131415161718")
PARENT_ID = bytes.fromhex("2122232425262728")
CONFIG = OTLPConfig(endpoint="http://localhost:4318", resource=Resource(attributes={"a": "b"}))


def make_client(transport):
    return OTLPClient(CONFIG, transport=transport, encoder=JSONEncoder())


def only_span(payload: bytes) -> dict:
    return json.loads(payload)["resourceSpans"][0]["scopeSpans"][0]["spans"][0]


async def test_export_traces_envelope_and_hex_ids():
    transport = FakeTransport()
    result = await make_client(transport).export_traces([
        span("handle_state_change", trace_id=TRACE_ID, span_id=SPAN_ID,
             parent_span_id=PARENT_ID, start_time_unix_nano=100, end_time_unix_nano=200,
             kind=SpanKind.INTERNAL, attributes={"entity_id": "light.kitchen"})
    ])
    assert isinstance(result, Success)
    kind, payload = transport.sent[0]
    assert kind is SignalKind.TRACES
    s = only_span(payload)
    assert s["traceId"] == "0102030405060708090a0b0c0d0e0f10"
    assert s["spanId"] == "1112131415161718"
    assert s["parentSpanId"] == "2122232425262728"
    assert s["name"] == "handle_state_change"
    assert s["startTimeUnixNano"] == "100"
    assert s["endTimeUnixNano"] == "200"
    assert s["kind"] == 1
    assert s["attributes"] == [{"key": "entity_id", "value": {"stringValue": "light.kitchen"}}]


async def test_span_kind_and_status_code_are_integers():
    transport = FakeTransport()
    await make_client(transport).export_traces([
        span("call", trace_id=TRACE_ID, span_id=SPAN_ID, start_time_unix_nano=1,
             end_time_unix_nano=2, kind=SpanKind.CLIENT,
             status_code=StatusCode.ERROR, status_message="timeout")
    ])
    s = only_span(transport.sent[0][1])
    assert s["kind"] == 3
    assert s["status"] == {"code": 2, "message": "timeout"}


async def test_root_span_omits_parent_span_id():
    transport = FakeTransport()
    await make_client(transport).export_traces([
        span("root", trace_id=TRACE_ID, span_id=SPAN_ID,
             start_time_unix_nano=1, end_time_unix_nano=2)
    ])
    assert "parentSpanId" not in only_span(transport.sent[0][1])


async def test_events_and_links_are_encoded():
    from otlp_client.model.traces import SpanEvent, SpanLink

    transport = FakeTransport()
    await make_client(transport).export_traces([
        span("s", trace_id=TRACE_ID, span_id=SPAN_ID, start_time_unix_nano=1,
             end_time_unix_nano=2,
             events=[SpanEvent(time_unix_nano=5, name="retry", attributes={"n": 2})],
             links=[SpanLink(trace_id=TRACE_ID, span_id=PARENT_ID)])
    ])
    s = only_span(transport.sent[0][1])
    assert s["events"] == [{
        "timeUnixNano": "5", "name": "retry",
        "attributes": [{"key": "n", "value": {"intValue": "2"}}],
    }]
    assert s["links"] == [{
        "traceId": "0102030405060708090a0b0c0d0e0f10", "spanId": "2122232425262728",
    }]


async def test_unset_status_is_omitted():
    transport = FakeTransport()
    await make_client(transport).export_traces([
        span("s", trace_id=TRACE_ID, span_id=SPAN_ID,
             start_time_unix_nano=1, end_time_unix_nano=2)
    ])
    assert "status" not in only_span(transport.sent[0][1])


async def test_processor_queues_and_flushes_traces():
    transport = FakeTransport()
    proc = BatchProcessor(make_client(transport), flush_interval=3600.0)
    assert proc.submit_traces([
        span("s", trace_id=TRACE_ID, span_id=SPAN_ID,
             start_time_unix_nano=1, end_time_unix_nano=2)
    ]) is True
    await proc.flush()
    assert transport.sent[0][0] is SignalKind.TRACES
    assert proc.stats.exported == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_traces.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'otlp_client.model.traces'`

- [ ] **Step 3: Write the traces model**

Create `src/otlp_client/model/traces.py`:

```python
"""The OTLP traces data model."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import IntEnum
from types import MappingProxyType

from otlp_client.model.common import AnyValue, InstrumentationScope, Resource

_EMPTY: Mapping[str, AnyValue] = MappingProxyType({})


class SpanKind(IntEnum):
    UNSPECIFIED = 0
    INTERNAL = 1
    SERVER = 2
    CLIENT = 3
    PRODUCER = 4
    CONSUMER = 5


class StatusCode(IntEnum):
    UNSET = 0
    OK = 1
    ERROR = 2


@dataclass(frozen=True, slots=True)
class Status:
    code: StatusCode = StatusCode.UNSET
    message: str = ""


@dataclass(frozen=True, slots=True)
class SpanEvent:
    time_unix_nano: int
    name: str
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY)


@dataclass(frozen=True, slots=True)
class SpanLink:
    trace_id: bytes
    span_id: bytes
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY)


@dataclass(frozen=True, slots=True)
class Span:
    trace_id: bytes
    span_id: bytes
    name: str
    start_time_unix_nano: int
    end_time_unix_nano: int
    kind: SpanKind = SpanKind.UNSPECIFIED
    parent_span_id: bytes | None = None
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY)
    events: Sequence[SpanEvent] = ()
    links: Sequence[SpanLink] = ()
    status: Status | None = None


@dataclass(frozen=True, slots=True)
class ScopeSpans:
    scope: InstrumentationScope
    spans: Sequence[Span]


@dataclass(frozen=True, slots=True)
class ResourceSpans:
    resource: Resource
    scope_spans: Sequence[ScopeSpans]


def span(
    name: str,
    *,
    trace_id: bytes,
    span_id: bytes,
    start_time_unix_nano: int,
    end_time_unix_nano: int,
    kind: SpanKind = SpanKind.UNSPECIFIED,
    parent_span_id: bytes | None = None,
    attributes: Mapping[str, AnyValue] | None = None,
    events: Sequence[SpanEvent] = (),
    links: Sequence[SpanLink] = (),
    status_code: StatusCode = StatusCode.UNSET,
    status_message: str = "",
) -> Span:
    """Build a finished span.

    `status` stays None when the code is UNSET so the field is omitted on the
    wire, which is what collectors expect for a span that reported no status.
    """
    status = (
        None
        if status_code is StatusCode.UNSET and not status_message
        else Status(code=status_code, message=status_message)
    )
    return Span(
        trace_id=trace_id,
        span_id=span_id,
        name=name,
        start_time_unix_nano=start_time_unix_nano,
        end_time_unix_nano=end_time_unix_nano,
        kind=kind,
        parent_span_id=parent_span_id,
        attributes=attributes or _EMPTY,
        events=events,
        links=links,
        status=status,
    )
```

- [ ] **Step 4: Extend the JSON encoder**

In `src/otlp_client/encoding/json.py`, add to the imports:

```python
from otlp_client.model.traces import ResourceSpans, Span, SpanEvent, SpanLink
```

Add these functions above the `JSONEncoder` class:

```python
def _encode_span_event(event: SpanEvent) -> dict[str, Any]:
    return omit_empty(
        {
            "timeUnixNano": u64(event.time_unix_nano),
            "name": event.name,
            "attributes": encode_attributes(event.attributes),
        }
    )


def _encode_span_link(link: SpanLink) -> dict[str, Any]:
    return omit_empty(
        {
            "traceId": hex_id(link.trace_id),
            "spanId": hex_id(link.span_id),
            "attributes": encode_attributes(link.attributes),
        }
    )


def _encode_span(item: Span) -> dict[str, Any]:
    status = (
        omit_empty({"code": int(item.status.code) or None, "message": item.status.message})
        if item.status is not None
        else None
    )
    return omit_empty(
        {
            "traceId": hex_id(item.trace_id),
            "spanId": hex_id(item.span_id),
            "parentSpanId": hex_id(item.parent_span_id) if item.parent_span_id else None,
            "name": item.name,
            "kind": int(item.kind) or None,
            "startTimeUnixNano": u64(item.start_time_unix_nano),
            "endTimeUnixNano": u64(item.end_time_unix_nano),
            "attributes": encode_attributes(item.attributes),
            "events": [_encode_span_event(e) for e in item.events],
            "links": [_encode_span_link(link) for link in item.links],
            "status": status,
        }
    )


def _encode_resource_spans(data: Sequence[ResourceSpans]) -> dict[str, Any]:
    return {
        "resourceSpans": [
            omit_empty(
                {
                    "resource": _encode_resource(rs.resource),
                    "scopeSpans": [
                        omit_empty(
                            {
                                "scope": _encode_scope(ss.scope),
                                "spans": [_encode_span(s) for s in ss.spans],
                            }
                        )
                        for ss in rs.scope_spans
                    ],
                }
            )
            for rs in data
        ]
    }
```

In `JSONEncoder.encode`, add the traces branch after the logs branch:

```python
        if kind is SignalKind.TRACES:
            return _dumps(_encode_resource_spans(data))
```

- [ ] **Step 5: Extend the client**

In `src/otlp_client/client.py`, add to the imports:

```python
from otlp_client.model.traces import ResourceSpans, ScopeSpans, Span
```

Add these methods to `OTLPClient`, after `export_resource_logs`:

```python
    async def export_traces(
        self,
        spans: Sequence[Span],
        *,
        resource: Resource | None = None,
        scope: InstrumentationScope | None = None,
    ) -> Success | PartialSuccess:
        """Export spans, wrapping them in the client's resource and scope."""
        if not spans:
            return Success()
        envelope = ResourceSpans(
            resource=resource or self.resource,
            scope_spans=[ScopeSpans(scope=scope or self._scope, spans=list(spans))],
        )
        return await self.export_resource_spans([envelope])

    async def export_resource_spans(
        self, data: Sequence[ResourceSpans]
    ) -> Success | PartialSuccess:
        """Export fully built span envelopes."""
        return await self._export(SignalKind.TRACES, data)
```

- [ ] **Step 6: Extend the processor**

In `src/otlp_client/processor.py`, add the import:

```python
from otlp_client.model.traces import Span
```

Add `SignalKind.TRACES` to the queue dict in `__init__`:

```python
            SignalKind.TRACES: deque(maxlen=max_queue),
```

Add the submit method after `submit_logs`:

```python
    def submit_traces(self, spans: Sequence[Span]) -> bool:
        """Queue spans. Returns False if anything was dropped or we are closed."""
        return self._submit(SignalKind.TRACES, spans)
```

Add the dispatch branch in `_export_batch`, before the `else`:

```python
        elif kind is SignalKind.TRACES:
            result = await self._client.export_traces(
                cast("Sequence[Span]", batch), resource=self._resource, scope=self._scope
            )
```

- [ ] **Step 7: Export the new names**

In `src/otlp_client/__init__.py`, add the import and the corresponding `__all__` entries, keeping `__all__` alphabetically sorted:

```python
from otlp_client.model.traces import (
    ResourceSpans,
    ScopeSpans,
    Span,
    SpanEvent,
    SpanKind,
    SpanLink,
    Status,
    StatusCode,
    span,
)
```

- [ ] **Step 8: Run the full suite and lint**

Run: `uv run pytest -v && uv run mypy && uv run ruff check`
Expected: all tests pass including the 6 new ones, clean

- [ ] **Step 9: Commit**

```bash
git add src/otlp_client tests/test_traces.py
git commit -m "feat: traces signal end to end"
```

---

### Task 14: Protobuf encoder (`[protobuf]` extra)

**Files:**
- Create: `src/otlp_client/encoding/protobuf.py`
- Test: `tests/test_encoding_protobuf.py`

**Interfaces:**
- Consumes: the full model, `SignalKind`, `PartialSuccess`
- Produces: `build_protobuf_encoder() -> ProtobufEncoder` raising `OTLPConfigError` when the extra is missing; `ProtobufEncoder` implementing `Encoder` with `content_type == "application/x-protobuf"`

**Critical constraint:** every `opentelemetry.proto` import lives **inside** `build_protobuf_encoder()` or inside methods — never at module top level. Task 17 adds a CI job that fails if this regresses. `client.py` already imports this module lazily.

- [ ] **Step 1: Write the failing test**

Create `tests/test_encoding_protobuf.py`:

```python
import pytest

pytest.importorskip("opentelemetry.proto")

from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
    ExportMetricsServiceResponse,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from otlp_client.encoding.protobuf import build_protobuf_encoder
from otlp_client.model.common import InstrumentationScope, Resource
from otlp_client.model.logs import ResourceLogs, ScopeLogs, SeverityNumber, log_record
from otlp_client.model.metrics import ResourceMetrics, ScopeMetrics, gauge, sum_
from otlp_client.model.traces import ResourceSpans, ScopeSpans, span
from otlp_client.signals import SignalKind

TRACE_ID = bytes.fromhex("0102030405060708090a0b0c0d0e0f10")
SPAN_ID = bytes.fromhex("1112131415161718")
RESOURCE = Resource(attributes={"service.name": "hass"})
SCOPE = InstrumentationScope(name="otlp_client", version="0.1.0")


def test_content_type():
    assert build_protobuf_encoder().content_type == "application/x-protobuf"


def test_metrics_round_trip_through_the_real_proto():
    payload = build_protobuf_encoder().encode(SignalKind.METRICS, [
        ResourceMetrics(resource=RESOURCE, scope_metrics=[ScopeMetrics(
            scope=SCOPE, metrics=[gauge("t", 21.5, unit="Cel", time_unix_nano=7)])])
    ])
    request = ExportMetricsServiceRequest.FromString(payload)
    (rm,) = request.resource_metrics
    assert rm.resource.attributes[0].key == "service.name"
    metric = rm.scope_metrics[0].metrics[0]
    assert metric.name == "t"
    assert metric.unit == "Cel"
    point = metric.gauge.data_points[0]
    assert point.as_double == 21.5
    assert point.time_unix_nano == 7


def test_integer_sum_uses_as_int_and_carries_temporality():
    payload = build_protobuf_encoder().encode(SignalKind.METRICS, [
        ResourceMetrics(resource=RESOURCE, scope_metrics=[ScopeMetrics(
            scope=SCOPE, metrics=[sum_("e", 42, time_unix_nano=1)])])
    ])
    metric = ExportMetricsServiceRequest.FromString(payload).resource_metrics[0]\
        .scope_metrics[0].metrics[0]
    assert metric.sum.data_points[0].as_int == 42
    assert metric.sum.is_monotonic is True
    assert metric.sum.aggregation_temporality == 2


def test_logs_round_trip():
    payload = build_protobuf_encoder().encode(SignalKind.LOGS, [
        ResourceLogs(resource=RESOURCE, scope_logs=[ScopeLogs(
            scope=SCOPE,
            log_records=[log_record("hello", time_unix_nano=7, severity=SeverityNumber.WARN,
                                    trace_id=TRACE_ID, span_id=SPAN_ID)])])
    ])
    record = ExportLogsServiceRequest.FromString(payload).resource_logs[0]\
        .scope_logs[0].log_records[0]
    assert record.body.string_value == "hello"
    assert record.severity_number == 13
    assert record.trace_id == TRACE_ID
    assert record.span_id == SPAN_ID


def test_traces_round_trip():
    payload = build_protobuf_encoder().encode(SignalKind.TRACES, [
        ResourceSpans(resource=RESOURCE, scope_spans=[ScopeSpans(
            scope=SCOPE,
            spans=[span("s", trace_id=TRACE_ID, span_id=SPAN_ID,
                        start_time_unix_nano=1, end_time_unix_nano=2)])])
    ])
    pb_span = ExportTraceServiceRequest.FromString(payload).resource_spans[0]\
        .scope_spans[0].spans[0]
    assert pb_span.name == "s"
    assert pb_span.trace_id == TRACE_ID
    assert pb_span.end_time_unix_nano == 2


def test_decode_partial_success():
    response = ExportMetricsServiceResponse()
    response.partial_success.rejected_data_points = 5
    response.partial_success.error_message = "bad unit"
    result = build_protobuf_encoder().decode_response(
        SignalKind.METRICS, response.SerializeToString()
    )
    assert result is not None
    assert result.rejected == 5
    assert result.message == "bad unit"


def test_decode_full_success_returns_none():
    encoder = build_protobuf_encoder()
    empty = ExportMetricsServiceResponse().SerializeToString()
    assert encoder.decode_response(SignalKind.METRICS, empty) is None
    assert encoder.decode_response(SignalKind.METRICS, b"") is None


def test_profiles_is_not_implemented():
    with pytest.raises(NotImplementedError, match="profiles"):
        build_protobuf_encoder().encode(SignalKind.PROFILES, [])


def test_module_does_not_import_protobuf_at_top_level():
    import ast
    import pathlib

    source = pathlib.Path("src/otlp_client/encoding/protobuf.py").read_text()
    tree = ast.parse(source)
    for node in tree.body:  # module level only
        if isinstance(node, ast.Import | ast.ImportFrom):
            name = getattr(node, "module", "") or ""
            names = " ".join(a.name for a in node.names)
            assert "opentelemetry" not in name + names, (
                "opentelemetry.proto must be imported lazily, not at module level"
            )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_encoding_protobuf.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'otlp_client.encoding.protobuf'`

- [ ] **Step 3: Write the protobuf encoder**

Create `src/otlp_client/encoding/protobuf.py`:

```python
"""OTLP/protobuf encoding. Requires the `protobuf` extra.

Every `opentelemetry.proto` import is deliberately inside a function. Importing
this module must stay free for a core-only install.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from otlp_client.errors import OTLPConfigError
from otlp_client.model.common import AnyValue, InstrumentationScope, Resource
from otlp_client.model.logs import LogRecord, ResourceLogs
from otlp_client.model.metrics import Gauge, Histogram, Metric, ResourceMetrics, Sum
from otlp_client.model.traces import ResourceSpans, Span
from otlp_client.outcomes import PartialSuccess
from otlp_client.signals import SignalKind

_MISSING = (
    "the protobuf encoder needs the optional extra: "
    "pip install 'asyncio-otlp-client[protobuf]'"
)


def build_protobuf_encoder() -> ProtobufEncoder:
    """Construct the encoder, failing with a usable message if the extra is absent."""
    try:
        import opentelemetry.proto.common.v1.common_pb2  # noqa: F401
    except ImportError as exc:
        raise OTLPConfigError(_MISSING) from exc
    return ProtobufEncoder()


class ProtobufEncoder:
    """Encodes the model tree as binary OTLP/protobuf."""

    @property
    def content_type(self) -> str:
        return "application/x-protobuf"

    def encode(self, kind: SignalKind, data: Sequence[Any]) -> bytes:
        if kind is SignalKind.METRICS:
            return _encode_metrics(data)
        if kind is SignalKind.LOGS:
            return _encode_logs(data)
        if kind is SignalKind.TRACES:
            return _encode_traces(data)
        if kind is SignalKind.PROFILES:
            raise NotImplementedError(
                "the profiles signal is still in development and is not encoded yet"
            )
        raise NotImplementedError(f"no encoder registered for {kind}")

    def decode_response(self, kind: SignalKind, body: bytes) -> PartialSuccess | None:
        if not body:
            return None
        response = _response_type(kind).FromString(body)
        partial = response.partial_success
        rejected = {
            SignalKind.METRICS: lambda p: p.rejected_data_points,
            SignalKind.LOGS: lambda p: p.rejected_log_records,
            SignalKind.TRACES: lambda p: p.rejected_spans,
        }[kind](partial)
        if not rejected and not partial.error_message:
            return None
        return PartialSuccess(rejected=rejected, message=partial.error_message)


def _response_type(kind: SignalKind) -> Any:
    if kind is SignalKind.METRICS:
        from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2

        return metrics_service_pb2.ExportMetricsServiceResponse
    if kind is SignalKind.LOGS:
        from opentelemetry.proto.collector.logs.v1 import logs_service_pb2

        return logs_service_pb2.ExportLogsServiceResponse
    if kind is SignalKind.TRACES:
        from opentelemetry.proto.collector.trace.v1 import trace_service_pb2

        return trace_service_pb2.ExportTraceServiceResponse
    raise NotImplementedError(f"no response type for {kind}")


def _any_value(value: AnyValue) -> Any:
    from opentelemetry.proto.common.v1 import common_pb2

    # bool before int: bool subclasses int.
    if isinstance(value, bool):
        return common_pb2.AnyValue(bool_value=value)
    if isinstance(value, int):
        return common_pb2.AnyValue(int_value=value)
    if isinstance(value, float):
        return common_pb2.AnyValue(double_value=value)
    if isinstance(value, str):
        return common_pb2.AnyValue(string_value=value)
    if isinstance(value, bytes):
        return common_pb2.AnyValue(bytes_value=value)
    if isinstance(value, Mapping):
        return common_pb2.AnyValue(
            kvlist_value=common_pb2.KeyValueList(values=_key_values(value))
        )
    if isinstance(value, Sequence):
        return common_pb2.AnyValue(
            array_value=common_pb2.ArrayValue(values=[_any_value(v) for v in value])
        )
    raise TypeError(f"unsupported attribute value type: {type(value)!r}")


def _key_values(attributes: Mapping[str, AnyValue]) -> list[Any]:
    from opentelemetry.proto.common.v1 import common_pb2

    return [
        common_pb2.KeyValue(key=key, value=_any_value(value))
        for key, value in attributes.items()
    ]


def _resource(resource: Resource) -> Any:
    from opentelemetry.proto.resource.v1 import resource_pb2

    return resource_pb2.Resource(
        attributes=_key_values(resource.attributes),
        dropped_attributes_count=resource.dropped_attributes_count,
    )


def _scope(scope: InstrumentationScope) -> Any:
    from opentelemetry.proto.common.v1 import common_pb2

    return common_pb2.InstrumentationScope(
        name=scope.name,
        version=scope.version or "",
        attributes=_key_values(scope.attributes),
    )


def _number_point(point: Any) -> Any:
    from opentelemetry.proto.metrics.v1 import metrics_pb2

    common = {
        "attributes": _key_values(point.attributes),
        "time_unix_nano": point.time_unix_nano,
        "start_time_unix_nano": point.start_time_unix_nano or 0,
    }
    if isinstance(point.value, bool):
        raise TypeError("metric data point values must be int or float, not bool")
    if isinstance(point.value, int):
        return metrics_pb2.NumberDataPoint(as_int=point.value, **common)
    return metrics_pb2.NumberDataPoint(as_double=point.value, **common)


def _metric(metric: Metric) -> Any:
    from opentelemetry.proto.metrics.v1 import metrics_pb2

    kwargs: dict[str, Any] = {
        "name": metric.name,
        "description": metric.description,
        "unit": metric.unit,
    }
    data = metric.data
    if isinstance(data, Gauge):
        kwargs["gauge"] = metrics_pb2.Gauge(
            data_points=[_number_point(p) for p in data.data_points]
        )
    elif isinstance(data, Sum):
        kwargs["sum"] = metrics_pb2.Sum(
            data_points=[_number_point(p) for p in data.data_points],
            aggregation_temporality=int(data.aggregation_temporality),
            is_monotonic=data.is_monotonic,
        )
    elif isinstance(data, Histogram):
        kwargs["histogram"] = metrics_pb2.Histogram(
            aggregation_temporality=int(data.aggregation_temporality),
            data_points=[
                metrics_pb2.HistogramDataPoint(
                    attributes=_key_values(p.attributes),
                    time_unix_nano=p.time_unix_nano,
                    start_time_unix_nano=p.start_time_unix_nano or 0,
                    count=p.count,
                    sum=p.sum,
                    bucket_counts=list(p.bucket_counts),
                    explicit_bounds=list(p.explicit_bounds),
                )
                for p in data.data_points
            ],
        )
    else:  # pragma: no cover - exhaustive over MetricData
        raise TypeError(f"unsupported metric data type: {type(data)!r}")
    return metrics_pb2.Metric(**kwargs)


def _encode_metrics(data: Sequence[ResourceMetrics]) -> bytes:
    from opentelemetry.proto.collector.metrics.v1 import metrics_service_pb2
    from opentelemetry.proto.metrics.v1 import metrics_pb2

    request = metrics_service_pb2.ExportMetricsServiceRequest(
        resource_metrics=[
            metrics_pb2.ResourceMetrics(
                resource=_resource(rm.resource),
                scope_metrics=[
                    metrics_pb2.ScopeMetrics(
                        scope=_scope(sm.scope),
                        metrics=[_metric(m) for m in sm.metrics],
                    )
                    for sm in rm.scope_metrics
                ],
            )
            for rm in data
        ]
    )
    return request.SerializeToString()


def _log_record(record: LogRecord) -> Any:
    from opentelemetry.proto.logs.v1 import logs_pb2

    return logs_pb2.LogRecord(
        time_unix_nano=record.time_unix_nano,
        observed_time_unix_nano=record.observed_time_unix_nano,
        severity_number=int(record.severity_number),
        severity_text=record.severity_text,
        body=_any_value(record.body),
        attributes=_key_values(record.attributes),
        trace_id=record.trace_id or b"",
        span_id=record.span_id or b"",
        flags=record.flags,
    )


def _encode_logs(data: Sequence[ResourceLogs]) -> bytes:
    from opentelemetry.proto.collector.logs.v1 import logs_service_pb2
    from opentelemetry.proto.logs.v1 import logs_pb2

    request = logs_service_pb2.ExportLogsServiceRequest(
        resource_logs=[
            logs_pb2.ResourceLogs(
                resource=_resource(rl.resource),
                scope_logs=[
                    logs_pb2.ScopeLogs(
                        scope=_scope(sl.scope),
                        log_records=[_log_record(r) for r in sl.log_records],
                    )
                    for sl in rl.scope_logs
                ],
            )
            for rl in data
        ]
    )
    return request.SerializeToString()


def _span(item: Span) -> Any:
    from opentelemetry.proto.trace.v1 import trace_pb2

    kwargs: dict[str, Any] = {
        "trace_id": item.trace_id,
        "span_id": item.span_id,
        "parent_span_id": item.parent_span_id or b"",
        "name": item.name,
        "kind": int(item.kind),
        "start_time_unix_nano": item.start_time_unix_nano,
        "end_time_unix_nano": item.end_time_unix_nano,
        "attributes": _key_values(item.attributes),
        "events": [
            trace_pb2.Span.Event(
                time_unix_nano=e.time_unix_nano,
                name=e.name,
                attributes=_key_values(e.attributes),
            )
            for e in item.events
        ],
        "links": [
            trace_pb2.Span.Link(
                trace_id=link.trace_id,
                span_id=link.span_id,
                attributes=_key_values(link.attributes),
            )
            for link in item.links
        ],
    }
    if item.status is not None:
        kwargs["status"] = trace_pb2.Status(
            code=int(item.status.code), message=item.status.message
        )
    return trace_pb2.Span(**kwargs)


def _encode_traces(data: Sequence[ResourceSpans]) -> bytes:
    from opentelemetry.proto.collector.trace.v1 import trace_service_pb2
    from opentelemetry.proto.trace.v1 import trace_pb2

    request = trace_service_pb2.ExportTraceServiceRequest(
        resource_spans=[
            trace_pb2.ResourceSpans(
                resource=_resource(rs.resource),
                scope_spans=[
                    trace_pb2.ScopeSpans(
                        scope=_scope(ss.scope), spans=[_span(s) for s in ss.spans]
                    )
                    for ss in rs.scope_spans
                ],
            )
            for rs in data
        ]
    )
    return request.SerializeToString()
```

- [ ] **Step 4: Run tests and lint**

Run: `uv run pytest tests/test_encoding_protobuf.py -v && uv run mypy && uv run ruff check`
Expected: 9 passed, clean

- [ ] **Step 5: Commit**

```bash
git add src/otlp_client/encoding/protobuf.py tests/test_encoding_protobuf.py
git commit -m "feat: protobuf encoder behind the protobuf extra"
```

---

### Task 15: Encoder oracle property tests

**Files:**
- Create: `tests/test_encoder_oracle.py`, `tests/support/strategies.py`
- Test: itself

**Interfaces:**
- Consumes: both encoders, the full model, `hypothesis`, `google.protobuf.json_format`
- Produces: Hypothesis strategies `attributes()`, `resources()`, `scopes()`, `resource_metrics()`, `resource_logs()`, `resource_spans()`

**Why this task exists:** the JSON encoder is hand-written against a spec. This cross-checks it against the canonical schema. If our JSON parses into a real `Export*ServiceRequest` and that message equals what the protobuf encoder produced from the same input, both encoders are correct together. This is what catches int-as-string slips, base64-versus-hex identifier bugs, and enum-name regressions on inputs nobody thought to write a test for.

- [ ] **Step 1: Write the strategies**

Create `tests/support/strategies.py`:

```python
"""Hypothesis strategies over the OTLP model tree."""

from __future__ import annotations

from hypothesis import strategies as st

from otlp_client.model.common import InstrumentationScope, Resource
from otlp_client.model.logs import LogRecord, ResourceLogs, ScopeLogs, SeverityNumber
from otlp_client.model.metrics import (
    AggregationTemporality,
    Gauge,
    Histogram,
    HistogramDataPoint,
    Metric,
    NumberDataPoint,
    ResourceMetrics,
    ScopeMetrics,
    Sum,
)
from otlp_client.model.traces import (
    ResourceSpans,
    ScopeSpans,
    Span,
    SpanEvent,
    SpanKind,
    Status,
    StatusCode,
)

# u64 range; protobuf rejects anything wider.
u64 = st.integers(min_value=0, max_value=2**63 - 1)
i64 = st.integers(min_value=-(2**63), max_value=2**63 - 1)
text = st.text(max_size=20)
# No NaN or infinity: protobuf JSON renders those as strings, which would make
# the comparison test about float formatting rather than about our encoder.
finite = st.floats(allow_nan=False, allow_infinity=False, width=32)

scalars = st.one_of(text, st.booleans(), i64, finite, st.binary(max_size=8))
attributes = st.dictionaries(text, scalars, max_size=4)

resources = st.builds(Resource, attributes=attributes)
scopes = st.builds(
    InstrumentationScope, name=text, version=st.one_of(st.none(), text), attributes=attributes
)

number_points = st.builds(
    NumberDataPoint,
    time_unix_nano=u64,
    value=st.one_of(i64, finite),
    attributes=attributes,
    start_time_unix_nano=st.one_of(st.none(), u64),
)

histogram_points = st.builds(
    HistogramDataPoint,
    time_unix_nano=u64,
    count=u64,
    bucket_counts=st.lists(u64, max_size=3),
    explicit_bounds=st.lists(finite, max_size=2),
    sum=st.one_of(st.none(), finite),
    attributes=attributes,
)

metric_data = st.one_of(
    st.builds(Gauge, data_points=st.lists(number_points, min_size=1, max_size=3)),
    st.builds(
        Sum,
        data_points=st.lists(number_points, min_size=1, max_size=3),
        aggregation_temporality=st.sampled_from(AggregationTemporality),
        is_monotonic=st.booleans(),
    ),
    st.builds(
        Histogram,
        data_points=st.lists(histogram_points, min_size=1, max_size=3),
        aggregation_temporality=st.sampled_from(AggregationTemporality),
    ),
)

metrics = st.builds(Metric, name=text, data=metric_data, description=text, unit=text)

resource_metrics = st.builds(
    ResourceMetrics,
    resource=resources,
    scope_metrics=st.lists(
        st.builds(ScopeMetrics, scope=scopes, metrics=st.lists(metrics, min_size=1, max_size=3)),
        min_size=1,
        max_size=2,
    ),
)

log_records = st.builds(
    LogRecord,
    time_unix_nano=u64,
    observed_time_unix_nano=u64,
    body=scalars,
    severity_number=st.sampled_from(SeverityNumber),
    severity_text=text,
    attributes=attributes,
    trace_id=st.one_of(st.none(), st.binary(min_size=16, max_size=16)),
    span_id=st.one_of(st.none(), st.binary(min_size=8, max_size=8)),
)

resource_logs = st.builds(
    ResourceLogs,
    resource=resources,
    scope_logs=st.lists(
        st.builds(
            ScopeLogs, scope=scopes, log_records=st.lists(log_records, min_size=1, max_size=3)
        ),
        min_size=1,
        max_size=2,
    ),
)

spans = st.builds(
    Span,
    trace_id=st.binary(min_size=16, max_size=16),
    span_id=st.binary(min_size=8, max_size=8),
    name=text,
    start_time_unix_nano=u64,
    end_time_unix_nano=u64,
    kind=st.sampled_from(SpanKind),
    parent_span_id=st.one_of(st.none(), st.binary(min_size=8, max_size=8)),
    attributes=attributes,
    events=st.lists(
        st.builds(SpanEvent, time_unix_nano=u64, name=text, attributes=attributes), max_size=2
    ),
    status=st.one_of(
        st.none(), st.builds(Status, code=st.sampled_from(StatusCode), message=text)
    ),
)

resource_spans = st.builds(
    ResourceSpans,
    resource=resources,
    scope_spans=st.lists(
        st.builds(ScopeSpans, scope=scopes, spans=st.lists(spans, min_size=1, max_size=3)),
        min_size=1,
        max_size=2,
    ),
)
```

- [ ] **Step 2: Write the oracle test**

Create `tests/test_encoder_oracle.py`:

```python
"""Cross-check the hand-written JSON encoder against the canonical proto schema."""

import pytest

pytest.importorskip("opentelemetry.proto")

from google.protobuf import json_format
from hypothesis import HealthCheck, given, settings
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from otlp_client.encoding.json import JSONEncoder
from otlp_client.encoding.protobuf import build_protobuf_encoder
from otlp_client.signals import SignalKind
from tests.support import strategies as s

SETTINGS = settings(
    max_examples=200, suppress_health_check=[HealthCheck.too_slow], deadline=None
)


def assert_encoders_agree(kind: SignalKind, request_type, envelope) -> None:
    """Both encoders must describe the same message.

    The JSON is parsed by the official protobuf JSON parser, which enforces the
    schema: wrong key casing, an enum name where an integer belongs, a bare
    number where a decimal string belongs, or base64 where hex belongs all fail
    here rather than silently at a collector.
    """
    json_bytes = JSONEncoder().encode(kind, [envelope])
    proto_bytes = build_protobuf_encoder().encode(kind, [envelope])

    from_json = json_format.Parse(json_bytes.decode("utf-8"), request_type())
    from_proto = request_type.FromString(proto_bytes)
    assert from_json == from_proto


@given(envelope=s.resource_metrics)
@SETTINGS
def test_metrics_encoders_agree(envelope):
    assert_encoders_agree(SignalKind.METRICS, ExportMetricsServiceRequest, envelope)


@given(envelope=s.resource_logs)
@SETTINGS
def test_logs_encoders_agree(envelope):
    assert_encoders_agree(SignalKind.LOGS, ExportLogsServiceRequest, envelope)


@given(envelope=s.resource_spans)
@SETTINGS
def test_traces_encoders_agree(envelope):
    assert_encoders_agree(SignalKind.TRACES, ExportTraceServiceRequest, envelope)
```

- [ ] **Step 3: Run the oracle**

Run: `uv run pytest tests/test_encoder_oracle.py -v`
Expected: 3 passed. If any fail, the failure is a **real encoder bug** — Hypothesis prints the minimal input. Fix `encoding/json.py` (or `encoding/protobuf.py`) rather than weakening the strategy. The likely culprits, in order: a 64-bit field emitted as a number instead of a string, a `traceId`/`spanId` emitted as base64, an enum emitted as a name, or a field omitted by `omit_empty` that protobuf treats as meaningful.

- [ ] **Step 4: Run the full suite and lint**

Run: `uv run pytest -v && uv run mypy && uv run ruff check`
Expected: everything passes, clean

- [ ] **Step 5: Commit**

```bash
git add tests/support/strategies.py tests/test_encoder_oracle.py
git commit -m "test: property-based encoder oracle against the canonical proto schema"
```

---

### Task 16: gRPC transport (`[grpc]` extra)

**Files:**
- Create: `src/otlp_client/transport/grpc.py`
- Test: `tests/test_transport_grpc.py`

**Interfaces:**
- Consumes: `OTLPConfig`, `Encoder`, outcomes
- Produces: `async GRPCTransport.create(config, encoder) -> GRPCTransport`; implements `Transport`

**Notes:**
- **`[grpc]` implies `[protobuf]`.** OTLP over gRPC has no JSON encoding, so the extra installs both and `create()` rejects `OTLPProtocol.HTTP_JSON`-style JSON encoders.
- All `grpc` imports are lazy, same rule as Task 14.
- The channel ships **raw bytes**: identity serializers let the encoder stay the single owner of the wire format.
- `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED` are retryable; everything else is permanent.

- [ ] **Step 1: Write the failing test**

Create `tests/test_transport_grpc.py`:

```python
import pytest

pytest.importorskip("grpc")

import grpc
from grpc import aio
from otlp_client.config import OTLPConfig, OTLPProtocol
from otlp_client.encoding.protobuf import build_protobuf_encoder
from otlp_client.errors import OTLPConfigError
from otlp_client.outcomes import Permanent, Retryable, Success
from otlp_client.signals import SignalKind
from otlp_client.transport.grpc import GRPCTransport

METRICS_METHOD = "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export"


class EchoHandler(grpc.GenericRpcHandler):
    """Answers any method with a fixed response or a fixed error."""

    def __init__(self, response: bytes = b"", code: grpc.StatusCode | None = None):
        self.response, self.code = response, code
        self.received: list[tuple[str, bytes]] = []

    def service(self, details):
        async def handle(request: bytes, context) -> bytes:
            self.received.append((details.method, request))
            if self.code is not None:
                await context.abort(self.code, "scripted failure")
            return self.response

        return grpc.unary_unary_rpc_method_handler(
            handle, request_deserializer=lambda b: b, response_serializer=lambda b: b
        )


@pytest.fixture
async def grpc_server():
    servers: list[aio.Server] = []

    async def start(handler: EchoHandler) -> str:
        server = aio.server()
        server.add_generic_rpc_handlers((handler,))
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        servers.append(server)
        return f"127.0.0.1:{port}"

    yield start
    for server in servers:
        await server.stop(None)


async def make_transport(target: str) -> GRPCTransport:
    config = OTLPConfig(endpoint=f"http://{target}", protocol=OTLPProtocol.GRPC)
    return await GRPCTransport.create(config, build_protobuf_encoder())


async def test_export_hits_the_metrics_service_method(grpc_server):
    handler = EchoHandler()
    transport = await make_transport(await grpc_server(handler))
    result = await transport.send(SignalKind.METRICS, b"payload-bytes")
    assert isinstance(result, Success)
    assert handler.received == [(METRICS_METHOD, b"payload-bytes")]
    await transport.aclose()


async def test_logs_and_traces_use_their_own_methods(grpc_server):
    handler = EchoHandler()
    transport = await make_transport(await grpc_server(handler))
    await transport.send(SignalKind.LOGS, b"a")
    await transport.send(SignalKind.TRACES, b"b")
    methods = [method for method, _ in handler.received]
    assert methods == [
        "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
        "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
    ]
    await transport.aclose()


@pytest.mark.parametrize(
    "code",
    [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED,
     grpc.StatusCode.RESOURCE_EXHAUSTED],
)
async def test_transient_status_codes_are_retryable(grpc_server, code):
    transport = await make_transport(await grpc_server(EchoHandler(code=code)))
    assert isinstance(await transport.send(SignalKind.METRICS, b"x"), Retryable)
    await transport.aclose()


@pytest.mark.parametrize(
    "code",
    [grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.PERMISSION_DENIED,
     grpc.StatusCode.UNIMPLEMENTED],
)
async def test_other_status_codes_are_permanent(grpc_server, code):
    transport = await make_transport(await grpc_server(EchoHandler(code=code)))
    assert isinstance(await transport.send(SignalKind.METRICS, b"x"), Permanent)
    await transport.aclose()


async def test_partial_success_response_is_decoded(grpc_server):
    from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
        ExportMetricsServiceResponse,
    )

    response = ExportMetricsServiceResponse()
    response.partial_success.rejected_data_points = 4
    handler = EchoHandler(response=response.SerializeToString())
    transport = await make_transport(await grpc_server(handler))
    result = await transport.send(SignalKind.METRICS, b"x")
    assert getattr(result, "rejected", None) == 4
    await transport.aclose()


async def test_unreachable_server_is_retryable():
    config = OTLPConfig(endpoint="http://127.0.0.1:1", protocol=OTLPProtocol.GRPC, timeout=1.0)
    transport = await GRPCTransport.create(config, build_protobuf_encoder())
    assert isinstance(await transport.send(SignalKind.METRICS, b"x"), Retryable)
    await transport.aclose()


async def test_json_encoder_is_rejected():
    from otlp_client.encoding.json import JSONEncoder

    config = OTLPConfig(endpoint="http://127.0.0.1:4317", protocol=OTLPProtocol.GRPC)
    with pytest.raises(OTLPConfigError, match="protobuf"):
        await GRPCTransport.create(config, JSONEncoder())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_transport_grpc.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'otlp_client.transport.grpc'`

- [ ] **Step 3: Write the gRPC transport**

Create `src/otlp_client/transport/grpc.py`:

```python
"""OTLP/gRPC transport. Requires the `grpc` extra.

All `grpc` imports are lazy: importing this module must stay free for a
core-only install.
"""

from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import urlparse

from otlp_client.config import OTLPConfig
from otlp_client.encoding.base import Encoder
from otlp_client.errors import OTLPConfigError
from otlp_client.outcomes import ExportOutcome, Permanent, Retryable, Success
from otlp_client.signals import SignalKind

_MISSING = (
    "the gRPC transport needs the optional extra: pip install 'asyncio-otlp-client[grpc]'"
)

_METHODS: dict[SignalKind, str] = {
    SignalKind.METRICS: "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
    SignalKind.LOGS: "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
    SignalKind.TRACES: "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
    SignalKind.PROFILES: (
        "/opentelemetry.proto.collector.profiles.v1development.ProfilesService/Export"
    ),
}


def _target(endpoint: str) -> tuple[str, bool]:
    """Split an endpoint into a gRPC target and whether it is plaintext."""
    parsed = urlparse(endpoint if "//" in endpoint else f"//{endpoint}")
    host = parsed.netloc or parsed.path
    return host, parsed.scheme != "https"


def _read_credentials(config: OTLPConfig) -> Any:
    """Build channel credentials. Blocking: only call via asyncio.to_thread."""
    import grpc

    def read(path: str | None) -> bytes | None:
        if not path:
            return None
        with open(path, "rb") as handle:
            return handle.read()

    return grpc.ssl_channel_credentials(
        root_certificates=read(config.certificate_file),
        private_key=read(config.client_key_file),
        certificate_chain=read(config.client_certificate_file),
    )


class GRPCTransport:
    """Ships already-encoded protobuf bytes over an asyncio gRPC channel."""

    def __init__(self, config: OTLPConfig, encoder: Encoder, channel: Any) -> None:
        self._config = config
        self._encoder = encoder
        self._channel = channel

    @classmethod
    async def create(cls, config: OTLPConfig, encoder: Encoder) -> GRPCTransport:
        """Open a channel, doing all blocking credential loading off the loop."""
        if encoder.content_type != "application/x-protobuf":
            raise OTLPConfigError(
                "OTLP over gRPC has no JSON encoding; use the protobuf encoder"
            )
        try:
            from grpc import aio
        except ImportError as exc:
            raise OTLPConfigError(_MISSING) from exc

        target, plaintext = _target(config.endpoint)
        options = [("grpc.primary_user_agent", "asyncio-otlp-client")]
        if plaintext:
            channel = aio.insecure_channel(target, options=options)
        else:
            credentials = await asyncio.to_thread(_read_credentials, config)
            channel = aio.secure_channel(target, credentials, options=options)
        return cls(config, encoder, channel)

    def _classify(self, exc: Any) -> ExportOutcome:
        import grpc

        retryable = {
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
        }
        code = exc.code()
        message = exc.details() or str(code)
        if code in retryable:
            return Retryable(message=message, retry_after=_pushback(exc))
        return Permanent(message=message)

    async def send(self, kind: SignalKind, payload: bytes) -> ExportOutcome:
        from grpc.aio import AioRpcError

        call = self._channel.unary_unary(
            _METHODS[kind],
            request_serializer=lambda value: value,
            response_deserializer=lambda value: value,
        )
        metadata = tuple(self._config.headers.items())
        try:
            raw = await call(payload, timeout=self._config.timeout, metadata=metadata)
        except AioRpcError as exc:
            return self._classify(exc)
        partial = self._encoder.decode_response(kind, raw)
        return partial if partial is not None else Success()

    async def aclose(self) -> None:
        await self._channel.close()


def _pushback(exc: Any) -> float | None:
    """Read the server's `grpc-retry-pushback-ms` hint, if it sent one."""
    for key, value in exc.trailing_metadata() or ():
        if key == "grpc-retry-pushback-ms":
            try:
                return max(0.0, float(value) / 1000.0)
            except (TypeError, ValueError):
                return None
    return None
```

- [ ] **Step 4: Run tests and lint**

Run: `uv run pytest tests/test_transport_grpc.py -v && uv run mypy && uv run ruff check`
Expected: 13 passed (parametrized cases included), clean

- [ ] **Step 5: Commit**

```bash
git add src/otlp_client/transport/grpc.py tests/test_transport_grpc.py
git commit -m "feat: asyncio gRPC transport behind the grpc extra"
```

---

### Task 17: Core-only guarantee, integration suite, and CI

**Files:**
- Create: `tests/test_core_only.py`, `tests/integration/__init__.py`, `tests/integration/test_collector.py`, `docker-compose.test.yml`, `otel-collector-config.yaml`, `.github/workflows/ci.yml`
- Test: the files above

**Interfaces:**
- Consumes: the whole library
- Produces: a CI job matrix that fails if an optional extra leaks into the core import path

**Why the core-only job matters:** the pure-Python core install is the entire reason a Home Assistant integration can depend on this library without touching the HA wheel builder. A stray top-level `import grpc` would break that silently on a user's machine. This job makes it break in CI instead.

- [ ] **Step 1: Write the core-only guard test**

Create `tests/test_core_only.py`:

```python
"""Guards the promise that the core install needs only aiohttp."""

import subprocess
import sys

FORBIDDEN = ("grpc", "google.protobuf", "opentelemetry")

SCRIPT = """
import sys
import otlp_client
from otlp_client import OTLPClient, OTLPConfig, gauge  # noqa: F401
from otlp_client.encoding.json import JSONEncoder
from otlp_client.processor import BatchProcessor  # noqa: F401

payload = JSONEncoder().encode(
    otlp_client.SignalKind.METRICS,
    [otlp_client.ResourceMetrics(
        resource=otlp_client.Resource(attributes={"a": "b"}),
        scope_metrics=[otlp_client.ScopeMetrics(
            scope=otlp_client.InstrumentationScope(name="t"),
            metrics=[gauge("m", 1.0, time_unix_nano=1)])])],
)
assert b'"resourceMetrics"' in payload
leaked = sorted(m for m in sys.modules if m.split(".")[0] in {"grpc", "opentelemetry"})
assert not leaked, f"optional extras leaked into the core import path: {leaked}"
print("core-only OK")
"""


def test_core_import_path_does_not_touch_optional_extras():
    """Runs in a subprocess so already-imported test dependencies cannot mask a leak."""
    result = subprocess.run(
        [sys.executable, "-c", SCRIPT], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    assert "core-only OK" in result.stdout
```

- [ ] **Step 2: Run it**

Run: `uv run pytest tests/test_core_only.py -v`
Expected: PASS. If it fails, a module reachable from `import otlp_client` imports an extra at top level — move that import inside the function that needs it.

- [ ] **Step 3: Write the collector configuration**

Create `otel-collector-config.yaml`:

```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
      grpc:
        endpoint: 0.0.0.0:4317

exporters:
  file:
    path: /out/telemetry.json

service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [file]
    logs:
      receivers: [otlp]
      exporters: [file]
    traces:
      receivers: [otlp]
      exporters: [file]
```

Create `docker-compose.test.yml`:

```yaml
services:
  collector:
    image: otel/opentelemetry-collector-contrib:latest
    command: ["--config=/etc/otel-collector-config.yaml"]
    volumes:
      - ./otel-collector-config.yaml:/etc/otel-collector-config.yaml:ro
      - ./out:/out
    ports:
      - "4317:4317"
      - "4318:4318"
```

- [ ] **Step 4: Write the integration test**

Create `tests/integration/__init__.py` (empty) and `tests/integration/test_collector.py`:

```python
"""End-to-end checks against a real collector.

Marked `integration` and excluded by default (see addopts in pyproject.toml).
Run with: docker compose -f docker-compose.test.yml up -d
          uv run pytest -m integration
"""

import asyncio
import json
import pathlib

import pytest
from aiohttp import ClientSession
from otlp_client.client import OTLPClient
from otlp_client.config import OTLPConfig, OTLPProtocol
from otlp_client.model.common import Resource
from otlp_client.model.logs import SeverityNumber, log_record
from otlp_client.model.metrics import gauge
from otlp_client.model.traces import span
from otlp_client.outcomes import Success

pytestmark = pytest.mark.integration

OUTPUT = pathlib.Path("out/telemetry.json")
RESOURCE = Resource(attributes={"service.name": "asyncio-otlp-client-itest"})
TRACE_ID = bytes.fromhex("0102030405060708090a0b0c0d0e0f10")
SPAN_ID = bytes.fromhex("1112131415161718")


async def wait_for(predicate, timeout: float = 15.0) -> str:
    """Poll the collector's file exporter until it contains what we sent."""
    async with asyncio.timeout(timeout):
        while True:
            text = OUTPUT.read_text() if OUTPUT.exists() else ""
            if predicate(text):
                return text
            await asyncio.sleep(0.2)


async def export_all(config: OTLPConfig, marker: str) -> None:
    async with ClientSession() as session:
        client = await OTLPClient.create(config, session=session)
        assert isinstance(
            await client.export_metrics([gauge(f"m.{marker}", 21.5, time_unix_nano=1)]), Success
        )
        assert isinstance(
            await client.export_logs(
                [log_record(f"log-{marker}", time_unix_nano=1, severity=SeverityNumber.INFO)]
            ),
            Success,
        )
        assert isinstance(
            await client.export_traces([
                span(f"span-{marker}", trace_id=TRACE_ID, span_id=SPAN_ID,
                     start_time_unix_nano=1, end_time_unix_nano=2)
            ]),
            Success,
        )
        await client.aclose()


async def test_json_over_http_is_accepted_by_a_real_collector():
    config = OTLPConfig(
        endpoint="http://localhost:4318", protocol=OTLPProtocol.HTTP_JSON, resource=RESOURCE
    )
    await export_all(config, "json")
    text = await wait_for(lambda t: "m.json" in t and "log-json" in t and "span-json" in t)
    assert json.loads(text.splitlines()[0])


async def test_protobuf_over_http_is_accepted():
    config = OTLPConfig(
        endpoint="http://localhost:4318", protocol=OTLPProtocol.HTTP_PROTOBUF, resource=RESOURCE
    )
    await export_all(config, "pb")
    await wait_for(lambda t: "m.pb" in t and "log-pb" in t and "span-pb" in t)


async def test_grpc_is_accepted():
    pytest.importorskip("grpc")
    config = OTLPConfig(
        endpoint="http://localhost:4317", protocol=OTLPProtocol.GRPC, resource=RESOURCE
    )
    client = await OTLPClient.create(config)
    assert isinstance(
        await client.export_metrics([gauge("m.grpc", 1.0, time_unix_nano=1)]), Success
    )
    await client.aclose()
    await wait_for(lambda t: "m.grpc" in t)
```

- [ ] **Step 5: Write the CI workflow**

Create `.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        python-version: ["3.12", "3.13", "3.14"]
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
        with:
          python-version: ${{ matrix.python-version }}
      - run: uv sync --all-extras
      - run: uv run ruff check
      - run: uv run ruff format --check
      - run: uv run mypy
      - run: uv run pytest -v

  core-only:
    # Proves the core install needs nothing but aiohttp. If this job fails, an
    # optional extra leaked into the core import path.
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
        with:
          python-version: "3.14"
      - name: Install the package with no extras
        run: |
          uv venv
          uv pip install . pytest pytest-asyncio
      - name: Verify no optional extra is installed
        run: |
          ! uv pip show grpcio 2>/dev/null
          ! uv pip show protobuf 2>/dev/null
      - name: Run the core-only suite
        run: |
          uv run pytest tests/test_core_only.py tests/test_encoding_json_metrics.py \
                        tests/test_client.py tests/test_processor.py -v

  integration:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
        with:
          python-version: "3.14"
      - run: uv sync --all-extras
      - name: Start the collector
        run: |
          mkdir -p out && chmod 777 out
          docker compose -f docker-compose.test.yml up -d
      - name: Wait for the collector to accept connections
        run: |
          for _ in $(seq 1 30); do
            curl -sf -X POST http://localhost:4318/v1/metrics \
              -H 'Content-Type: application/json' -d '{}' && break
            sleep 1
          done
      - run: uv run pytest -m integration -v
      - if: always()
        run: docker compose -f docker-compose.test.yml logs
```

- [ ] **Step 6: Add the out directory to .gitignore**

Append to `.gitignore`:

```
out/
```

- [ ] **Step 7: Verify locally**

Run:
```bash
mkdir -p out && docker compose -f docker-compose.test.yml up -d
uv run pytest -m integration -v
docker compose -f docker-compose.test.yml down
```
Expected: 3 passed (the gRPC case skips if the extra is absent)

- [ ] **Step 8: Commit**

```bash
git add tests/test_core_only.py tests/integration docker-compose.test.yml \
        otel-collector-config.yaml .github/workflows/ci.yml .gitignore
git commit -m "test: core-only guarantee, collector integration suite, and CI"
```

---

### Task 18: Documentation

**Files:**
- Create: `README.md`, `docs/home-assistant.md`
- Test: none (documentation)

**Interfaces:**
- Consumes: the finished public API
- Produces: user-facing documentation

**Constraint:** the Home Assistant example lives in documentation only. No file under `src/` may import `homeassistant`.

- [ ] **Step 1: Write the README**

Create `README.md`:

````markdown
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
dropped. When the queue is full the oldest record is discarded. Check
`proc.stats` for `submitted`, `exported`, `dropped`, `consecutive_failures`, and
`last_error`.

## Configuration

`OTLPConfig` is the only source of settings. To read the standard environment
variables instead, opt in explicitly:

```python
config = OTLPConfig.from_env()  # OTEL_EXPORTER_OTLP_*
```

## Scope

This is a client, not an SDK. It owns the data model, encoding, transport,
retry, and batching. It does not provide `Tracer`/`Meter`/`Logger` APIs, does
not aggregate metrics, and does not instrument anything — you construct data
points and hand them over.

The profiles signal is defined as a seam (`SignalKind.PROFILES` carries its
`/v1development/profiles` path) but is not encoded yet; it remains in
development upstream.

## Home Assistant

See [docs/home-assistant.md](docs/home-assistant.md). The core install is pure
Python and publishes a `py3-none-any` wheel, so it installs on every Home
Assistant architecture with no wheel-builder involvement.
````

- [ ] **Step 2: Write the Home Assistant guide**

Create `docs/home-assistant.md`:

````markdown
# Using asyncio-otlp-client in a Home Assistant integration

This library never imports `homeassistant`. This page is the glue.

## Why the core install works in HA

Home Assistant ships `aiohttp` and `orjson` but **neither `protobuf` nor
`grpcio`**. A core install of this library adds no new packages inside HA, and
because it is pure Python it publishes a `py3-none-any` wheel — no musllinux
tags, no native compilation, no involvement from the HA wheel builder.

Add it to `manifest.json` with no extras:

```json
{
  "domain": "my_integration",
  "requirements": ["asyncio-otlp-client==0.1.0"]
}
```

Installing the `[grpc]` extra pulls a native `grpcio` wheel into the HA
environment. That works on aarch64 and x86_64 but forfeits the property above;
prefer OTLP/HTTP inside HA.

## Wiring it into a config entry

Two HA rules shape this code:

1. **Never create your own `ClientSession`** — use `async_get_clientsession(hass)`.
   `OTLPClient.create()` takes the session you pass and only builds one if you
   pass none.
2. **Own your background tasks** — start the processor in `async_setup_entry`
   and stop it in `async_unload_entry`, so a reload does not leak a flush task.

```python
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from otlp_client import BatchProcessor, OTLPClient, OTLPConfig, Resource

type MyConfigEntry = ConfigEntry[BatchProcessor]


async def async_setup_entry(hass: HomeAssistant, entry: MyConfigEntry) -> bool:
    config = OTLPConfig(
        endpoint=entry.data["endpoint"],
        headers={"api-key": entry.data["api_key"]},
        resource=Resource(attributes={
            "service.name": "home-assistant",
            "service.instance.id": hass.config.location_name,
        }),
    )
    client = await OTLPClient.create(config, session=async_get_clientsession(hass))
    processor = BatchProcessor(client, flush_interval=10.0, max_queue=4096)
    await processor.__aenter__()
    entry.runtime_data = processor

    entry.async_on_unload(lambda: hass.async_create_task(processor.__aexit__(None, None, None)))
    return True
```

## Feeding state changes as metrics

`submit_metrics` never blocks and never raises, which is what makes it safe to
call from a state-change listener:

```python
import time

from homeassistant.core import Event, EventStateChangedData
from homeassistant.helpers.event import async_track_state_change_event
from otlp_client import gauge


def _handle(event: Event[EventStateChangedData]) -> None:
    new_state = event.data["new_state"]
    if new_state is None:
        return
    try:
        value = float(new_state.state)
    except ValueError:
        return  # non-numeric states are not metrics
    processor.submit_metrics([
        gauge(
            "homeassistant.state",
            value,
            time_unix_nano=time.time_ns(),
            unit=new_state.attributes.get("unit_of_measurement", ""),
            attributes={
                "entity_id": new_state.entity_id,
                "domain": new_state.domain,
            },
        )
    ])


entry.async_on_unload(
    async_track_state_change_event(hass, ["sensor.living_room"], _handle)
)
```

## Surfacing client health

`processor.stats` is a plain frozen dataclass, which makes a natural diagnostic
sensor:

```python
stats = processor.stats
attributes = {
    "submitted": stats.submitted,
    "exported": stats.exported,
    "dropped": stats.dropped,
    "consecutive_failures": stats.consecutive_failures,
    "last_error": stats.last_error,
}
```

A rising `dropped` means the collector is unreachable and the bounded queue is
wrapping. Nothing is written to disk, so telemetry does not survive a restart —
that is deliberate.
````

- [ ] **Step 3: Verify the documented examples match the real API**

Run: `uv run python -c "
from otlp_client import BatchProcessor, OTLPClient, OTLPConfig, Resource, gauge
import inspect
assert 'session' in inspect.signature(OTLPClient.create).parameters
assert 'max_queue' in inspect.signature(BatchProcessor.__init__).parameters
assert 'time_unix_nano' in inspect.signature(gauge).parameters
assert 'resource' in inspect.signature(OTLPConfig.__init__).parameters
print('README and HA guide match the public API')
"`
Expected: `README and HA guide match the public API`

- [ ] **Step 4: Run the whole suite one last time**

Run: `uv run pytest -v && uv run mypy && uv run ruff check && uv run ruff format --check`
Expected: everything passes, clean

- [ ] **Step 5: Commit**

```bash
git add README.md docs/home-assistant.md
git commit -m "docs: README and Home Assistant integration guide"
```

---

### Task 19: ExponentialHistogram and Summary metric types

**Files:**
- Modify: `src/otlp_client/model/metrics.py`, `src/otlp_client/encoding/json.py`, `src/otlp_client/encoding/protobuf.py`, `src/otlp_client/__init__.py`, `tests/support/strategies.py`
- Test: `tests/test_metrics_advanced.py`

**Interfaces:**
- Consumes: the metrics model and both encoders
- Produces: `Buckets(offset, bucket_counts)`; `ExponentialHistogramDataPoint(time_unix_nano, count, scale, zero_count, positive, negative, sum, min, max, attributes, start_time_unix_nano)`; `ExponentialHistogram(data_points, aggregation_temporality)`; `ValueAtQuantile(quantile, value)`; `SummaryDataPoint(time_unix_nano, count, sum, quantile_values, attributes, start_time_unix_nano)`; `Summary(data_points)`; both added to the `MetricData` union

**Why this task exists:** the spec's data model lists five metric types. Tasks 2, 6 and 14 implemented three. This closes the gap. `Summary` has no `aggregation_temporality` field in the proto — it is a legacy type that carries none.

- [ ] **Step 1: Write the failing test**

Create `tests/test_metrics_advanced.py`:

```python
import json

import pytest
from otlp_client.encoding.json import JSONEncoder
from otlp_client.model.common import InstrumentationScope, Resource
from otlp_client.model.metrics import (
    AggregationTemporality,
    Buckets,
    ExponentialHistogram,
    ExponentialHistogramDataPoint,
    Metric,
    ResourceMetrics,
    ScopeMetrics,
    Summary,
    SummaryDataPoint,
    ValueAtQuantile,
)
from otlp_client.signals import SignalKind

EXPONENTIAL = Metric(name="eh", data=ExponentialHistogram(
    aggregation_temporality=AggregationTemporality.DELTA,
    data_points=[ExponentialHistogramDataPoint(
        time_unix_nano=9, count=5, scale=2, zero_count=1, sum=12.5, min=0.5, max=9.0,
        positive=Buckets(offset=3, bucket_counts=[1, 2]),
        negative=Buckets(offset=-1, bucket_counts=[1]),
    )]))

SUMMARY = Metric(name="sm", data=Summary(data_points=[SummaryDataPoint(
    time_unix_nano=9, count=4, sum=8.0,
    quantile_values=[ValueAtQuantile(quantile=0.5, value=2.0)])]))


def encode_json(metric):
    payload = JSONEncoder().encode(SignalKind.METRICS, [ResourceMetrics(
        resource=Resource(attributes={"a": "b"}),
        scope_metrics=[ScopeMetrics(scope=InstrumentationScope(name="t"), metrics=[metric])],
    )])
    return json.loads(payload)["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]


def test_exponential_histogram_json_uses_strings_for_64_bit_fields():
    data = encode_json(EXPONENTIAL)["exponentialHistogram"]
    assert data["aggregationTemporality"] == 1
    (point,) = data["dataPoints"]
    assert point["count"] == "5"
    assert point["zeroCount"] == "1"
    assert point["scale"] == 2
    assert point["positive"] == {"offset": 3, "bucketCounts": ["1", "2"]}
    assert point["negative"] == {"offset": -1, "bucketCounts": ["1"]}
    assert point["sum"] == 12.5
    assert point["min"] == 0.5
    assert point["max"] == 9.0


def test_summary_json_shape():
    data = encode_json(SUMMARY)["summary"]
    assert "aggregationTemporality" not in data
    (point,) = data["dataPoints"]
    assert point["count"] == "4"
    assert point["sum"] == 8.0
    assert point["quantileValues"] == [{"quantile": 0.5, "value": 2.0}]


@pytest.mark.parametrize("metric", [EXPONENTIAL, SUMMARY], ids=["exponential", "summary"])
def test_both_encoders_agree(metric):
    pytest.importorskip("opentelemetry.proto")
    from google.protobuf import json_format
    from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
        ExportMetricsServiceRequest,
    )
    from otlp_client.encoding.protobuf import build_protobuf_encoder

    envelope = ResourceMetrics(
        resource=Resource(attributes={"a": "b"}),
        scope_metrics=[ScopeMetrics(scope=InstrumentationScope(name="t"), metrics=[metric])],
    )
    from_json = json_format.Parse(
        JSONEncoder().encode(SignalKind.METRICS, [envelope]).decode(),
        ExportMetricsServiceRequest(),
    )
    from_proto = ExportMetricsServiceRequest.FromString(
        build_protobuf_encoder().encode(SignalKind.METRICS, [envelope])
    )
    assert from_json == from_proto
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_metrics_advanced.py -v`
Expected: FAIL with `ImportError: cannot import name 'Buckets' from 'otlp_client.model.metrics'`

- [ ] **Step 3: Extend the model**

In `src/otlp_client/model/metrics.py`, add these dataclasses after `Histogram`:

```python
@dataclass(frozen=True, slots=True)
class Buckets:
    """One side of an exponential histogram."""

    offset: int = 0
    bucket_counts: Sequence[int] = ()


@dataclass(frozen=True, slots=True)
class ExponentialHistogramDataPoint:
    time_unix_nano: int
    count: int
    scale: int = 0
    zero_count: int = 0
    positive: Buckets = Buckets()
    negative: Buckets = Buckets()
    sum: float | None = None
    min: float | None = None
    max: float | None = None
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY)
    start_time_unix_nano: int | None = None


@dataclass(frozen=True, slots=True)
class ExponentialHistogram:
    data_points: Sequence[ExponentialHistogramDataPoint]
    aggregation_temporality: AggregationTemporality = AggregationTemporality.CUMULATIVE


@dataclass(frozen=True, slots=True)
class ValueAtQuantile:
    quantile: float
    value: float


@dataclass(frozen=True, slots=True)
class SummaryDataPoint:
    time_unix_nano: int
    count: int
    sum: float = 0.0
    quantile_values: Sequence[ValueAtQuantile] = ()
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY)
    start_time_unix_nano: int | None = None


@dataclass(frozen=True, slots=True)
class Summary:
    """Legacy metric type. The proto carries no aggregation temporality."""

    data_points: Sequence[SummaryDataPoint]
```

Replace the `MetricData` alias with:

```python
type MetricData = Gauge | Sum | Histogram | ExponentialHistogram | Summary
```

- [ ] **Step 4: Extend the JSON encoder**

In `src/otlp_client/encoding/json.py`, extend the metrics model import to include
`Buckets`, `ExponentialHistogram`, `ExponentialHistogramDataPoint`, `Summary`, and
`SummaryDataPoint`, then add these functions above `_encode_metric`:

```python
def _encode_buckets(buckets: Buckets) -> dict[str, Any]:
    return omit_empty(
        {
            "offset": buckets.offset or None,
            "bucketCounts": [u64(c) for c in buckets.bucket_counts],
        }
    )


def _encode_exponential_point(point: ExponentialHistogramDataPoint) -> dict[str, Any]:
    return omit_empty(
        {
            "attributes": encode_attributes(point.attributes),
            "startTimeUnixNano": u64(point.start_time_unix_nano)
            if point.start_time_unix_nano is not None
            else None,
            "timeUnixNano": u64(point.time_unix_nano),
            "count": u64(point.count),
            "sum": point.sum,
            "scale": point.scale or None,
            "zeroCount": u64(point.zero_count) if point.zero_count else None,
            "positive": _encode_buckets(point.positive),
            "negative": _encode_buckets(point.negative),
            "min": point.min,
            "max": point.max,
        }
    )


def _encode_summary_point(point: SummaryDataPoint) -> dict[str, Any]:
    return omit_empty(
        {
            "attributes": encode_attributes(point.attributes),
            "startTimeUnixNano": u64(point.start_time_unix_nano)
            if point.start_time_unix_nano is not None
            else None,
            "timeUnixNano": u64(point.time_unix_nano),
            "count": u64(point.count),
            "sum": point.sum or None,
            "quantileValues": [
                {"quantile": q.quantile, "value": q.value} for q in point.quantile_values
            ],
        }
    )
```

In `_encode_metric`, add these branches before the final `else`:

```python
    elif isinstance(data, ExponentialHistogram):
        body = {
            "exponentialHistogram": {
                "dataPoints": [_encode_exponential_point(p) for p in data.data_points],
                "aggregationTemporality": int(data.aggregation_temporality),
            }
        }
    elif isinstance(data, Summary):
        body = {"summary": {"dataPoints": [_encode_summary_point(p) for p in data.data_points]}}
```

- [ ] **Step 5: Extend the protobuf encoder**

In `src/otlp_client/encoding/protobuf.py`, extend the metrics model import to include
`ExponentialHistogram` and `Summary`, then add these branches in `_metric`, before the
final `else`:

```python
    elif isinstance(data, ExponentialHistogram):
        kwargs["exponential_histogram"] = metrics_pb2.ExponentialHistogram(
            aggregation_temporality=int(data.aggregation_temporality),
            data_points=[
                metrics_pb2.ExponentialHistogramDataPoint(
                    attributes=_key_values(p.attributes),
                    time_unix_nano=p.time_unix_nano,
                    start_time_unix_nano=p.start_time_unix_nano or 0,
                    count=p.count,
                    sum=p.sum,
                    scale=p.scale,
                    zero_count=p.zero_count,
                    positive=metrics_pb2.ExponentialHistogramDataPoint.Buckets(
                        offset=p.positive.offset, bucket_counts=list(p.positive.bucket_counts)
                    ),
                    negative=metrics_pb2.ExponentialHistogramDataPoint.Buckets(
                        offset=p.negative.offset, bucket_counts=list(p.negative.bucket_counts)
                    ),
                    min=p.min,
                    max=p.max,
                )
                for p in data.data_points
            ],
        )
    elif isinstance(data, Summary):
        kwargs["summary"] = metrics_pb2.Summary(
            data_points=[
                metrics_pb2.SummaryDataPoint(
                    attributes=_key_values(p.attributes),
                    time_unix_nano=p.time_unix_nano,
                    start_time_unix_nano=p.start_time_unix_nano or 0,
                    count=p.count,
                    sum=p.sum,
                    quantile_values=[
                        metrics_pb2.SummaryDataPoint.ValueAtQuantile(
                            quantile=q.quantile, value=q.value
                        )
                        for q in p.quantile_values
                    ],
                )
                for p in data.data_points
            ],
        )
```

- [ ] **Step 6: Extend the oracle strategies**

In `tests/support/strategies.py`, import the new types and add them to `metric_data`:

```python
exponential_points = st.builds(
    ExponentialHistogramDataPoint,
    time_unix_nano=u64,
    count=u64,
    scale=st.integers(min_value=-10, max_value=10),
    zero_count=u64,
    positive=st.builds(
        Buckets,
        offset=st.integers(min_value=-100, max_value=100),
        bucket_counts=st.lists(u64, max_size=3),
    ),
    negative=st.builds(
        Buckets,
        offset=st.integers(min_value=-100, max_value=100),
        bucket_counts=st.lists(u64, max_size=3),
    ),
    sum=st.one_of(st.none(), finite),
    min=st.one_of(st.none(), finite),
    max=st.one_of(st.none(), finite),
    attributes=attributes,
)

summary_points = st.builds(
    SummaryDataPoint,
    time_unix_nano=u64,
    count=u64,
    sum=finite,
    quantile_values=st.lists(
        st.builds(
            ValueAtQuantile,
            quantile=st.floats(min_value=0, max_value=1, width=32),
            value=finite,
        ),
        max_size=3,
    ),
    attributes=attributes,
)
```

Then add these two to the `st.one_of(...)` inside `metric_data`:

```python
    st.builds(
        ExponentialHistogram,
        data_points=st.lists(exponential_points, min_size=1, max_size=3),
        aggregation_temporality=st.sampled_from(AggregationTemporality),
    ),
    st.builds(Summary, data_points=st.lists(summary_points, min_size=1, max_size=3)),
```

- [ ] **Step 7: Export the new names**

In `src/otlp_client/__init__.py`, add the new types to the metrics import and to
`__all__`, keeping `__all__` alphabetically sorted:
`Buckets`, `ExponentialHistogram`, `ExponentialHistogramDataPoint`, `Summary`,
`SummaryDataPoint`, `ValueAtQuantile`.

- [ ] **Step 8: Run the full suite and lint**

Run: `uv run pytest -v && uv run mypy && uv run ruff check`
Expected: everything passes. The oracle in `tests/test_encoder_oracle.py` now
also generates exponential histograms and summaries; a failure there is a real
encoder bug in the new branches, not a strategy problem.

- [ ] **Step 9: Commit**

```bash
git add src/otlp_client tests/support/strategies.py tests/test_metrics_advanced.py
git commit -m "feat: exponential histogram and summary metric types"
```
