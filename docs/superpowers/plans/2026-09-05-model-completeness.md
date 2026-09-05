# Model Completeness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add W3C trace context (`trace_state`, `flags`), `event_name`, histogram `min`/`max`, and exemplars to the OTLP data model, so the client can express metrics-to-traces correlation and trace propagation.

**Architecture:** Additive field work on the existing model, followed through both encoders and the Hypothesis strategies that feed the encoder oracle. The model first becomes `kw_only=True` so field order stops being an API concern; new fields are then declared in proto order.

**Tech Stack:** Python 3.12+, `aiohttp`, `uv`, `pytest`, `hypothesis`, `ruff`, `mypy --strict`. Dev-only: `opentelemetry-proto` (the encoder oracle).

**Spec:** `docs/superpowers/specs/2026-09-05-model-completeness-design.md`

## Global Constraints

- Core runtime dependency is `aiohttp` alone. `protobuf`/`grpcio` are optional extras, imported **lazily inside functions** — never at module import time.
- Nothing imports `homeassistant`.
- All model dataclasses are `frozen=True, slots=True`, and after Task 1 also `kw_only=True`.
- Every `Mapping`-typed field uses `field(default=_EMPTY, hash=False)` — `frozen=True` otherwise generates a `__hash__` that raises `TypeError` on the `MappingProxyType` default.
- Every test function needs an explicit `-> None`; `mypy --strict` covers `tests/`.
- `int | float` in a **parameter** annotation must be written `float` (ruff PYI041). In a **field** annotation it stays `int | float` — the encoders branch on `isinstance(v, int)`.
- `mypy --strict`, `ruff check`, and `ruff format --check` must pass before every commit.
- **`flags` are 32-bit** (`fixed32`/`uint32`) and encode as JSON **numbers**. Do not apply the 64-bit decimal-string rule to them. Only `Exemplar.time_unix_nano` (`fixed64`) and `Exemplar.as_int` (`sfixed64`) are 64-bit.
- **`min`/`max` have explicit presence**: emit unconditionally inside `omit_empty`, so only `None` is dropped. Do NOT use a truthiness check — that is what broke `Summary.sum` on `-0.0`.
- Partial success is never retried; OTLP/JSON wire rules (lowerCamelCase, hex `traceId`/`spanId`, integer enums, omitted defaults) are unchanged.

## Sequencing note

The spec names three tasks. This plan uses **four**: the `kw_only` conversion is split into its own task because it is a mechanical, breaking, whole-model change with no new behaviour, and a reviewer should be able to accept or reject it independently of the additive field work that follows.

## File Structure

| File | Change |
|---|---|
| `src/otlp_client/model/common.py` | `kw_only` |
| `src/otlp_client/model/traces.py` | `kw_only`; `trace_state`/`flags` on `Span` and `SpanLink`; span-flag constants + `span_flags()` |
| `src/otlp_client/model/logs.py` | `kw_only`; `event_name` on `LogRecord`; log-flag constant |
| `src/otlp_client/model/metrics.py` | `kw_only`; `flags` on four data-point types; `min`/`max` on `HistogramDataPoint`; `Exemplar`; `exemplars` on three data-point types; data-point-flag constant + `data_point_flags()` |
| `src/otlp_client/encoding/json.py` | encode every new field |
| `src/otlp_client/encoding/protobuf.py` | encode every new field |
| `src/otlp_client/__init__.py` | export new names, `__all__` sorted |
| `tests/support/strategies.py` | generate every new field |
| `tests/test_model_completeness.py` | new, explicit unit tests |

---

### Task 1: Make the model keyword-only

**Files:**
- Modify: `src/otlp_client/model/common.py`, `model/traces.py`, `model/logs.py`, `model/metrics.py`
- Test: `tests/test_model_completeness.py` (create)

**Interfaces:**
- Consumes: nothing
- Produces: every dataclass in `src/otlp_client/model/` declared `@dataclass(frozen=True, slots=True, kw_only=True)`

**Why:** field order stops being an API concern, permanently. OTLP keeps growing; without this, every future field addition either breaks positional construction or forces the model to drift from proto order. Nothing is published to PyPI and there are no consumers, so this is free now and a major version later.

**Scope:** ONLY the four files under `src/otlp_client/model/`. Do **not** touch `config.py`, `outcomes.py`, `processor.py`, or `retry.py` — those are not model types and their construction sites are different.

- [ ] **Step 1: Write the failing test**

Create `tests/test_model_completeness.py`:

```python
"""Tests for the fields added in the model-completeness work."""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from otlp_client.model.common import InstrumentationScope, Resource
from otlp_client.model.logs import LogRecord
from otlp_client.model.metrics import (
    ExponentialHistogramDataPoint,
    HistogramDataPoint,
    NumberDataPoint,
    SummaryDataPoint,
)
from otlp_client.model.traces import Span, SpanEvent, SpanLink

KW_ONLY_TYPES = [
    Resource,
    InstrumentationScope,
    LogRecord,
    NumberDataPoint,
    HistogramDataPoint,
    ExponentialHistogramDataPoint,
    SummaryDataPoint,
    Span,
    SpanEvent,
    SpanLink,
]


@pytest.mark.parametrize("cls", KW_ONLY_TYPES, ids=lambda c: c.__name__)
def test_model_types_are_keyword_only(cls: type) -> None:
    """Every field is kw_only, so field order can never break a caller."""
    assert all(f.kw_only for f in dataclasses.fields(cls)), f"{cls.__name__} has positional fields"


def test_positional_construction_is_rejected() -> None:
    # Routed through an Any-typed alias so mypy does not analyse the call:
    # a `# type: ignore` here would become an `unused-ignore` error instead.
    ctor: Any = Resource
    with pytest.raises(TypeError):
        ctor({"a": "b"})
```

- [ ] **Step 2: Run it and watch it fail**

Run: `uv run pytest tests/test_model_completeness.py -v`
Expected: FAIL — the parametrized cases assert `f.kw_only`, which is currently `False` for every field.

- [ ] **Step 3: Add `kw_only=True` to every model dataclass**

In each of `model/common.py`, `model/traces.py`, `model/logs.py`, `model/metrics.py`, change every occurrence of:

```python
@dataclass(frozen=True, slots=True)
```

to:

```python
@dataclass(frozen=True, slots=True, kw_only=True)
```

There are 2 in `common.py`, 6 in `traces.py`, 3 in `logs.py`, and 14 in `metrics.py`. Change all of them, including the envelope types (`ScopeMetrics`, `ResourceSpans`, and so on) — consistency across the model is the point.

- [ ] **Step 4: Fix any construction site that breaks**

Run the full suite: `uv run pytest -q`

A survey found no positional construction in `src/` or `tests/`, so this should pass unchanged. If anything does break, convert that call site to keyword arguments — do **not** revert `kw_only` on the type. Report in your report exactly which sites you had to change, or state plainly that none did.

- [ ] **Step 5: Verify**

Run: `uv run pytest -q && uv run mypy && uv run ruff check && uv run ruff format --check`
Expected: 183 pre-existing + 11 new tests pass; all clean.

- [ ] **Step 6: Commit**

```bash
git add src/otlp_client/model tests/test_model_completeness.py
git commit -m "refactor(model)!: make every model dataclass keyword-only

Field order stops being an API concern, so future OTLP additions can be
declared in proto order without breaking callers. Nothing is published,
so this costs nothing now."
```

---

### Task 2: Scalar fields — trace_state, flags, event_name, min/max

**Files:**
- Modify: `src/otlp_client/model/traces.py`, `model/logs.py`, `model/metrics.py`, `encoding/json.py`, `encoding/protobuf.py`, `src/otlp_client/__init__.py`, `tests/support/strategies.py`, `tests/test_model_completeness.py`

**Interfaces:**
- Consumes: the `kw_only` model from Task 1
- Produces: `Span.trace_state: str`, `Span.flags: int`, `SpanLink.trace_state: str`, `SpanLink.flags: int`, `LogRecord.event_name: str`, `HistogramDataPoint.min: float | None`, `HistogramDataPoint.max: float | None`, and `flags: int` on `NumberDataPoint`, `HistogramDataPoint`, `ExponentialHistogramDataPoint`, `SummaryDataPoint`; constants `SPAN_FLAGS_TRACE_FLAGS_MASK`, `SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE`, `SPAN_FLAGS_CONTEXT_IS_REMOTE`, `LOG_RECORD_FLAGS_TRACE_FLAGS_MASK`, `DATA_POINT_FLAGS_NO_RECORDED_VALUE`; helpers `span_flags()` and `data_point_flags()`

**Note:** `LogRecord.flags` already exists and is already encoded — do not re-add it.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_model_completeness.py` (and extend the imports at the top of the file to cover the new names):

```python
def test_span_flags_helper_composes_the_w3c_bits() -> None:
    from otlp_client.model.traces import (
        SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE,
        SPAN_FLAGS_CONTEXT_IS_REMOTE,
        span_flags,
    )

    assert span_flags() == 0
    assert span_flags(sampled=True) == 0x01
    # is_remote unknown leaves both context bits clear
    assert span_flags(sampled=True) & SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE == 0
    # known-but-false sets the "has" bit only
    assert span_flags(is_remote=False) == SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE
    assert span_flags(sampled=True, is_remote=True) == (
        0x01 | SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE | SPAN_FLAGS_CONTEXT_IS_REMOTE
    )


def test_data_point_flags_helper() -> None:
    from otlp_client.model.metrics import (
        DATA_POINT_FLAGS_NO_RECORDED_VALUE,
        data_point_flags,
    )

    assert data_point_flags() == 0
    assert data_point_flags(no_recorded_value=True) == DATA_POINT_FLAGS_NO_RECORDED_VALUE


def test_flags_encode_as_json_numbers_not_strings() -> None:
    """flags are 32-bit, so protobuf-JSON renders them as numbers."""
    import json as _json

    from otlp_client.encoding.json import JSONEncoder
    from otlp_client.model.common import InstrumentationScope, Resource
    from otlp_client.model.traces import ResourceSpans, ScopeSpans, Span
    from otlp_client.signals import SignalKind

    payload = JSONEncoder().encode(
        SignalKind.TRACES,
        [
            ResourceSpans(
                resource=Resource(attributes={"a": "b"}),
                scope_spans=[
                    ScopeSpans(
                        scope=InstrumentationScope(name="t"),
                        spans=[
                            Span(
                                trace_id=bytes(range(16)),
                                span_id=bytes(range(8)),
                                name="s",
                                start_time_unix_nano=1,
                                end_time_unix_nano=2,
                                trace_state="vendor=abc",
                                flags=0x101,
                            )
                        ],
                    )
                ],
            )
        ],
    )
    span_json = _json.loads(payload)["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
    assert span_json["flags"] == 0x101
    assert not isinstance(span_json["flags"], str)
    assert span_json["traceState"] == "vendor=abc"


@pytest.mark.parametrize("value", [0.0, -0.0, 2.5])
def test_histogram_min_max_survive_zero_and_negative_zero(value: float) -> None:
    """min/max have explicit presence: only None is omitted, never a zero."""
    import json as _json
    import math

    from otlp_client.encoding.json import JSONEncoder
    from otlp_client.model.common import InstrumentationScope, Resource
    from otlp_client.model.metrics import (
        Histogram,
        HistogramDataPoint,
        Metric,
        ResourceMetrics,
        ScopeMetrics,
    )
    from otlp_client.signals import SignalKind

    point = HistogramDataPoint(
        time_unix_nano=1, count=1, bucket_counts=[1], explicit_bounds=[], min=value, max=value
    )
    payload = JSONEncoder().encode(
        SignalKind.METRICS,
        [
            ResourceMetrics(
                resource=Resource(attributes={"a": "b"}),
                scope_metrics=[
                    ScopeMetrics(
                        scope=InstrumentationScope(name="t"),
                        metrics=[Metric(name="h", data=Histogram(data_points=[point]))],
                    )
                ],
            )
        ],
    )
    doc = _json.loads(payload)["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]
    encoded = doc["histogram"]["dataPoints"][0]
    assert encoded["min"] == value
    assert math.copysign(1.0, encoded["min"]) == math.copysign(1.0, value)


def test_histogram_min_max_omitted_when_none() -> None:
    import json as _json

    from otlp_client.encoding.json import JSONEncoder
    from otlp_client.model.common import InstrumentationScope, Resource
    from otlp_client.model.metrics import (
        Histogram,
        HistogramDataPoint,
        Metric,
        ResourceMetrics,
        ScopeMetrics,
    )
    from otlp_client.signals import SignalKind

    point = HistogramDataPoint(time_unix_nano=1, count=1, bucket_counts=[1], explicit_bounds=[])
    payload = JSONEncoder().encode(
        SignalKind.METRICS,
        [
            ResourceMetrics(
                resource=Resource(attributes={"a": "b"}),
                scope_metrics=[
                    ScopeMetrics(
                        scope=InstrumentationScope(name="t"),
                        metrics=[Metric(name="h", data=Histogram(data_points=[point]))],
                    )
                ],
            )
        ],
    )
    doc = _json.loads(payload)["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]
    encoded = doc["histogram"]["dataPoints"][0]
    assert "min" not in encoded
    assert "max" not in encoded
```

- [ ] **Step 2: Run and watch them fail**

Run: `uv run pytest tests/test_model_completeness.py -v`
Expected: FAIL — `ImportError` for the constants and helpers, and `TypeError` for the unexpected `trace_state`/`flags`/`min`/`max` keyword arguments.

- [ ] **Step 3: Add the trace fields, constants and helper**

In `src/otlp_client/model/traces.py`, add the constants just after the `StatusCode` enum:

```python
# W3C span flags. The low eight bits carry the W3C trace flags (bit 0 is
# "sampled"); bit 8 records that is_remote is known, and bit 9 its value.
SPAN_FLAGS_TRACE_FLAGS_MASK = 0x000000FF
SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE = 0x00000100
SPAN_FLAGS_CONTEXT_IS_REMOTE = 0x00000200
```

Add `trace_state` and `flags` to `SpanLink`, in proto order (after `span_id`, before `attributes`):

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class SpanLink:
    trace_id: bytes
    span_id: bytes
    trace_state: str = ""
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY, hash=False)
    flags: int = 0
```

Add them to `Span` in proto order (`trace_state` after `parent_span_id`, `flags` after it):

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class Span:
    trace_id: bytes
    span_id: bytes
    name: str
    start_time_unix_nano: int
    end_time_unix_nano: int
    kind: SpanKind = SpanKind.UNSPECIFIED
    parent_span_id: bytes | None = None
    trace_state: str = ""
    flags: int = 0
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY, hash=False)
    events: Sequence[SpanEvent] = ()
    links: Sequence[SpanLink] = ()
    status: Status | None = None
```

Add the helper at the bottom of the module, after `span()`:

```python
def span_flags(*, sampled: bool = False, is_remote: bool | None = None) -> int:
    """Build a span's W3C flags.

    `is_remote=None` means "unknown", which leaves both context bits clear —
    that is different from `is_remote=False`, which records that the parent is
    known not to be remote.
    """
    flags = 0x01 if sampled else 0
    if is_remote is not None:
        flags |= SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE
        if is_remote:
            flags |= SPAN_FLAGS_CONTEXT_IS_REMOTE
    return flags
```

Also extend `span()` to accept and pass through `trace_state: str = ""` and `flags: int = 0`.

- [ ] **Step 4: Add the log field and constant**

In `src/otlp_client/model/logs.py`, add above `LogRecord`:

```python
# The low eight bits of a log record's flags carry the W3C trace flags.
LOG_RECORD_FLAGS_TRACE_FLAGS_MASK = 0x000000FF
```

Add `event_name` to `LogRecord` in proto order — it follows `span_id`, before `flags`:

```python
    trace_id: bytes | None = None
    span_id: bytes | None = None
    event_name: str = ""
    flags: int = 0
```

Extend `log_record()` to accept and pass through `event_name: str = ""`.

- [ ] **Step 5: Add the metric fields, constant and helper**

In `src/otlp_client/model/metrics.py`, add above `NumberDataPoint`:

```python
# Set when a data point represents "no measurement was recorded", which is
# distinct from a measurement whose value happens to be zero.
DATA_POINT_FLAGS_NO_RECORDED_VALUE = 0x00000001
```

Add `flags: int = 0` as the last field of `NumberDataPoint`, `ExponentialHistogramDataPoint`, and `SummaryDataPoint`. Add `min`, `max` and `flags` to `HistogramDataPoint`:

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class HistogramDataPoint:
    time_unix_nano: int
    count: int
    bucket_counts: Sequence[int]
    explicit_bounds: Sequence[float]
    sum: float | None = None
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY, hash=False)
    start_time_unix_nano: int | None = None
    flags: int = 0
    min: float | None = None
    max: float | None = None
```

Add the helper at the bottom of the module:

```python
def data_point_flags(*, no_recorded_value: bool = False) -> int:
    """Build a data point's flags."""
    return DATA_POINT_FLAGS_NO_RECORDED_VALUE if no_recorded_value else 0
```

- [ ] **Step 6: Encode the new fields in JSON**

In `src/otlp_client/encoding/json.py`:

In `_encode_span`, add inside the `omit_empty({...})` call — `flags` is **32-bit, so a number, not a `u64()` string**:

```python
            "traceState": item.trace_state,
            "flags": item.flags or None,
```

In `_encode_span_link`, add the same two keys.

In `_encode_log_record`, add `"eventName": record.event_name,`.

In `_encode_number_point`, `_encode_exponential_point` and `_encode_summary_point`, add `"flags": point.flags or None,`.

In `_encode_histogram_point`, add all three. **`min`/`max` are emitted unconditionally** — they have explicit presence, so `omit_empty` dropping only `None` is exactly right, and a truthiness check here would lose a legitimate `0.0` or `-0.0`:

```python
            "flags": point.flags or None,
            "min": point.min,
            "max": point.max,
```

- [ ] **Step 7: Encode the new fields in protobuf**

In `src/otlp_client/encoding/protobuf.py`:

In `_span`'s `kwargs`, add `"trace_state": item.trace_state,` and `"flags": item.flags,`.

In the `trace_pb2.Span.Link(...)` construction inside `_span`, add `trace_state=link.trace_state, flags=link.flags`.

In `_log_record`, add `event_name=record.event_name,`.

In `_number_point`'s `common` dict, add `"flags": point.flags,`.

In `_metric`'s `HistogramDataPoint` construction, add `flags=p.flags, min=p.min, max=p.max`. In the `ExponentialHistogramDataPoint` and `SummaryDataPoint` constructions, add `flags=p.flags`.

- [ ] **Step 8: Extend the oracle strategies**

In `tests/support/strategies.py`, add near the other primitives:

```python
flags = st.integers(min_value=0, max_value=1023)
```

Then: add `trace_state=text, flags=flags` to the `spans` and `span_links` builders; add `event_name=text` to `log_records`; add `flags=flags` to `number_points`, `exponential_points` and `summary_points`; and add `flags=flags, min=st.one_of(st.none(), finite), max=st.one_of(st.none(), finite)` to `histogram_points`.

- [ ] **Step 9: Export the new names**

In `src/otlp_client/__init__.py`, import and add to `__all__` (keeping it sorted): `DATA_POINT_FLAGS_NO_RECORDED_VALUE`, `LOG_RECORD_FLAGS_TRACE_FLAGS_MASK`, `SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE`, `SPAN_FLAGS_CONTEXT_IS_REMOTE`, `SPAN_FLAGS_TRACE_FLAGS_MASK`, `data_point_flags`, `span_flags`.

- [ ] **Step 10: Verify**

Run: `uv run pytest -q && uv run mypy && uv run ruff check && uv run ruff format --check`

**If the encoder oracle fails, it has found a real divergence between the two encoders — fix the encoder, do not weaken the strategy.** The likely causes, in order: `flags` emitted as a string rather than a number; `min`/`max` gated on truthiness so a `0.0` is dropped on one side; or a field added to one encoder and forgotten in the other.

- [ ] **Step 11: Commit**

```bash
git add src/otlp_client tests
git commit -m "feat(model): add trace_state, flags, event_name and histogram min/max"
```

---

### Task 3: Exemplars

**Files:**
- Modify: `src/otlp_client/model/metrics.py`, `encoding/json.py`, `encoding/protobuf.py`, `src/otlp_client/__init__.py`, `tests/support/strategies.py`, `tests/test_model_completeness.py`

**Interfaces:**
- Consumes: the model and encoders as left by Task 2
- Produces: `Exemplar(filtered_attributes, time_unix_nano, value, span_id, trace_id)`; `exemplars: Sequence[Exemplar] = ()` on `NumberDataPoint`, `HistogramDataPoint` and `ExponentialHistogramDataPoint`

**Why this is its own task:** an exemplar is a nested message carrying *both* an `int`/`double` oneof *and* hex trace and span ids — the two rules behind every encoder defect this project has had. Isolating it means a divergence surfaces attached to the thing that caused it.

**`SummaryDataPoint` gets no exemplars** — the proto defines none there. Do not add them.

- [ ] **Step 1: Write the failing tests**

Extend the file's top-level imports with `from collections.abc import Sequence`, then append:

```python
def _encode_number_metric_with(exemplars: Sequence[Any]) -> dict[str, Any]:
    import json as _json

    from otlp_client.encoding.json import JSONEncoder
    from otlp_client.model.common import InstrumentationScope, Resource
    from otlp_client.model.metrics import (
        Gauge,
        Metric,
        NumberDataPoint,
        ResourceMetrics,
        ScopeMetrics,
    )
    from otlp_client.signals import SignalKind

    point = NumberDataPoint(time_unix_nano=1, value=1.5, exemplars=exemplars)
    payload = JSONEncoder().encode(
        SignalKind.METRICS,
        [
            ResourceMetrics(
                resource=Resource(attributes={"a": "b"}),
                scope_metrics=[
                    ScopeMetrics(
                        scope=InstrumentationScope(name="t"),
                        metrics=[Metric(name="g", data=Gauge(data_points=[point]))],
                    )
                ],
            )
        ],
    )
    doc = _json.loads(payload)["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]
    return doc["gauge"]["dataPoints"][0]


def test_exemplar_ids_are_hex_not_base64() -> None:
    from otlp_client.model.metrics import Exemplar

    trace_id = bytes.fromhex("0102030405060708090a0b0c0d0e0f10")
    span_id = bytes.fromhex("1112131415161718")
    point = _encode_number_metric_with(
        [Exemplar(time_unix_nano=7, value=2.5, trace_id=trace_id, span_id=span_id)]
    )
    (ex,) = point["exemplars"]
    assert ex["traceId"] == "0102030405060708090a0b0c0d0e0f10"
    assert ex["spanId"] == "1112131415161718"
    assert ex["timeUnixNano"] == "7"
    assert ex["asDouble"] == 2.5


def test_exemplar_int_value_uses_asInt_as_a_string() -> None:
    from otlp_client.model.metrics import Exemplar

    point = _encode_number_metric_with([Exemplar(time_unix_nano=1, value=42)])
    (ex,) = point["exemplars"]
    assert ex["asInt"] == "42"
    assert "asDouble" not in ex


def test_exemplar_rejects_a_bool_value() -> None:
    from otlp_client.model.metrics import Exemplar

    with pytest.raises(TypeError):
        _encode_number_metric_with([Exemplar(time_unix_nano=1, value=True)])


def test_exemplar_omits_absent_ids_and_attributes() -> None:
    from otlp_client.model.metrics import Exemplar

    point = _encode_number_metric_with([Exemplar(time_unix_nano=1, value=1.0)])
    (ex,) = point["exemplars"]
    assert "traceId" not in ex
    assert "spanId" not in ex
    assert "filteredAttributes" not in ex


def test_exemplars_absent_by_default() -> None:
    point = _encode_number_metric_with([])
    assert "exemplars" not in point
```

- [ ] **Step 2: Run and watch them fail**

Run: `uv run pytest tests/test_model_completeness.py -k exemplar -v`
Expected: FAIL — `ImportError: cannot import name 'Exemplar'`.

- [ ] **Step 3: Add the `Exemplar` dataclass**

In `src/otlp_client/model/metrics.py`, add above `NumberDataPoint` (it is referenced by the data-point types):

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class Exemplar:
    """A sample measurement linked to the trace that produced it.

    `filtered_attributes` carries attributes that are NOT already present on
    the parent data point; duplicating the point's own attributes here is
    what the proto's name is warning against.
    """

    time_unix_nano: int
    value: int | float
    filtered_attributes: Mapping[str, AnyValue] = field(default=_EMPTY, hash=False)
    span_id: bytes | None = None
    trace_id: bytes | None = None
```

Then add `exemplars: Sequence[Exemplar] = ()` as the last field of `NumberDataPoint`, `HistogramDataPoint` and `ExponentialHistogramDataPoint`. **Not** `SummaryDataPoint`.

- [ ] **Step 4: Encode exemplars in JSON**

In `src/otlp_client/encoding/json.py`, extend the metrics-model import to include `Exemplar`, and add this function above `_encode_number_point`:

```python
def _encode_exemplar(exemplar: Exemplar) -> dict[str, Any]:
    # bool before int: bool subclasses int, and a boolean is not a measurement.
    if isinstance(exemplar.value, bool):
        raise TypeError("exemplar values must be int or float, not bool")
    value_field = (
        {"asInt": u64(exemplar.value)}
        if isinstance(exemplar.value, int)
        else {"asDouble": exemplar.value}
    )
    return omit_empty(
        {
            "filteredAttributes": encode_attributes(exemplar.filtered_attributes),
            "timeUnixNano": u64(exemplar.time_unix_nano),
            **value_field,
            # hex, never base64 -- the same rule as span and log record ids.
            "spanId": hex_id(exemplar.span_id) if exemplar.span_id else None,
            "traceId": hex_id(exemplar.trace_id) if exemplar.trace_id else None,
        }
    )
```

Then add `"exemplars": [_encode_exemplar(e) for e in point.exemplars],` inside the `omit_empty({...})` of `_encode_number_point`, `_encode_histogram_point` and `_encode_exponential_point`.

- [ ] **Step 5: Encode exemplars in protobuf**

In `src/otlp_client/encoding/protobuf.py`, extend the metrics-model import to include `Exemplar`, and add above `_number_point`:

```python
def _exemplar(exemplar: Exemplar) -> Any:
    from opentelemetry.proto.metrics.v1 import metrics_pb2

    if isinstance(exemplar.value, bool):
        raise TypeError("exemplar values must be int or float, not bool")
    common = {
        "filtered_attributes": _key_values(exemplar.filtered_attributes),
        "time_unix_nano": exemplar.time_unix_nano,
        "span_id": exemplar.span_id or b"",
        "trace_id": exemplar.trace_id or b"",
    }
    if isinstance(exemplar.value, int):
        return metrics_pb2.Exemplar(as_int=exemplar.value, **common)
    return metrics_pb2.Exemplar(as_double=exemplar.value, **common)
```

Then add `"exemplars": [_exemplar(e) for e in point.exemplars],` to `_number_point`'s `common` dict, and `exemplars=[_exemplar(e) for e in p.exemplars]` to the `HistogramDataPoint` and `ExponentialHistogramDataPoint` constructions in `_metric`.

- [ ] **Step 6: Extend the oracle strategies**

In `tests/support/strategies.py`, import `Exemplar` and add:

```python
exemplars = st.builds(
    Exemplar,
    time_unix_nano=u64,
    value=st.one_of(i64, finite),
    filtered_attributes=attributes,
    span_id=st.one_of(st.none(), st.binary(min_size=8, max_size=8)),
    trace_id=st.one_of(st.none(), st.binary(min_size=16, max_size=16)),
)
```

Then add `exemplars=st.lists(exemplars, max_size=2)` to the `number_points`, `histogram_points` and `exponential_points` builders.

- [ ] **Step 7: Export `Exemplar`**

Add `Exemplar` to the imports and `__all__` in `src/otlp_client/__init__.py`, keeping `__all__` sorted.

- [ ] **Step 8: Verify**

Run: `uv run pytest -q && uv run mypy && uv run ruff check && uv run ruff format --check`

**A property failure here is a real divergence — fix the encoder, never the strategy.** Watch specifically for: exemplar ids emitted as base64 rather than hex; `timeUnixNano` or `asInt` emitted as a number rather than a decimal string; or an exemplar list present on one encoder and absent on the other.

Run the oracle on its own a few times to shake the generator: `uv run pytest tests/test_encoder_oracle.py -v`

- [ ] **Step 9: Commit**

```bash
git add src/otlp_client tests
git commit -m "feat(model): add exemplars to number, histogram and exponential data points"
```

---

### Task 4: Documentation and version bump

**Files:**
- Modify: `README.md`, `docs/home-assistant.md`, `pyproject.toml`, `src/otlp_client/client.py`, `docs/superpowers/specs/2026-09-04-asyncio-otlp-client-design.md`

**Interfaces:**
- Consumes: the finished model from Tasks 1-3
- Produces: user-facing docs describing the new fields; version `0.2.0`

- [ ] **Step 1: Bump the version in both places**

`pyproject.toml`'s `version` and `src/otlp_client/client.py`'s `__version__` must match — `DEFAULT_SCOPE` reports `__version__` on the wire, so a mismatch is visible to collectors. Set both to `0.2.0`.

Verify: `uv run python -c "import otlp_client, tomllib, pathlib; v=tomllib.loads(pathlib.Path('pyproject.toml').read_text())['project']['version']; assert otlp_client.__version__ == v == '0.2.0', (otlp_client.__version__, v); print('versions agree:', v)"`

- [ ] **Step 2: Update the older spec's Known Limitations**

In `docs/superpowers/specs/2026-09-04-asyncio-otlp-client-design.md`, the Known Limitations paragraph currently names `trace_state`, `exemplars`, `flags` and the `dropped_*_count` family as excluded. Three of those are now implemented. Trim it to the `dropped_attributes_count` / `dropped_events_count` / `dropped_links_count` family only, and say why they remain out: this client enforces no limits and drops nothing, so they would be hardcoded zero.

- [ ] **Step 3: Document the new capability in the README**

Add a short section after the existing Batching section. Keep it to what the code does — no aspirational claims:

````markdown
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
`is_remote=False`, meaning "known not to be remote".

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
different from measuring zero:

```python
from otlp_client import data_point_flags

point = NumberDataPoint(
    time_unix_nano=now, value=0, flags=data_point_flags(no_recorded_value=True)
)
```
````

- [ ] **Step 4: Note the unavailable-entity case in the Home Assistant guide**

In `docs/home-assistant.md`, after the state-change listener example, add a short paragraph and snippet showing `data_point_flags(no_recorded_value=True)` for an entity whose state is `unavailable` or `unknown` — the distinction between "the sensor read zero" and "the sensor had no reading", which is the field's most common real use in Home Assistant.

- [ ] **Step 5: Verify the documented API against the real one**

Run:
```bash
uv run python -c "
import inspect
from otlp_client import Exemplar, NumberDataPoint, span, span_flags, data_point_flags
assert 'trace_state' in inspect.signature(span).parameters
assert 'flags' in inspect.signature(span).parameters
assert {'sampled','is_remote'} <= set(inspect.signature(span_flags).parameters)
assert 'no_recorded_value' in inspect.signature(data_point_flags).parameters
assert 'exemplars' in {f.name for f in __import__('dataclasses').fields(NumberDataPoint)}
assert 'filtered_attributes' in {f.name for f in __import__('dataclasses').fields(Exemplar)}
print('docs match the public API')
"
```
Expected: `docs match the public API`

- [ ] **Step 6: Final verification**

Run: `uv run pytest -q && uv run mypy && uv run ruff check && uv run ruff format --check`

- [ ] **Step 7: Commit**

```bash
git add README.md docs pyproject.toml src/otlp_client/client.py
git commit -m "docs: document trace context, flags and exemplars; bump to 0.2.0"
```
