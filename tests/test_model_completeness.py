"""Tests for the fields added in the model-completeness work."""

from __future__ import annotations

import dataclasses
import inspect
import json
from collections.abc import Callable, Sequence
from typing import Any, cast

import pytest

from otlp_client.model import common, logs, metrics, traces
from otlp_client.model.common import Resource


def _dataclasses_defined_in(module: Any) -> list[type]:
    """Every dataclass defined directly in `module` (not merely imported into it)."""
    return [
        obj
        for _name, obj in vars(module).items()
        if inspect.isclass(obj)
        and dataclasses.is_dataclass(obj)
        and obj.__module__ == module.__name__
    ]


KW_ONLY_TYPES = [
    cls for module in (common, logs, metrics, traces) for cls in _dataclasses_defined_in(module)
]

# pytest reports @pytest.mark.parametrize over an empty list as SKIPPED, not
# FAILED (there is no empty_parameter_set_mark override in pyproject.toml). If
# discovery ever collapses -- a module rename, dataclasses moved behind a
# different import alias, anything that breaks the __module__ comparison in
# _dataclasses_defined_in -- test_model_types_are_keyword_only would quietly
# show green-adjacent (skipped) instead of red, defeating the whole point of
# discovering the type list instead of hand-writing it. Guard against that
# explicitly. >= rather than == so this isn't a chore every time a legitimate
# new model type is added.
assert len(KW_ONLY_TYPES) >= 25, (
    f"model dataclass discovery found {len(KW_ONLY_TYPES)}; it has broken, and an empty "
    "parametrize would SKIP rather than fail"
)


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


# --- flags-as-number coverage -----------------------------------------------
#
# flags fields are 32-bit, so protobuf-JSON renders them as numbers -- unlike
# the 64-bit fields elsewhere in the model, which this project's `u64()` rule
# renders as decimal strings. The property-based encoder oracle (comparing
# this encoder's output against `google.protobuf.json_format`) CANNOT catch a
# `flags`-as-string regression: `json_format.Parse` accepts either a number or
# a decimal string for an integer field of any width, so a `u64(flags)` bug
# round-trips cleanly through the oracle and stays green. This explicit test
# is the only guard, so every `flags` field in the model must appear below.


def _span_flags_value(flags: int) -> Any:
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
                                flags=flags,
                            )
                        ],
                    )
                ],
            )
        ],
    )
    doc = json.loads(payload)
    return doc["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["flags"]


def _span_link_flags_value(flags: int) -> Any:
    from otlp_client.encoding.json import JSONEncoder
    from otlp_client.model.common import InstrumentationScope, Resource
    from otlp_client.model.traces import ResourceSpans, ScopeSpans, Span, SpanLink
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
                                links=[
                                    SpanLink(
                                        trace_id=bytes(range(16)),
                                        span_id=bytes(range(8)),
                                        flags=flags,
                                    )
                                ],
                            )
                        ],
                    )
                ],
            )
        ],
    )
    doc = json.loads(payload)
    span = doc["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
    return span["links"][0]["flags"]


def _log_record_flags_value(flags: int) -> Any:
    from otlp_client.encoding.json import JSONEncoder
    from otlp_client.model.common import InstrumentationScope, Resource
    from otlp_client.model.logs import LogRecord, ResourceLogs, ScopeLogs
    from otlp_client.signals import SignalKind

    payload = JSONEncoder().encode(
        SignalKind.LOGS,
        [
            ResourceLogs(
                resource=Resource(attributes={"a": "b"}),
                scope_logs=[
                    ScopeLogs(
                        scope=InstrumentationScope(name="t"),
                        log_records=[
                            LogRecord(
                                time_unix_nano=1,
                                body="x",
                                observed_time_unix_nano=1,
                                flags=flags,
                            )
                        ],
                    )
                ],
            )
        ],
    )
    doc = json.loads(payload)
    return doc["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["flags"]


def _number_point_flags_value(flags: int) -> Any:
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

    point = NumberDataPoint(time_unix_nano=1, value=1.5, flags=flags)
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
    doc = json.loads(payload)
    metric = doc["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]
    return metric["gauge"]["dataPoints"][0]["flags"]


def _histogram_point_flags_value(flags: int) -> Any:
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
        time_unix_nano=1, count=1, bucket_counts=[1], explicit_bounds=[], flags=flags
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
    doc = json.loads(payload)
    metric = doc["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]
    return metric["histogram"]["dataPoints"][0]["flags"]


def _exponential_histogram_point_flags_value(flags: int) -> Any:
    from otlp_client.encoding.json import JSONEncoder
    from otlp_client.model.common import InstrumentationScope, Resource
    from otlp_client.model.metrics import (
        ExponentialHistogram,
        ExponentialHistogramDataPoint,
        Metric,
        ResourceMetrics,
        ScopeMetrics,
    )
    from otlp_client.signals import SignalKind

    point = ExponentialHistogramDataPoint(time_unix_nano=1, count=1, flags=flags)
    payload = JSONEncoder().encode(
        SignalKind.METRICS,
        [
            ResourceMetrics(
                resource=Resource(attributes={"a": "b"}),
                scope_metrics=[
                    ScopeMetrics(
                        scope=InstrumentationScope(name="t"),
                        metrics=[Metric(name="e", data=ExponentialHistogram(data_points=[point]))],
                    )
                ],
            )
        ],
    )
    doc = json.loads(payload)
    metric = doc["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]
    return metric["exponentialHistogram"]["dataPoints"][0]["flags"]


def _summary_point_flags_value(flags: int) -> Any:
    from otlp_client.encoding.json import JSONEncoder
    from otlp_client.model.common import InstrumentationScope, Resource
    from otlp_client.model.metrics import (
        Metric,
        ResourceMetrics,
        ScopeMetrics,
        Summary,
        SummaryDataPoint,
    )
    from otlp_client.signals import SignalKind

    point = SummaryDataPoint(time_unix_nano=1, count=1, flags=flags)
    payload = JSONEncoder().encode(
        SignalKind.METRICS,
        [
            ResourceMetrics(
                resource=Resource(attributes={"a": "b"}),
                scope_metrics=[
                    ScopeMetrics(
                        scope=InstrumentationScope(name="t"),
                        metrics=[Metric(name="s", data=Summary(data_points=[point]))],
                    )
                ],
            )
        ],
    )
    doc = json.loads(payload)
    metric = doc["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]
    return metric["summary"]["dataPoints"][0]["flags"]


_FLAGS_FIELDS: list[tuple[str, Callable[[int], Any]]] = [
    ("Span.flags", _span_flags_value),
    ("SpanLink.flags", _span_link_flags_value),
    ("LogRecord.flags", _log_record_flags_value),
    ("NumberDataPoint.flags", _number_point_flags_value),
    ("HistogramDataPoint.flags", _histogram_point_flags_value),
    ("ExponentialHistogramDataPoint.flags", _exponential_histogram_point_flags_value),
    ("SummaryDataPoint.flags", _summary_point_flags_value),
]


@pytest.mark.parametrize("build", [f[1] for f in _FLAGS_FIELDS], ids=[f[0] for f in _FLAGS_FIELDS])
def test_flags_encode_as_json_numbers_not_strings(build: Callable[[int], Any]) -> None:
    """flags are 32-bit, so protobuf-JSON renders them as numbers, never u64 strings.

    See the module comment above `_span_flags_value` for why the
    property-based oracle cannot catch a regression here and this explicit
    test is the only guard.
    """
    encoded = build(0x101)
    assert encoded == 0x101
    assert not isinstance(encoded, str)


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
    return cast("dict[str, Any]", doc["gauge"]["dataPoints"][0])


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


def _encode_exponential_point_with(zero_threshold: float) -> dict[str, Any]:
    """Encode a one-point exponential histogram through the real JSONEncoder."""
    import json as _json

    from otlp_client.encoding.json import JSONEncoder
    from otlp_client.model.common import InstrumentationScope, Resource
    from otlp_client.model.metrics import (
        ExponentialHistogram,
        ExponentialHistogramDataPoint,
        Metric,
        ResourceMetrics,
        ScopeMetrics,
    )
    from otlp_client.signals import SignalKind

    point = ExponentialHistogramDataPoint(time_unix_nano=1, count=1, zero_threshold=zero_threshold)
    payload = JSONEncoder().encode(
        SignalKind.METRICS,
        [
            ResourceMetrics(
                resource=Resource(attributes={"a": "b"}),
                scope_metrics=[
                    ScopeMetrics(
                        scope=InstrumentationScope(name="t"),
                        metrics=[
                            Metric(
                                name="eh",
                                data=ExponentialHistogram(data_points=[point]),
                            )
                        ],
                    )
                ],
            )
        ],
    )
    doc = _json.loads(payload)["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]
    return cast("dict[str, Any]", doc["exponentialHistogram"]["dataPoints"][0])


def test_zero_threshold_is_omitted_when_positive_zero() -> None:
    """zero_threshold has IMPLICIT presence, so +0.0 is the default and is omitted."""
    assert "zeroThreshold" not in _encode_exponential_point_with(0.0)


def test_zero_threshold_negative_zero_is_kept() -> None:
    """protobuf's presence check is a bit pattern, so -0.0 is on the wire.

    A plain `or None` gate would drop it (``-0.0`` is falsy in Python) and
    diverge from the protobuf encoder for that one value -- the same defect
    that hit Summary.sum.
    """
    import math

    encoded = _encode_exponential_point_with(-0.0)
    assert "zeroThreshold" in encoded
    assert math.copysign(1.0, encoded["zeroThreshold"]) == -1.0


def test_zero_threshold_non_zero_is_kept() -> None:
    assert _encode_exponential_point_with(1e-9)["zeroThreshold"] == 1e-9
