"""Tests for the fields added in the model-completeness work."""

from __future__ import annotations

import dataclasses
import inspect
from collections.abc import Sequence
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
