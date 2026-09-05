import json
from typing import Any

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


def encode_one(metric: Metric) -> dict[str, Any]:
    payload = JSONEncoder().encode(
        SignalKind.METRICS,
        [
            ResourceMetrics(
                resource=Resource(attributes={"service.name": "hass"}),
                scope_metrics=[
                    ScopeMetrics(
                        scope=InstrumentationScope(name="otlp_client", version="0.1.0"),
                        metrics=[metric],
                    )
                ],
            )
        ],
    )
    result: dict[str, Any] = json.loads(payload)
    return result


def test_envelope_shape_and_resource_attributes() -> None:
    doc = encode_one(gauge("t", 21.5, time_unix_nano=1700000000000000000))
    (rm,) = doc["resourceMetrics"]
    assert rm["resource"]["attributes"] == [
        {"key": "service.name", "value": {"stringValue": "hass"}}
    ]
    (sm,) = rm["scopeMetrics"]
    assert sm["scope"] == {"name": "otlp_client", "version": "0.1.0"}


def test_gauge_double_point_uses_asDouble_and_string_timestamp() -> None:
    doc = encode_one(gauge("t", 21.5, unit="Cel", time_unix_nano=1700000000000000000))
    metric = doc["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]
    assert metric["name"] == "t"
    assert metric["unit"] == "Cel"
    (point,) = metric["gauge"]["dataPoints"]
    assert point["asDouble"] == 21.5
    assert point["timeUnixNano"] == "1700000000000000000"
    assert isinstance(point["timeUnixNano"], str)


def test_integer_point_uses_asInt_as_a_string() -> None:
    doc = encode_one(sum_("e", 42, time_unix_nano=5))
    (point,) = doc["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]["sum"]["dataPoints"]
    assert point["asInt"] == "42"


def test_sum_enum_is_an_integer_not_a_name() -> None:
    doc = encode_one(sum_("e", 1, time_unix_nano=5))
    data = doc["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]["sum"]
    assert data["aggregationTemporality"] == 2
    assert data["isMonotonic"] is True


def test_histogram_count_and_bucket_counts_are_strings() -> None:
    point = HistogramDataPoint(
        time_unix_nano=9,
        count=3,
        sum=6.0,
        bucket_counts=[1, 2],
        explicit_bounds=[10.0],
    )
    doc = encode_one(
        Metric(
            name="h",
            data=Histogram(
                data_points=[point], aggregation_temporality=AggregationTemporality.DELTA
            ),
        )
    )
    hist = doc["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]["histogram"]
    assert hist["aggregationTemporality"] == 1
    (p,) = hist["dataPoints"]
    assert p["count"] == "3"
    assert p["bucketCounts"] == ["1", "2"]
    assert p["explicitBounds"] == [10.0]
    assert p["sum"] == 6.0


def test_empty_fields_are_omitted() -> None:
    doc = encode_one(gauge("t", 1.0, time_unix_nano=1))
    metric = doc["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]
    assert "description" not in metric
    assert "unit" not in metric
    point = metric["gauge"]["dataPoints"][0]
    assert "attributes" not in point
    assert "startTimeUnixNano" not in point


def test_content_type() -> None:
    assert JSONEncoder().content_type == "application/json"


def test_decode_partial_success() -> None:
    body = json.dumps(
        {"partialSuccess": {"rejectedDataPoints": "7", "errorMessage": "bad unit"}}
    ).encode()
    result = JSONEncoder().decode_response(SignalKind.METRICS, body)
    assert result is not None
    assert result.rejected == 7
    assert result.message == "bad unit"


def test_decode_full_success_returns_none() -> None:
    assert JSONEncoder().decode_response(SignalKind.METRICS, b"{}") is None
    assert JSONEncoder().decode_response(SignalKind.METRICS, b"") is None


def test_decode_partial_success_with_non_numeric_rejected_count_does_not_raise() -> None:
    # A collector could hand back junk in this field; decode_response's job is
    # to tolerate it, not raise a bare ValueError past the OTLPError contract.
    body = json.dumps(
        {"partialSuccess": {"rejectedDataPoints": "not-a-number", "errorMessage": "bad unit"}}
    ).encode()
    result = JSONEncoder().decode_response(SignalKind.METRICS, body)
    assert result is not None
    assert result.rejected == 0
    assert result.message == "bad unit"


def test_decode_partial_success_with_structurally_odd_rejected_count_does_not_raise() -> None:
    body = json.dumps(
        {"partialSuccess": {"rejectedDataPoints": {"nested": "dict"}, "errorMessage": "odd"}}
    ).encode()
    result = JSONEncoder().decode_response(SignalKind.METRICS, body)
    assert result is not None
    assert result.rejected == 0
    assert result.message == "odd"

    body_list = json.dumps(
        {"partialSuccess": {"rejectedDataPoints": [1, 2, 3], "errorMessage": "odd list"}}
    ).encode()
    result_list = JSONEncoder().decode_response(SignalKind.METRICS, body_list)
    assert result_list is not None
    assert result_list.rejected == 0


def test_profiles_is_a_defined_seam_that_is_not_yet_implemented() -> None:
    with pytest.raises(NotImplementedError, match="profiles"):
        JSONEncoder().encode(SignalKind.PROFILES, [])
