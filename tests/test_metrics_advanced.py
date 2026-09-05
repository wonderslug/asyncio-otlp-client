import json
from typing import Any

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

EXPONENTIAL = Metric(
    name="eh",
    data=ExponentialHistogram(
        aggregation_temporality=AggregationTemporality.DELTA,
        data_points=[
            ExponentialHistogramDataPoint(
                time_unix_nano=9,
                count=5,
                scale=2,
                zero_count=1,
                sum=12.5,
                min=0.5,
                max=9.0,
                positive=Buckets(offset=3, bucket_counts=[1, 2]),
                negative=Buckets(offset=-1, bucket_counts=[1]),
            )
        ],
    ),
)

SUMMARY = Metric(
    name="sm",
    data=Summary(
        data_points=[
            SummaryDataPoint(
                time_unix_nano=9,
                count=4,
                sum=8.0,
                quantile_values=[ValueAtQuantile(quantile=0.5, value=2.0)],
            )
        ]
    ),
)


def encode_json(metric: Metric) -> dict[str, Any]:
    payload = JSONEncoder().encode(
        SignalKind.METRICS,
        [
            ResourceMetrics(
                resource=Resource(attributes={"a": "b"}),
                scope_metrics=[
                    ScopeMetrics(scope=InstrumentationScope(name="t"), metrics=[metric])
                ],
            )
        ],
    )
    result: dict[str, Any] = json.loads(payload)["resourceMetrics"][0]["scopeMetrics"][0][
        "metrics"
    ][0]
    return result


def test_exponential_histogram_json_uses_strings_for_64_bit_fields() -> None:
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


def test_summary_json_shape() -> None:
    data = encode_json(SUMMARY)["summary"]
    assert "aggregationTemporality" not in data
    (point,) = data["dataPoints"]
    assert point["count"] == "4"
    assert point["sum"] == 8.0
    assert point["quantileValues"] == [{"quantile": 0.5, "value": 2.0}]


@pytest.mark.parametrize("metric", [EXPONENTIAL, SUMMARY], ids=["exponential", "summary"])
def test_both_encoders_agree(metric: Metric) -> None:
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
