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


def test_content_type() -> None:
    assert build_protobuf_encoder().content_type == "application/x-protobuf"


def test_metrics_round_trip_through_the_real_proto() -> None:
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


def test_integer_sum_uses_as_int_and_carries_temporality() -> None:
    payload = build_protobuf_encoder().encode(SignalKind.METRICS, [
        ResourceMetrics(resource=RESOURCE, scope_metrics=[ScopeMetrics(
            scope=SCOPE, metrics=[sum_("e", 42, time_unix_nano=1)])])
    ])
    metric = ExportMetricsServiceRequest.FromString(payload).resource_metrics[0]\
        .scope_metrics[0].metrics[0]
    assert metric.sum.data_points[0].as_int == 42
    assert metric.sum.is_monotonic is True
    assert metric.sum.aggregation_temporality == 2


def test_logs_round_trip() -> None:
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


def test_traces_round_trip() -> None:
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


def test_decode_partial_success() -> None:
    response = ExportMetricsServiceResponse()
    response.partial_success.rejected_data_points = 5
    response.partial_success.error_message = "bad unit"
    result = build_protobuf_encoder().decode_response(
        SignalKind.METRICS, response.SerializeToString()
    )
    assert result is not None
    assert result.rejected == 5
    assert result.message == "bad unit"


def test_decode_full_success_returns_none() -> None:
    encoder = build_protobuf_encoder()
    empty = ExportMetricsServiceResponse().SerializeToString()
    assert encoder.decode_response(SignalKind.METRICS, empty) is None
    assert encoder.decode_response(SignalKind.METRICS, b"") is None


def test_profiles_is_not_implemented() -> None:
    with pytest.raises(NotImplementedError, match="profiles"):
        build_protobuf_encoder().encode(SignalKind.PROFILES, [])


def test_module_does_not_import_protobuf_at_top_level() -> None:
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
