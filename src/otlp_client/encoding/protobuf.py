"""OTLP/protobuf encoding. Requires the `protobuf` extra.

Every `opentelemetry.proto` import is deliberately inside a function. Importing
this module must stay free for a core-only install.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

from otlp_client.errors import OTLPConfigError
from otlp_client.model.common import AnyValue, InstrumentationScope, Resource
from otlp_client.model.logs import LogRecord, ResourceLogs
from otlp_client.model.metrics import Gauge, Histogram, Metric, ResourceMetrics, Sum
from otlp_client.model.traces import ResourceSpans, Span, StatusCode
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
        from google.protobuf.message import DecodeError

        try:
            response = _response_type(kind).FromString(body)
        except DecodeError:
            return None
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
            aggregation_temporality=cast(Any, int(data.aggregation_temporality)),
            is_monotonic=data.is_monotonic,
        )
    elif isinstance(data, Histogram):
        kwargs["histogram"] = metrics_pb2.Histogram(
            aggregation_temporality=cast(Any, int(data.aggregation_temporality)),
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
    return cast(bytes, request.SerializeToString())


def _log_record(record: LogRecord) -> Any:
    from opentelemetry.proto.logs.v1 import logs_pb2

    return logs_pb2.LogRecord(
        time_unix_nano=record.time_unix_nano,
        observed_time_unix_nano=record.observed_time_unix_nano,
        severity_number=cast(Any, int(record.severity_number)),
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
    return cast(bytes, request.SerializeToString())


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
    # Mirror model/traces.py's span(): only set status when it carries
    # information, so a default Status doesn't create wire presence that
    # the JSON encoder's omit_empty already drops for the equivalent input.
    if item.status is not None and (
        item.status.code is not StatusCode.UNSET or item.status.message
    ):
        kwargs["status"] = trace_pb2.Status(
            code=cast(Any, int(item.status.code)), message=item.status.message
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
    return cast(bytes, request.SerializeToString())
