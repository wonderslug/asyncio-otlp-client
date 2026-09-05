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
from otlp_client.model.metrics import (
    Buckets,
    Exemplar,
    ExponentialHistogram,
    Gauge,
    Histogram,
    Metric,
    ResourceMetrics,
    Sum,
    Summary,
)
from otlp_client.model.traces import ResourceSpans, Span, StatusCode
from otlp_client.outcomes import PartialSuccess
from otlp_client.signals import SignalKind

_MISSING = (
    "the protobuf encoder needs the optional extra: pip install 'asyncio-otlp-client[protobuf]'"
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
        return common_pb2.AnyValue(kvlist_value=common_pb2.KeyValueList(values=_key_values(value)))
    if isinstance(value, Sequence):
        return common_pb2.AnyValue(
            array_value=common_pb2.ArrayValue(values=[_any_value(v) for v in value])
        )
    raise TypeError(f"unsupported attribute value type: {type(value)!r}")


def _key_values(attributes: Mapping[str, AnyValue]) -> list[Any]:
    from opentelemetry.proto.common.v1 import common_pb2

    return [
        common_pb2.KeyValue(key=key, value=_any_value(value)) for key, value in attributes.items()
    ]


def _resource_is_empty(resource: Resource) -> bool:
    """True when a Resource carries nothing worth wire presence for.

    Mirrors the JSON encoder's omit_empty, which drops the "resource" key
    entirely once its encoded form is an empty dict. A collector treats an
    absent resource and a present-but-empty one identically, so the two
    encoders must agree on which one this is.
    """
    return not resource.attributes and not resource.dropped_attributes_count


def _resource(resource: Resource) -> Any:
    from opentelemetry.proto.resource.v1 import resource_pb2

    return resource_pb2.Resource(
        attributes=_key_values(resource.attributes),
        dropped_attributes_count=resource.dropped_attributes_count,
    )


def _scope_is_empty(scope: InstrumentationScope) -> bool:
    """True when an InstrumentationScope carries nothing worth wire presence for.

    Mirrors the JSON encoder's omit_empty, which drops the "scope" key
    entirely once name, version, and attributes are all empty.
    """
    return not scope.name and not scope.version and not scope.attributes


def _scope(scope: InstrumentationScope) -> Any:
    from opentelemetry.proto.common.v1 import common_pb2

    return common_pb2.InstrumentationScope(
        name=scope.name,
        version=scope.version or "",
        attributes=_key_values(scope.attributes),
    )


def _buckets_is_empty(buckets: Buckets) -> bool:
    """True when a Buckets side carries nothing worth wire presence for.

    Mirrors the JSON encoder's omit_empty, which drops the "positive"/"negative"
    key entirely once offset and bucketCounts are both empty. Unlike the plain
    scalar fields on ExponentialHistogramDataPoint, `positive` and `negative`
    are message-typed and so have explicit presence: setting one to an
    all-default Buckets() would still register as "set" and break equality
    against the JSON path, which never emits the key at all.
    """
    return not buckets.offset and not buckets.bucket_counts


def _buckets(buckets: Buckets) -> Any:
    from opentelemetry.proto.metrics.v1 import metrics_pb2

    return metrics_pb2.ExponentialHistogramDataPoint.Buckets(
        offset=buckets.offset, bucket_counts=list(buckets.bucket_counts)
    )


def _exemplar(exemplar: Exemplar) -> Any:
    from opentelemetry.proto.metrics.v1 import metrics_pb2

    if isinstance(exemplar.value, bool):
        raise TypeError("exemplar values must be int or float, not bool")
    common: dict[str, Any] = {
        "filtered_attributes": _key_values(exemplar.filtered_attributes),
        "time_unix_nano": exemplar.time_unix_nano,
        "span_id": exemplar.span_id or b"",
        "trace_id": exemplar.trace_id or b"",
    }
    if isinstance(exemplar.value, int):
        return metrics_pb2.Exemplar(as_int=exemplar.value, **common)
    return metrics_pb2.Exemplar(as_double=exemplar.value, **common)


def _number_point(point: Any) -> Any:
    from opentelemetry.proto.metrics.v1 import metrics_pb2

    common = {
        "attributes": _key_values(point.attributes),
        "time_unix_nano": point.time_unix_nano,
        "start_time_unix_nano": point.start_time_unix_nano or 0,
        "flags": point.flags,
        "exemplars": [_exemplar(e) for e in point.exemplars],
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
                    flags=p.flags,
                    min=p.min,
                    max=p.max,
                    exemplars=[_exemplar(e) for e in p.exemplars],
                )
                for p in data.data_points
            ],
        )
    elif isinstance(data, ExponentialHistogram):
        points = []
        for p in data.data_points:
            point_kwargs: dict[str, Any] = {
                "attributes": _key_values(p.attributes),
                "time_unix_nano": p.time_unix_nano,
                "start_time_unix_nano": p.start_time_unix_nano or 0,
                "count": p.count,
                "sum": p.sum,
                "scale": p.scale,
                "zero_count": p.zero_count,
                "min": p.min,
                "max": p.max,
                "flags": p.flags,
                "exemplars": [_exemplar(e) for e in p.exemplars],
                "zero_threshold": p.zero_threshold,
            }
            # positive/negative are message-typed fields with explicit
            # presence: only set them when they carry information, matching
            # the JSON encoder's omit_empty for an all-default Buckets.
            if not _buckets_is_empty(p.positive):
                point_kwargs["positive"] = _buckets(p.positive)
            if not _buckets_is_empty(p.negative):
                point_kwargs["negative"] = _buckets(p.negative)
            points.append(metrics_pb2.ExponentialHistogramDataPoint(**point_kwargs))
        kwargs["exponential_histogram"] = metrics_pb2.ExponentialHistogram(
            aggregation_temporality=cast(Any, int(data.aggregation_temporality)),
            data_points=points,
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
                    flags=p.flags,
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
                resource=None if _resource_is_empty(rm.resource) else _resource(rm.resource),
                scope_metrics=[
                    metrics_pb2.ScopeMetrics(
                        scope=None if _scope_is_empty(sm.scope) else _scope(sm.scope),
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
        event_name=record.event_name,
        flags=record.flags,
    )


def _encode_logs(data: Sequence[ResourceLogs]) -> bytes:
    from opentelemetry.proto.collector.logs.v1 import logs_service_pb2
    from opentelemetry.proto.logs.v1 import logs_pb2

    request = logs_service_pb2.ExportLogsServiceRequest(
        resource_logs=[
            logs_pb2.ResourceLogs(
                resource=None if _resource_is_empty(rl.resource) else _resource(rl.resource),
                scope_logs=[
                    logs_pb2.ScopeLogs(
                        scope=None if _scope_is_empty(sl.scope) else _scope(sl.scope),
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
        "trace_state": item.trace_state,
        "flags": item.flags,
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
                trace_state=link.trace_state,
                attributes=_key_values(link.attributes),
                flags=link.flags,
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
                resource=None if _resource_is_empty(rs.resource) else _resource(rs.resource),
                scope_spans=[
                    trace_pb2.ScopeSpans(
                        scope=None if _scope_is_empty(ss.scope) else _scope(ss.scope),
                        spans=[_span(s) for s in ss.spans],
                    )
                    for ss in rs.scope_spans
                ],
            )
            for rs in data
        ]
    )
    return cast(bytes, request.SerializeToString())
