"""OTLP/JSON encoding. The core encoder: pure Python, no dependencies."""

from __future__ import annotations

import json as _stdlib_json
from collections.abc import Sequence
from typing import Any

from otlp_client.encoding.primitives import (
    encode_any_value,
    encode_attributes,
    hex_id,
    omit_empty,
    u64,
)
from otlp_client.model.common import InstrumentationScope, Resource
from otlp_client.model.logs import LogRecord, ResourceLogs
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
from otlp_client.model.traces import ResourceSpans, Span, SpanEvent, SpanLink
from otlp_client.outcomes import PartialSuccess
from otlp_client.signals import SignalKind

try:  # pragma: no cover - exercised by whichever path is installed
    import orjson

    def _dumps(doc: dict[str, Any]) -> bytes:
        """Serialize with orjson when available.

        orjson is never a declared dependency; Home Assistant already ships it,
        so this is a free speedup there and a no-op everywhere else.
        """
        result: bytes = orjson.dumps(doc)
        return result

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
        {"asInt": u64(point.value)} if isinstance(point.value, int) else {"asDouble": point.value}
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
        body: dict[str, Any] = {
            "gauge": {"dataPoints": [_encode_number_point(p) for p in data.data_points]}
        }
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


class JSONEncoder:
    """Encodes the model tree as OTLP/JSON."""

    @property
    def content_type(self) -> str:
        return "application/json"

    def encode(self, kind: SignalKind, data: Sequence[Any]) -> bytes:
        if kind is SignalKind.METRICS:
            return _dumps(_encode_resource_metrics(data))
        if kind is SignalKind.LOGS:
            return _dumps(_encode_resource_logs(data))
        if kind is SignalKind.TRACES:
            return _dumps(_encode_resource_spans(data))
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
