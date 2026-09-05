"""Cross-check the hand-written JSON encoder against the canonical proto schema."""

import base64
import json as _stdlib_json
from typing import Any

import pytest

pytest.importorskip("opentelemetry.proto")

from google.protobuf import json_format
from google.protobuf.message import Message
from hypothesis import HealthCheck, given, settings
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from otlp_client.encoding.json import JSONEncoder
from otlp_client.encoding.protobuf import build_protobuf_encoder
from otlp_client.model.logs import ResourceLogs
from otlp_client.model.metrics import ResourceMetrics
from otlp_client.model.traces import ResourceSpans
from otlp_client.signals import SignalKind
from tests.support import strategies as s

SETTINGS = settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow], deadline=None)


_HEX_ID_KEYS = frozenset({"traceId", "spanId", "parentSpanId"})


def _hex_ids_to_base64(doc: Any) -> Any:
    """Rewrite traceId/spanId/parentSpanId hex strings as base64, recursively.

    OTLP/JSON deliberately encodes these three fields as hex -- the one
    documented deviation from the standard protobuf-JSON mapping, which
    encodes every `bytes` field as base64 instead. `google.protobuf.
    json_format.Parse` only implements the standard mapping: fed a hex
    string it does not error, it silently base64-decodes it into the wrong
    bytes (e.g. the 16 hex "0" characters for an all-zero 8-byte span ID
    become 12 garbage bytes instead of 8 zero bytes). That would make every
    envelope with a non-empty span/trace id fail this oracle even though the
    encoders are correct, which is a flaw in using the vanilla parser here,
    not an encoder bug.

    This adapter step translates those fields from hex to base64 before
    handing the JSON to the real parser, exactly what a spec-aware OTLP/JSON
    unmarshaler does internally. It keeps the comparison meaningful for the
    property actually under test: that our hex encoding decodes to the same
    bytes the protobuf encoder produced. A hex/base64 identifier bug in the
    encoder still fails loudly here -- wrong bytes decode to wrong bytes.
    """
    if isinstance(doc, dict):
        return {
            key: (
                base64.b64encode(bytes.fromhex(value)).decode("ascii")
                if key in _HEX_ID_KEYS and isinstance(value, str)
                else _hex_ids_to_base64(value)
            )
            for key, value in doc.items()
        }
    if isinstance(doc, list):
        return [_hex_ids_to_base64(item) for item in doc]
    return doc


def assert_encoders_agree(kind: SignalKind, request_type: type[Message], envelope: Any) -> None:
    """Both encoders must describe the same message.

    The JSON is parsed by the official protobuf JSON parser, which enforces the
    schema: wrong key casing, an enum name where an integer belongs, a bare
    number where a decimal string belongs, or base64 where hex belongs all fail
    here rather than silently at a collector. traceId/spanId/parentSpanId are
    translated from hex to base64 first -- see `_hex_ids_to_base64`.
    """
    json_bytes = JSONEncoder().encode(kind, [envelope])
    proto_bytes = build_protobuf_encoder().encode(kind, [envelope])

    doc = _hex_ids_to_base64(_stdlib_json.loads(json_bytes))
    from_json = json_format.Parse(_stdlib_json.dumps(doc), request_type())
    from_proto = request_type.FromString(proto_bytes)
    assert from_json == from_proto


@given(envelope=s.resource_metrics)
@SETTINGS
def test_metrics_encoders_agree(envelope: ResourceMetrics) -> None:
    assert_encoders_agree(SignalKind.METRICS, ExportMetricsServiceRequest, envelope)


@given(envelope=s.resource_logs)
@SETTINGS
def test_logs_encoders_agree(envelope: ResourceLogs) -> None:
    assert_encoders_agree(SignalKind.LOGS, ExportLogsServiceRequest, envelope)


@given(envelope=s.resource_spans)
@SETTINGS
def test_traces_encoders_agree(envelope: ResourceSpans) -> None:
    assert_encoders_agree(SignalKind.TRACES, ExportTraceServiceRequest, envelope)
