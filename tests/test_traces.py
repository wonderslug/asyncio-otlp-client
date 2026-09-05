import json
from typing import Any

from otlp_client.client import OTLPClient
from otlp_client.config import OTLPConfig
from otlp_client.encoding.json import JSONEncoder
from otlp_client.model.common import Resource
from otlp_client.model.traces import SpanEvent, SpanKind, SpanLink, StatusCode, span
from otlp_client.outcomes import Success
from otlp_client.processor import BatchProcessor
from otlp_client.signals import SignalKind
from tests.support.fakes import FakeTransport

TRACE_ID = bytes.fromhex("0102030405060708090a0b0c0d0e0f10")
SPAN_ID = bytes.fromhex("1112131415161718")
PARENT_ID = bytes.fromhex("2122232425262728")
CONFIG = OTLPConfig(endpoint="http://localhost:4318", resource=Resource(attributes={"a": "b"}))


def make_client(transport: FakeTransport) -> OTLPClient:
    return OTLPClient(CONFIG, transport=transport, encoder=JSONEncoder())


def only_span(payload: bytes) -> dict[str, Any]:
    doc: dict[str, Any] = json.loads(payload)
    span_result: dict[str, Any] = doc["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
    return span_result


async def test_export_traces_envelope_and_hex_ids() -> None:
    transport = FakeTransport()
    result = await make_client(transport).export_traces(
        [
            span(
                "handle_state_change",
                trace_id=TRACE_ID,
                span_id=SPAN_ID,
                parent_span_id=PARENT_ID,
                start_time_unix_nano=100,
                end_time_unix_nano=200,
                kind=SpanKind.INTERNAL,
                attributes={"entity_id": "light.kitchen"},
            )
        ]
    )
    assert isinstance(result, Success)
    kind, payload = transport.sent[0]
    assert kind is SignalKind.TRACES
    s = only_span(payload)
    assert s["traceId"] == "0102030405060708090a0b0c0d0e0f10"
    assert s["spanId"] == "1112131415161718"
    assert s["parentSpanId"] == "2122232425262728"
    assert s["name"] == "handle_state_change"
    assert s["startTimeUnixNano"] == "100"
    assert s["endTimeUnixNano"] == "200"
    assert s["kind"] == 1
    assert s["attributes"] == [{"key": "entity_id", "value": {"stringValue": "light.kitchen"}}]


async def test_span_kind_and_status_code_are_integers() -> None:
    transport = FakeTransport()
    await make_client(transport).export_traces(
        [
            span(
                "call",
                trace_id=TRACE_ID,
                span_id=SPAN_ID,
                start_time_unix_nano=1,
                end_time_unix_nano=2,
                kind=SpanKind.CLIENT,
                status_code=StatusCode.ERROR,
                status_message="timeout",
            )
        ]
    )
    s = only_span(transport.sent[0][1])
    assert s["kind"] == 3
    assert s["status"] == {"code": 2, "message": "timeout"}


async def test_root_span_omits_parent_span_id() -> None:
    transport = FakeTransport()
    await make_client(transport).export_traces(
        [
            span(
                "root",
                trace_id=TRACE_ID,
                span_id=SPAN_ID,
                start_time_unix_nano=1,
                end_time_unix_nano=2,
            )
        ]
    )
    assert "parentSpanId" not in only_span(transport.sent[0][1])


async def test_events_and_links_are_encoded() -> None:
    transport = FakeTransport()
    await make_client(transport).export_traces(
        [
            span(
                "s",
                trace_id=TRACE_ID,
                span_id=SPAN_ID,
                start_time_unix_nano=1,
                end_time_unix_nano=2,
                events=[SpanEvent(time_unix_nano=5, name="retry", attributes={"n": 2})],
                links=[SpanLink(trace_id=TRACE_ID, span_id=PARENT_ID)],
            )
        ]
    )
    s = only_span(transport.sent[0][1])
    assert s["events"] == [
        {
            "timeUnixNano": "5",
            "name": "retry",
            "attributes": [{"key": "n", "value": {"intValue": "2"}}],
        }
    ]
    assert s["links"] == [
        {
            "traceId": "0102030405060708090a0b0c0d0e0f10",
            "spanId": "2122232425262728",
        }
    ]


async def test_unset_status_is_omitted() -> None:
    transport = FakeTransport()
    await make_client(transport).export_traces(
        [
            span(
                "s",
                trace_id=TRACE_ID,
                span_id=SPAN_ID,
                start_time_unix_nano=1,
                end_time_unix_nano=2,
            )
        ]
    )
    assert "status" not in only_span(transport.sent[0][1])


async def test_processor_queues_and_flushes_traces() -> None:
    transport = FakeTransport()
    proc = BatchProcessor(make_client(transport), flush_interval=3600.0)
    assert (
        proc.submit_traces(
            [
                span(
                    "s",
                    trace_id=TRACE_ID,
                    span_id=SPAN_ID,
                    start_time_unix_nano=1,
                    end_time_unix_nano=2,
                )
            ]
        )
        is True
    )
    await proc.flush()
    assert transport.sent[0][0] is SignalKind.TRACES
    assert proc.stats.exported == 1
