import json

from otlp_client.client import OTLPClient
from otlp_client.config import OTLPConfig
from otlp_client.encoding.json import JSONEncoder
from otlp_client.model.common import Resource
from otlp_client.model.logs import SeverityNumber, log_record
from otlp_client.outcomes import Success
from otlp_client.processor import BatchProcessor
from otlp_client.signals import SignalKind
from tests.support.fakes import FakeTransport

CONFIG = OTLPConfig(endpoint="http://localhost:4318", resource=Resource(attributes={"a": "b"}))


def make_client(transport: FakeTransport) -> OTLPClient:
    return OTLPClient(CONFIG, transport=transport, encoder=JSONEncoder())


def test_log_record_helper_defaults_observed_time_to_time() -> None:
    rec = log_record("boot complete", time_unix_nano=42, severity=SeverityNumber.INFO)
    assert rec.time_unix_nano == 42
    assert rec.observed_time_unix_nano == 42
    assert rec.severity_number is SeverityNumber.INFO
    assert rec.severity_text == "INFO"


async def test_export_logs_envelope_and_field_encoding() -> None:
    transport = FakeTransport()
    result = await make_client(transport).export_logs(
        [log_record("hello", time_unix_nano=7, severity=SeverityNumber.WARN,
                    attributes={"logger": "hass.core"})]
    )
    assert isinstance(result, Success)
    kind, payload = transport.sent[0]
    assert kind is SignalKind.LOGS
    doc = json.loads(payload)
    (rl,) = doc["resourceLogs"]
    (sl,) = rl["scopeLogs"]
    (record,) = sl["logRecords"]
    assert record["timeUnixNano"] == "7"
    assert record["observedTimeUnixNano"] == "7"
    assert record["severityNumber"] == 13
    assert record["severityText"] == "WARN"
    assert record["body"] == {"stringValue": "hello"}
    assert record["attributes"] == [
        {"key": "logger", "value": {"stringValue": "hass.core"}}
    ]


async def test_severity_number_is_an_integer_not_a_name() -> None:
    transport = FakeTransport()
    await make_client(transport).export_logs(
        [log_record("x", time_unix_nano=1, severity=SeverityNumber.ERROR)]
    )
    record = json.loads(transport.sent[0][1])["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]
    assert record["severityNumber"] == 17
    assert not isinstance(record["severityNumber"], str)


async def test_trace_and_span_ids_are_hex_not_base64() -> None:
    transport = FakeTransport()
    await make_client(transport).export_logs([
        log_record("x", time_unix_nano=1,
                   trace_id=bytes.fromhex("0102030405060708090a0b0c0d0e0f10"),
                   span_id=bytes.fromhex("1112131415161718"))
    ])
    record = json.loads(transport.sent[0][1])["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]
    assert record["traceId"] == "0102030405060708090a0b0c0d0e0f10"
    assert record["spanId"] == "1112131415161718"


async def test_structured_body_is_encoded_as_any_value() -> None:
    transport = FakeTransport()
    await make_client(transport).export_logs(
        [log_record({"event": "state_changed", "count": 3}, time_unix_nano=1)]
    )
    record = json.loads(transport.sent[0][1])["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]
    assert record["body"] == {"kvlistValue": {"values": [
        {"key": "event", "value": {"stringValue": "state_changed"}},
        {"key": "count", "value": {"intValue": "3"}},
    ]}}


async def test_processor_queues_and_flushes_logs() -> None:
    transport = FakeTransport()
    proc = BatchProcessor(make_client(transport), flush_interval=3600.0)
    assert proc.submit_logs([log_record("a", time_unix_nano=1)]) is True
    await proc.flush()
    assert transport.sent[0][0] is SignalKind.LOGS
    assert proc.stats.exported == 1


async def test_processor_keeps_metrics_and_logs_in_separate_queues() -> None:
    from otlp_client.model.metrics import gauge

    transport = FakeTransport()
    proc = BatchProcessor(make_client(transport), flush_interval=3600.0)
    proc.submit_metrics([gauge("t", 1.0, time_unix_nano=1)])
    proc.submit_logs([log_record("a", time_unix_nano=1)])
    await proc.flush()
    assert {kind for kind, _ in transport.sent} == {SignalKind.METRICS, SignalKind.LOGS}
    assert proc.stats.exported == 2
