import json
import sys
from typing import Any

import pytest

from otlp_client.client import OTLPClient, _build_encoder
from otlp_client.config import OTLPConfig, OTLPProtocol
from otlp_client.encoding.json import JSONEncoder
from otlp_client.errors import OTLPConfigError, OTLPPermanentError, OTLPTransportError
from otlp_client.model.common import InstrumentationScope, Resource
from otlp_client.model.metrics import ResourceMetrics, ScopeMetrics, gauge
from otlp_client.model.traces import span
from otlp_client.outcomes import PartialSuccess, Permanent, Retryable, Success
from otlp_client.retry import RetryPolicy
from otlp_client.signals import SignalKind
from tests.support.fakes import FakeClock, FakeTransport

CONFIG = OTLPConfig(
    endpoint="http://localhost:4318", resource=Resource(attributes={"service.name": "hass"})
)


def make_client(transport: FakeTransport, **kwargs: Any) -> OTLPClient:
    return OTLPClient(CONFIG, transport=transport, encoder=JSONEncoder(), **kwargs)


async def test_export_metrics_wraps_metrics_in_config_resource() -> None:
    transport = FakeTransport()
    client = make_client(transport)
    result = await client.export_metrics([gauge("t", 21.5, time_unix_nano=1)])
    assert isinstance(result, Success)
    (kind, payload, _headers) = transport.sent[0]
    assert kind is SignalKind.METRICS
    doc = json.loads(payload)
    (rm,) = doc["resourceMetrics"]
    assert rm["resource"]["attributes"][0]["value"]["stringValue"] == "hass"
    assert rm["scopeMetrics"][0]["metrics"][0]["name"] == "t"


async def test_explicit_resource_overrides_config_resource() -> None:
    transport = FakeTransport()
    client = make_client(transport)
    await client.export_metrics(
        [gauge("t", 1.0, time_unix_nano=1)], resource=Resource(attributes={"host": "pi"})
    )
    doc = json.loads(transport.sent[0][1])
    assert doc["resourceMetrics"][0]["resource"]["attributes"][0]["key"] == "host"


async def test_default_scope_names_this_library() -> None:
    transport = FakeTransport()
    client = make_client(transport)
    await client.export_metrics([gauge("t", 1.0, time_unix_nano=1)])
    doc = json.loads(transport.sent[0][1])
    assert doc["resourceMetrics"][0]["scopeMetrics"][0]["scope"]["name"] == "otlp_client"


async def test_export_resource_metrics_passes_the_envelope_through() -> None:
    transport = FakeTransport()
    client = make_client(transport)
    envelope = ResourceMetrics(
        resource=Resource(attributes={"a": "b"}),
        scope_metrics=[
            ScopeMetrics(
                scope=InstrumentationScope(name="custom"),
                metrics=[gauge("t", 1.0, time_unix_nano=1)],
            )
        ],
    )
    await client.export_resource_metrics([envelope])
    doc = json.loads(transport.sent[0][1])
    assert doc["resourceMetrics"][0]["scopeMetrics"][0]["scope"]["name"] == "custom"


async def test_partial_success_is_returned_not_raised() -> None:
    transport = FakeTransport(outcomes=[PartialSuccess(rejected=2, message="bad")])
    result = await make_client(transport).export_metrics([gauge("t", 1.0, time_unix_nano=1)])
    assert isinstance(result, PartialSuccess)
    assert result.rejected == 2


async def test_permanent_failure_raises() -> None:
    transport = FakeTransport(outcomes=[Permanent(status=400, message="bad request")])
    with pytest.raises(OTLPPermanentError, match="400"):
        await make_client(transport).export_metrics([gauge("t", 1.0, time_unix_nano=1)])


async def test_exhausted_retries_raise_transport_error() -> None:
    clock = FakeClock()
    transport = FakeTransport(outcomes=[Retryable(status=503, message="down")])
    client = make_client(
        transport,
        policy=RetryPolicy(initial_backoff=1.0, max_backoff=1.0, multiplier=1.0, max_elapsed=2.0),
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )
    with pytest.raises(OTLPTransportError, match="down"):
        await client.export_metrics([gauge("t", 1.0, time_unix_nano=1)])
    assert len(transport.sent) >= 2


async def test_retry_then_success_returns_success() -> None:
    clock = FakeClock()
    transport = FakeTransport(outcomes=[Retryable(status=503), Success()])
    client = make_client(transport, sleep=clock.sleep, monotonic=clock.monotonic)
    assert isinstance(await client.export_metrics([gauge("t", 1.0, time_unix_nano=1)]), Success)
    assert len(transport.sent) == 2


async def test_empty_metrics_list_does_not_hit_the_transport() -> None:
    transport = FakeTransport()
    assert isinstance(await make_client(transport).export_metrics([]), Success)
    assert transport.sent == []


async def test_context_manager_closes_the_transport() -> None:
    transport = FakeTransport()
    async with make_client(transport):
        pass
    assert transport.closed is True


def test_http_protobuf_without_extra_names_the_protobuf_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The dev environment installs opentelemetry-proto (so the real protobuf
    # encoder's own tests can run), so simulate the extra being absent by
    # poisoning sys.modules for the one module build_protobuf_encoder() probes.
    monkeypatch.setitem(sys.modules, "opentelemetry.proto.common.v1.common_pb2", None)
    config = OTLPConfig(endpoint="http://localhost:4318", protocol=OTLPProtocol.HTTP_PROTOBUF)
    with pytest.raises(OTLPConfigError, match=r"pip install 'asyncio-otlp-client\[protobuf\]'"):
        _build_encoder(config)


def test_grpc_without_extra_names_the_grpc_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "opentelemetry.proto.common.v1.common_pb2", None)
    config = OTLPConfig(endpoint="http://localhost:4318", protocol=OTLPProtocol.GRPC)
    with pytest.raises(OTLPConfigError, match=r"pip install 'asyncio-otlp-client\[grpc\]'"):
        _build_encoder(config)


async def test_client_resolves_per_signal_headers_and_passes_them_to_the_transport() -> None:
    config = OTLPConfig(
        endpoint="http://localhost:4318",
        headers={"api-key": "secret"},
        traces_headers={"x-tenant": "acme"},
    )
    transport = FakeTransport()
    client = OTLPClient(config, transport=transport, encoder=JSONEncoder())
    await client.export_traces(
        [
            span(
                "s",
                trace_id=b"\x01" * 16,
                span_id=b"\x02" * 8,
                start_time_unix_nano=1,
                end_time_unix_nano=2,
            )
        ]
    )
    await client.export_metrics([gauge("t", 1.0, time_unix_nano=1)])
    assert transport.sent[0][2] == {"x-tenant": "acme"}
    assert transport.sent[1][2] == {"api-key": "secret"}
