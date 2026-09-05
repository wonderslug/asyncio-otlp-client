"""End-to-end checks against a real collector.

Marked `integration` and excluded by default (see addopts in pyproject.toml).
Run with: docker compose -f docker-compose.test.yml up -d
          uv run pytest -m integration
"""

import asyncio
import json
import pathlib
from collections.abc import Callable

import pytest
from aiohttp import ClientSession

from otlp_client.client import OTLPClient
from otlp_client.config import OTLPConfig, OTLPProtocol
from otlp_client.model.common import Resource
from otlp_client.model.logs import SeverityNumber, log_record
from otlp_client.model.metrics import gauge
from otlp_client.model.traces import span
from otlp_client.outcomes import Success

pytestmark = pytest.mark.integration

OUTPUT = pathlib.Path("out/telemetry.json")
RESOURCE = Resource(attributes={"service.name": "asyncio-otlp-client-itest"})
TRACE_ID = bytes.fromhex("0102030405060708090a0b0c0d0e0f10")
SPAN_ID = bytes.fromhex("1112131415161718")


async def wait_for(predicate: Callable[[str], bool], timeout: float = 15.0) -> str:
    """Poll the collector's file exporter until it contains what we sent."""
    async with asyncio.timeout(timeout):
        while True:
            text = OUTPUT.read_text() if OUTPUT.exists() else ""
            if predicate(text):
                return text
            await asyncio.sleep(0.2)


async def export_all(config: OTLPConfig, marker: str) -> None:
    async with ClientSession() as session:
        client = await OTLPClient.create(config, session=session)
        assert isinstance(
            await client.export_metrics([gauge(f"m.{marker}", 21.5, time_unix_nano=1)]), Success
        )
        assert isinstance(
            await client.export_logs(
                [log_record(f"log-{marker}", time_unix_nano=1, severity=SeverityNumber.INFO)]
            ),
            Success,
        )
        assert isinstance(
            await client.export_traces(
                [
                    span(
                        f"span-{marker}",
                        trace_id=TRACE_ID,
                        span_id=SPAN_ID,
                        start_time_unix_nano=1,
                        end_time_unix_nano=2,
                    )
                ]
            ),
            Success,
        )
        await client.aclose()


async def test_json_over_http_is_accepted_by_a_real_collector() -> None:
    config = OTLPConfig(
        endpoint="http://localhost:4318", protocol=OTLPProtocol.HTTP_JSON, resource=RESOURCE
    )
    await export_all(config, "json")
    text = await wait_for(lambda t: "m.json" in t and "log-json" in t and "span-json" in t)
    assert json.loads(text.splitlines()[0])


async def test_protobuf_over_http_is_accepted() -> None:
    config = OTLPConfig(
        endpoint="http://localhost:4318", protocol=OTLPProtocol.HTTP_PROTOBUF, resource=RESOURCE
    )
    await export_all(config, "pb")
    await wait_for(lambda t: "m.pb" in t and "log-pb" in t and "span-pb" in t)


async def test_grpc_is_accepted() -> None:
    pytest.importorskip("grpc")
    config = OTLPConfig(
        endpoint="http://localhost:4317", protocol=OTLPProtocol.GRPC, resource=RESOURCE
    )
    client = await OTLPClient.create(config)
    assert isinstance(
        await client.export_metrics([gauge("m.grpc", 1.0, time_unix_nano=1)]), Success
    )
    await client.aclose()
    await wait_for(lambda t: "m.grpc" in t)
