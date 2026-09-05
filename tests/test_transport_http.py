import gzip
import json
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer

from otlp_client.config import Compression, OTLPConfig
from otlp_client.encoding.json import JSONEncoder
from otlp_client.outcomes import PartialSuccess, Permanent, Retryable, Success
from otlp_client.signals import SignalKind
from otlp_client.transport.http import HTTPTransport


class Recorder:
    def __init__(
        self,
        status: int = 200,
        body: bytes = b"{}",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status, self.body, self.headers = status, body, headers or {}
        self.requests: list[tuple[str, dict[str, str], bytes]] = []

    async def handle(self, request: web.Request) -> web.Response:
        self.requests.append((request.path, dict(request.headers), await request.read()))
        return web.Response(status=self.status, body=self.body, headers=self.headers)


ServerFactory = Callable[[Recorder], Awaitable[tuple[str, ClientSession]]]


@pytest.fixture
async def server_factory() -> AsyncIterator[ServerFactory]:
    servers: list[TestServer] = []
    sessions: list[ClientSession] = []

    async def make(recorder: Recorder) -> tuple[str, ClientSession]:
        # auto_decompress=False: aiohttp's web server otherwise transparently
        # ungzips the request body based on Content-Encoding before handlers
        # ever see it, which would defeat test_gzip_sets_content_encoding_and_compresses.
        app = web.Application(handler_args={"auto_decompress": False})
        app.router.add_route("POST", "/{tail:.*}", recorder.handle)
        server = TestServer(app)
        await server.start_server()
        servers.append(server)
        session = ClientSession()
        sessions.append(session)
        return str(server.make_url("")).rstrip("/"), session

    yield make
    for s in sessions:
        await s.close()
    for srv in servers:
        await srv.close()


async def test_posts_to_the_signal_path_with_content_type(server_factory: ServerFactory) -> None:
    rec = Recorder()
    base, session = await server_factory(rec)
    transport = HTTPTransport(OTLPConfig(endpoint=base), JSONEncoder(), session=session)
    result = await transport.send(SignalKind.METRICS, b'{"resourceMetrics":[]}', {})
    assert isinstance(result, Success)
    path, headers, body = rec.requests[0]
    assert path == "/v1/metrics"
    assert headers["Content-Type"] == "application/json"
    assert body == b'{"resourceMetrics":[]}'


async def test_custom_headers_are_sent(server_factory: ServerFactory) -> None:
    # Header resolution moved to the client; the transport just forwards
    # whatever headers it is handed.
    rec = Recorder()
    base, session = await server_factory(rec)
    transport = HTTPTransport(OTLPConfig(endpoint=base), JSONEncoder(), session=session)
    await transport.send(SignalKind.LOGS, b"{}", {"api-key": "secret"})
    assert rec.requests[0][1]["api-key"] == "secret"
    assert rec.requests[0][0] == "/v1/logs"


async def test_gzip_sets_content_encoding_and_compresses(server_factory: ServerFactory) -> None:
    rec = Recorder()
    base, session = await server_factory(rec)
    cfg = OTLPConfig(endpoint=base, compression=Compression.GZIP, gzip_threshold=0)
    transport = HTTPTransport(cfg, JSONEncoder(), session=session)
    await transport.send(SignalKind.METRICS, b'{"a":1}', {})
    _, headers, body = rec.requests[0]
    assert headers["Content-Encoding"] == "gzip"
    assert gzip.decompress(body) == b'{"a":1}'


async def test_partial_success_is_surfaced(server_factory: ServerFactory) -> None:
    body = json.dumps({"partialSuccess": {"rejectedDataPoints": "2", "errorMessage": "x"}})
    rec = Recorder(body=body.encode())
    base, session = await server_factory(rec)
    transport = HTTPTransport(OTLPConfig(endpoint=base), JSONEncoder(), session=session)
    result = await transport.send(SignalKind.METRICS, b"{}", {})
    assert isinstance(result, PartialSuccess)
    assert result.rejected == 2


@pytest.mark.parametrize("status", [429, 502, 503, 504])
async def test_retryable_statuses(server_factory: ServerFactory, status: int) -> None:
    rec = Recorder(status=status, body=b"busy")
    base, session = await server_factory(rec)
    transport = HTTPTransport(OTLPConfig(endpoint=base), JSONEncoder(), session=session)
    result = await transport.send(SignalKind.METRICS, b"{}", {})
    assert isinstance(result, Retryable)
    assert result.status == status


async def test_retry_after_header_is_parsed(server_factory: ServerFactory) -> None:
    rec = Recorder(status=503, headers={"Retry-After": "12"})
    base, session = await server_factory(rec)
    transport = HTTPTransport(OTLPConfig(endpoint=base), JSONEncoder(), session=session)
    result = await transport.send(SignalKind.METRICS, b"{}", {})
    assert isinstance(result, Retryable)
    assert result.retry_after == 12.0


@pytest.mark.parametrize("status", [400, 401, 404, 422])
async def test_client_errors_are_permanent(server_factory: ServerFactory, status: int) -> None:
    rec = Recorder(status=status, body=b"nope")
    base, session = await server_factory(rec)
    transport = HTTPTransport(OTLPConfig(endpoint=base), JSONEncoder(), session=session)
    result = await transport.send(SignalKind.METRICS, b"{}", {})
    assert isinstance(result, Permanent)
    assert result.status == status


async def test_connection_failure_is_retryable() -> None:
    async with ClientSession() as session:
        cfg = OTLPConfig(endpoint="http://127.0.0.1:1", timeout=1.0)
        transport = HTTPTransport(cfg, JSONEncoder(), session=session)
        result = await transport.send(SignalKind.METRICS, b"{}", {})
        assert isinstance(result, Retryable)


async def test_injected_session_is_not_closed(server_factory: ServerFactory) -> None:
    rec = Recorder()
    base, session = await server_factory(rec)
    transport = HTTPTransport(OTLPConfig(endpoint=base), JSONEncoder(), session=session)
    await transport.aclose()
    assert session.closed is False


async def test_created_session_is_owned_and_closed() -> None:
    transport = await HTTPTransport.create(
        OTLPConfig(endpoint="http://localhost:4318"), JSONEncoder()
    )
    session = transport._session
    await transport.aclose()
    assert session.closed is True


async def test_sends_exactly_the_headers_it_was_given(
    server_factory: ServerFactory,
) -> None:
    # Header resolution moved to the client; this asserts only that the
    # transport transmits what it is handed. Resolution itself is covered by
    # the headers_for tests in test_config.py.
    rec = Recorder()
    base, session = await server_factory(rec)
    transport = HTTPTransport(OTLPConfig(endpoint=base), JSONEncoder(), session=session)
    await transport.send(SignalKind.TRACES, b"{}", {"x-tenant": "acme"})
    sent = rec.requests[0][1]
    assert sent["x-tenant"] == "acme"
    assert "api-key" not in sent


async def test_per_signal_compression_applies_to_that_signal_only(
    server_factory: ServerFactory,
) -> None:
    rec = Recorder()
    base, session = await server_factory(rec)
    cfg = OTLPConfig(endpoint=base, logs_compression=Compression.GZIP)
    transport = HTTPTransport(cfg, JSONEncoder(), session=session)
    await transport.send(SignalKind.LOGS, b'{"resourceLogs":[]}', {})
    await transport.send(SignalKind.METRICS, b'{"resourceMetrics":[]}', {})
    logs_headers, logs_body = rec.requests[0][1], rec.requests[0][2]
    metrics_headers = rec.requests[1][1]
    assert logs_headers["Content-Encoding"] == "gzip"
    assert gzip.decompress(logs_body) == b'{"resourceLogs":[]}'
    assert "Content-Encoding" not in metrics_headers


async def test_per_signal_timeout_is_used_for_that_signal(
    server_factory: ServerFactory,
) -> None:
    # White-box on purpose: asserting a real timeout would need a deliberately
    # slow server, and the behaviour worth pinning here is that the transport
    # builds one ClientTimeout per signal from timeout_for.
    rec = Recorder()
    base, session = await server_factory(rec)
    cfg = OTLPConfig(endpoint=base, timeout=10.0, metrics_timeout=2.5)
    transport = HTTPTransport(cfg, JSONEncoder(), session=session)
    assert transport._timeouts[SignalKind.METRICS].total == 2.5
    assert transport._timeouts[SignalKind.LOGS].total == 10.0
