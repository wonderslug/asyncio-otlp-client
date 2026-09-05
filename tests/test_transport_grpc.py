from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

pytest.importorskip("grpc")

import grpc
from grpc import aio

from otlp_client.config import Compression, OTLPConfig, OTLPProtocol
from otlp_client.encoding.protobuf import build_protobuf_encoder
from otlp_client.errors import OTLPConfigError
from otlp_client.outcomes import Permanent, Retryable, Success
from otlp_client.signals import SignalKind
from otlp_client.transport.grpc import GRPCTransport

METRICS_METHOD = "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export"

ServerFactory = Callable[["EchoHandler"], Awaitable[str]]


class EchoHandler(grpc.GenericRpcHandler):
    """Answers any method with a fixed response or a fixed error."""

    def __init__(self, response: bytes = b"", code: grpc.StatusCode | None = None) -> None:
        self.response, self.code = response, code
        self.received: list[tuple[str, bytes]] = []

    def service(
        self, handler_call_details: grpc.HandlerCallDetails
    ) -> grpc.RpcMethodHandler[bytes, bytes]:
        async def handle(request: bytes, context: aio.ServicerContext[bytes, bytes]) -> bytes:
            self.received.append((handler_call_details.method, request))
            if self.code is not None:
                await context.abort(self.code, "scripted failure")
            return self.response

        return grpc.unary_unary_rpc_method_handler(
            handle, request_deserializer=lambda b: b, response_serializer=lambda b: b
        )


@pytest.fixture
async def grpc_server() -> AsyncIterator[ServerFactory]:
    servers: list[aio.Server] = []

    async def start(handler: EchoHandler) -> str:
        server = aio.server()
        server.add_generic_rpc_handlers((handler,))
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        servers.append(server)
        return f"127.0.0.1:{port}"

    yield start
    for server in servers:
        await server.stop(None)


async def make_transport(target: str) -> GRPCTransport:
    config = OTLPConfig(endpoint=f"http://{target}", protocol=OTLPProtocol.GRPC)
    return await GRPCTransport.create(config, build_protobuf_encoder())


async def test_export_hits_the_metrics_service_method(grpc_server: ServerFactory) -> None:
    handler = EchoHandler()
    transport = await make_transport(await grpc_server(handler))
    result = await transport.send(SignalKind.METRICS, b"payload-bytes")
    assert isinstance(result, Success)
    assert handler.received == [(METRICS_METHOD, b"payload-bytes")]
    await transport.aclose()


async def test_logs_and_traces_use_their_own_methods(grpc_server: ServerFactory) -> None:
    handler = EchoHandler()
    transport = await make_transport(await grpc_server(handler))
    await transport.send(SignalKind.LOGS, b"a")
    await transport.send(SignalKind.TRACES, b"b")
    methods = [method for method, _ in handler.received]
    assert methods == [
        "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
        "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
    ]
    await transport.aclose()


@pytest.mark.parametrize(
    "code",
    [
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    ],
)
async def test_transient_status_codes_are_retryable(
    grpc_server: ServerFactory, code: grpc.StatusCode
) -> None:
    transport = await make_transport(await grpc_server(EchoHandler(code=code)))
    assert isinstance(await transport.send(SignalKind.METRICS, b"x"), Retryable)
    await transport.aclose()


@pytest.mark.parametrize(
    "code",
    [
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.UNIMPLEMENTED,
    ],
)
async def test_other_status_codes_are_permanent(
    grpc_server: ServerFactory, code: grpc.StatusCode
) -> None:
    transport = await make_transport(await grpc_server(EchoHandler(code=code)))
    assert isinstance(await transport.send(SignalKind.METRICS, b"x"), Permanent)
    await transport.aclose()


async def test_partial_success_response_is_decoded(grpc_server: ServerFactory) -> None:
    from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
        ExportMetricsServiceResponse,
    )

    response = ExportMetricsServiceResponse()
    response.partial_success.rejected_data_points = 4
    handler = EchoHandler(response=response.SerializeToString())
    transport = await make_transport(await grpc_server(handler))
    result = await transport.send(SignalKind.METRICS, b"x")
    assert getattr(result, "rejected", None) == 4
    await transport.aclose()


async def test_unreachable_server_is_retryable() -> None:
    config = OTLPConfig(endpoint="http://127.0.0.1:1", protocol=OTLPProtocol.GRPC, timeout=1.0)
    transport = await GRPCTransport.create(config, build_protobuf_encoder())
    assert isinstance(await transport.send(SignalKind.METRICS, b"x"), Retryable)
    await transport.aclose()


async def test_json_encoder_is_rejected() -> None:
    from otlp_client.encoding.json import JSONEncoder

    config = OTLPConfig(endpoint="http://127.0.0.1:4317", protocol=OTLPProtocol.GRPC)
    with pytest.raises(OTLPConfigError, match="protobuf"):
        await GRPCTransport.create(config, JSONEncoder())


async def test_insecure_skip_verify_is_rejected_on_a_tls_target() -> None:
    config = OTLPConfig(
        endpoint="https://127.0.0.1:4317",
        protocol=OTLPProtocol.GRPC,
        insecure_skip_verify=True,
    )
    with pytest.raises(OTLPConfigError, match="insecure_skip_verify"):
        await GRPCTransport.create(config, build_protobuf_encoder())


async def test_insecure_skip_verify_is_harmless_on_a_plaintext_target() -> None:
    config = OTLPConfig(
        endpoint="http://127.0.0.1:4317",
        protocol=OTLPProtocol.GRPC,
        insecure_skip_verify=True,
    )
    transport = await GRPCTransport.create(config, build_protobuf_encoder())
    await transport.aclose()


async def test_gzip_compression_round_trips_successfully(grpc_server: ServerFactory) -> None:
    # Wiring compression=grpc.Compression.Gzip onto the call must not change
    # behavior against a well-behaved server: the payload still arrives and
    # the response still decodes, exactly like an uncompressed call.
    handler = EchoHandler()
    target = await grpc_server(handler)
    config = OTLPConfig(
        endpoint=f"http://{target}", protocol=OTLPProtocol.GRPC, compression=Compression.GZIP
    )
    transport = await GRPCTransport.create(config, build_protobuf_encoder())
    result = await transport.send(SignalKind.METRICS, b"payload-bytes")
    assert isinstance(result, Success)
    assert handler.received == [(METRICS_METHOD, b"payload-bytes")]
    await transport.aclose()


async def test_metrics_endpoint_override_is_rejected_over_grpc() -> None:
    # A single gRPC channel targets one host; the signal is already selected
    # by the RPC method path. Silently ignoring these fields (as the HTTP-only
    # endpoint_for() plumbing would otherwise let happen) would route traffic
    # to the wrong place with no indication anything was wrong.
    config = OTLPConfig(
        endpoint="http://127.0.0.1:4317",
        protocol=OTLPProtocol.GRPC,
        metrics_endpoint="https://elsewhere.example",
    )
    with pytest.raises(OTLPConfigError, match="per-signal endpoint"):
        await GRPCTransport.create(config, build_protobuf_encoder())


async def test_logs_endpoint_override_is_rejected_over_grpc() -> None:
    config = OTLPConfig(
        endpoint="http://127.0.0.1:4317",
        protocol=OTLPProtocol.GRPC,
        logs_endpoint="https://elsewhere.example",
    )
    with pytest.raises(OTLPConfigError, match="per-signal endpoint"):
        await GRPCTransport.create(config, build_protobuf_encoder())


async def test_traces_endpoint_override_is_rejected_over_grpc() -> None:
    config = OTLPConfig(
        endpoint="http://127.0.0.1:4317",
        protocol=OTLPProtocol.GRPC,
        traces_endpoint="https://elsewhere.example",
    )
    with pytest.raises(OTLPConfigError, match="per-signal endpoint"):
        await GRPCTransport.create(config, build_protobuf_encoder())
