"""OTLP/gRPC transport. Requires the `grpc` extra.

All `grpc` imports are lazy: importing this module must stay free for a
core-only install.
"""

from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import urlparse

from otlp_client.config import Compression, OTLPConfig
from otlp_client.encoding.base import Encoder
from otlp_client.errors import OTLPConfigError
from otlp_client.outcomes import ExportOutcome, Permanent, Retryable, Success
from otlp_client.signals import SignalKind

_MISSING = "the gRPC transport needs the optional extra: pip install 'asyncio-otlp-client[grpc]'"

_METHODS: dict[SignalKind, str] = {
    SignalKind.METRICS: "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
    SignalKind.LOGS: "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
    SignalKind.TRACES: "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
    SignalKind.PROFILES: (
        "/opentelemetry.proto.collector.profiles.v1development.ProfilesService/Export"
    ),
}


def _target(endpoint: str) -> tuple[str, bool]:
    """Split an endpoint into a gRPC target and whether it is plaintext."""
    parsed = urlparse(endpoint if "//" in endpoint else f"//{endpoint}")
    host = parsed.netloc or parsed.path
    return host, parsed.scheme != "https"


def _read_credentials(config: OTLPConfig) -> Any:
    """Build channel credentials. Blocking: only call via asyncio.to_thread."""
    import grpc

    def read(path: str | None) -> bytes | None:
        if not path:
            return None
        with open(path, "rb") as handle:
            return handle.read()

    return grpc.ssl_channel_credentials(
        root_certificates=read(config.certificate_file),
        private_key=read(config.client_key_file),
        certificate_chain=read(config.client_certificate_file),
    )


class GRPCTransport:
    """Ships already-encoded protobuf bytes over an asyncio gRPC channel."""

    def __init__(self, config: OTLPConfig, encoder: Encoder, channel: Any) -> None:
        self._config = config
        self._encoder = encoder
        self._channel = channel

    @classmethod
    async def create(cls, config: OTLPConfig, encoder: Encoder) -> GRPCTransport:
        """Open a channel, doing all blocking credential loading off the loop."""
        if encoder.content_type != "application/x-protobuf":
            raise OTLPConfigError("OTLP over gRPC has no JSON encoding; use the protobuf encoder")
        try:
            from grpc import aio
        except ImportError as exc:
            raise OTLPConfigError(_MISSING) from exc

        target, plaintext = _target(config.endpoint)
        if not plaintext and config.insecure_skip_verify:
            raise OTLPConfigError(
                "insecure_skip_verify is not supported over gRPC: grpcio provides no way "
                "to disable certificate verification. Use certificate_file to trust a "
                "self-signed CA, or switch to an http:// endpoint."
            )
        if config.metrics_endpoint or config.logs_endpoint or config.traces_endpoint:
            raise OTLPConfigError(
                "per-signal endpoint overrides (metrics_endpoint/logs_endpoint/"
                "traces_endpoint) are not supported over gRPC: a single gRPC channel "
                "targets one host and the signal is already selected by the RPC "
                "method path. Use separate OTLPClient instances (one per signal, "
                "each with its own endpoint) if you need this."
            )
        options = [("grpc.primary_user_agent", "asyncio-otlp-client")]
        if plaintext:
            channel = aio.insecure_channel(target, options=options)
        else:
            credentials = await asyncio.to_thread(_read_credentials, config)
            channel = aio.secure_channel(target, credentials, options=options)
        return cls(config, encoder, channel)

    def _classify(self, exc: Any) -> ExportOutcome:
        import grpc

        retryable = {
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
        }
        code = exc.code()
        message = exc.details() or str(code)
        if code in retryable:
            return Retryable(message=message, retry_after=_pushback(exc))
        return Permanent(message=message)

    async def send(self, kind: SignalKind, payload: bytes) -> ExportOutcome:
        import grpc
        from grpc.aio import AioRpcError

        call = self._channel.unary_unary(
            _METHODS[kind],
            request_serializer=lambda value: value,
            response_deserializer=lambda value: value,
        )
        metadata = tuple(self._config.headers.items())
        compression = (
            grpc.Compression.Gzip if self._config.compression is Compression.GZIP else None
        )
        try:
            raw = await call(
                payload, timeout=self._config.timeout, metadata=metadata, compression=compression
            )
        except AioRpcError as exc:
            return self._classify(exc)
        partial = self._encoder.decode_response(kind, raw)
        return partial if partial is not None else Success()

    async def aclose(self) -> None:
        await self._channel.close()


def _pushback(exc: Any) -> float | None:
    """Read the server's `grpc-retry-pushback-ms` hint, if it sent one."""
    for key, value in exc.trailing_metadata() or ():
        if key == "grpc-retry-pushback-ms":
            try:
                return max(0.0, float(value) / 1000.0)
            except (TypeError, ValueError):
                return None
    return None
