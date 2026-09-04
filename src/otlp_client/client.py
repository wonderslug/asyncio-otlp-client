"""The OTLP client: encode, ship, retry, classify."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from types import TracebackType
from typing import Self

from otlp_client.config import OTLPConfig, OTLPProtocol
from otlp_client.encoding.base import Encoder
from otlp_client.errors import OTLPConfigError, OTLPPermanentError, OTLPTransportError
from otlp_client.model.common import InstrumentationScope, Resource
from otlp_client.model.metrics import Metric, ResourceMetrics, ScopeMetrics
from otlp_client.outcomes import ExportOutcome, PartialSuccess, Permanent, Retryable, Success
from otlp_client.retry import RetryPolicy, with_retry
from otlp_client.signals import SignalKind
from otlp_client.transport.base import Transport

__version__ = "0.1.0"

DEFAULT_SCOPE = InstrumentationScope(name="otlp_client", version=__version__)
_EMPTY_RESOURCE = Resource()


def _build_encoder(config: OTLPConfig) -> Encoder:
    """Pick an encoder, importing optional extras only when they are asked for."""
    if config.protocol is OTLPProtocol.HTTP_JSON:
        from otlp_client.encoding.json import JSONEncoder

        return JSONEncoder()
    from otlp_client.encoding.protobuf import build_protobuf_encoder

    encoder: Encoder = build_protobuf_encoder()  # type: ignore[assignment]
    return encoder


class OTLPClient:
    """Exports OTLP signals. One round trip per call, with retries."""

    def __init__(
        self,
        config: OTLPConfig,
        *,
        transport: Transport,
        encoder: Encoder,
        scope: InstrumentationScope | None = None,
        policy: RetryPolicy | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._config = config
        self._transport = transport
        self._encoder = encoder
        self._scope = scope or DEFAULT_SCOPE
        self._policy = policy or RetryPolicy.from_config(config)
        self._sleep = sleep
        self._monotonic = monotonic

    @classmethod
    async def create(
        cls,
        config: OTLPConfig,
        *,
        session: object | None = None,
        scope: InstrumentationScope | None = None,
    ) -> OTLPClient:
        """Build a client for the configured protocol.

        `session` is an `aiohttp.ClientSession` for the HTTP protocols. Home
        Assistant integrations must pass `async_get_clientsession(hass)`.
        """
        encoder = _build_encoder(config)
        if config.protocol is OTLPProtocol.GRPC:
            from otlp_client.transport.grpc import GRPCTransport

            transport: Transport = await GRPCTransport.create(config, encoder)
        else:
            import aiohttp

            from otlp_client.transport.http import HTTPTransport

            if session is not None and not isinstance(session, aiohttp.ClientSession):
                raise OTLPConfigError("session must be an aiohttp.ClientSession")
            transport = await HTTPTransport.create(config, encoder, session=session)
        return cls(config, transport=transport, encoder=encoder, scope=scope)

    @property
    def resource(self) -> Resource:
        return self._config.resource or _EMPTY_RESOURCE

    async def _export(self, kind: SignalKind, data: Sequence[object]) -> Success | PartialSuccess:
        if not data:
            return Success()
        payload = self._encoder.encode(kind, data)

        async def attempt() -> ExportOutcome:
            return await self._transport.send(kind, payload)

        outcome = await with_retry(
            attempt,
            self._policy,
            sleep=self._sleep,
            monotonic=self._monotonic,
        )
        if isinstance(outcome, Permanent):
            raise OTLPPermanentError(
                f"collector rejected {kind} (status {outcome.status}): {outcome.message}"
            )
        if isinstance(outcome, Retryable):
            raise OTLPTransportError(f"could not deliver {kind} after retries: {outcome.message}")
        return outcome

    async def export_metrics(
        self,
        metrics: Sequence[Metric],
        *,
        resource: Resource | None = None,
        scope: InstrumentationScope | None = None,
    ) -> Success | PartialSuccess:
        """Export metrics, wrapping them in the client's resource and scope."""
        if not metrics:
            return Success()
        envelope = ResourceMetrics(
            resource=resource or self.resource,
            scope_metrics=[ScopeMetrics(scope=scope or self._scope, metrics=list(metrics))],
        )
        return await self.export_resource_metrics([envelope])

    async def export_resource_metrics(
        self, data: Sequence[ResourceMetrics]
    ) -> Success | PartialSuccess:
        """Export fully built metric envelopes."""
        return await self._export(SignalKind.METRICS, data)

    async def aclose(self) -> None:
        await self._transport.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()
