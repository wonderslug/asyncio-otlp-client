"""OTLP/gRPC transport. Requires the `grpc` extra.

Placeholder: the real transport lands in a later task. Every `grpc` or
`opentelemetry.proto` import must stay inside a function so that importing
this module remains free for a core-only install.
"""

from __future__ import annotations

from otlp_client.config import OTLPConfig
from otlp_client.encoding.base import Encoder
from otlp_client.errors import OTLPConfigError
from otlp_client.outcomes import ExportOutcome
from otlp_client.signals import SignalKind

_MISSING = "the gRPC transport needs the optional extra: pip install 'asyncio-otlp-client[grpc]'"


class GRPCTransport:
    """Not yet implemented; `create` raises with the install hint."""

    @classmethod
    async def create(cls, config: OTLPConfig, encoder: Encoder) -> GRPCTransport:
        """Not yet implemented; raises with the install hint."""
        raise OTLPConfigError(_MISSING)

    async def send(self, kind: SignalKind, payload: bytes) -> ExportOutcome:
        """Not yet implemented; raises with the install hint."""
        raise OTLPConfigError(_MISSING)

    async def aclose(self) -> None:
        """Not yet implemented; raises with the install hint."""
        raise OTLPConfigError(_MISSING)
