"""The transport seam."""

from __future__ import annotations

from typing import Protocol

from otlp_client.outcomes import ExportOutcome
from otlp_client.signals import SignalKind


class Transport(Protocol):
    """Ships already-encoded bytes and classifies the response."""

    async def send(self, kind: SignalKind, payload: bytes) -> ExportOutcome:
        """Deliver one encoded batch. Never retries; classification only."""

    async def aclose(self) -> None:
        """Release any resources this transport owns."""
