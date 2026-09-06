"""The transport seam."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from otlp_client.outcomes import ExportOutcome
from otlp_client.signals import SignalKind


class Transport(Protocol):
    """Ships already-encoded bytes and classifies the response."""

    async def send(
        self, kind: SignalKind, payload: bytes, headers: Mapping[str, str]
    ) -> ExportOutcome:
        """Deliver one encoded batch with the headers the client resolved.

        Never retries; classification only. The caller owns header resolution,
        so a transport adds only what the wire format itself requires.
        """

    async def aclose(self) -> None:
        """Release any resources this transport owns."""
