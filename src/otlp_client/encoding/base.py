"""The encoder seam."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from otlp_client.outcomes import PartialSuccess
from otlp_client.signals import SignalKind


class Encoder(Protocol):
    """Turns model dataclasses into request bytes and reads responses back."""

    @property
    def content_type(self) -> str:
        """The Content-Type this encoder produces."""

    def encode(self, kind: SignalKind, data: Sequence[Any]) -> bytes:
        """Encode a sequence of Resource-level envelopes into a request body."""

    def decode_response(self, kind: SignalKind, body: bytes) -> PartialSuccess | None:
        """Return a PartialSuccess if the response reports one, else None."""
