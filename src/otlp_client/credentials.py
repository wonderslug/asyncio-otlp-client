"""Credentials resolved per request, rather than frozen into OTLPConfig.

The OTLP specification defines no authentication concept, so nothing here is
conformance work: a provider is consulted on every export attempt, which is
what makes a rotating token possible without rebuilding the client.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, runtime_checkable

from otlp_client.signals import SignalKind


@runtime_checkable
class CredentialProvider(Protocol):
    """Supplies credential headers for one export attempt.

    Both methods take the signal so a provider can hold a different credential
    per signal. The shipped helpers ignore it.
    """

    async def headers(self, kind: SignalKind) -> Mapping[str, str]:
        """Return the credential headers for this attempt.

        Merged over the configured static headers, winning any key collision.
        Awaited on every attempt, so an implementation that caches owns its own
        expiry.
        """
        ...

    async def invalidate(self, kind: SignalKind) -> None:
        """The collector rejected what `headers()` last returned.

        Drop any cached credential so the next `headers()` call mints a fresh
        one. A stateless provider implements this as `pass`.
        """
        ...
