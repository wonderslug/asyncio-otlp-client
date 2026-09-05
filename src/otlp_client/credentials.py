"""Credentials resolved per request, rather than frozen into OTLPConfig.

The OTLP specification defines no authentication concept, so nothing here is
conformance work: a provider is consulted on every export attempt, which is
what makes a rotating token possible without rebuilding the client.
"""

from __future__ import annotations

import base64
from collections.abc import Awaitable, Callable, Mapping
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


class BearerToken:
    """An `authorization: Bearer ...` header, static or fetched per attempt.

    A `str` source is a token you already hold. A callable source is consulted
    on every attempt and its result is never cached here -- the caching belongs
    to whoever owns the token, and a cache this class did not agree to would be
    exactly what makes a rotated token look stale.
    """

    def __init__(self, source: str | Callable[[], Awaitable[str]]) -> None:
        self._source = source

    async def headers(self, kind: SignalKind) -> Mapping[str, str]:
        token = self._source if isinstance(self._source, str) else await self._source()
        return {"authorization": f"Bearer {token}"}

    async def invalidate(self, kind: SignalKind) -> None:
        """No cache to drop: a callable source is re-consulted next call anyway."""


class BasicAuth:
    """An RFC 7617 `authorization: Basic ...` header.

    Static, but it keeps the secret out of the config and environment surface,
    where it would otherwise sit inside an OTEL_EXPORTER_OTLP_HEADERS string.
    """

    def __init__(self, username: str, password: str) -> None:
        raw = f"{username}:{password}".encode()
        self._header = f"Basic {base64.b64encode(raw).decode('ascii')}"

    async def headers(self, kind: SignalKind) -> Mapping[str, str]:
        return {"authorization": self._header}

    async def invalidate(self, kind: SignalKind) -> None:
        """Nothing is cached; the credential cannot go stale."""
