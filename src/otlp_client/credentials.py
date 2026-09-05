"""Credentials resolved per request, rather than frozen into OTLPConfig.

The OTLP specification defines no authentication concept, so nothing here is
conformance work: a provider is consulted on every export attempt, which is
what makes a rotating token possible without rebuilding the client.
"""

from __future__ import annotations

import base64
import time
from collections.abc import Awaitable, Callable, Mapping
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from otlp_client.signals import SignalKind

if TYPE_CHECKING:
    import aiohttp


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


class AuthStyle(StrEnum):
    """How client credentials are presented, per RFC 6749 section 2.3.1."""

    POST = "post"
    BASIC = "basic"


class OAuth2ClientCredentials:
    """An OAuth2 client-credentials token, cached until it nears expiry.

    Pass a `session` whenever one exists; Home Assistant integrations must pass
    `async_get_clientsession(hass)` rather than letting this create one. A
    session this provider creates is owned by it and released by `aclose()`.

    The client never closes a provider -- one provider shared across several
    OTLPClient instances is a first-class pattern -- so the caller owns the
    lifetime of whatever it passes here.
    """

    def __init__(
        self,
        *,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: str | None = None,
        extra_params: Mapping[str, str] | None = None,
        auth_style: AuthStyle = AuthStyle.POST,
        session: aiohttp.ClientSession | None = None,
        expiry_skew: float = 30.0,
        default_ttl: float = 300.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._extra_params = dict(extra_params or {})
        self._auth_style = auth_style
        self._session = session
        self._owns_session = False
        self._expiry_skew = expiry_skew
        self._default_ttl = default_ttl
        self._monotonic = monotonic
        self._token: str | None = None
        self._expires_at = 0.0

    async def headers(self, kind: SignalKind) -> Mapping[str, str]:
        if self._token is None or self._monotonic() >= self._expires_at - self._expiry_skew:
            await self._refresh()
        return {"authorization": f"Bearer {self._token}"}

    async def invalidate(self, kind: SignalKind) -> None:
        self._token = None

    async def aclose(self) -> None:
        """Close the session only if this provider created it."""
        if self._owns_session and self._session is not None:
            await self._session.close()

    def _form(self) -> dict[str, str]:
        form = {"grant_type": "client_credentials", **self._extra_params}
        if self._scope:
            form["scope"] = self._scope
        if self._auth_style is AuthStyle.POST:
            form["client_id"] = self._client_id
            form["client_secret"] = self._client_secret
        return form

    def _auth_header(self) -> dict[str, str]:
        if self._auth_style is not AuthStyle.BASIC:
            return {}
        raw = f"{self._client_id}:{self._client_secret}".encode()
        return {"Authorization": f"Basic {base64.b64encode(raw).decode('ascii')}"}

    async def _ensure_session(self) -> aiohttp.ClientSession:
        import aiohttp

        if self._session is None:
            self._session = aiohttp.ClientSession()
            self._owns_session = True
        return self._session

    async def _refresh(self) -> None:
        session = await self._ensure_session()
        async with session.post(
            self._token_url, data=self._form(), headers=self._auth_header()
        ) as response:
            body = await response.json(content_type=None)
        token = body.get("access_token")
        expires_in = body.get("expires_in")
        self._token = token
        ttl = float(expires_in) if expires_in is not None else self._default_ttl
        self._expires_at = self._monotonic() + ttl
