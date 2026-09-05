from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncIterator, Awaitable, Callable
from urllib.parse import quote_plus

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer

from otlp_client.credentials import AuthStyle, CredentialProvider, OAuth2ClientCredentials
from otlp_client.errors import OTLPPermanentError, OTLPTransportError
from otlp_client.signals import SignalKind
from tests.support.fakes import FakeClock

METRICS = SignalKind.METRICS


class TokenEndpoint:
    """A token endpoint that hands out a numbered token per request."""

    def __init__(
        self,
        status: int = 200,
        body: dict[str, object] | None = None,
        expires_in: int | None = 3600,
    ) -> None:
        self.status = status
        self.body = body
        self.expires_in = expires_in
        self.requests: list[tuple[dict[str, str], dict[str, str]]] = []

    async def handle(self, request: web.Request) -> web.Response:
        form = dict(await request.post())
        self.requests.append((dict(request.headers), {k: str(v) for k, v in form.items()}))
        if self.body is not None:
            return web.json_response(self.body, status=self.status)
        payload: dict[str, object] = {
            "access_token": f"token-{len(self.requests)}",
            "token_type": "Bearer",
        }
        if self.expires_in is not None:
            payload["expires_in"] = self.expires_in
        return web.json_response(payload, status=self.status)


EndpointFactory = Callable[[TokenEndpoint], Awaitable[tuple[str, ClientSession]]]


@pytest.fixture
async def token_server() -> AsyncIterator[EndpointFactory]:
    servers: list[TestServer] = []
    sessions: list[ClientSession] = []

    async def make(endpoint: TokenEndpoint) -> tuple[str, ClientSession]:
        app = web.Application()
        app.router.add_route("POST", "/token", endpoint.handle)
        server = TestServer(app)
        await server.start_server()
        servers.append(server)
        session = ClientSession()
        sessions.append(session)
        return str(server.make_url("/token")), session

    yield make
    for s in sessions:
        await s.close()
    for srv in servers:
        await srv.close()


async def test_mints_a_token_and_returns_it_as_a_bearer_header(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint()
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="sh", session=session
    )
    assert await provider.headers(METRICS) == {"authorization": "Bearer token-1"}
    _, form = endpoint.requests[0]
    assert form["grant_type"] == "client_credentials"
    assert form["client_id"] == "id"
    assert form["client_secret"] == "sh"


async def test_the_token_is_cached_across_calls(token_server: EndpointFactory) -> None:
    endpoint = TokenEndpoint()
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="sh", session=session
    )
    await provider.headers(METRICS)
    await provider.headers(SignalKind.LOGS)
    assert len(endpoint.requests) == 1


async def test_the_token_refreshes_inside_the_expiry_skew(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint(expires_in=100)
    url, session = await token_server(endpoint)
    clock = FakeClock()
    provider = OAuth2ClientCredentials(
        token_url=url,
        client_id="id",
        client_secret="sh",
        session=session,
        expiry_skew=30.0,
        monotonic=clock.monotonic,
    )
    assert await provider.headers(METRICS) == {"authorization": "Bearer token-1"}
    await clock.sleep(69.0)  # 31s of life left: still outside the skew
    assert await provider.headers(METRICS) == {"authorization": "Bearer token-1"}
    await clock.sleep(2.0)  # 29s left: inside the skew, refresh
    assert await provider.headers(METRICS) == {"authorization": "Bearer token-2"}


async def test_a_response_without_expires_in_uses_the_default_ttl(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint(expires_in=None)
    url, session = await token_server(endpoint)
    clock = FakeClock()
    provider = OAuth2ClientCredentials(
        token_url=url,
        client_id="id",
        client_secret="sh",
        session=session,
        default_ttl=300.0,
        expiry_skew=0.0,
        monotonic=clock.monotonic,
    )
    await provider.headers(METRICS)
    await clock.sleep(299.0)
    assert len(endpoint.requests) == 1
    await clock.sleep(2.0)
    await provider.headers(METRICS)
    assert len(endpoint.requests) == 2


async def test_scope_and_extra_params_reach_the_form(token_server: EndpointFactory) -> None:
    endpoint = TokenEndpoint()
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url,
        client_id="id",
        client_secret="sh",
        scope="metrics:write",
        extra_params={"audience": "collector"},
        session=session,
    )
    await provider.headers(METRICS)
    _, form = endpoint.requests[0]
    assert form["scope"] == "metrics:write"
    assert form["audience"] == "collector"


async def test_basic_auth_style_sends_credentials_in_the_header(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint()
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url,
        client_id="id",
        client_secret="sh",
        auth_style=AuthStyle.BASIC,
        session=session,
    )
    await provider.headers(METRICS)
    headers, form = endpoint.requests[0]
    assert headers["Authorization"].startswith("Basic ")
    assert "client_secret" not in form


async def test_basic_auth_style_percent_encodes_each_half(
    token_server: EndpointFactory,
) -> None:
    """RFC 6749 section 2.3.1: each half is form-urlencoded before the
    colon-join, so a secret with a ':' or a non-ASCII character survives
    intact rather than corrupting the split between id and secret.

    Against the old `f"{id}:{secret}".encode()` join, the decoded header
    would be "id:sh:ut€up" -- splitting on the first ':' recovers the raw
    secret "sh:ut€up" (not percent-encoded), so this assertion fails
    against that code and only passes once each half is quote_plus'd first.
    """
    endpoint = TokenEndpoint()
    url, session = await token_server(endpoint)
    secret = "sh:ut€up"
    provider = OAuth2ClientCredentials(
        token_url=url,
        client_id="id",
        client_secret=secret,
        auth_style=AuthStyle.BASIC,
        session=session,
    )
    await provider.headers(METRICS)
    headers, _ = endpoint.requests[0]
    raw = base64.b64decode(headers["Authorization"].removeprefix("Basic "))
    decoded_id, decoded_secret = raw.decode("utf-8").split(":", 1)
    assert decoded_id == quote_plus("id")
    assert decoded_secret == quote_plus(secret)


async def test_a_borrowed_session_is_not_closed(token_server: EndpointFactory) -> None:
    endpoint = TokenEndpoint()
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="sh", session=session
    )
    await provider.headers(METRICS)
    await provider.aclose()
    assert session.closed is False


async def test_an_owned_session_is_created_lazily_and_closed(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint()
    url, _ = await token_server(endpoint)
    provider = OAuth2ClientCredentials(token_url=url, client_id="id", client_secret="sh")
    await provider.headers(METRICS)
    owned = provider._session
    assert owned is not None
    await provider.aclose()
    assert owned.closed is True


def test_the_helper_satisfies_the_protocol() -> None:
    provider = OAuth2ClientCredentials(token_url="http://x/token", client_id="i", client_secret="s")
    assert isinstance(provider, CredentialProvider)


def test_the_helper_is_exported_from_the_package() -> None:
    import otlp_client

    for name in ("OAuth2ClientCredentials", "AuthStyle"):
        assert name in otlp_client.__all__
        assert hasattr(otlp_client, name)


async def test_concurrent_calls_mint_exactly_one_token(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint()
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="sh", session=session
    )
    results = await asyncio.gather(*(provider.headers(METRICS) for _ in range(10)))
    assert len(endpoint.requests) == 1
    assert {tuple(r.items()) for r in results} == {(("authorization", "Bearer token-1"),)}


async def test_invalidate_forces_a_fresh_mint(token_server: EndpointFactory) -> None:
    endpoint = TokenEndpoint()
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="sh", session=session
    )
    assert await provider.headers(METRICS) == {"authorization": "Bearer token-1"}
    await provider.invalidate(METRICS)
    assert await provider.headers(METRICS) == {"authorization": "Bearer token-2"}


async def test_concurrent_calls_after_an_invalidate_still_mint_once(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint()
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="sh", session=session
    )
    await provider.headers(METRICS)
    await provider.invalidate(METRICS)
    await asyncio.gather(*(provider.headers(METRICS) for _ in range(10)))
    assert len(endpoint.requests) == 2


@pytest.mark.parametrize("status", [400, 401])
async def test_an_rfc_6749_error_response_is_permanent(
    token_server: EndpointFactory, status: int
) -> None:
    endpoint = TokenEndpoint(
        status=status,
        body={"error": "invalid_client", "error_description": "bad secret"},
    )
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="hunter2", session=session
    )
    with pytest.raises(OTLPPermanentError) as caught:
        await provider.headers(METRICS)
    assert "invalid_client" in str(caught.value)
    assert "bad secret" in str(caught.value)


async def test_a_server_error_is_transport_shaped(token_server: EndpointFactory) -> None:
    endpoint = TokenEndpoint(status=503, body={"error": "temporarily_unavailable"})
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="sh", session=session
    )
    with pytest.raises(OTLPTransportError):
        await provider.headers(METRICS)


async def test_an_unreachable_token_endpoint_is_transport_shaped() -> None:
    provider = OAuth2ClientCredentials(
        token_url="http://127.0.0.1:1/token", client_id="id", client_secret="sh"
    )
    with pytest.raises(OTLPTransportError):
        await provider.headers(METRICS)
    await provider.aclose()


async def test_a_response_without_an_access_token_is_permanent(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint(body={"token_type": "Bearer"})
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="sh", session=session
    )
    with pytest.raises(OTLPPermanentError):
        await provider.headers(METRICS)


async def test_a_non_json_body_is_permanent(token_server: EndpointFactory) -> None:
    async def handle(request: web.Request) -> web.Response:
        return web.Response(status=200, body=b"<html>not json</html>")

    app = web.Application()
    app.router.add_route("POST", "/token", handle)
    server = TestServer(app)
    await server.start_server()
    session = ClientSession()
    provider = OAuth2ClientCredentials(
        token_url=str(server.make_url("/token")),
        client_id="id",
        client_secret="sh",
        session=session,
    )
    with pytest.raises(OTLPPermanentError):
        await provider.headers(METRICS)
    await session.close()
    await server.close()


async def test_a_server_error_with_a_non_json_body_is_still_transport_shaped(
    token_server: EndpointFactory,
) -> None:
    # A proxy or load balancer answering 503 with an HTML error page must be
    # retried, not treated as a malformed response.
    async def handle(request: web.Request) -> web.Response:
        return web.Response(status=503, body=b"<html>bad gateway</html>")

    app = web.Application()
    app.router.add_route("POST", "/token", handle)
    server = TestServer(app)
    await server.start_server()
    session = ClientSession()
    provider = OAuth2ClientCredentials(
        token_url=str(server.make_url("/token")),
        client_id="id",
        client_secret="sh",
        session=session,
    )
    with pytest.raises(OTLPTransportError):
        await provider.headers(METRICS)
    await session.close()
    await server.close()


async def test_the_client_secret_never_appears_in_an_error(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint(status=401, body={"error": "invalid_client"})
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="hunter2", session=session
    )
    with pytest.raises(OTLPPermanentError) as caught:
        await provider.headers(METRICS)
    assert "hunter2" not in str(caught.value)
