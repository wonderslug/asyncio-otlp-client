from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer

from otlp_client.credentials import AuthStyle, CredentialProvider, OAuth2ClientCredentials
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
