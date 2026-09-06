from __future__ import annotations

import base64

from otlp_client.credentials import BasicAuth, BearerToken, CredentialProvider
from otlp_client.signals import SignalKind

METRICS = SignalKind.METRICS


async def test_a_static_bearer_token_becomes_an_authorization_header() -> None:
    provider = BearerToken("abc123")
    assert await provider.headers(METRICS) == {"authorization": "Bearer abc123"}


async def test_a_callable_source_is_consulted_on_every_call() -> None:
    tokens = iter(["one", "two"])

    async def source() -> str:
        return next(tokens)

    provider = BearerToken(source)
    assert await provider.headers(METRICS) == {"authorization": "Bearer one"}
    assert await provider.headers(METRICS) == {"authorization": "Bearer two"}


async def test_invalidating_a_bearer_token_is_harmless() -> None:
    provider = BearerToken("abc123")
    await provider.invalidate(METRICS)
    assert await provider.headers(METRICS) == {"authorization": "Bearer abc123"}


async def test_basic_auth_matches_the_rfc_7617_vector() -> None:
    # RFC 7617 section 2: "Aladdin:open sesame" -> QWxhZGRpbjpvcGVuIHNlc2FtZQ==
    provider = BasicAuth("Aladdin", "open sesame")
    headers = await provider.headers(METRICS)
    assert headers == {"authorization": "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="}


async def test_basic_auth_encodes_non_ascii_as_utf8() -> None:
    provider = BasicAuth("user", "pässwörd")
    (value,) = (await provider.headers(METRICS)).values()
    encoded = value.removeprefix("Basic ")
    assert base64.b64decode(encoded).decode("utf-8") == "user:pässwörd"


def test_the_helpers_satisfy_the_protocol() -> None:
    assert isinstance(BearerToken("t"), CredentialProvider)
    assert isinstance(BasicAuth("u", "p"), CredentialProvider)


def test_the_helpers_are_exported_from_the_package() -> None:
    import otlp_client

    for name in ("CredentialProvider", "BearerToken", "BasicAuth"):
        assert name in otlp_client.__all__
        assert hasattr(otlp_client, name)
