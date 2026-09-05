from __future__ import annotations

import pytest

from otlp_client.client import OTLPClient
from otlp_client.config import OTLPConfig
from otlp_client.credentials import CredentialProvider
from otlp_client.encoding.json import JSONEncoder
from otlp_client.errors import OTLPPermanentError, OTLPTransportError
from otlp_client.model.metrics import gauge
from otlp_client.outcomes import Permanent, Retryable, Success
from otlp_client.signals import SignalKind
from tests.support.fakes import FakeClock, FakeCredentials, FakeTransport

CONFIG = OTLPConfig(endpoint="http://localhost:4318", headers={"x-tenant": "acme"})
METRIC = [gauge("t", 1.0, time_unix_nano=1)]


def make_client(
    transport: FakeTransport, credentials: CredentialProvider | None = None
) -> OTLPClient:
    # FakeClock keeps the retry backoff instant while still advancing the
    # budget, so a test that exercises retries costs no wall-clock time.
    clock = FakeClock()
    return OTLPClient(
        CONFIG,
        transport=transport,
        encoder=JSONEncoder(),
        credentials=credentials,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )


async def test_provider_headers_merge_over_static_ones() -> None:
    creds = FakeCredentials([{"authorization": "Bearer one"}])
    transport = FakeTransport()
    await make_client(transport, creds).export_metrics(METRIC)
    assert transport.sent[0][2] == {"x-tenant": "acme", "authorization": "Bearer one"}


async def test_provider_wins_a_key_collision() -> None:
    config = OTLPConfig(endpoint="http://localhost:4318", headers={"authorization": "stale"})
    creds = FakeCredentials([{"authorization": "Bearer fresh"}])
    transport = FakeTransport()
    client = OTLPClient(config, transport=transport, encoder=JSONEncoder(), credentials=creds)
    await client.export_metrics(METRIC)
    assert transport.sent[0][2]["authorization"] == "Bearer fresh"


async def test_provider_sees_the_signal_kind() -> None:
    creds = FakeCredentials([{"a": "1"}])
    await make_client(FakeTransport(), creds).export_metrics(METRIC)
    assert creds.calls == [SignalKind.METRICS]


async def test_a_rejection_invalidates_and_resends_once() -> None:
    creds = FakeCredentials([{"authorization": "stale"}, {"authorization": "fresh"}])
    transport = FakeTransport([Permanent(status=401, message="expired"), Success()])
    result = await make_client(transport, creds).export_metrics(METRIC)
    assert isinstance(result, Success)
    assert creds.invalidated == [SignalKind.METRICS]
    assert [sent[2]["authorization"] for sent in transport.sent] == ["stale", "fresh"]


async def test_a_second_rejection_is_permanent_with_only_one_invalidate() -> None:
    creds = FakeCredentials([{"authorization": "stale"}, {"authorization": "also stale"}])
    transport = FakeTransport([Permanent(status=401, message="expired")])
    with pytest.raises(OTLPPermanentError):
        await make_client(transport, creds).export_metrics(METRIC)
    assert creds.invalidated == [SignalKind.METRICS]
    assert len(transport.sent) == 2


async def test_the_reauth_budget_is_one_per_export_not_per_attempt() -> None:
    # 503 -> retry -> 401 -> one re-auth -> 401 again. Still one invalidate for
    # the whole export, proving the budget is scoped to _export.
    creds = FakeCredentials([{"authorization": "t"}])
    transport = FakeTransport(
        [
            Retryable(status=503, message="later"),
            Permanent(status=401, message="expired"),
            Permanent(status=401, message="expired"),
        ]
    )
    with pytest.raises(OTLPPermanentError):
        await make_client(transport, creds).export_metrics(METRIC)
    assert creds.invalidated == [SignalKind.METRICS]


async def test_a_rejection_without_a_provider_stays_permanent() -> None:
    transport = FakeTransport([Permanent(status=401, message="expired")])
    with pytest.raises(OTLPPermanentError):
        await make_client(transport).export_metrics(METRIC)
    assert len(transport.sent) == 1


async def test_a_permanent_provider_error_fails_fast() -> None:
    creds = FakeCredentials([], error=OTLPPermanentError("invalid_client"))
    transport = FakeTransport()
    with pytest.raises(OTLPPermanentError, match="invalid_client"):
        await make_client(transport, creds).export_metrics(METRIC)
    assert len(creds.calls) == 1  # a rejected secret is not retried
    assert transport.sent == []  # nothing reached the wire


async def test_a_transport_provider_error_rides_the_retry_budget() -> None:
    creds = FakeCredentials([], error=OTLPTransportError("token endpoint down"))
    transport = FakeTransport()
    with pytest.raises(OTLPTransportError):
        await make_client(transport, creds).export_metrics(METRIC)
    assert len(creds.calls) > 1  # retried rather than surfaced on first failure
    assert transport.sent == []


async def test_an_unexpected_provider_error_propagates_uncaught() -> None:
    creds = FakeCredentials([], error=KeyError("bug in the provider"))
    with pytest.raises(KeyError):
        await make_client(FakeTransport(), creds).export_metrics(METRIC)
