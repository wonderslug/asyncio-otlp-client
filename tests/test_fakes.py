from otlp_client.outcomes import Permanent, Success
from otlp_client.signals import SignalKind
from tests.support.fakes import FakeClock, FakeTransport


async def test_fake_transport_records_and_replays_outcomes() -> None:
    transport = FakeTransport(outcomes=[Success(), Permanent(status=400, message="nope")])
    assert isinstance(await transport.send(SignalKind.METRICS, b"one"), Success)
    second = await transport.send(SignalKind.LOGS, b"two")
    assert isinstance(second, Permanent)
    assert transport.sent == [(SignalKind.METRICS, b"one"), (SignalKind.LOGS, b"two")]


async def test_fake_transport_repeats_its_last_outcome() -> None:
    transport = FakeTransport(outcomes=[Success()])
    await transport.send(SignalKind.METRICS, b"a")
    assert isinstance(await transport.send(SignalKind.METRICS, b"b"), Success)


async def test_fake_transport_close() -> None:
    transport = FakeTransport()
    await transport.aclose()
    assert transport.closed is True


async def test_fake_clock_advances_only_when_slept() -> None:
    clock = FakeClock()
    assert clock.monotonic() == 0.0
    await clock.sleep(2.5)
    await clock.sleep(1.0)
    assert clock.monotonic() == 3.5
    assert clock.slept == [2.5, 1.0]
