"""Test doubles shared across the suite."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence

from otlp_client.outcomes import ExportOutcome, Success
from otlp_client.signals import SignalKind


class FakeTransport:
    """An in-memory Transport. Records what was sent, replays scripted outcomes.

    Once the script is exhausted the last outcome repeats, so a test that only
    cares about a steady state does not have to enumerate every attempt.
    """

    def __init__(self, outcomes: Sequence[ExportOutcome] | None = None) -> None:
        self._outcomes = list(outcomes) if outcomes else [Success()]
        self._index = 0
        self.sent: list[tuple[SignalKind, bytes, Mapping[str, str]]] = []
        self.closed = False

    async def send(
        self, kind: SignalKind, payload: bytes, headers: Mapping[str, str]
    ) -> ExportOutcome:
        self.sent.append((kind, payload, dict(headers)))
        outcome = self._outcomes[min(self._index, len(self._outcomes) - 1)]
        self._index += 1
        return outcome

    async def aclose(self) -> None:
        self.closed = True


class HangingTransport:
    """A Transport whose `send` blocks on a gate the test controls.

    Lets a test suspend an export mid-flight and cancel the task awaiting it
    -- e.g. to exercise `BatchProcessor.flush()` racing shutdown cancellation
    -- with no real I/O and no wall-clock wait.
    """

    def __init__(self) -> None:
        self.gate = asyncio.Event()
        self.sent: list[tuple[SignalKind, bytes, Mapping[str, str]]] = []

    async def send(
        self, kind: SignalKind, payload: bytes, headers: Mapping[str, str]
    ) -> ExportOutcome:
        self.sent.append((kind, payload, dict(headers)))
        await self.gate.wait()
        return Success()

    async def aclose(self) -> None:
        pass


class FakeClock:
    """A monotonic clock that only moves when something sleeps on it.

    Lets retry and flush schedules be asserted exactly, with no wall-clock wait.
    """

    def __init__(self, start: float = 0.0) -> None:
        self._now = start
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self._now

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self._now += seconds
