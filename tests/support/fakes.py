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


class RaisingTransport:
    """A Transport whose `send` raises instead of returning an outcome.

    Represents a custom transport's own failure, distinct from anything a
    credential provider could raise -- proves _export does not mislabel it.
    """

    def __init__(self, error: BaseException) -> None:
        self._error = error
        self.sent: list[tuple[SignalKind, bytes, Mapping[str, str]]] = []

    async def send(
        self, kind: SignalKind, payload: bytes, headers: Mapping[str, str]
    ) -> ExportOutcome:
        self.sent.append((kind, payload, dict(headers)))
        raise self._error

    async def aclose(self) -> None:
        pass


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


class FakeCredentials:
    """A CredentialProvider double. Replays scripted header maps.

    Once the script is exhausted the last map repeats, matching FakeTransport.
    `error`, when set, is raised from `headers()` instead.
    """

    def __init__(
        self,
        headers_by_call: Sequence[Mapping[str, str]],
        error: BaseException | None = None,
    ) -> None:
        self._headers = [dict(h) for h in headers_by_call] or [{}]
        self._index = 0
        self._error = error
        self.calls: list[SignalKind] = []
        self.invalidated: list[SignalKind] = []

    async def headers(self, kind: SignalKind) -> Mapping[str, str]:
        self.calls.append(kind)
        if self._error is not None:
            raise self._error
        headers = self._headers[min(self._index, len(self._headers) - 1)]
        self._index += 1
        return headers

    async def invalidate(self, kind: SignalKind) -> None:
        self.invalidated.append(kind)


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
