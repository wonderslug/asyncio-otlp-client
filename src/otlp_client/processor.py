"""Bounded queueing and background flushing on top of OTLPClient."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self, cast

from otlp_client.client import OTLPClient
from otlp_client.errors import OTLPError
from otlp_client.model.common import InstrumentationScope, Resource
from otlp_client.model.metrics import Metric
from otlp_client.outcomes import PartialSuccess
from otlp_client.signals import SignalKind

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ProcessorStats:
    """A snapshot of processor health, safe to surface in a UI."""

    submitted: int = 0
    exported: int = 0
    dropped: int = 0
    consecutive_failures: int = 0
    last_error: str | None = None


class BatchProcessor:
    """Queues records and flushes them on size, interval, or demand.

    `submit_*` never blocks and never raises: a caller such as a state-change
    listener has nowhere to handle an exception. Overflow drops the oldest
    record and increments `stats.dropped`.

    Backoff on a failing collector comes from the client's retry engine, which
    bounds each flush by `max_elapsed`, so the flush loop cannot hot-loop.
    """

    def __init__(
        self,
        client: OTLPClient,
        *,
        max_batch: int = 512,
        flush_interval: float = 5.0,
        max_queue: int = 2048,
        resource: Resource | None = None,
        scope: InstrumentationScope | None = None,
    ) -> None:
        self._client = client
        self._max_batch = max_batch
        self._flush_interval = flush_interval
        self._resource = resource
        self._scope = scope
        self._max_queue = max_queue
        # One bounded queue per signal. Tasks 12 and 13 add LOGS and TRACES.
        self._queues: dict[SignalKind, deque[Any]] = {
            SignalKind.METRICS: deque(maxlen=max_queue),
        }
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._closed = False
        self._lock = asyncio.Lock()
        self.flushed = asyncio.Event()
        self._submitted = 0
        self._exported = 0
        self._dropped = 0
        self._consecutive_failures = 0
        self._last_error: str | None = None

    @property
    def stats(self) -> ProcessorStats:
        return ProcessorStats(
            submitted=self._submitted,
            exported=self._exported,
            dropped=self._dropped,
            consecutive_failures=self._consecutive_failures,
            last_error=self._last_error,
        )

    def _submit(self, kind: SignalKind, records: Sequence[Any]) -> bool:
        """Queue records for one signal. Never blocks, never raises."""
        if self._closed:
            return False
        queue = self._queues[kind]
        accepted = True
        for record in records:
            if queue.maxlen is not None and len(queue) == queue.maxlen:
                self._dropped += 1
                accepted = False
            queue.append(record)
            self._submitted += 1
        if len(queue) >= self._max_batch:
            self._wake.set()
        return accepted

    def submit_metrics(self, metrics: Sequence[Metric]) -> bool:
        """Queue metrics. Returns False if anything was dropped or we are closed."""
        return self._submit(SignalKind.METRICS, metrics)

    async def _export_batch(
        self, kind: SignalKind, batch: Sequence[Any]
    ) -> PartialSuccess | None:
        """Dispatch one drained batch to the right client method.

        Tasks 12 and 13 add the LOGS and TRACES branches.
        """
        if kind is SignalKind.METRICS:
            result = await self._client.export_metrics(
                cast("Sequence[Metric]", batch), resource=self._resource, scope=self._scope
            )
        else:  # pragma: no cover - unreachable until later signals are added
            raise NotImplementedError(f"no processor branch for {kind}")
        return result if isinstance(result, PartialSuccess) else None

    async def flush(self) -> None:
        """Export everything queued, for every signal.

        Never raises; failures land in stats so a caller can surface health.
        """
        async with self._lock:
            for kind, queue in self._queues.items():
                batch = list(queue)
                queue.clear()
                if not batch:
                    continue
                try:
                    partial = await self._export_batch(kind, batch)
                except OTLPError as exc:
                    self._consecutive_failures += 1
                    self._last_error = str(exc)
                    _LOGGER.debug("OTLP %s export failed: %s", kind, exc)
                    continue
                self._consecutive_failures = 0
                self._last_error = None
                exported = len(batch)
                if partial is not None:
                    exported -= partial.rejected
                    self._last_error = f"partial success: {partial.message}"
                self._exported += max(0, exported)

    async def _run(self) -> None:
        while True:
            try:
                async with asyncio.timeout(self._flush_interval):
                    await self._wake.wait()
            except TimeoutError:
                pass
            self._wake.clear()
            await self.flush()
            self.flushed.set()

    async def __aenter__(self) -> Self:
        self._task = asyncio.create_task(self._run())
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Stop accepting work, make one final flush, then cancel the task."""
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self.flush()
