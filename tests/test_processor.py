import asyncio
import logging

import pytest

from otlp_client.client import OTLPClient
from otlp_client.config import OTLPConfig
from otlp_client.encoding.json import JSONEncoder
from otlp_client.model.common import Resource
from otlp_client.model.metrics import Metric, gauge
from otlp_client.outcomes import Permanent, Success
from otlp_client.processor import BatchProcessor
from tests.support.fakes import FakeTransport

CONFIG = OTLPConfig(endpoint="http://localhost:4318", resource=Resource(attributes={"a": "b"}))


def make_client(transport: FakeTransport) -> OTLPClient:
    return OTLPClient(CONFIG, transport=transport, encoder=JSONEncoder())


def one(n: int = 1) -> list[Metric]:
    return [gauge("t", float(n), time_unix_nano=n)]


async def test_submit_is_non_blocking_and_queues() -> None:
    proc = BatchProcessor(make_client(FakeTransport()), flush_interval=3600.0)
    assert proc.submit_metrics(one()) is True
    assert proc.stats.submitted == 1
    assert proc.stats.exported == 0


async def test_explicit_flush_exports_everything_queued() -> None:
    transport = FakeTransport()
    proc = BatchProcessor(make_client(transport), flush_interval=3600.0)
    proc.submit_metrics(one(1))
    proc.submit_metrics(one(2))
    await proc.flush()
    assert len(transport.sent) == 1
    assert proc.stats.exported == 2
    assert proc.stats.submitted == 2


async def test_flush_with_empty_queue_is_a_no_op() -> None:
    transport = FakeTransport()
    proc = BatchProcessor(make_client(transport), flush_interval=3600.0)
    await proc.flush()
    assert transport.sent == []


async def test_queue_overflow_drops_oldest_and_counts() -> None:
    transport = FakeTransport()
    proc = BatchProcessor(make_client(transport), max_queue=2, flush_interval=3600.0)
    assert proc.submit_metrics(one(1)) is True
    assert proc.submit_metrics(one(2)) is True
    assert proc.submit_metrics(one(3)) is False
    assert proc.stats.dropped == 1
    await proc.flush()
    # The oldest record was evicted; the two newest survived.
    assert proc.stats.exported == 2


async def test_export_failure_records_error_and_does_not_raise() -> None:
    transport = FakeTransport(outcomes=[Permanent(status=400, message="bad")])
    proc = BatchProcessor(make_client(transport), flush_interval=3600.0)
    proc.submit_metrics(one())
    await proc.flush()
    assert proc.stats.consecutive_failures == 1
    assert proc.stats.last_error is not None
    assert "400" in proc.stats.last_error
    # The drained batch never reached the collector, so it counts as dropped
    # even though it was never evicted by overflow.
    assert proc.stats.dropped == 1


async def test_consecutive_failures_reset_after_a_success() -> None:
    transport = FakeTransport(outcomes=[Permanent(status=400), Success()])
    proc = BatchProcessor(make_client(transport), flush_interval=3600.0)
    proc.submit_metrics(one(1))
    await proc.flush()
    assert proc.stats.consecutive_failures == 1
    proc.submit_metrics(one(2))
    await proc.flush()
    assert proc.stats.consecutive_failures == 0


async def test_reaching_max_batch_triggers_a_background_flush() -> None:
    transport = FakeTransport()
    async with BatchProcessor(
        make_client(transport), max_batch=2, flush_interval=3600.0
    ) as proc:
        proc.submit_metrics(one(1))
        proc.submit_metrics(one(2))
        async with asyncio.timeout(5):
            await proc.flushed.wait()
    assert len(transport.sent) == 1
    assert proc.stats.exported == 2


async def test_context_manager_drains_on_exit() -> None:
    transport = FakeTransport()
    async with BatchProcessor(make_client(transport), flush_interval=3600.0) as proc:
        proc.submit_metrics(one())
    assert len(transport.sent) == 1
    assert proc.stats.exported == 1


async def test_submitting_after_close_is_rejected() -> None:
    transport = FakeTransport()
    async with BatchProcessor(make_client(transport), flush_interval=3600.0) as proc:
        pass
    assert proc.submit_metrics(one()) is False


async def test_flush_task_is_cancelled_on_exit() -> None:
    transport = FakeTransport()
    async with BatchProcessor(make_client(transport), flush_interval=3600.0) as proc:
        task = proc._task
    assert task is not None
    assert task.done()


async def test_non_otlp_error_during_flush_is_treated_like_a_failed_export() -> None:
    """A bool metric value is a realistic trigger: `gauge()`'s `value: float`
    parameter accepts a `bool` under mypy's numeric tower, but the JSON
    encoder's `_encode_number_point` raises a bare `TypeError` for it. That
    error must be handled exactly like an `OTLPError` — counted as a dropped,
    failed export — not left to escape `flush()`.
    """
    transport = FakeTransport()
    proc = BatchProcessor(make_client(transport), flush_interval=3600.0)
    proc.submit_metrics([gauge("t", True, time_unix_nano=1)])
    await proc.flush()
    assert transport.sent == []
    assert proc.stats.dropped == 1
    assert proc.stats.consecutive_failures == 1
    assert proc.stats.last_error is not None
    assert "bool" in proc.stats.last_error


async def test_background_loop_survives_a_non_otlp_error_and_keeps_flushing() -> None:
    """The bad batch must not kill the background task: a later, good batch
    still has to reach the collector.
    """
    transport = FakeTransport()
    async with BatchProcessor(
        make_client(transport), max_batch=1, flush_interval=3600.0
    ) as proc:
        proc.submit_metrics([gauge("t", True, time_unix_nano=1)])
        async with asyncio.timeout(5):
            await proc.flushed.wait()
        assert transport.sent == []
        assert proc.stats.dropped == 1
        assert proc._task is not None
        assert not proc._task.done()

        proc.flushed.clear()
        proc.submit_metrics(one(1))
        async with asyncio.timeout(5):
            await proc.flushed.wait()
    assert len(transport.sent) == 1
    assert proc.stats.exported == 1


async def test_aexit_tolerates_a_background_task_that_died_unexpectedly() -> None:
    """`_run` should never end this way in practice — `flush()` and `_run`
    itself both guard their own errors — but if it somehow does, `__aexit__`
    must still keep its own never-raises contract: shutdown has nowhere to
    hand an exception either.
    """
    transport = FakeTransport()

    class BrokenProcessor(BatchProcessor):
        async def _run(self) -> None:
            raise RuntimeError("boom")

    proc = BrokenProcessor(make_client(transport), flush_interval=3600.0)
    await proc.__aenter__()
    task = proc._task
    assert task is not None
    with pytest.raises(RuntimeError, match="boom"):
        await task
    await proc.__aexit__(None, None, None)  # must not raise


async def test_flushed_event_reflects_a_real_subsequent_flush() -> None:
    """A second `wait()` must correspond to a second, real flush, not the
    first `set()` left dangling forever. Clearing before triggering the next
    flush (the correct way to consume a level-triggered `Event`) must
    actually block until that next flush happens.
    """
    transport = FakeTransport()
    async with BatchProcessor(
        make_client(transport), max_batch=1, flush_interval=3600.0
    ) as proc:
        proc.submit_metrics(one(1))
        async with asyncio.timeout(5):
            await proc.flushed.wait()
        assert len(transport.sent) == 1

        proc.flushed.clear()
        proc.submit_metrics(one(2))
        async with asyncio.timeout(5):
            await proc.flushed.wait()
        assert len(transport.sent) == 2
        assert proc.stats.exported == 2


async def test_first_failure_logs_at_warning_later_failures_at_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    transport = FakeTransport(outcomes=[Permanent(status=400), Permanent(status=400)])
    proc = BatchProcessor(make_client(transport), flush_interval=3600.0)
    with caplog.at_level(logging.DEBUG, logger="otlp_client.processor"):
        proc.submit_metrics(one(1))
        await proc.flush()
        proc.submit_metrics(one(2))
        await proc.flush()
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    debugs = [r for r in caplog.records if r.levelno == logging.DEBUG]
    assert len(warnings) == 1
    assert len(debugs) == 1
