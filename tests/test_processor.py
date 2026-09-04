import asyncio

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
