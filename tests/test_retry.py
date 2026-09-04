from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import pytest

from otlp_client.config import OTLPConfig
from otlp_client.outcomes import ExportOutcome, PartialSuccess, Permanent, Retryable, Success
from otlp_client.retry import RetryPolicy, parse_retry_after, with_retry
from tests.support.fakes import FakeClock

POLICY = RetryPolicy(initial_backoff=1.0, max_backoff=30.0, multiplier=2.0, max_elapsed=90.0)


def scripted(
    *outcomes: ExportOutcome,
) -> tuple[Callable[[], Awaitable[ExportOutcome]], list[int]]:
    """An op that returns each outcome in turn, repeating the last."""
    calls: list[int] = []

    async def op() -> ExportOutcome:
        result = outcomes[min(len(calls), len(outcomes) - 1)]
        calls.append(1)
        return result

    return op, calls


async def test_success_on_first_attempt_does_not_sleep() -> None:
    clock = FakeClock()
    op, calls = scripted(Success())
    result = await with_retry(op, POLICY, sleep=clock.sleep, monotonic=clock.monotonic,
                              jitter=lambda: 1.0)
    assert isinstance(result, Success)
    assert len(calls) == 1
    assert clock.slept == []


async def test_retries_until_success_with_exponential_backoff() -> None:
    clock = FakeClock()
    op, calls = scripted(Retryable(status=503), Retryable(status=503), Success())
    result = await with_retry(op, POLICY, sleep=clock.sleep, monotonic=clock.monotonic,
                              jitter=lambda: 1.0)
    assert isinstance(result, Success)
    assert len(calls) == 3
    assert clock.slept == [1.0, 2.0]


async def test_full_jitter_scales_each_delay() -> None:
    clock = FakeClock()
    op, _ = scripted(Retryable(), Retryable(), Success())
    await with_retry(op, POLICY, sleep=clock.sleep, monotonic=clock.monotonic,
                     jitter=lambda: 0.5)
    assert clock.slept == [0.5, 1.0]


async def test_backoff_is_capped_at_max_backoff() -> None:
    clock = FakeClock()
    policy = RetryPolicy(initial_backoff=10.0, max_backoff=15.0, multiplier=10.0,
                         max_elapsed=1000.0)
    op, _ = scripted(Retryable(), Retryable(), Retryable(), Success())
    await with_retry(op, policy, sleep=clock.sleep, monotonic=clock.monotonic,
                     jitter=lambda: 1.0)
    assert clock.slept == [10.0, 15.0, 15.0]


async def test_retry_after_overrides_computed_backoff() -> None:
    clock = FakeClock()
    op, _ = scripted(Retryable(status=429, retry_after=7.0), Success())
    await with_retry(op, POLICY, sleep=clock.sleep, monotonic=clock.monotonic,
                     jitter=lambda: 1.0)
    assert clock.slept == [7.0]


async def test_retry_after_exceeding_budget_returns_immediately_without_sleeping() -> None:
    # A retry_after longer than the remaining budget must not be truncated and
    # retried anyway: that would send earlier than the server permitted, on a
    # request whose result is discarded the moment it returns (the budget is
    # already spent). The Retryable is returned as-is instead.
    clock = FakeClock()
    op, calls = scripted(Retryable(status=429, retry_after=120.0), Success())
    result = await with_retry(op, POLICY, sleep=clock.sleep, monotonic=clock.monotonic,
                              jitter=lambda: 1.0)
    assert isinstance(result, Retryable)
    assert result.retry_after == 120.0
    assert len(calls) == 1
    assert clock.slept == []


async def test_permanent_is_returned_immediately() -> None:
    clock = FakeClock()
    op, calls = scripted(Permanent(status=400, message="bad"))
    result = await with_retry(op, POLICY, sleep=clock.sleep, monotonic=clock.monotonic,
                              jitter=lambda: 1.0)
    assert isinstance(result, Permanent)
    assert len(calls) == 1


async def test_partial_success_is_never_retried() -> None:
    clock = FakeClock()
    op, calls = scripted(PartialSuccess(rejected=3), Success())
    result = await with_retry(op, POLICY, sleep=clock.sleep, monotonic=clock.monotonic,
                              jitter=lambda: 1.0)
    assert isinstance(result, PartialSuccess)
    assert len(calls) == 1
    assert clock.slept == []


async def test_budget_exhaustion_returns_the_last_retryable() -> None:
    clock = FakeClock()
    policy = RetryPolicy(initial_backoff=1.0, max_backoff=1.0, multiplier=1.0, max_elapsed=3.0)
    op, calls = scripted(Retryable(status=503, message="down"))
    result = await with_retry(op, policy, sleep=clock.sleep, monotonic=clock.monotonic,
                              jitter=lambda: 1.0)
    assert isinstance(result, Retryable)
    assert result.message == "down"
    assert clock.monotonic() <= 3.0 + 1.0
    assert len(calls) >= 2


def test_parse_retry_after_delay_seconds() -> None:
    assert parse_retry_after("120", now_wall=0.0) == 120.0


def test_parse_retry_after_http_date() -> None:
    when = datetime(2026, 9, 4, 12, 0, 30, tzinfo=UTC)
    now = datetime(2026, 9, 4, 12, 0, 0, tzinfo=UTC).timestamp()
    header = "Fri, 04 Sep 2026 12:00:30 GMT"
    assert parse_retry_after(header, now_wall=now) == pytest.approx(30.0, abs=1.0)
    assert when.timestamp() > now


def test_parse_retry_after_past_date_clamps_to_zero() -> None:
    now = datetime(2026, 9, 4, 12, 0, 0, tzinfo=UTC).timestamp()
    assert parse_retry_after("Fri, 04 Sep 2026 11:59:00 GMT", now_wall=now) == 0.0


def test_parse_retry_after_garbage_returns_none() -> None:
    assert parse_retry_after("soon please", now_wall=0.0) is None


def test_policy_from_config_uses_config_values() -> None:
    cfg = OTLPConfig(endpoint="http://localhost:4318", initial_backoff=2.0, max_backoff=8.0,
                     backoff_multiplier=3.0, max_elapsed=40.0)
    policy = RetryPolicy.from_config(cfg)
    assert (policy.initial_backoff, policy.max_backoff) == (2.0, 8.0)
    assert (policy.multiplier, policy.max_elapsed) == (3.0, 40.0)
