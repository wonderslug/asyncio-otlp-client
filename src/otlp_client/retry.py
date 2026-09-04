"""Retry policy and the retry loop.

Retryable statuses are exactly 429, 502, 503 and 504 plus connection errors and
timeouts; transports classify those. Everything else is permanent. Partial
success is never retried, per the OTLP spec.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from email.utils import parsedate_to_datetime

from otlp_client.config import OTLPConfig
from otlp_client.outcomes import ExportOutcome, Retryable

RETRYABLE_STATUSES = frozenset({429, 502, 503, 504})


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    initial_backoff: float = 1.0
    max_backoff: float = 30.0
    multiplier: float = 1.5
    max_elapsed: float = 90.0

    @classmethod
    def from_config(cls, config: OTLPConfig) -> RetryPolicy:
        return cls(
            initial_backoff=config.initial_backoff,
            max_backoff=config.max_backoff,
            multiplier=config.backoff_multiplier,
            max_elapsed=config.max_elapsed,
        )


def parse_retry_after(value: str, *, now_wall: float) -> float | None:
    """Parse a Retry-After header in either delay-seconds or HTTP-date form.

    Returns seconds to wait, never negative, or None if unparseable.
    """
    text = value.strip()
    if not text:
        return None
    try:
        return max(0.0, float(text))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    return max(0.0, when.timestamp() - now_wall)


async def with_retry(
    op: Callable[[], Awaitable[ExportOutcome]],
    policy: RetryPolicy,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    jitter: Callable[[], float] = random.random,
) -> ExportOutcome:
    """Run `op` until it succeeds, fails permanently, or the budget runs out.

    Uses full jitter: each delay is `jitter() * capped_backoff`. Cancellation
    during a backoff sleep propagates rather than being swallowed. If the next
    delay (computed or a server-supplied `retry_after`) would outlast the
    remaining budget, the attempt is skipped and the `Retryable` is returned
    immediately rather than sleeping a truncated interval and retrying anyway
    — that would both retry earlier than a rate-limiter asked for and spend
    the budget on a request whose outcome is discarded regardless.
    """
    started = monotonic()
    attempt = 0
    while True:
        outcome = await op()
        if not isinstance(outcome, Retryable):
            return outcome

        elapsed = monotonic() - started
        if elapsed >= policy.max_elapsed:
            return outcome

        capped = min(policy.max_backoff, policy.initial_backoff * policy.multiplier**attempt)
        delay = outcome.retry_after if outcome.retry_after is not None else jitter() * capped
        remaining = policy.max_elapsed - elapsed
        if delay > remaining:
            return outcome
        await sleep(delay)
        attempt += 1
