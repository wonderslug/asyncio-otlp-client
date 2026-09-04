"""The result of one export attempt.

Transports classify a response into one of these; `retry.py` decides what to
do about it. Neither knows the other's wire format.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Success:
    """Everything was accepted."""


@dataclass(frozen=True, slots=True)
class PartialSuccess:
    """Some records were rejected. Per the OTLP spec this is never retried."""

    rejected: int
    message: str = ""


@dataclass(frozen=True, slots=True)
class Retryable:
    """A transient failure. Safe to send again after a backoff."""

    status: int | None = None
    message: str = ""
    retry_after: float | None = None


@dataclass(frozen=True, slots=True)
class Permanent:
    """The collector will never accept this payload. Drop it."""

    status: int | None = None
    message: str = ""


type ExportOutcome = Success | PartialSuccess | Retryable | Permanent
