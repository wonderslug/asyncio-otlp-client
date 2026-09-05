"""The OTLP logs data model."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import IntEnum
from types import MappingProxyType

from otlp_client.model.common import AnyValue, InstrumentationScope, Resource

_EMPTY: Mapping[str, AnyValue] = MappingProxyType({})


class SeverityNumber(IntEnum):
    UNSPECIFIED = 0
    TRACE = 1
    DEBUG = 5
    INFO = 9
    WARN = 13
    ERROR = 17
    FATAL = 21


# The low eight bits of a log record's flags carry the W3C trace flags.
LOG_RECORD_FLAGS_TRACE_FLAGS_MASK = 0x000000FF


@dataclass(frozen=True, slots=True, kw_only=True)
class LogRecord:
    time_unix_nano: int
    body: AnyValue
    observed_time_unix_nano: int
    severity_number: SeverityNumber = SeverityNumber.UNSPECIFIED
    severity_text: str = ""
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY, hash=False)
    trace_id: bytes | None = None
    span_id: bytes | None = None
    event_name: str = ""
    flags: int = 0


@dataclass(frozen=True, slots=True, kw_only=True)
class ScopeLogs:
    scope: InstrumentationScope
    log_records: Sequence[LogRecord]


@dataclass(frozen=True, slots=True, kw_only=True)
class ResourceLogs:
    resource: Resource
    scope_logs: Sequence[ScopeLogs]


def log_record(
    body: AnyValue,
    *,
    time_unix_nano: int,
    observed_time_unix_nano: int | None = None,
    severity: SeverityNumber = SeverityNumber.UNSPECIFIED,
    severity_text: str | None = None,
    attributes: Mapping[str, AnyValue] | None = None,
    trace_id: bytes | None = None,
    span_id: bytes | None = None,
    event_name: str = "",
) -> LogRecord:
    """Build a log record.

    `observed_time_unix_nano` defaults to `time_unix_nano`, and `severity_text`
    defaults to the severity's name, which is what collectors display.
    """
    return LogRecord(
        time_unix_nano=time_unix_nano,
        observed_time_unix_nano=(
            time_unix_nano if observed_time_unix_nano is None else observed_time_unix_nano
        ),
        body=body,
        severity_number=severity,
        severity_text=(
            severity_text
            if severity_text is not None
            else ("" if severity is SeverityNumber.UNSPECIFIED else severity.name)
        ),
        attributes=attributes or _EMPTY,
        trace_id=trace_id,
        span_id=span_id,
        event_name=event_name,
    )
