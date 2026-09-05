"""The OTLP traces data model."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import IntEnum
from types import MappingProxyType

from otlp_client.model.common import AnyValue, InstrumentationScope, Resource

_EMPTY: Mapping[str, AnyValue] = MappingProxyType({})


class SpanKind(IntEnum):
    UNSPECIFIED = 0
    INTERNAL = 1
    SERVER = 2
    CLIENT = 3
    PRODUCER = 4
    CONSUMER = 5


class StatusCode(IntEnum):
    UNSET = 0
    OK = 1
    ERROR = 2


@dataclass(frozen=True, slots=True)
class Status:
    code: StatusCode = StatusCode.UNSET
    message: str = ""


@dataclass(frozen=True, slots=True)
class SpanEvent:
    time_unix_nano: int
    name: str
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY, hash=False)


@dataclass(frozen=True, slots=True)
class SpanLink:
    trace_id: bytes
    span_id: bytes
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY, hash=False)


@dataclass(frozen=True, slots=True)
class Span:
    trace_id: bytes
    span_id: bytes
    name: str
    start_time_unix_nano: int
    end_time_unix_nano: int
    kind: SpanKind = SpanKind.UNSPECIFIED
    parent_span_id: bytes | None = None
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY, hash=False)
    events: Sequence[SpanEvent] = ()
    links: Sequence[SpanLink] = ()
    status: Status | None = None


@dataclass(frozen=True, slots=True)
class ScopeSpans:
    scope: InstrumentationScope
    spans: Sequence[Span]


@dataclass(frozen=True, slots=True)
class ResourceSpans:
    resource: Resource
    scope_spans: Sequence[ScopeSpans]


def span(
    name: str,
    *,
    trace_id: bytes,
    span_id: bytes,
    start_time_unix_nano: int,
    end_time_unix_nano: int,
    kind: SpanKind = SpanKind.UNSPECIFIED,
    parent_span_id: bytes | None = None,
    attributes: Mapping[str, AnyValue] | None = None,
    events: Sequence[SpanEvent] = (),
    links: Sequence[SpanLink] = (),
    status_code: StatusCode = StatusCode.UNSET,
    status_message: str = "",
) -> Span:
    """Build a finished span.

    `status` stays None when the code is UNSET so the field is omitted on the
    wire, which is what collectors expect for a span that reported no status.
    """
    status = (
        None
        if status_code is StatusCode.UNSET and not status_message
        else Status(code=status_code, message=status_message)
    )
    return Span(
        trace_id=trace_id,
        span_id=span_id,
        name=name,
        start_time_unix_nano=start_time_unix_nano,
        end_time_unix_nano=end_time_unix_nano,
        kind=kind,
        parent_span_id=parent_span_id,
        attributes=attributes or _EMPTY,
        events=events,
        links=links,
        status=status,
    )
