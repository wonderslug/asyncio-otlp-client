"""Types shared by every OTLP signal."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType

type AnyValue = (
    str | bool | int | float | bytes | Sequence["AnyValue"] | Mapping[str, "AnyValue"]
)

_EMPTY: Mapping[str, AnyValue] = MappingProxyType({})


@dataclass(frozen=True, slots=True)
class Resource:
    """The entity producing telemetry."""

    attributes: Mapping[str, AnyValue] = field(default=_EMPTY, hash=False)
    dropped_attributes_count: int = 0


@dataclass(frozen=True, slots=True)
class InstrumentationScope:
    """The library or component that emitted a signal."""

    name: str
    version: str | None = None
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY, hash=False)
