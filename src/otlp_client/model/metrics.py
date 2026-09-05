"""The OTLP metrics data model."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import IntEnum
from types import MappingProxyType

from otlp_client.model.common import AnyValue, InstrumentationScope, Resource

_EMPTY: Mapping[str, AnyValue] = MappingProxyType({})


class AggregationTemporality(IntEnum):
    UNSPECIFIED = 0
    DELTA = 1
    CUMULATIVE = 2


@dataclass(frozen=True, slots=True)
class NumberDataPoint:
    time_unix_nano: int
    value: int | float
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY, hash=False)
    start_time_unix_nano: int | None = None


@dataclass(frozen=True, slots=True)
class HistogramDataPoint:
    time_unix_nano: int
    count: int
    bucket_counts: Sequence[int]
    explicit_bounds: Sequence[float]
    sum: float | None = None
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY, hash=False)
    start_time_unix_nano: int | None = None


@dataclass(frozen=True, slots=True)
class Gauge:
    data_points: Sequence[NumberDataPoint]


@dataclass(frozen=True, slots=True)
class Sum:
    data_points: Sequence[NumberDataPoint]
    aggregation_temporality: AggregationTemporality = AggregationTemporality.CUMULATIVE
    is_monotonic: bool = True


@dataclass(frozen=True, slots=True)
class Histogram:
    data_points: Sequence[HistogramDataPoint]
    aggregation_temporality: AggregationTemporality = AggregationTemporality.CUMULATIVE


type MetricData = Gauge | Sum | Histogram


@dataclass(frozen=True, slots=True)
class Metric:
    name: str
    data: MetricData
    description: str = ""
    unit: str = ""


@dataclass(frozen=True, slots=True)
class ScopeMetrics:
    scope: InstrumentationScope
    metrics: Sequence[Metric]


@dataclass(frozen=True, slots=True)
class ResourceMetrics:
    resource: Resource
    scope_metrics: Sequence[ScopeMetrics]


def gauge(
    name: str,
    value: float,
    *,
    time_unix_nano: int,
    unit: str = "",
    description: str = "",
    attributes: Mapping[str, AnyValue] | None = None,
) -> Metric:
    """Build a single-point gauge metric."""
    point = NumberDataPoint(
        time_unix_nano=time_unix_nano, value=value, attributes=attributes or _EMPTY
    )
    return Metric(name=name, data=Gauge(data_points=(point,)), unit=unit, description=description)


def sum_(
    name: str,
    value: float,
    *,
    time_unix_nano: int,
    start_time_unix_nano: int | None = None,
    unit: str = "",
    description: str = "",
    attributes: Mapping[str, AnyValue] | None = None,
    is_monotonic: bool = True,
    temporality: AggregationTemporality = AggregationTemporality.CUMULATIVE,
) -> Metric:
    """Build a single-point sum metric."""
    point = NumberDataPoint(
        time_unix_nano=time_unix_nano,
        value=value,
        attributes=attributes or _EMPTY,
        start_time_unix_nano=start_time_unix_nano,
    )
    data = Sum(data_points=(point,), aggregation_temporality=temporality, is_monotonic=is_monotonic)
    return Metric(name=name, data=data, unit=unit, description=description)
