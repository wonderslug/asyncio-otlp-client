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


# Set when a data point represents "no measurement was recorded", which is
# distinct from a measurement whose value happens to be zero.
DATA_POINT_FLAGS_NO_RECORDED_VALUE = 0x00000001


@dataclass(frozen=True, slots=True, kw_only=True)
class NumberDataPoint:
    time_unix_nano: int
    value: int | float
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY, hash=False)
    start_time_unix_nano: int | None = None
    flags: int = 0


@dataclass(frozen=True, slots=True, kw_only=True)
class HistogramDataPoint:
    time_unix_nano: int
    count: int
    bucket_counts: Sequence[int]
    explicit_bounds: Sequence[float]
    sum: float | None = None
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY, hash=False)
    start_time_unix_nano: int | None = None
    flags: int = 0
    min: float | None = None
    max: float | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class Gauge:
    data_points: Sequence[NumberDataPoint]


@dataclass(frozen=True, slots=True, kw_only=True)
class Sum:
    data_points: Sequence[NumberDataPoint]
    aggregation_temporality: AggregationTemporality = AggregationTemporality.CUMULATIVE
    is_monotonic: bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class Histogram:
    data_points: Sequence[HistogramDataPoint]
    aggregation_temporality: AggregationTemporality = AggregationTemporality.CUMULATIVE


@dataclass(frozen=True, slots=True, kw_only=True)
class Buckets:
    """One side of an exponential histogram."""

    offset: int = 0
    bucket_counts: Sequence[int] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class ExponentialHistogramDataPoint:
    time_unix_nano: int
    count: int
    scale: int = 0
    zero_count: int = 0
    positive: Buckets = Buckets()
    negative: Buckets = Buckets()
    sum: float | None = None
    min: float | None = None
    max: float | None = None
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY, hash=False)
    start_time_unix_nano: int | None = None
    flags: int = 0


@dataclass(frozen=True, slots=True, kw_only=True)
class ExponentialHistogram:
    data_points: Sequence[ExponentialHistogramDataPoint]
    aggregation_temporality: AggregationTemporality = AggregationTemporality.CUMULATIVE


@dataclass(frozen=True, slots=True, kw_only=True)
class ValueAtQuantile:
    quantile: float
    value: float


@dataclass(frozen=True, slots=True, kw_only=True)
class SummaryDataPoint:
    time_unix_nano: int
    count: int
    sum: float = 0.0
    quantile_values: Sequence[ValueAtQuantile] = ()
    attributes: Mapping[str, AnyValue] = field(default=_EMPTY, hash=False)
    start_time_unix_nano: int | None = None
    flags: int = 0


@dataclass(frozen=True, slots=True, kw_only=True)
class Summary:
    """Legacy metric type. The proto carries no aggregation temporality."""

    data_points: Sequence[SummaryDataPoint]


type MetricData = Gauge | Sum | Histogram | ExponentialHistogram | Summary


@dataclass(frozen=True, slots=True, kw_only=True)
class Metric:
    name: str
    data: MetricData
    description: str = ""
    unit: str = ""


@dataclass(frozen=True, slots=True, kw_only=True)
class ScopeMetrics:
    scope: InstrumentationScope
    metrics: Sequence[Metric]


@dataclass(frozen=True, slots=True, kw_only=True)
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


def data_point_flags(*, no_recorded_value: bool = False) -> int:
    """Build a data point's flags."""
    return DATA_POINT_FLAGS_NO_RECORDED_VALUE if no_recorded_value else 0
