from otlp_client.model.metrics import (
    AggregationTemporality,
    Gauge,
    Metric,
    NumberDataPoint,
    Sum,
    gauge,
    sum_,
)


def test_gauge_helper_builds_single_point_metric() -> None:
    m = gauge("home.temperature", 21.5, unit="Cel",
              attributes={"entity_id": "sensor.living_room"}, time_unix_nano=1700000000000000000)
    assert isinstance(m, Metric)
    assert m.name == "home.temperature"
    assert m.unit == "Cel"
    assert isinstance(m.data, Gauge)
    (point,) = m.data.data_points
    assert point.value == 21.5
    assert point.attributes == {"entity_id": "sensor.living_room"}
    assert point.time_unix_nano == 1700000000000000000


def test_sum_helper_defaults_to_monotonic_cumulative() -> None:
    m = sum_("home.energy", 42, time_unix_nano=1)
    assert isinstance(m.data, Sum)
    assert m.data.is_monotonic is True
    assert m.data.aggregation_temporality is AggregationTemporality.CUMULATIVE


def test_number_data_point_preserves_int_vs_float() -> None:
    assert NumberDataPoint(time_unix_nano=1, value=3).value == 3
    assert isinstance(NumberDataPoint(time_unix_nano=1, value=3.0).value, float)
