"""Hypothesis strategies over the OTLP model tree."""

from __future__ import annotations

from hypothesis import strategies as st

from otlp_client.model.common import InstrumentationScope, Resource
from otlp_client.model.logs import LogRecord, ResourceLogs, ScopeLogs, SeverityNumber
from otlp_client.model.metrics import (
    AggregationTemporality,
    Buckets,
    Exemplar,
    ExponentialHistogram,
    ExponentialHistogramDataPoint,
    Gauge,
    Histogram,
    HistogramDataPoint,
    Metric,
    NumberDataPoint,
    ResourceMetrics,
    ScopeMetrics,
    Sum,
    Summary,
    SummaryDataPoint,
    ValueAtQuantile,
)
from otlp_client.model.traces import (
    ResourceSpans,
    ScopeSpans,
    Span,
    SpanEvent,
    SpanKind,
    SpanLink,
    Status,
    StatusCode,
)

# u64 range; protobuf rejects anything wider.
u64 = st.integers(min_value=0, max_value=2**63 - 1)
i64 = st.integers(min_value=-(2**63), max_value=2**63 - 1)
text = st.text(max_size=20)
# No NaN or infinity: protobuf JSON renders those as strings, which would make
# the comparison test about float formatting rather than about our encoder.
finite = st.floats(allow_nan=False, allow_infinity=False, width=32)

scalars = st.one_of(text, st.booleans(), i64, finite, st.binary(max_size=8))
# Recursive AnyValue: exercises encode_any_value's two container branches
# (arrayValue/kvlistValue), not just the five scalar leaves. Bounded by
# max_leaves so generated trees stay small and shrinking stays fast.
any_values = st.recursive(
    scalars,
    lambda children: st.one_of(
        st.lists(children, max_size=3),
        st.dictionaries(text, children, max_size=3),
    ),
    max_leaves=5,
)
attributes = st.dictionaries(text, any_values, max_size=4)
# A small non-negative range: enough to exercise the `x or None` omit-on-zero
# branch (via 0) and real wire presence (via a nonzero value) for fields that
# otherwise never carry any information in these strategies.
small_uint = st.integers(min_value=0, max_value=255)
flags = st.integers(min_value=0, max_value=1023)

exemplars = st.builds(
    Exemplar,
    time_unix_nano=u64,
    value=st.one_of(i64, finite),
    filtered_attributes=attributes,
    span_id=st.one_of(st.none(), st.binary(min_size=8, max_size=8)),
    trace_id=st.one_of(st.none(), st.binary(min_size=16, max_size=16)),
)

resources = st.builds(Resource, attributes=attributes, dropped_attributes_count=small_uint)
scopes = st.builds(
    InstrumentationScope, name=text, version=st.one_of(st.none(), text), attributes=attributes
)

number_points = st.builds(
    NumberDataPoint,
    time_unix_nano=u64,
    value=st.one_of(i64, finite),
    attributes=attributes,
    start_time_unix_nano=st.one_of(st.none(), u64),
    flags=flags,
    exemplars=st.lists(exemplars, max_size=2),
)

histogram_points = st.builds(
    HistogramDataPoint,
    time_unix_nano=u64,
    count=u64,
    bucket_counts=st.lists(u64, max_size=3),
    explicit_bounds=st.lists(finite, max_size=2),
    sum=st.one_of(st.none(), finite),
    attributes=attributes,
    flags=flags,
    min=st.one_of(st.none(), finite),
    max=st.one_of(st.none(), finite),
    exemplars=st.lists(exemplars, max_size=2),
)

exponential_points = st.builds(
    ExponentialHistogramDataPoint,
    time_unix_nano=u64,
    count=u64,
    scale=st.integers(min_value=-10, max_value=10),
    zero_count=u64,
    positive=st.builds(
        Buckets,
        offset=st.integers(min_value=-100, max_value=100),
        bucket_counts=st.lists(u64, max_size=3),
    ),
    negative=st.builds(
        Buckets,
        offset=st.integers(min_value=-100, max_value=100),
        bucket_counts=st.lists(u64, max_size=3),
    ),
    sum=st.one_of(st.none(), finite),
    min=st.one_of(st.none(), finite),
    max=st.one_of(st.none(), finite),
    zero_threshold=finite,
    attributes=attributes,
    flags=flags,
    exemplars=st.lists(exemplars, max_size=2),
)

summary_points = st.builds(
    SummaryDataPoint,
    time_unix_nano=u64,
    count=u64,
    sum=finite,
    quantile_values=st.lists(
        st.builds(
            ValueAtQuantile,
            quantile=st.floats(min_value=0, max_value=1, width=32),
            value=finite,
        ),
        max_size=3,
    ),
    attributes=attributes,
    flags=flags,
)

metric_data = st.one_of(
    st.builds(Gauge, data_points=st.lists(number_points, min_size=1, max_size=3)),
    st.builds(
        Sum,
        data_points=st.lists(number_points, min_size=1, max_size=3),
        aggregation_temporality=st.sampled_from(AggregationTemporality),
        is_monotonic=st.booleans(),
    ),
    st.builds(
        Histogram,
        data_points=st.lists(histogram_points, min_size=1, max_size=3),
        aggregation_temporality=st.sampled_from(AggregationTemporality),
    ),
    st.builds(
        ExponentialHistogram,
        data_points=st.lists(exponential_points, min_size=1, max_size=3),
        aggregation_temporality=st.sampled_from(AggregationTemporality),
    ),
    st.builds(Summary, data_points=st.lists(summary_points, min_size=1, max_size=3)),
)

metrics = st.builds(Metric, name=text, data=metric_data, description=text, unit=text)

resource_metrics = st.builds(
    ResourceMetrics,
    resource=resources,
    scope_metrics=st.lists(
        st.builds(ScopeMetrics, scope=scopes, metrics=st.lists(metrics, min_size=1, max_size=3)),
        min_size=1,
        max_size=2,
    ),
)

log_records = st.builds(
    LogRecord,
    time_unix_nano=u64,
    observed_time_unix_nano=u64,
    body=any_values,
    severity_number=st.sampled_from(SeverityNumber),
    severity_text=text,
    attributes=attributes,
    trace_id=st.one_of(st.none(), st.binary(min_size=16, max_size=16)),
    span_id=st.one_of(st.none(), st.binary(min_size=8, max_size=8)),
    event_name=text,
    flags=small_uint,
)

resource_logs = st.builds(
    ResourceLogs,
    resource=resources,
    scope_logs=st.lists(
        st.builds(
            ScopeLogs, scope=scopes, log_records=st.lists(log_records, min_size=1, max_size=3)
        ),
        min_size=1,
        max_size=2,
    ),
)

span_links = st.builds(
    SpanLink,
    trace_id=st.binary(min_size=16, max_size=16),
    span_id=st.binary(min_size=8, max_size=8),
    trace_state=text,
    attributes=attributes,
    flags=flags,
)

spans = st.builds(
    Span,
    trace_id=st.binary(min_size=16, max_size=16),
    span_id=st.binary(min_size=8, max_size=8),
    name=text,
    start_time_unix_nano=u64,
    end_time_unix_nano=u64,
    kind=st.sampled_from(SpanKind),
    parent_span_id=st.one_of(st.none(), st.binary(min_size=8, max_size=8)),
    trace_state=text,
    flags=flags,
    attributes=attributes,
    events=st.lists(
        st.builds(SpanEvent, time_unix_nano=u64, name=text, attributes=attributes), max_size=2
    ),
    links=st.lists(span_links, max_size=2),
    status=st.one_of(st.none(), st.builds(Status, code=st.sampled_from(StatusCode), message=text)),
)

resource_spans = st.builds(
    ResourceSpans,
    resource=resources,
    scope_spans=st.lists(
        st.builds(ScopeSpans, scope=scopes, spans=st.lists(spans, min_size=1, max_size=3)),
        min_size=1,
        max_size=2,
    ),
)
