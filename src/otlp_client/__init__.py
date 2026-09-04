"""A pure-asyncio OpenTelemetry OTLP client."""

from otlp_client.client import OTLPClient, __version__
from otlp_client.config import Compression, OTLPConfig, OTLPProtocol
from otlp_client.errors import (
    OTLPConfigError,
    OTLPError,
    OTLPPermanentError,
    OTLPTransportError,
)
from otlp_client.model.common import AnyValue, InstrumentationScope, Resource
from otlp_client.model.metrics import (
    AggregationTemporality,
    Gauge,
    Histogram,
    HistogramDataPoint,
    Metric,
    NumberDataPoint,
    ResourceMetrics,
    ScopeMetrics,
    Sum,
    gauge,
    sum_,
)
from otlp_client.outcomes import ExportOutcome, PartialSuccess, Permanent, Retryable, Success
from otlp_client.signals import SignalKind

__all__ = [
    "AggregationTemporality",
    "AnyValue",
    "Compression",
    "ExportOutcome",
    "Gauge",
    "Histogram",
    "HistogramDataPoint",
    "InstrumentationScope",
    "Metric",
    "NumberDataPoint",
    "OTLPClient",
    "OTLPConfig",
    "OTLPConfigError",
    "OTLPError",
    "OTLPPermanentError",
    "OTLPProtocol",
    "OTLPTransportError",
    "PartialSuccess",
    "Permanent",
    "Resource",
    "ResourceMetrics",
    "Retryable",
    "ScopeMetrics",
    "SignalKind",
    "Success",
    "Sum",
    "__version__",
    "gauge",
    "sum_",
]
