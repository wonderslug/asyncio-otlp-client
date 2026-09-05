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
from otlp_client.model.logs import LogRecord, ResourceLogs, ScopeLogs, SeverityNumber, log_record
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
from otlp_client.processor import BatchProcessor, ProcessorStats
from otlp_client.signals import SignalKind

__all__ = [
    "AggregationTemporality",
    "AnyValue",
    "BatchProcessor",
    "Compression",
    "ExportOutcome",
    "Gauge",
    "Histogram",
    "HistogramDataPoint",
    "InstrumentationScope",
    "LogRecord",
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
    "ProcessorStats",
    "Resource",
    "ResourceLogs",
    "ResourceMetrics",
    "Retryable",
    "ScopeLogs",
    "ScopeMetrics",
    "SeverityNumber",
    "SignalKind",
    "Success",
    "Sum",
    "__version__",
    "gauge",
    "log_record",
    "sum_",
]
