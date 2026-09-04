"""Client configuration."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType

from otlp_client.errors import OTLPConfigError
from otlp_client.model.common import Resource
from otlp_client.signals import SignalKind, http_path

_NO_HEADERS: Mapping[str, str] = MappingProxyType({})


class OTLPProtocol(StrEnum):
    HTTP_JSON = "http/json"
    HTTP_PROTOBUF = "http/protobuf"
    GRPC = "grpc"


class Compression(StrEnum):
    NONE = "none"
    GZIP = "gzip"


@dataclass(frozen=True, slots=True)
class OTLPConfig:
    """Every knob the client reads. The only source of settings."""

    endpoint: str
    protocol: OTLPProtocol = OTLPProtocol.HTTP_JSON
    headers: Mapping[str, str] = field(default=_NO_HEADERS, hash=False)
    timeout: float = 10.0
    compression: Compression = Compression.NONE
    gzip_threshold: int = 32 * 1024
    resource: Resource | None = None

    metrics_endpoint: str | None = None
    logs_endpoint: str | None = None
    traces_endpoint: str | None = None

    certificate_file: str | None = None
    client_certificate_file: str | None = None
    client_key_file: str | None = None
    insecure_skip_verify: bool = False

    initial_backoff: float = 1.0
    max_backoff: float = 30.0
    backoff_multiplier: float = 1.5
    max_elapsed: float = 90.0

    def __post_init__(self) -> None:
        if not self.endpoint:
            raise OTLPConfigError("endpoint must be a non-empty URL")
        if self.timeout <= 0:
            raise OTLPConfigError("timeout must be greater than zero")
        if self.max_elapsed <= 0:
            raise OTLPConfigError("max_elapsed must be greater than zero")

    def endpoint_for(self, kind: SignalKind) -> str:
        """Resolve the URL for a signal.

        A per-signal endpoint is used verbatim; the base endpoint gets the
        signal path appended. This asymmetry is required by the OTLP spec.
        """
        override = {
            SignalKind.METRICS: self.metrics_endpoint,
            SignalKind.LOGS: self.logs_endpoint,
            SignalKind.TRACES: self.traces_endpoint,
            SignalKind.PROFILES: None,
        }[kind]
        if override:
            return override
        return self.endpoint.rstrip("/") + http_path(kind)
