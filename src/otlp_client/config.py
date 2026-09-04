"""Client configuration."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from urllib.parse import unquote

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

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> OTLPConfig:
        """Build a config from the standard OTEL_EXPORTER_OTLP_* variables.

        Never called implicitly. Callers opt in so that ambient environment
        cannot silently change client behaviour.
        """
        src = os.environ if env is None else env

        raw_protocol = src.get("OTEL_EXPORTER_OTLP_PROTOCOL", OTLPProtocol.HTTP_JSON.value)
        try:
            protocol = OTLPProtocol(raw_protocol)
        except ValueError as exc:
            raise OTLPConfigError(f"unknown protocol {raw_protocol!r}") from exc

        raw_compression = src.get("OTEL_EXPORTER_OTLP_COMPRESSION", Compression.NONE.value)
        try:
            compression = Compression(raw_compression)
        except ValueError as exc:
            raise OTLPConfigError(f"unknown compression {raw_compression!r}") from exc

        timeout_ms = src.get("OTEL_EXPORTER_OTLP_TIMEOUT")
        try:
            timeout = float(timeout_ms) / 1000.0 if timeout_ms else 10.0
        except ValueError as exc:
            raise OTLPConfigError(f"invalid timeout {timeout_ms!r}") from exc

        return cls(
            endpoint=src.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"),
            protocol=protocol,
            headers=_parse_headers(src.get("OTEL_EXPORTER_OTLP_HEADERS", "")),
            timeout=timeout,
            compression=compression,
            metrics_endpoint=src.get("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"),
            logs_endpoint=src.get("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"),
            traces_endpoint=src.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
            certificate_file=src.get("OTEL_EXPORTER_OTLP_CERTIFICATE"),
            client_certificate_file=src.get("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE"),
            client_key_file=src.get("OTEL_EXPORTER_OTLP_CLIENT_KEY"),
        )

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


def _parse_headers(raw: str) -> Mapping[str, str]:
    """Parse the OTEL_EXPORTER_OTLP_HEADERS `k=v,k=v` form."""
    headers: dict[str, str] = {}
    for pair in raw.split(","):
        if not pair.strip():
            continue
        key, sep, value = pair.partition("=")
        if not sep:
            raise OTLPConfigError(f"malformed header entry {pair!r}, expected key=value")
        headers[unquote(key.strip())] = unquote(value.strip())
    return MappingProxyType(headers)
