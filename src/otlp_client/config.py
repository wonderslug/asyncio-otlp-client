"""Client configuration."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from urllib.parse import unquote

from otlp_client.errors import OTLPConfigError
from otlp_client.model.common import Resource
from otlp_client.signals import SignalKind, http_path

_LOGGER = logging.getLogger(__name__)

_NO_HEADERS: Mapping[str, str] = MappingProxyType({})


class OTLPProtocol(StrEnum):
    HTTP_JSON = "http/json"
    HTTP_PROTOBUF = "http/protobuf"
    GRPC = "grpc"


class Compression(StrEnum):
    NONE = "none"
    GZIP = "gzip"


# Per the OTLP spec, OTLP/gRPC and OTLP/HTTP have different default ports.
_DEFAULT_ENDPOINTS: Mapping[OTLPProtocol, str] = MappingProxyType(
    {
        OTLPProtocol.HTTP_JSON: "http://localhost:4318",
        OTLPProtocol.HTTP_PROTOBUF: "http://localhost:4318",
        OTLPProtocol.GRPC: "http://localhost:4317",
    }
)


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

    # Per-signal overrides. A value here REPLACES the general one rather than
    # merging with it, per the spec: each option is overridable by a signal
    # specific option. None means "not configured"; an empty mapping is a real
    # override meaning "send no headers for this signal".
    metrics_headers: Mapping[str, str] | None = field(default=None, hash=False)
    logs_headers: Mapping[str, str] | None = field(default=None, hash=False)
    traces_headers: Mapping[str, str] | None = field(default=None, hash=False)

    metrics_timeout: float | None = None
    logs_timeout: float | None = None
    traces_timeout: float | None = None

    metrics_compression: Compression | None = None
    logs_compression: Compression | None = None
    traces_compression: Compression | None = None

    # `insecure` chooses whether TLS is used at all and only applies to gRPC
    # endpoints written without a scheme. `insecure_skip_verify` is a different
    # knob: it keeps TLS but stops verifying the server's certificate.
    insecure: bool = False
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

        base_compression = _parse_compression(
            src.get("OTEL_EXPORTER_OTLP_COMPRESSION"), "OTEL_EXPORTER_OTLP_COMPRESSION"
        )
        base_timeout = _parse_timeout(
            src.get("OTEL_EXPORTER_OTLP_TIMEOUT"), "OTEL_EXPORTER_OTLP_TIMEOUT"
        )

        return cls(
            endpoint=src.get("OTEL_EXPORTER_OTLP_ENDPOINT", _DEFAULT_ENDPOINTS[protocol]),
            protocol=protocol,
            headers=_parse_headers(src.get("OTEL_EXPORTER_OTLP_HEADERS", "")),
            insecure=_parse_bool(
                src.get("OTEL_EXPORTER_OTLP_INSECURE", ""), "OTEL_EXPORTER_OTLP_INSECURE"
            ),
            timeout=10.0 if base_timeout is None else base_timeout,
            compression=Compression.NONE if base_compression is None else base_compression,
            metrics_endpoint=src.get("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"),
            logs_endpoint=src.get("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"),
            traces_endpoint=src.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
            metrics_headers=_signal_headers(src, "METRICS"),
            logs_headers=_signal_headers(src, "LOGS"),
            traces_headers=_signal_headers(src, "TRACES"),
            metrics_timeout=_parse_timeout(
                src.get("OTEL_EXPORTER_OTLP_METRICS_TIMEOUT"),
                "OTEL_EXPORTER_OTLP_METRICS_TIMEOUT",
            ),
            logs_timeout=_parse_timeout(
                src.get("OTEL_EXPORTER_OTLP_LOGS_TIMEOUT"), "OTEL_EXPORTER_OTLP_LOGS_TIMEOUT"
            ),
            traces_timeout=_parse_timeout(
                src.get("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT"),
                "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT",
            ),
            metrics_compression=_parse_compression(
                src.get("OTEL_EXPORTER_OTLP_METRICS_COMPRESSION"),
                "OTEL_EXPORTER_OTLP_METRICS_COMPRESSION",
            ),
            logs_compression=_parse_compression(
                src.get("OTEL_EXPORTER_OTLP_LOGS_COMPRESSION"),
                "OTEL_EXPORTER_OTLP_LOGS_COMPRESSION",
            ),
            traces_compression=_parse_compression(
                src.get("OTEL_EXPORTER_OTLP_TRACES_COMPRESSION"),
                "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION",
            ),
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

    def headers_for(self, kind: SignalKind) -> Mapping[str, str]:
        """Resolve the headers for a signal.

        A per-signal value replaces the general one rather than merging with
        it. An empty mapping is a valid override meaning "send none"; None
        means "not configured, use the general value".
        """
        override = {
            SignalKind.METRICS: self.metrics_headers,
            SignalKind.LOGS: self.logs_headers,
            SignalKind.TRACES: self.traces_headers,
            SignalKind.PROFILES: None,
        }[kind]
        return self.headers if override is None else override

    def timeout_for(self, kind: SignalKind) -> float:
        """Resolve the timeout for a signal, in seconds."""
        override = {
            SignalKind.METRICS: self.metrics_timeout,
            SignalKind.LOGS: self.logs_timeout,
            SignalKind.TRACES: self.traces_timeout,
            SignalKind.PROFILES: None,
        }[kind]
        return self.timeout if override is None else override

    def compression_for(self, kind: SignalKind) -> Compression:
        """Resolve the compression for a signal."""
        override = {
            SignalKind.METRICS: self.metrics_compression,
            SignalKind.LOGS: self.logs_compression,
            SignalKind.TRACES: self.traces_compression,
            SignalKind.PROFILES: None,
        }[kind]
        return self.compression if override is None else override


def _parse_bool(raw: str, name: str) -> bool:
    """Parse a spec Boolean.

    Only the case-insensitive string "true" is true, and implementations are
    forbidden from extending that set, so "1" and "yes" are false. Anything
    other than true/false/empty falls back to false with a warning rather than
    raising, per the spec: false is defined to be the safe default, so a loud
    failure would buy nothing over the safe fallback.
    """
    value = raw.strip().lower()
    if value == "true":
        return True
    if value not in ("false", ""):
        _LOGGER.warning("ignoring unrecognised %s value %r, falling back to false", name, raw)
    return False


def _parse_timeout(raw: str | None, name: str) -> float | None:
    """Parse a millisecond timeout variable into seconds, or None if unset."""
    if not raw:
        return None
    try:
        return float(raw) / 1000.0
    except ValueError as exc:
        raise OTLPConfigError(f"invalid timeout in {name}: {raw!r}") from exc


def _parse_compression(raw: str | None, name: str) -> Compression | None:
    """Parse a compression variable, or None if unset."""
    if not raw:
        return None
    try:
        return Compression(raw)
    except ValueError as exc:
        raise OTLPConfigError(f"unknown compression in {name}: {raw!r}") from exc


def _signal_headers(src: Mapping[str, str], signal: str) -> Mapping[str, str] | None:
    """Read one per-signal headers variable.

    Absent gives None, meaning "use the general value". Present gives a
    mapping that replaces it — including an empty one, which means "send no
    headers for this signal".
    """
    raw = src.get(f"OTEL_EXPORTER_OTLP_{signal}_HEADERS")
    return None if raw is None else _parse_headers(raw)


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
