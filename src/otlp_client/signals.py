"""Signal identity and the OTLP/HTTP path table."""

from __future__ import annotations

from enum import StrEnum


class SignalKind(StrEnum):
    METRICS = "metrics"
    LOGS = "logs"
    TRACES = "traces"
    PROFILES = "profiles"


_PATHS: dict[SignalKind, str] = {
    SignalKind.METRICS: "/v1/metrics",
    SignalKind.LOGS: "/v1/logs",
    SignalKind.TRACES: "/v1/traces",
    SignalKind.PROFILES: "/v1development/profiles",
}


def http_path(kind: SignalKind) -> str:
    """Return the OTLP/HTTP path for a signal."""
    return _PATHS[kind]
