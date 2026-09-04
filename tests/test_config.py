import pytest

from otlp_client.config import Compression, OTLPConfig, OTLPProtocol
from otlp_client.errors import OTLPConfigError
from otlp_client.signals import SignalKind, http_path


def test_http_paths_cover_every_signal() -> None:
    assert http_path(SignalKind.METRICS) == "/v1/metrics"
    assert http_path(SignalKind.LOGS) == "/v1/logs"
    assert http_path(SignalKind.TRACES) == "/v1/traces"
    assert http_path(SignalKind.PROFILES) == "/v1development/profiles"


def test_base_endpoint_gets_signal_path_appended() -> None:
    cfg = OTLPConfig(endpoint="https://collector.local:4318")
    assert cfg.endpoint_for(SignalKind.METRICS) == "https://collector.local:4318/v1/metrics"


def test_trailing_slash_on_base_endpoint_does_not_double_up() -> None:
    cfg = OTLPConfig(endpoint="https://collector.local:4318/")
    assert cfg.endpoint_for(SignalKind.LOGS) == "https://collector.local:4318/v1/logs"


def test_per_signal_endpoint_is_used_verbatim() -> None:
    cfg = OTLPConfig(
        endpoint="https://collector.local:4318",
        metrics_endpoint="https://elsewhere.example/ingest",
    )
    assert cfg.endpoint_for(SignalKind.METRICS) == "https://elsewhere.example/ingest"
    assert cfg.endpoint_for(SignalKind.TRACES) == "https://collector.local:4318/v1/traces"


def test_defaults_match_spec() -> None:
    cfg = OTLPConfig(endpoint="http://localhost:4318")
    assert cfg.protocol is OTLPProtocol.HTTP_JSON
    assert cfg.compression is Compression.NONE
    assert cfg.timeout == 10.0
    assert cfg.gzip_threshold == 32 * 1024
    assert (cfg.initial_backoff, cfg.max_backoff) == (1.0, 30.0)
    assert (cfg.backoff_multiplier, cfg.max_elapsed) == (1.5, 90.0)


def test_empty_endpoint_is_rejected() -> None:
    with pytest.raises(OTLPConfigError, match="endpoint"):
        OTLPConfig(endpoint="")


def test_non_positive_timeout_is_rejected() -> None:
    with pytest.raises(OTLPConfigError, match="timeout"):
        OTLPConfig(endpoint="http://localhost:4318", timeout=0)
