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


def test_per_signal_timeout_of_zero_is_rejected() -> None:
    # aiohttp treats ClientTimeout(total=0) as NO timeout at all, so a
    # per-signal zero must be rejected the same way the general timeout is,
    # rather than silently disabling the timeout for that signal.
    with pytest.raises(OTLPConfigError, match="timeout"):
        OTLPConfig(endpoint="http://localhost:4318", metrics_timeout=0)
    with pytest.raises(OTLPConfigError, match="timeout"):
        OTLPConfig(endpoint="http://localhost:4318", logs_timeout=0)
    with pytest.raises(OTLPConfigError, match="timeout"):
        OTLPConfig(endpoint="http://localhost:4318", traces_timeout=0)


def test_negative_per_signal_timeout_is_rejected() -> None:
    with pytest.raises(OTLPConfigError, match="timeout"):
        OTLPConfig(endpoint="http://localhost:4318", metrics_timeout=-1.0)
    with pytest.raises(OTLPConfigError, match="timeout"):
        OTLPConfig(endpoint="http://localhost:4318", logs_timeout=-1.0)
    with pytest.raises(OTLPConfigError, match="timeout"):
        OTLPConfig(endpoint="http://localhost:4318", traces_timeout=-1.0)


def test_unset_per_signal_timeout_is_fine() -> None:
    # None means "not configured" and must not trip the non-positive check.
    cfg = OTLPConfig(endpoint="http://localhost:4318")
    assert cfg.metrics_timeout is None
    assert cfg.logs_timeout is None
    assert cfg.traces_timeout is None


def test_headers_fall_back_to_the_general_value() -> None:
    cfg = OTLPConfig(endpoint="http://localhost:4318", headers={"api-key": "secret"})
    assert cfg.headers_for(SignalKind.TRACES) == {"api-key": "secret"}


def test_per_signal_headers_replace_rather_than_merge() -> None:
    cfg = OTLPConfig(
        endpoint="http://localhost:4318",
        headers={"api-key": "secret"},
        traces_headers={"x-tenant": "acme"},
    )
    assert cfg.headers_for(SignalKind.TRACES) == {"x-tenant": "acme"}
    assert cfg.headers_for(SignalKind.METRICS) == {"api-key": "secret"}


def test_empty_per_signal_headers_send_nothing() -> None:
    # An empty mapping is a real override under replace semantics, and must be
    # distinguishable from None, which means "use the general value".
    cfg = OTLPConfig(
        endpoint="http://localhost:4318",
        headers={"api-key": "secret"},
        logs_headers={},
    )
    assert cfg.headers_for(SignalKind.LOGS) == {}


def test_per_signal_timeout_and_compression_resolve_independently() -> None:
    cfg = OTLPConfig(
        endpoint="http://localhost:4318",
        metrics_timeout=2.5,
        logs_compression=Compression.GZIP,
    )
    assert cfg.timeout_for(SignalKind.METRICS) == 2.5
    assert cfg.timeout_for(SignalKind.LOGS) == 10.0
    assert cfg.compression_for(SignalKind.LOGS) is Compression.GZIP
    assert cfg.compression_for(SignalKind.METRICS) is Compression.NONE


def test_profiles_resolves_to_the_general_values() -> None:
    cfg = OTLPConfig(
        endpoint="http://localhost:4318",
        headers={"api-key": "secret"},
        traces_headers={"x-tenant": "acme"},
        traces_timeout=1.0,
    )
    assert cfg.headers_for(SignalKind.PROFILES) == {"api-key": "secret"}
    assert cfg.timeout_for(SignalKind.PROFILES) == 10.0
