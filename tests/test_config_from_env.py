import logging

import pytest

from otlp_client.config import Compression, OTLPConfig, OTLPProtocol
from otlp_client.errors import OTLPConfigError
from otlp_client.signals import SignalKind


def test_reads_base_endpoint_and_protocol() -> None:
    cfg = OTLPConfig.from_env(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "https://collector.local:4318",
            "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
        }
    )
    assert cfg.endpoint == "https://collector.local:4318"
    assert cfg.protocol is OTLPProtocol.HTTP_PROTOBUF


def test_per_signal_endpoint_override() -> None:
    cfg = OTLPConfig.from_env(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "https://base.local:4318",
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "https://metrics.local/ingest",
        }
    )
    assert cfg.endpoint_for(SignalKind.METRICS) == "https://metrics.local/ingest"
    assert cfg.endpoint_for(SignalKind.LOGS) == "https://base.local:4318/v1/logs"


def test_headers_are_comma_separated_key_value_pairs() -> None:
    cfg = OTLPConfig.from_env(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
            "OTEL_EXPORTER_OTLP_HEADERS": "api-key=secret,x-tenant=home",
        }
    )
    assert cfg.headers == {"api-key": "secret", "x-tenant": "home"}


def test_header_values_are_url_decoded_and_whitespace_stripped() -> None:
    cfg = OTLPConfig.from_env(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
            "OTEL_EXPORTER_OTLP_HEADERS": " authorization = Bearer%20abc ",
        }
    )
    assert cfg.headers == {"authorization": "Bearer abc"}


def test_timeout_env_var_is_milliseconds() -> None:
    cfg = OTLPConfig.from_env(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
            "OTEL_EXPORTER_OTLP_TIMEOUT": "2500",
        }
    )
    assert cfg.timeout == 2.5


def test_compression_and_certificate() -> None:
    cfg = OTLPConfig.from_env(
        {
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
            "OTEL_EXPORTER_OTLP_COMPRESSION": "gzip",
            "OTEL_EXPORTER_OTLP_CERTIFICATE": "/etc/ssl/ca.pem",
        }
    )
    assert cfg.compression is Compression.GZIP
    assert cfg.certificate_file == "/etc/ssl/ca.pem"


def test_missing_endpoint_defaults_to_localhost() -> None:
    assert OTLPConfig.from_env({}).endpoint == "http://localhost:4318"


def test_unknown_protocol_is_rejected() -> None:
    with pytest.raises(OTLPConfigError, match="protocol"):
        OTLPConfig.from_env({"OTEL_EXPORTER_OTLP_PROTOCOL": "carrier-pigeon"})


def test_grpc_protocol_defaults_to_the_grpc_port() -> None:
    cfg = OTLPConfig.from_env({"OTEL_EXPORTER_OTLP_PROTOCOL": "grpc"})
    assert cfg.endpoint == "http://localhost:4317"


def test_http_protobuf_protocol_defaults_to_the_http_port() -> None:
    cfg = OTLPConfig.from_env({"OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf"})
    assert cfg.endpoint == "http://localhost:4318"


def test_explicit_endpoint_still_wins_over_the_grpc_default() -> None:
    cfg = OTLPConfig.from_env(
        {
            "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
            "OTEL_EXPORTER_OTLP_ENDPOINT": "https://collector.local:9999",
        }
    )
    assert cfg.endpoint == "https://collector.local:9999"


def test_explicitly_empty_endpoint_is_rejected_rather_than_defaulted() -> None:
    with pytest.raises(OTLPConfigError, match="endpoint"):
        OTLPConfig.from_env(
            {"OTEL_EXPORTER_OTLP_PROTOCOL": "grpc", "OTEL_EXPORTER_OTLP_ENDPOINT": ""}
        )


def test_insecure_defaults_to_false() -> None:
    assert OTLPConfig.from_env({}).insecure is False
    assert OTLPConfig(endpoint="http://localhost:4317").insecure is False


@pytest.mark.parametrize("raw", ["true", "TRUE", "True"])
def test_insecure_is_true_only_for_case_insensitive_true(raw: str) -> None:
    assert OTLPConfig.from_env({"OTEL_EXPORTER_OTLP_INSECURE": raw}).insecure is True


@pytest.mark.parametrize("raw", ["false", "FALSE", ""])
def test_insecure_is_false_for_false_and_empty(raw: str) -> None:
    assert OTLPConfig.from_env({"OTEL_EXPORTER_OTLP_INSECURE": raw}).insecure is False


@pytest.mark.parametrize("raw", ["1", "yes", "on", "banana"])
def test_insecure_rejects_values_the_spec_forbids_extending_to(
    raw: str, caplog: pytest.LogCaptureFixture
) -> None:
    # The spec forbids implementations adding their own true values, and
    # requires falling back to false with a warning rather than raising.
    with caplog.at_level(logging.WARNING):
        cfg = OTLPConfig.from_env({"OTEL_EXPORTER_OTLP_INSECURE": raw})
    assert cfg.insecure is False
    assert "OTEL_EXPORTER_OTLP_INSECURE" in caplog.text


def test_per_signal_headers_are_read_and_replace_the_general_ones() -> None:
    cfg = OTLPConfig.from_env(
        {
            "OTEL_EXPORTER_OTLP_HEADERS": "api-key=secret",
            "OTEL_EXPORTER_OTLP_TRACES_HEADERS": "x-tenant=acme",
        }
    )
    assert cfg.headers_for(SignalKind.TRACES) == {"x-tenant": "acme"}
    assert cfg.headers_for(SignalKind.METRICS) == {"api-key": "secret"}


def test_empty_per_signal_headers_variable_means_send_none() -> None:
    # Absent -> None -> fall back. Present but empty -> {} -> replace with nothing.
    cfg = OTLPConfig.from_env(
        {
            "OTEL_EXPORTER_OTLP_HEADERS": "api-key=secret",
            "OTEL_EXPORTER_OTLP_LOGS_HEADERS": "",
        }
    )
    assert cfg.logs_headers == {}
    assert cfg.headers_for(SignalKind.LOGS) == {}
    assert cfg.metrics_headers is None


def test_per_signal_timeouts_are_milliseconds() -> None:
    cfg = OTLPConfig.from_env(
        {
            "OTEL_EXPORTER_OTLP_TIMEOUT": "10000",
            "OTEL_EXPORTER_OTLP_METRICS_TIMEOUT": "2500",
        }
    )
    assert cfg.timeout_for(SignalKind.METRICS) == 2.5
    assert cfg.timeout_for(SignalKind.LOGS) == 10.0


def test_per_signal_compression_is_read() -> None:
    cfg = OTLPConfig.from_env({"OTEL_EXPORTER_OTLP_LOGS_COMPRESSION": "gzip"})
    assert cfg.compression_for(SignalKind.LOGS) is Compression.GZIP
    assert cfg.compression_for(SignalKind.TRACES) is Compression.NONE


def test_invalid_per_signal_timeout_is_rejected() -> None:
    with pytest.raises(OTLPConfigError, match="timeout"):
        OTLPConfig.from_env({"OTEL_EXPORTER_OTLP_TRACES_TIMEOUT": "soon"})


def test_invalid_per_signal_compression_is_rejected() -> None:
    with pytest.raises(OTLPConfigError, match="compression"):
        OTLPConfig.from_env({"OTEL_EXPORTER_OTLP_TRACES_COMPRESSION": "brotli"})


def test_empty_compression_variable_is_treated_as_unset() -> None:
    # An empty environment variable is conventionally the same as an unset one
    # (`export X="${MAYBE_UNSET}"` yields ""), and the spec defines the
    # compression default as "no value explicitly specified".
    assert (
        OTLPConfig.from_env({"OTEL_EXPORTER_OTLP_COMPRESSION": ""}).compression is Compression.NONE
    )


def test_general_timeout_of_zero_is_rejected() -> None:
    with pytest.raises(OTLPConfigError, match="timeout"):
        OTLPConfig.from_env({"OTEL_EXPORTER_OTLP_TIMEOUT": "0"})
