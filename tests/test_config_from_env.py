import pytest

from otlp_client.config import Compression, OTLPConfig, OTLPProtocol
from otlp_client.errors import OTLPConfigError
from otlp_client.signals import SignalKind


def test_reads_base_endpoint_and_protocol() -> None:
    cfg = OTLPConfig.from_env({
        "OTEL_EXPORTER_OTLP_ENDPOINT": "https://collector.local:4318",
        "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
    })
    assert cfg.endpoint == "https://collector.local:4318"
    assert cfg.protocol is OTLPProtocol.HTTP_PROTOBUF


def test_per_signal_endpoint_override() -> None:
    cfg = OTLPConfig.from_env({
        "OTEL_EXPORTER_OTLP_ENDPOINT": "https://base.local:4318",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "https://metrics.local/ingest",
    })
    assert cfg.endpoint_for(SignalKind.METRICS) == "https://metrics.local/ingest"
    assert cfg.endpoint_for(SignalKind.LOGS) == "https://base.local:4318/v1/logs"


def test_headers_are_comma_separated_key_value_pairs() -> None:
    cfg = OTLPConfig.from_env({
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
        "OTEL_EXPORTER_OTLP_HEADERS": "api-key=secret,x-tenant=home",
    })
    assert cfg.headers == {"api-key": "secret", "x-tenant": "home"}


def test_header_values_are_url_decoded_and_whitespace_stripped() -> None:
    cfg = OTLPConfig.from_env({
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
        "OTEL_EXPORTER_OTLP_HEADERS": " authorization = Bearer%20abc ",
    })
    assert cfg.headers == {"authorization": "Bearer abc"}


def test_timeout_env_var_is_milliseconds() -> None:
    cfg = OTLPConfig.from_env({
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
        "OTEL_EXPORTER_OTLP_TIMEOUT": "2500",
    })
    assert cfg.timeout == 2.5


def test_compression_and_certificate() -> None:
    cfg = OTLPConfig.from_env({
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
        "OTEL_EXPORTER_OTLP_COMPRESSION": "gzip",
        "OTEL_EXPORTER_OTLP_CERTIFICATE": "/etc/ssl/ca.pem",
    })
    assert cfg.compression is Compression.GZIP
    assert cfg.certificate_file == "/etc/ssl/ca.pem"


def test_missing_endpoint_defaults_to_localhost() -> None:
    assert OTLPConfig.from_env({}).endpoint == "http://localhost:4318"


def test_unknown_protocol_is_rejected() -> None:
    with pytest.raises(OTLPConfigError, match="protocol"):
        OTLPConfig.from_env({"OTEL_EXPORTER_OTLP_PROTOCOL": "carrier-pigeon"})
