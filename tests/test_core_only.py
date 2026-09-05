"""Guards the promise that the core install needs only aiohttp."""

import subprocess
import sys

FORBIDDEN = ("grpc", "google.protobuf", "opentelemetry")

SCRIPT = """
import sys
import otlp_client
from otlp_client import OTLPClient, OTLPConfig, gauge  # noqa: F401
from otlp_client.encoding.json import JSONEncoder
from otlp_client.processor import BatchProcessor  # noqa: F401

payload = JSONEncoder().encode(
    otlp_client.SignalKind.METRICS,
    [otlp_client.ResourceMetrics(
        resource=otlp_client.Resource(attributes={"a": "b"}),
        scope_metrics=[otlp_client.ScopeMetrics(
            scope=otlp_client.InstrumentationScope(name="t"),
            metrics=[gauge("m", 1.0, time_unix_nano=1)])])],
)
assert b'"resourceMetrics"' in payload
leaked = sorted(m for m in sys.modules if m.split(".")[0] in {"grpc", "opentelemetry"})
assert not leaked, f"optional extras leaked into the core import path: {leaked}"
print("core-only OK")
"""


def test_core_import_path_does_not_touch_optional_extras() -> None:
    """Runs in a subprocess so already-imported test dependencies cannot mask a leak."""
    result = subprocess.run(
        [sys.executable, "-c", SCRIPT], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    assert "core-only OK" in result.stdout
