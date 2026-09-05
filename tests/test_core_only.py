"""Guards the promise that the core install needs only aiohttp.

Two independent guards prove this:

- A static AST scan (below) over every module under `src/otlp_client/`,
  asserting none of them import a forbidden extra at module import time.
- A runtime subprocess (`test_core_import_path_does_not_touch_optional_extras`)
  that actually imports the public surface -- including the optional-extra
  modules themselves -- and asserts the forbidden packages never landed in
  `sys.modules`.

Both need to cover every module reachable from `import otlp_client`,
`transport/grpc.py` and `encoding/protobuf.py` included, since those are
exactly the modules a "cache the availability probe" refactor would touch
next.
"""

from __future__ import annotations

import ast
import pathlib
import subprocess
import sys

import pytest

FORBIDDEN = ("grpc", "google.protobuf", "opentelemetry")

SRC_ROOT = pathlib.Path(__file__).resolve().parent.parent / "src" / "otlp_client"


def _forbidden_module_level_import(source: str, forbidden: tuple[str, ...]) -> str | None:
    """Return the first forbidden package `source` imports at module-import
    time, or None if it imports none of them there.

    Recurses into module-level compound statements (`if`/`try`/`with`/`for`/
    `while`, and their async variants) since those execute at import time too,
    but stops at `FunctionDef`/`AsyncFunctionDef`/`ClassDef` -- imports inside
    those are exactly what a module with a lazily-imported extra is supposed
    to contain. A plain `ast.walk` would descend into those bodies too and
    produce false positives.
    """

    def hit(name: str) -> str | None:
        for prefix in forbidden:
            if name == prefix or name.startswith(prefix + "."):
                return prefix
        return None

    def check_node(node: ast.stmt) -> str | None:
        if isinstance(node, ast.Import):
            for alias in node.names:
                found = hit(alias.name)
                if found:
                    return found
            return None
        if isinstance(node, ast.ImportFrom):
            return hit(node.module or "")
        return None

    def walk(nodes: list[ast.stmt]) -> str | None:
        for node in nodes:
            found = check_node(node)
            if found:
                return found
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                continue  # lazy imports live here by design; do not descend
            if isinstance(node, ast.If | ast.For | ast.AsyncFor | ast.While):
                found = walk(node.body) or walk(node.orelse)
            elif isinstance(node, ast.Try):
                found = (
                    walk(node.body)
                    or walk(node.orelse)
                    or walk(node.finalbody)
                    or next((r for h in node.handlers if (r := walk(h.body)) is not None), None)
                )
            elif isinstance(node, ast.With | ast.AsyncWith):
                found = walk(node.body)
            else:
                found = None
            if found:
                return found
        return None

    return walk(ast.parse(source).body)


def _discover_source_files() -> list[pathlib.Path]:
    """Every module under `src/otlp_client/`.

    Parameterizing over this (rather than a hand-written list of files) means
    a new module is covered automatically the moment it exists, with nothing
    to remember to add.
    """
    return sorted(SRC_ROOT.rglob("*.py"))


@pytest.mark.parametrize(
    "path", _discover_source_files(), ids=lambda p: str(p.relative_to(SRC_ROOT))
)
def test_no_module_imports_a_forbidden_extra_at_module_level(path: pathlib.Path) -> None:
    source = path.read_text()
    found = _forbidden_module_level_import(source, FORBIDDEN)
    assert found is None, f"{path} imports {found!r} at module level; it must be lazy"


def test_the_module_level_import_checker_catches_a_try_wrapped_import() -> None:
    # A natural "cache the availability probe" refactor: this executes at
    # module-import time even though it never appears as a top-level
    # Import/ImportFrom node, which is exactly the gap being closed here.
    snippet = (
        "try:\n    import opentelemetry.proto.common.v1.common_pb2\nexcept ImportError:\n    pass\n"
    )
    assert _forbidden_module_level_import(snippet, FORBIDDEN) == "opentelemetry"


def test_the_module_level_import_checker_allows_a_lazy_import_inside_a_function() -> None:
    snippet = "def f() -> None:\n    import grpc\n"
    assert _forbidden_module_level_import(snippet, FORBIDDEN) is None


# FORBIDDEN is spliced in as its own line (rather than making the whole
# script an f-string) so the dict/set literals below keep their braces
# unescaped.
SCRIPT = (
    f"FORBIDDEN = {FORBIDDEN!r}\n"
    + """
import sys
import otlp_client
from otlp_client import OTLPClient, OTLPConfig, gauge  # noqa: F401
from otlp_client.encoding.json import JSONEncoder
from otlp_client.processor import BatchProcessor  # noqa: F401

# Importing these two modules directly must be as safe as importing the
# public surface above -- that is the whole guarantee this test proves.
# Merely importing them (never calling GRPCTransport.create() or
# build_protobuf_encoder()) must not pull in grpc/google.protobuf/opentelemetry.
import otlp_client.transport.grpc  # noqa: F401
import otlp_client.encoding.protobuf  # noqa: F401

payload = JSONEncoder().encode(
    otlp_client.SignalKind.METRICS,
    [otlp_client.ResourceMetrics(
        resource=otlp_client.Resource(attributes={"a": "b"}),
        scope_metrics=[otlp_client.ScopeMetrics(
            scope=otlp_client.InstrumentationScope(name="t"),
            metrics=[gauge("m", 1.0, time_unix_nano=1)])])],
)
assert b'"resourceMetrics"' in payload
# Match on module path prefix, not just the top-level segment, so a bare
# "google.protobuf" import (not routed through "opentelemetry.proto") is
# caught too -- a top-level-only check would miss it, since "google" alone
# is not forbidden.
leaked = sorted(
    m for m in sys.modules
    if any(m == prefix or m.startswith(prefix + ".") for prefix in FORBIDDEN)
)
assert not leaked, f"optional extras leaked into the core import path: {leaked}"
print("core-only OK")
"""
)


def test_core_import_path_does_not_touch_optional_extras() -> None:
    """Runs in a subprocess so already-imported test dependencies cannot mask a leak."""
    result = subprocess.run(
        [sys.executable, "-c", SCRIPT], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
    assert "core-only OK" in result.stdout
