# Per-signal exporter configuration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `HEADERS`, `TIMEOUT` and `COMPRESSION` be configured per signal via the
`OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_*` environment variables, and reject the
per-signal variables this client cannot honour.

**Architecture:** Nine new `T | None` fields on the frozen `OTLPConfig` dataclass, mirroring
the existing `metrics_endpoint`/`logs_endpoint`/`traces_endpoint` trio, resolved by three
`*_for(kind)` methods beside the existing `endpoint_for(kind)`. A per-signal value *replaces*
the general one. Both transports call the resolvers per request instead of reading the general
fields.

**Tech Stack:** Python 3.12+, frozen slotted dataclasses, pytest, aiohttp (HTTP transport),
grpcio (gRPC transport, optional extra), ruff, mypy strict.

**Spec:** `docs/superpowers/specs/2026-09-05-per-signal-config-design.md`

## Global Constraints

- **TDD is mandatory.** Every task writes a failing test, runs it to watch it fail for the
  right reason, then implements. Never write implementation before a red test.
- **Core install depends on `aiohttp` alone.** Add no new runtime dependencies. Never import
  `grpc`, `google.protobuf` or `opentelemetry` at module level in `src/` — `tests/test_core_only.py`
  enforces this with an AST scan and a subprocess check.
- **mypy strict must pass.** Run `uv run mypy`. No `Any` leaking into public signatures, no
  stringly-typed attribute lookup.
- **ruff must pass**, both `uv run ruff check .` and `uv run ruff format --check .`.
- **Full suite must stay green:** `uv run pytest -q` — 254 tests pass before this work starts.
- **A per-signal value replaces the general value.** Never merge. `None` means "not configured,
  use the general value"; an empty mapping means "configured as empty, send nothing".
- **Commit messages** use conventional-commit prefixes (`feat:`, `fix:`, `test:`, `docs:`).
  **Never add `Co-Authored-By: Claude` or any Claude attribution line** — the repo owner
  forbids it in their global instructions.
- **Run all commands from the repo root** with `uv run`.

---

### Task 1: Per-signal fields and resolvers

Adds the nine config fields and the three resolver methods. Everything later depends on this.

**Files:**
- Modify: `src/otlp_client/config.py` (fields after line 57; resolvers after `endpoint_for`)
- Test: `tests/test_config.py`

**Interfaces:**
- Consumes: `SignalKind` from `otlp_client.signals`; existing `OTLPConfig` fields `headers`,
  `timeout`, `compression`.
- Produces: fields `metrics_headers`, `logs_headers`, `traces_headers` (`Mapping[str, str] | None`),
  `metrics_timeout`, `logs_timeout`, `traces_timeout` (`float | None`), `metrics_compression`,
  `logs_compression`, `traces_compression` (`Compression | None`); methods
  `headers_for(kind: SignalKind) -> Mapping[str, str]`, `timeout_for(kind: SignalKind) -> float`,
  `compression_for(kind: SignalKind) -> Compression`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_config.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_config.py -q`
Expected: FAIL with `TypeError: OTLPConfig.__init__() got an unexpected keyword argument
'traces_headers'` and `AttributeError: 'OTLPConfig' object has no attribute 'headers_for'`.
Both mean the feature is missing, which is the correct red.

- [ ] **Step 3: Add the nine fields**

In `src/otlp_client/config.py`, immediately after the `traces_endpoint: str | None = None`
line, add:

```python
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
```

`hash=False` is required on the mapping fields for the same reason the general `headers`
field carries it: the dataclass is frozen, so it generates `__hash__`, and a `Mapping` is
not hashable.

- [ ] **Step 4: Add the three resolvers**

In the same file, directly after the `endpoint_for` method:

```python
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
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_config.py -q`
Expected: PASS, all tests in the file.

- [ ] **Step 6: Run the full suite and the checks**

Run: `uv run pytest -q && uv run ruff check . && uv run ruff format --check . && uv run mypy`
Expected: all pass. The suite grows from 254 to 259.

- [ ] **Step 7: Commit**

```bash
git add src/otlp_client/config.py tests/test_config.py
git commit -m "feat(config): per-signal headers, timeout and compression fields

Nine T | None fields mirroring the existing per-signal endpoint trio, with
headers_for/timeout_for/compression_for resolvers. A per-signal value
replaces the general one; None means unset and an empty mapping is a real
override meaning send nothing."
```

---

### Task 2: Read the nine per-signal environment variables

**Files:**
- Modify: `src/otlp_client/config.py` (`from_env`, plus two new module-level helpers)
- Test: `tests/test_config_from_env.py`

**Interfaces:**
- Consumes: fields from Task 1; existing `_parse_headers(raw: str) -> Mapping[str, str]`.
- Produces: `_parse_timeout(raw: str | None, name: str) -> float | None`,
  `_parse_compression(raw: str | None, name: str) -> Compression | None`,
  `_signal_headers(src: Mapping[str, str], signal: str) -> Mapping[str, str] | None`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_config_from_env.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_config_from_env.py -q`
Expected: FAIL. The header and timeout tests fail on assertions (values fall back to the
general ones because nothing reads the per-signal variables); the two invalid-value tests
fail with `DID NOT RAISE`.

- [ ] **Step 3: Add the three helpers**

In `src/otlp_client/config.py`, after the existing `_parse_bool` function:

```python
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
```

The error messages keep the lowercase words `timeout` and `compression` so existing and new
`pytest.raises(match=...)` patterns match, while also naming the exact variable.

- [ ] **Step 4: Route the general timeout and compression through the helpers**

In `from_env`, replace the existing compression block (currently the `raw_compression = ...`
try/except) and the timeout block (`timeout_ms = ...` try/except) with:

```python
base_compression = _parse_compression(
    src.get("OTEL_EXPORTER_OTLP_COMPRESSION"), "OTEL_EXPORTER_OTLP_COMPRESSION"
)
base_timeout = _parse_timeout(src.get("OTEL_EXPORTER_OTLP_TIMEOUT"), "OTEL_EXPORTER_OTLP_TIMEOUT")
```

and in the `cls(...)` call change the two existing arguments to:

```python
timeout = (10.0 if base_timeout is None else base_timeout,)
compression = (Compression.NONE if base_compression is None else base_compression,)
```

Use the explicit `is None` checks rather than `or`, so a configured value of `0` is passed
through to `__post_init__` and rejected there instead of being silently replaced by the
default.

Leave the `OTEL_EXPORTER_OTLP_PROTOCOL` block exactly as it is.

- [ ] **Step 5: Read the nine per-signal variables**

In the same `cls(...)` call, after the `traces_endpoint=` argument, add:

```python
metrics_headers = (_signal_headers(src, "METRICS"),)
logs_headers = (_signal_headers(src, "LOGS"),)
traces_headers = (_signal_headers(src, "TRACES"),)
metrics_timeout = (
    _parse_timeout(
        src.get("OTEL_EXPORTER_OTLP_METRICS_TIMEOUT"),
        "OTEL_EXPORTER_OTLP_METRICS_TIMEOUT",
    ),
)
logs_timeout = (
    _parse_timeout(src.get("OTEL_EXPORTER_OTLP_LOGS_TIMEOUT"), "OTEL_EXPORTER_OTLP_LOGS_TIMEOUT"),
)
traces_timeout = (
    _parse_timeout(
        src.get("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT"),
        "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT",
    ),
)
metrics_compression = (
    _parse_compression(
        src.get("OTEL_EXPORTER_OTLP_METRICS_COMPRESSION"),
        "OTEL_EXPORTER_OTLP_METRICS_COMPRESSION",
    ),
)
logs_compression = (
    _parse_compression(
        src.get("OTEL_EXPORTER_OTLP_LOGS_COMPRESSION"),
        "OTEL_EXPORTER_OTLP_LOGS_COMPRESSION",
    ),
)
traces_compression = (
    _parse_compression(
        src.get("OTEL_EXPORTER_OTLP_TRACES_COMPRESSION"),
        "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION",
    ),
)
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run pytest tests/test_config_from_env.py -q`
Expected: PASS. In particular `test_timeout_env_var_is_milliseconds` and
`test_compression_and_certificate`, which already existed, must still pass — they are the
guard that routing the general values through the new helpers did not change behaviour.

- [ ] **Step 7: Run the full suite and the checks**

Run: `uv run pytest -q && uv run ruff check . && uv run ruff format --check . && uv run mypy`
Expected: all pass.

- [ ] **Step 8: Commit**

```bash
git add src/otlp_client/config.py tests/test_config_from_env.py
git commit -m "feat(config): read the per-signal HEADERS, TIMEOUT and COMPRESSION vars

Factors _parse_timeout and _parse_compression out of the general-form
parsing and reuses them for all four call sites each. An absent per-signal
headers variable gives None (use the general value); an empty one gives an
empty mapping (send nothing)."
```

---

### Task 3: Reject the per-signal variables this client cannot honour

**Files:**
- Modify: `src/otlp_client/config.py` (module constants plus a helper; called from `from_env`)
- Test: `tests/test_config_from_env.py`

**Interfaces:**
- Consumes: `OTLPConfigError` from `otlp_client.errors`.
- Produces: `_reject_unsupported_per_signal(src: Mapping[str, str]) -> None`.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_config_from_env.py`:

```python
@pytest.mark.parametrize("signal", ["TRACES", "METRICS", "LOGS"])
@pytest.mark.parametrize(
    "option", ["PROTOCOL", "INSECURE", "CERTIFICATE", "CLIENT_KEY", "CLIENT_CERTIFICATE"]
)
def test_unsupported_per_signal_variables_are_rejected(signal: str, option: str) -> None:
    # One OTLPClient holds one encoder and one transport, so these cannot vary
    # by signal. Ignoring them silently would send a signal in the wrong wire
    # format or quietly drop a certificate.
    name = f"OTEL_EXPORTER_OTLP_{signal}_{option}"
    with pytest.raises(OTLPConfigError, match=name):
        OTLPConfig.from_env({name: "x"})


def test_supported_per_signal_variables_are_not_rejected() -> None:
    cfg = OTLPConfig.from_env(
        {
            "OTEL_EXPORTER_OTLP_TRACES_HEADERS": "x-tenant=acme",
            "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT": "2500",
            "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION": "gzip",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "https://traces.example/v1/traces",
        }
    )
    assert cfg.traces_headers == {"x-tenant": "acme"}
    assert cfg.traces_timeout == 2.5


def test_an_empty_unsupported_variable_is_ignored() -> None:
    # An empty value is effectively unset, matching how endpoint overrides are
    # treated, so it must not trip the rejection.
    cfg = OTLPConfig.from_env({"OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE": ""})
    assert cfg.certificate_file is None
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_config_from_env.py -q -k unsupported`
Expected: FAIL with `DID NOT RAISE OTLPConfigError` for all 15 parametrized cases.

- [ ] **Step 3: Add the rejection helper**

In `src/otlp_client/config.py`, after the `_DEFAULT_ENDPOINTS` mapping, add:

```python
# Per-signal variants this client cannot honour: one OTLPClient holds one
# encoder and one transport, so the protocol and the connection-level TLS
# settings cannot vary by signal.
_UNSUPPORTED_PER_SIGNAL = (
    "PROTOCOL",
    "INSECURE",
    "CERTIFICATE",
    "CLIENT_KEY",
    "CLIENT_CERTIFICATE",
)
_SIGNAL_PREFIXES = ("TRACES", "METRICS", "LOGS")
```

and, after the `_signal_headers` function:

```python
def _reject_unsupported_per_signal(src: Mapping[str, str]) -> None:
    """Refuse per-signal variables that cannot be honoured.

    Failing loudly rather than ignoring them: silently dropping a per-signal
    protocol would send that signal in the wrong wire format, and silently
    dropping a per-signal certificate is a security surprise.
    """
    for signal in _SIGNAL_PREFIXES:
        for option in _UNSUPPORTED_PER_SIGNAL:
            name = f"OTEL_EXPORTER_OTLP_{signal}_{option}"
            if src.get(name):
                raise OTLPConfigError(
                    f"{name} is not supported: one OTLPClient holds a single encoder and "
                    f"transport, so this cannot vary per signal. Use separate OTLPClient "
                    f"instances, one per signal, each with its own config."
                )
```

The `if src.get(name)` truthiness check means an empty value is treated as unset, matching
how `endpoint_for` treats an empty override.

- [ ] **Step 4: Call it from `from_env`**

Make it the first statement after `src` is bound, so a bad environment fails before any
other parsing:

```python
        src = os.environ if env is None else env
        _reject_unsupported_per_signal(src)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_config_from_env.py -q`
Expected: PASS, all cases including the 15 parametrized rejections.

- [ ] **Step 6: Run the full suite and the checks**

Run: `uv run pytest -q && uv run ruff check . && uv run ruff format --check . && uv run mypy`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/otlp_client/config.py tests/test_config_from_env.py
git commit -m "feat(config): reject per-signal protocol and TLS variables

One OTLPClient holds one encoder and one transport, so PROTOCOL, INSECURE
and the three TLS settings cannot vary by signal. from_env raises rather
than ignoring them: a dropped per-signal protocol would send that signal in
the wrong wire format, and a dropped certificate is a security surprise."
```

---

### Task 4: Per-signal endpoint root-path conformance

The spec requires a per-signal endpoint URL to be used as-is, with one exception: a URL with
no path part gets the root path `/`.

**Files:**
- Modify: `src/otlp_client/config.py` (`endpoint_for`, and the `urllib.parse` import on line 11)
- Test: `tests/test_config.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: no new names; changes the behaviour of `endpoint_for` for pathless overrides only.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_config.py`:

```python
def test_pathless_per_signal_endpoint_gets_the_root_path() -> None:
    # The spec: a per-signal URL is used as-is, except that a URL with no path
    # part MUST use the root path.
    cfg = OTLPConfig(
        endpoint="https://collector.local:4318",
        traces_endpoint="http://collector:4318",
    )
    assert cfg.endpoint_for(SignalKind.TRACES) == "http://collector:4318/"


def test_per_signal_endpoint_with_a_path_is_left_alone() -> None:
    cfg = OTLPConfig(
        endpoint="https://collector.local:4318",
        metrics_endpoint="https://elsewhere.example/ingest",
    )
    assert cfg.endpoint_for(SignalKind.METRICS) == "https://elsewhere.example/ingest"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_config.py -q -k pathless`
Expected: FAIL with `assert 'http://collector:4318' == 'http://collector:4318/'`.

- [ ] **Step 3: Extend the import**

Change line 11 of `src/otlp_client/config.py` from:

```python
from urllib.parse import unquote
```

to:

```python
from urllib.parse import unquote, urlparse
```

- [ ] **Step 4: Apply the root path in `endpoint_for`**

Replace the `if override:` branch of `endpoint_for` with:

```python
        if override:
            # The spec requires a per-signal URL to be used as-is, with one
            # exception: a URL carrying no path part must use the root path.
            return override if urlparse(override).path else override + "/"
```

Leave the general-endpoint branch untouched — it already appends a signal path, so it can
never be pathless.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_config.py -q`
Expected: PASS. `test_per_signal_endpoint_is_used_verbatim`, which already existed, must
still pass — its override has a path, so it is the guard that this change touches only
pathless URLs.

- [ ] **Step 6: Run the full suite and the checks**

Run: `uv run pytest -q && uv run ruff check . && uv run ruff format --check . && uv run mypy`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/otlp_client/config.py tests/test_config.py
git commit -m "fix(config): give a pathless per-signal endpoint the root path

The spec uses a per-signal URL as-is except that one with no path part must
use the root path, so OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://collector:4318
posts to / rather than to an empty path."
```

---

### Task 5: HTTP transport reads the per-signal values

**Files:**
- Modify: `src/otlp_client/transport/http.py` (`__init__` and `send`)
- Test: `tests/test_transport_http.py`

**Interfaces:**
- Consumes: `headers_for`, `timeout_for`, `compression_for` from Task 1.
- Produces: `HTTPTransport._timeouts: dict[SignalKind, aiohttp.ClientTimeout]`, replacing the
  single `HTTPTransport._timeout`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_transport_http.py`:

```python
async def test_per_signal_headers_replace_the_general_ones(
    server_factory: ServerFactory,
) -> None:
    rec = Recorder()
    base, session = await server_factory(rec)
    cfg = OTLPConfig(
        endpoint=base,
        headers={"api-key": "secret"},
        traces_headers={"x-tenant": "acme"},
    )
    transport = HTTPTransport(cfg, JSONEncoder(), session=session)
    await transport.send(SignalKind.TRACES, b"{}")
    await transport.send(SignalKind.METRICS, b"{}")
    traces_headers = rec.requests[0][1]
    metrics_headers = rec.requests[1][1]
    assert traces_headers["x-tenant"] == "acme"
    assert "api-key" not in traces_headers
    assert metrics_headers["api-key"] == "secret"


async def test_per_signal_compression_applies_to_that_signal_only(
    server_factory: ServerFactory,
) -> None:
    rec = Recorder()
    base, session = await server_factory(rec)
    cfg = OTLPConfig(endpoint=base, logs_compression=Compression.GZIP)
    transport = HTTPTransport(cfg, JSONEncoder(), session=session)
    await transport.send(SignalKind.LOGS, b'{"resourceLogs":[]}')
    await transport.send(SignalKind.METRICS, b'{"resourceMetrics":[]}')
    logs_headers, logs_body = rec.requests[0][1], rec.requests[0][2]
    metrics_headers = rec.requests[1][1]
    assert logs_headers["Content-Encoding"] == "gzip"
    assert gzip.decompress(logs_body) == b'{"resourceLogs":[]}'
    assert "Content-Encoding" not in metrics_headers


async def test_per_signal_timeout_is_used_for_that_signal(
    server_factory: ServerFactory,
) -> None:
    # White-box on purpose: asserting a real timeout would need a deliberately
    # slow server, and the behaviour worth pinning here is that the transport
    # builds one ClientTimeout per signal from timeout_for.
    rec = Recorder()
    base, session = await server_factory(rec)
    cfg = OTLPConfig(endpoint=base, timeout=10.0, metrics_timeout=2.5)
    transport = HTTPTransport(cfg, JSONEncoder(), session=session)
    assert transport._timeouts[SignalKind.METRICS].total == 2.5
    assert transport._timeouts[SignalKind.LOGS].total == 10.0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_transport_http.py -q -k per_signal`
Expected: FAIL. The headers test fails because `api-key` is still present on the traces
request; the compression test fails with `KeyError: 'Content-Encoding'`; the timeout test
fails with `AttributeError: 'HTTPTransport' object has no attribute '_timeouts'`.

- [ ] **Step 3: Build one timeout per signal**

In `HTTPTransport.__init__`, replace:

```python
        self._timeout = aiohttp.ClientTimeout(total=config.timeout)
```

with:

```python
        self._timeouts = {
            kind: aiohttp.ClientTimeout(total=config.timeout_for(kind)) for kind in SignalKind
        }
```

- [ ] **Step 4: Resolve per signal in `send`**

In `HTTPTransport.send`, change the first three lines of the body from:

```python
        headers = {**self._config.headers, "Content-Type": self._encoder.content_type}
        body = payload
        if self._config.compression is Compression.GZIP:
```

to:

```python
        headers = {**self._config.headers_for(kind), "Content-Type": self._encoder.content_type}
        body = payload
        if self._config.compression_for(kind) is Compression.GZIP:
```

and change the `timeout=` argument of `self._session.post(...)` from `timeout=self._timeout`
to `timeout=self._timeouts[kind]`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_transport_http.py -q`
Expected: PASS. `test_custom_headers_are_sent` and
`test_gzip_sets_content_encoding_and_compresses`, both pre-existing, must still pass — they
are the guard that the general-value path still works.

- [ ] **Step 6: Run the full suite and the checks**

Run: `uv run pytest -q && uv run ruff check . && uv run ruff format --check . && uv run mypy`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/otlp_client/transport/http.py tests/test_transport_http.py
git commit -m "feat(http): send per-signal headers, compression and timeout

Resolves all three through the config per request, and precomputes one
ClientTimeout per signal in __init__ rather than rebuilding one per call."
```

---

### Task 6: gRPC transport reads the per-signal values

**Files:**
- Modify: `src/otlp_client/transport/grpc.py` (`send`)
- Test: `tests/test_transport_grpc.py` (extend `EchoHandler`, add tests)

**Interfaces:**
- Consumes: `headers_for`, `timeout_for`, `compression_for` from Task 1.
- Produces: `EchoHandler.metadata: list[tuple[str, str]]` in the test helper.

- [ ] **Step 1: Record metadata in the test handler**

In `tests/test_transport_grpc.py`, in `EchoHandler.__init__`, after the existing
`self.received` line, add:

```python
        self.metadata: list[tuple[str, str]] = []
```

and in the nested `handle` function, immediately before the existing
`self.received.append(...)` line, add:

```python
            self.metadata.extend((key, value) for key, value in context.invocation_metadata())
```

Append a new field rather than changing the shape of `self.received`, so the existing tests
that unpack it keep working.

- [ ] **Step 2: Write the failing tests**

Append to `tests/test_transport_grpc.py`:

```python
async def test_per_signal_headers_replace_the_general_ones_over_grpc(
    grpc_server: ServerFactory,
) -> None:
    handler = EchoHandler()
    target = await grpc_server(handler)
    config = OTLPConfig(
        endpoint=f"http://{target}",
        protocol=OTLPProtocol.GRPC,
        headers={"api-key": "secret"},
        metrics_headers={"x-tenant": "acme"},
    )
    transport = await GRPCTransport.create(config, build_protobuf_encoder())
    await transport.send(SignalKind.METRICS, b"")
    await transport.aclose()
    sent = dict(handler.metadata)
    assert sent["x-tenant"] == "acme"
    assert "api-key" not in sent


async def test_general_headers_still_reach_a_signal_without_an_override(
    grpc_server: ServerFactory,
) -> None:
    handler = EchoHandler()
    target = await grpc_server(handler)
    config = OTLPConfig(
        endpoint=f"http://{target}",
        protocol=OTLPProtocol.GRPC,
        headers={"api-key": "secret"},
        metrics_headers={"x-tenant": "acme"},
    )
    transport = await GRPCTransport.create(config, build_protobuf_encoder())
    await transport.send(SignalKind.LOGS, b"")
    await transport.aclose()
    assert dict(handler.metadata)["api-key"] == "secret"
```

Also append this round-trip test, mirroring the existing
`test_gzip_compression_round_trips_successfully`:

```python
async def test_per_signal_compression_round_trips_over_grpc(
    grpc_server: ServerFactory,
) -> None:
    # As with the general-compression test, a well-behaved server makes this a
    # wiring check rather than a compression assertion: the payload must still
    # arrive and still decode when compression is resolved per signal.
    handler = EchoHandler()
    target = await grpc_server(handler)
    config = OTLPConfig(
        endpoint=f"http://{target}",
        protocol=OTLPProtocol.GRPC,
        metrics_compression=Compression.GZIP,
    )
    transport = await GRPCTransport.create(config, build_protobuf_encoder())
    result = await transport.send(SignalKind.METRICS, b"payload-bytes")
    assert isinstance(result, Success)
    assert handler.received == [(METRICS_METHOD, b"payload-bytes")]
    await transport.aclose()
```

Per-signal *timeout* over gRPC is deliberately not asserted at this level:
proving it would need a deliberately slow server, and the resolution itself is
already pinned by `timeout_for` in Task 1. The line change in step 4 is covered
by review rather than by a test that would assert nothing.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/test_transport_grpc.py -q -k per_signal_headers_replace`
Expected: FAIL with `AssertionError` on `"api-key" not in sent` — the transport still sends
the general headers for every signal.

- [ ] **Step 4: Resolve per signal in `send`**

In `GRPCTransport.send`, replace:

```python
metadata = tuple(self._config.headers.items())
compression = grpc.Compression.Gzip if self._config.compression is Compression.GZIP else None
```

with:

```python
metadata = tuple(self._config.headers_for(kind).items())
compression = (
    grpc.Compression.Gzip if self._config.compression_for(kind) is Compression.GZIP else None
)
```

and change the `timeout=` argument of the `await call(...)` from
`timeout=self._config.timeout` to `timeout=self._config.timeout_for(kind)`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/test_transport_grpc.py -q`
Expected: PASS. `test_gzip_compression_round_trips_successfully` and the metadata-carrying
tests that already existed must still pass.

- [ ] **Step 6: Run the full suite and the checks**

Run: `uv run pytest -q && uv run ruff check . && uv run ruff format --check . && uv run mypy`
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/otlp_client/transport/grpc.py tests/test_transport_grpc.py
git commit -m "feat(grpc): send per-signal headers, compression and timeout

All three are per-call on a gRPC channel, so the channel and its
credentials are untouched."
```

---

### Task 7: Documentation

**Files:**
- Modify: `README.md` (after the TLS section, before `## Scope`)
- Modify: `docs/auth-audit.md` (finding 2, and the conformance table)

**Interfaces:** none.

- [ ] **Step 1: Add the README section**

Insert immediately before the `## Scope` heading in `README.md`:

```markdown
## Per-signal configuration

Headers, timeout and compression can be set per signal, via either the config
fields or the `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_*` environment
variables.

**A per-signal value replaces the general one — it does not merge with it.**
This follows the OTLP spec, and matches the other SDKs, but it is easy to get
caught by:

```bash
export OTEL_EXPORTER_OTLP_HEADERS=api-key=secret
export OTEL_EXPORTER_OTLP_TRACES_HEADERS=x-tenant=acme
```

Traces are now sent with **only** `x-tenant` — the shared `api-key` is not
included. Repeat any header the signal still needs:

```bash
export OTEL_EXPORTER_OTLP_TRACES_HEADERS=api-key=secret,x-tenant=acme
```

Setting a per-signal headers variable to the empty string sends no headers at
all for that signal, which is distinct from leaving it unset.

`PROTOCOL`, `INSECURE`, `CERTIFICATE`, `CLIENT_KEY` and `CLIENT_CERTIFICATE`
cannot vary per signal: one `OTLPClient` holds a single encoder and transport,
and those settings choose them or configure the connection itself. `from_env()`
raises `OTLPConfigError` on the per-signal forms rather than ignoring them.
Use one client per signal if you need them to differ.
```

- [ ] **Step 2: Update the audit doc conformance table**

In `docs/auth-audit.md`, change the per-signal row of the conformance table from:

```markdown
| Per-signal variants of all five | `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_*` | **Missing** |
```

to:

```markdown
| Per-signal `HEADERS` / `TIMEOUT` / `COMPRESSION` | `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_*` | Supported (2026-09-05) |
| Per-signal `PROTOCOL` / `INSECURE` / TLS settings | `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_*` | Rejected by design |
```

- [ ] **Step 3: Mark finding 2 resolved**

In `docs/auth-audit.md`, change the finding 2 heading from:

```markdown
### 2. No per-signal configuration variants
```

to:

```markdown
### 2. No per-signal configuration variants — RESOLVED 2026-09-05
```

and insert this paragraph directly beneath that heading, above the existing text:

```markdown
**Resolved 2026-09-05.** Headers, timeout and compression are now per-signal,
replacing rather than merging with the general value. Protocol, `insecure` and
the TLS settings stay general-only and their per-signal forms raise — see
"Intentional deviations" below. The paragraphs below describe the original gap.
```

- [ ] **Step 4: Record the certificate decision**

In `docs/auth-audit.md`, append to the "Intentional deviations" section:

```markdown
### Per-signal protocol and TLS settings are rejected, not honoured

One `OTLPClient` holds one encoder and one transport, both fixed at `create()`.
`PROTOCOL` selects both, and `INSECURE`, `CERTIFICATE`, `CLIENT_KEY` and
`CLIENT_CERTIFICATE` configure the connection, so none can vary per signal
within a client.

Per-signal certificates were considered for OTLP/HTTP alone, where aiohttp
accepts `ssl=` per request. They were rejected: nine more config fields, and a
field that silently behaves differently depending on the protocol. One clear
rule — per-request options vary per signal, connection-level options do not —
beats partial conformance with an asymmetry.

`from_env()` raises on these rather than ignoring them, because a silently
dropped per-signal protocol sends that signal in the wrong wire format and a
silently dropped certificate is a security surprise.
```

- [ ] **Step 5: Verify the docs did not break anything**

Run: `uv run pytest -q && uv run ruff format --check .`
Expected: all pass. `ruff-format` formats Python blocks inside markdown in this repo, so a
fenced ```python block in a doc must already be ruff-formatted; the bash blocks above are
unaffected.

- [ ] **Step 6: Commit**

```bash
git add README.md docs/auth-audit.md
git commit -m "docs: per-signal configuration and its replace semantics

States the replace-not-merge behaviour with the dropped-credential footgun
called out, and records why the connection-level options are rejected rather
than honoured. Closes finding 2 of the auth audit."
```

---

## Done when

- `uv run pytest -q` passes with roughly 285 tests (254 before this work, plus about 31 new).
- `uv run ruff check .`, `uv run ruff format --check .` and `uv run mypy` are all clean.
- `OTEL_EXPORTER_OTLP_TRACES_HEADERS` replaces `OTEL_EXPORTER_OTLP_HEADERS` for traces only.
- Every `OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_{PROTOCOL,INSECURE,CERTIFICATE,CLIENT_KEY,CLIENT_CERTIFICATE}`
  raises `OTLPConfigError` naming the variable.
- The README documents replace semantics and the footgun.
