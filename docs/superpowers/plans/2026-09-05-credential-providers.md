# Dynamic Credential Providers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a rotating credential be supplied per request by an awaitable provider, instead of being frozen into `OTLPConfig` at construction.

**Architecture:** `Transport.send` gains a `headers` parameter so the client — not the transports — resolves credentials; `OTLPClient` merges an optional `CredentialProvider`'s headers over `config.headers_for(kind)` and grants one re-auth attempt per export on a 401/403. Three helpers ship in a new `credentials.py`: `BearerToken`, `BasicAuth`, and `OAuth2ClientCredentials` with cached, single-flight refresh.

**Tech Stack:** Python 3.12+, aiohttp (sole core dependency), pytest + pytest-asyncio (`asyncio_mode = "auto"`), mypy strict, ruff.

**Spec:** `docs/superpowers/specs/2026-09-05-credential-providers-design.md`

## Global Constraints

- **Core install is aiohttp-only.** `tests/test_core_only.py` forbids module-level imports of `grpc`, `google.protobuf`, `opentelemetry` anywhere under `src/otlp_client/`. `aiohttp` is permitted but must stay lazily imported (inside functions/methods) so `import otlp_client` does not pull it in — verified true today and locked in by Task 8.
- **mypy strict** over `src` and `tests`. No `Any` escape hatches, no untyped defs.
- **ruff**, line-length 100. Pre-commit runs `ruff format` and will reformat; commit the reformatted result.
- **pytest** runs with `asyncio_mode = "auto"` — async tests need no decorator. `addopts = "-m 'not integration'"`.
- **Secrets never appear** in exception messages or log lines: no raw response bodies, no client secret, no token value.
- **Every command below runs from the repo root** using the project venv: `.venv/bin/python -m pytest ...`, or `make test` where available.
- **Commit after every task.** Do not add Claude attribution lines to commit messages.
- Branch: `credential-providers` (already created; the spec commit is on it).

---

### Task 1: Thread headers through the Transport seam

Pure refactor, no behaviour change. `Transport.send` takes resolved headers; the client resolves them. Nothing about credentials yet.

**Files:**
- Modify: `src/otlp_client/transport/base.py:14`
- Modify: `src/otlp_client/transport/http.py:84-98`
- Modify: `src/otlp_client/transport/grpc.py:119-128`
- Modify: `src/otlp_client/client.py:110-111`
- Modify: `tests/support/fakes.py:25,47`
- Test: `tests/test_client.py`, `tests/test_transport_http.py`, `tests/test_transport_grpc.py`, `tests/test_fakes.py`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: `Transport.send(self, kind: SignalKind, payload: bytes, headers: Mapping[str, str]) -> ExportOutcome` — a required third positional parameter on both real transports and both fakes. `FakeTransport.sent` becomes `list[tuple[SignalKind, bytes, Mapping[str, str]]]`.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_client.py`:

```python
async def test_client_resolves_per_signal_headers_and_passes_them_to_the_transport() -> None:
    config = OTLPConfig(
        endpoint="http://localhost:4318",
        headers={"api-key": "secret"},
        traces_headers={"x-tenant": "acme"},
    )
    transport = FakeTransport()
    client = OTLPClient(config, transport=transport, encoder=JSONEncoder())
    await client.export_traces(
        [
            span(
                "s",
                trace_id=b"\x01" * 16,
                span_id=b"\x02" * 8,
                start_time_unix_nano=1,
                end_time_unix_nano=2,
            )
        ]
    )
    await client.export_metrics([gauge("t", 1.0, time_unix_nano=1)])
    assert transport.sent[0][2] == {"x-tenant": "acme"}
    assert transport.sent[1][2] == {"api-key": "secret"}
```

Add `from otlp_client.model.traces import span` to that file's imports if absent.

- [ ] **Step 2: Run it and watch it fail**

Run: `.venv/bin/python -m pytest tests/test_client.py::test_client_resolves_per_signal_headers_and_passes_them_to_the_transport -v`
Expected: FAIL — `IndexError` or a tuple of length 2, because `FakeTransport.sent` records only `(kind, payload)`.

- [ ] **Step 3: Widen the Transport protocol**

`src/otlp_client/transport/base.py` — replace the `send` signature and add the `Mapping` import:

```python
from collections.abc import Mapping


class Transport(Protocol):
    """Ships already-encoded bytes and classifies the response."""

    async def send(
        self, kind: SignalKind, payload: bytes, headers: Mapping[str, str]
    ) -> ExportOutcome:
        """Deliver one encoded batch with the headers the client resolved.

        Never retries; classification only. The caller owns header resolution,
        so a transport adds only what the wire format itself requires.
        """

    async def aclose(self) -> None:
        """Release any resources this transport owns."""
```

- [ ] **Step 4: Update `HTTPTransport.send`**

In `src/otlp_client/transport/http.py`, change the signature and the first line of the body. Everything else in the method is untouched:

```python
async def send(self, kind: SignalKind, payload: bytes, headers: Mapping[str, str]) -> ExportOutcome:
    request_headers = {**headers, "Content-Type": self._encoder.content_type}
    body = payload
    if self._config.compression_for(kind) is Compression.GZIP:
        body = await self._compress(payload)
        request_headers["Content-Encoding"] = "gzip"
```

Then rename the two later uses inside the `session.post(...)` call: `headers=request_headers`. Add `from collections.abc import Mapping` to the imports.

- [ ] **Step 5: Update `GRPCTransport.send`**

In `src/otlp_client/transport/grpc.py`, change the signature and the metadata line:

```python
    async def send(
        self, kind: SignalKind, payload: bytes, headers: Mapping[str, str]
    ) -> ExportOutcome:
```

```python
        metadata = tuple(headers.items())
```

Add `from collections.abc import Mapping` to the imports.

- [ ] **Step 6: Update the client**

In `src/otlp_client/client.py`, inside `_export`:

```python
        async def attempt() -> ExportOutcome:
            return await self._transport.send(kind, payload, self._config.headers_for(kind))
```

- [ ] **Step 7: Update the fakes**

In `tests/support/fakes.py`, both doubles record the headers they were given:

```python
class FakeTransport:
    def __init__(self, outcomes: Sequence[ExportOutcome] | None = None) -> None:
        self._outcomes = list(outcomes) if outcomes else [Success()]
        self._index = 0
        self.sent: list[tuple[SignalKind, bytes, Mapping[str, str]]] = []
        self.closed = False

    async def send(
        self, kind: SignalKind, payload: bytes, headers: Mapping[str, str]
    ) -> ExportOutcome:
        self.sent.append((kind, payload, dict(headers)))
        outcome = self._outcomes[min(self._index, len(self._outcomes) - 1)]
        self._index += 1
        return outcome
```

```python
class HangingTransport:
    def __init__(self) -> None:
        self.gate = asyncio.Event()
        self.sent: list[tuple[SignalKind, bytes, Mapping[str, str]]] = []

    async def send(
        self, kind: SignalKind, payload: bytes, headers: Mapping[str, str]
    ) -> ExportOutcome:
        self.sent.append((kind, payload, dict(headers)))
        await self.gate.wait()
        return Success()
```

Add `from collections.abc import Mapping, Sequence` to that file's imports.

- [ ] **Step 8: Migrate every existing `.send(` call site in the tests**

Add a third argument `{}` to each direct transport call in `tests/test_transport_http.py`, `tests/test_transport_grpc.py`, and `tests/test_fakes.py`. Example:

```python
    result = await transport.send(SignalKind.METRICS, b'{"resourceMetrics":[]}', {})
```

Find them all with:

```bash
grep -rn "\.send(" tests --include='*.py' | grep -v session.post
```

Any test that unpacks `FakeTransport.sent` as a 2-tuple (e.g. `(kind, payload) = transport.sent[0]` in `tests/test_client.py`) must unpack three values or index explicitly.

- [ ] **Step 9: Rewrite the two per-signal header transport tests**

Their subject moved: resolution is now the client's job, so these must assert only that the transport sends what it was handed. In `tests/test_transport_http.py`, replace `test_per_signal_headers_replace_the_general_ones` with:

```python
async def test_sends_exactly_the_headers_it_was_given(
    server_factory: ServerFactory,
) -> None:
    # Header resolution moved to the client; this asserts only that the
    # transport transmits what it is handed. Resolution itself is covered by
    # the headers_for tests in test_config.py.
    rec = Recorder()
    base, session = await server_factory(rec)
    transport = HTTPTransport(OTLPConfig(endpoint=base), JSONEncoder(), session=session)
    await transport.send(SignalKind.TRACES, b"{}", {"x-tenant": "acme"})
    sent = rec.requests[0][1]
    assert sent["x-tenant"] == "acme"
    assert "api-key" not in sent
```

In `tests/test_transport_grpc.py`, replace `test_per_signal_headers_replace_the_general_ones_over_grpc` and `test_general_headers_still_reach_a_signal_without_an_override` with one test:

```python
async def test_sends_exactly_the_metadata_it_was_given(
    grpc_server: ServerFactory,
) -> None:
    handler = EchoHandler()
    target = await grpc_server(handler)
    config = OTLPConfig(endpoint=f"http://{target}", protocol=OTLPProtocol.GRPC)
    transport = await GRPCTransport.create(config, build_protobuf_encoder())
    await transport.send(SignalKind.METRICS, b"", {"x-tenant": "acme"})
    await transport.aclose()
    sent = dict(handler.metadata)
    assert sent["x-tenant"] == "acme"
    assert "api-key" not in sent
```

- [ ] **Step 10: Run the whole suite**

Run: `.venv/bin/python -m pytest -q`
Expected: PASS, including the new client test from Step 1.

- [ ] **Step 11: Typecheck and lint**

Run: `.venv/bin/python -m mypy && .venv/bin/python -m ruff check .`
Expected: no errors.

- [ ] **Step 12: Commit**

```bash
git add src/otlp_client/transport src/otlp_client/client.py tests/
git commit -m "refactor: resolve export headers in the client, not the transports

Transport.send now takes the resolved headers. Breaking for custom
transports; required so credential resolution lives in one place."
```

---

### Task 2: Detect credential rejection uniformly

**Files:**
- Modify: `src/otlp_client/outcomes.py`
- Modify: `src/otlp_client/transport/grpc.py:105-117`
- Create: `tests/test_outcomes.py`
- Test: `tests/test_transport_grpc.py`

**Interfaces:**
- Consumes: Task 1's `send(kind, payload, headers)`.
- Produces: `otlp_client.outcomes.is_credential_rejection(outcome: ExportOutcome) -> bool`, true for `Permanent` with `status` 401 or 403. `GRPCTransport._classify` sets `status=401` for `UNAUTHENTICATED` and `status=403` for `PERMISSION_DENIED`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_outcomes.py`:

```python
from __future__ import annotations

import pytest

from otlp_client.outcomes import (
    ExportOutcome,
    PartialSuccess,
    Permanent,
    Retryable,
    Success,
    is_credential_rejection,
)


@pytest.mark.parametrize("status", [401, 403])
def test_a_permanent_auth_status_is_a_credential_rejection(status: int) -> None:
    assert is_credential_rejection(Permanent(status=status, message="nope"))


@pytest.mark.parametrize(
    "outcome",
    [
        Permanent(status=400, message="bad payload"),
        Permanent(status=None, message="unclassified"),
        Retryable(status=401, message="not permanent"),
        Success(),
        PartialSuccess(rejected=1),
    ],
)
def test_everything_else_is_not_a_credential_rejection(outcome: ExportOutcome) -> None:
    assert not is_credential_rejection(outcome)
```

Add to `tests/test_transport_grpc.py`:

```python
@pytest.mark.parametrize(
    ("code", "expected_status"),
    [
        (grpc.StatusCode.UNAUTHENTICATED, 401),
        (grpc.StatusCode.PERMISSION_DENIED, 403),
    ],
)
async def test_auth_failures_carry_the_matching_http_status(
    grpc_server: ServerFactory, code: grpc.StatusCode, expected_status: int
) -> None:
    # The status field lets the client's one credential-rejection predicate
    # serve both transports without a gRPC-shaped special case.
    handler = EchoHandler(code=code)
    target = await grpc_server(handler)
    config = OTLPConfig(endpoint=f"http://{target}", protocol=OTLPProtocol.GRPC)
    transport = await GRPCTransport.create(config, build_protobuf_encoder())
    result = await transport.send(SignalKind.METRICS, b"x", {})
    await transport.aclose()
    assert isinstance(result, Permanent)
    assert result.status == expected_status
```

- [ ] **Step 2: Run them and watch them fail**

Run: `.venv/bin/python -m pytest tests/test_outcomes.py tests/test_transport_grpc.py -k "credential_rejection or matching_http_status" -v`
Expected: FAIL — `ImportError: cannot import name 'is_credential_rejection'`, and the gRPC test asserting `status == 401` against `None`.

- [ ] **Step 3: Add the predicate**

Append to `src/otlp_client/outcomes.py`:

```python
def is_credential_rejection(outcome: ExportOutcome) -> bool:
    """True when the collector refused the credentials rather than the payload.

    HTTP reports this as 401/403 directly; the gRPC transport maps
    UNAUTHENTICATED and PERMISSION_DENIED onto the same statuses so one
    predicate serves both.
    """
    return isinstance(outcome, Permanent) and outcome.status in (401, 403)
```

- [ ] **Step 4: Map the gRPC codes**

In `src/otlp_client/transport/grpc.py`, replace the body of `_classify`:

```python
    def _classify(self, exc: Any) -> ExportOutcome:
        import grpc

        retryable = {
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
        }
        # gRPC has no HTTP status, but mapping the two auth codes onto their
        # HTTP equivalents lets one predicate cover both transports.
        auth_statuses = {
            grpc.StatusCode.UNAUTHENTICATED: 401,
            grpc.StatusCode.PERMISSION_DENIED: 403,
        }
        code = exc.code()
        message = exc.details() or str(code)
        if code in retryable:
            return Retryable(message=message, retry_after=_pushback(exc))
        return Permanent(status=auth_statuses.get(code), message=message)
```

- [ ] **Step 5: Run the tests**

Run: `.venv/bin/python -m pytest tests/test_outcomes.py tests/test_transport_grpc.py -q`
Expected: PASS.

- [ ] **Step 6: Typecheck, lint, commit**

```bash
.venv/bin/python -m mypy && .venv/bin/python -m ruff check .
git add src/otlp_client/outcomes.py src/otlp_client/transport/grpc.py tests/test_outcomes.py tests/test_transport_grpc.py
git commit -m "feat: classify credential rejections uniformly across transports"
```

---

### Task 3: The CredentialProvider protocol and client wiring

**Files:**
- Create: `src/otlp_client/credentials.py`
- Modify: `src/otlp_client/client.py:54-99,105-125`
- Modify: `tests/support/fakes.py`
- Create: `tests/test_client_credentials.py`

**Interfaces:**
- Consumes: `is_credential_rejection` (Task 2); `Transport.send(kind, payload, headers)` (Task 1).
- Produces:
  - `otlp_client.credentials.CredentialProvider` — a `@runtime_checkable` Protocol with `async def headers(self, kind: SignalKind) -> Mapping[str, str]` and `async def invalidate(self, kind: SignalKind) -> None`.
  - `OTLPClient.__init__(..., credentials: CredentialProvider | None = None)` and `OTLPClient.create(..., credentials: CredentialProvider | None = None)`.
  - `tests.support.fakes.FakeCredentials(headers_by_call: Sequence[Mapping[str, str]])` with attributes `.calls: list[SignalKind]` and `.invalidated: list[SignalKind]`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_client_credentials.py`:

```python
from __future__ import annotations

from collections.abc import Mapping, Sequence

import pytest

from otlp_client.client import OTLPClient
from otlp_client.config import OTLPConfig
from otlp_client.encoding.json import JSONEncoder
from otlp_client.errors import OTLPPermanentError, OTLPTransportError
from otlp_client.model.metrics import gauge
from otlp_client.credentials import CredentialProvider
from otlp_client.outcomes import Permanent, Retryable, Success
from otlp_client.signals import SignalKind
from tests.support.fakes import FakeClock, FakeCredentials, FakeTransport

CONFIG = OTLPConfig(endpoint="http://localhost:4318", headers={"x-tenant": "acme"})
METRIC = [gauge("t", 1.0, time_unix_nano=1)]


def make_client(
    transport: FakeTransport, credentials: CredentialProvider | None = None
) -> OTLPClient:
    # FakeClock keeps the retry backoff instant while still advancing the
    # budget, so a test that exercises retries costs no wall-clock time.
    clock = FakeClock()
    return OTLPClient(
        CONFIG,
        transport=transport,
        encoder=JSONEncoder(),
        credentials=credentials,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )


async def test_provider_headers_merge_over_static_ones() -> None:
    creds = FakeCredentials([{"authorization": "Bearer one"}])
    transport = FakeTransport()
    await make_client(transport, creds).export_metrics(METRIC)
    assert transport.sent[0][2] == {"x-tenant": "acme", "authorization": "Bearer one"}


async def test_provider_wins_a_key_collision() -> None:
    config = OTLPConfig(endpoint="http://localhost:4318", headers={"authorization": "stale"})
    creds = FakeCredentials([{"authorization": "Bearer fresh"}])
    transport = FakeTransport()
    client = OTLPClient(config, transport=transport, encoder=JSONEncoder(), credentials=creds)
    await client.export_metrics(METRIC)
    assert transport.sent[0][2]["authorization"] == "Bearer fresh"


async def test_provider_sees_the_signal_kind() -> None:
    creds = FakeCredentials([{"a": "1"}])
    await make_client(FakeTransport(), creds).export_metrics(METRIC)
    assert creds.calls == [SignalKind.METRICS]


async def test_a_rejection_invalidates_and_resends_once() -> None:
    creds = FakeCredentials([{"authorization": "stale"}, {"authorization": "fresh"}])
    transport = FakeTransport([Permanent(status=401, message="expired"), Success()])
    result = await make_client(transport, creds).export_metrics(METRIC)
    assert isinstance(result, Success)
    assert creds.invalidated == [SignalKind.METRICS]
    assert [sent[2]["authorization"] for sent in transport.sent] == ["stale", "fresh"]


async def test_a_second_rejection_is_permanent_with_only_one_invalidate() -> None:
    creds = FakeCredentials([{"authorization": "stale"}, {"authorization": "also stale"}])
    transport = FakeTransport([Permanent(status=401, message="expired")])
    with pytest.raises(OTLPPermanentError):
        await make_client(transport, creds).export_metrics(METRIC)
    assert creds.invalidated == [SignalKind.METRICS]
    assert len(transport.sent) == 2


async def test_the_reauth_budget_is_one_per_export_not_per_attempt() -> None:
    # 503 -> retry -> 401 -> one re-auth -> 401 again. Still one invalidate for
    # the whole export, proving the budget is scoped to _export.
    creds = FakeCredentials([{"authorization": "t"}])
    transport = FakeTransport(
        [
            Retryable(status=503, message="later"),
            Permanent(status=401, message="expired"),
            Permanent(status=401, message="expired"),
        ]
    )
    with pytest.raises(OTLPPermanentError):
        await make_client(transport, creds).export_metrics(METRIC)
    assert creds.invalidated == [SignalKind.METRICS]


async def test_a_rejection_without_a_provider_stays_permanent() -> None:
    transport = FakeTransport([Permanent(status=401, message="expired")])
    with pytest.raises(OTLPPermanentError):
        await make_client(transport).export_metrics(METRIC)
    assert len(transport.sent) == 1


async def test_a_permanent_provider_error_fails_fast() -> None:
    creds = FakeCredentials([], error=OTLPPermanentError("invalid_client"))
    transport = FakeTransport()
    with pytest.raises(OTLPPermanentError, match="invalid_client"):
        await make_client(transport, creds).export_metrics(METRIC)
    assert len(creds.calls) == 1  # a rejected secret is not retried
    assert transport.sent == []  # nothing reached the wire


async def test_a_transport_provider_error_rides_the_retry_budget() -> None:
    creds = FakeCredentials([], error=OTLPTransportError("token endpoint down"))
    transport = FakeTransport()
    with pytest.raises(OTLPTransportError):
        await make_client(transport, creds).export_metrics(METRIC)
    assert len(creds.calls) > 1  # retried rather than surfaced on first failure
    assert transport.sent == []


async def test_an_unexpected_provider_error_propagates_uncaught() -> None:
    creds = FakeCredentials([], error=KeyError("bug in the provider"))
    with pytest.raises(KeyError):
        await make_client(FakeTransport(), creds).export_metrics(METRIC)
```

Add `FakeCredentials` to `tests/support/fakes.py`:

```python
class FakeCredentials:
    """A CredentialProvider double. Replays scripted header maps.

    Once the script is exhausted the last map repeats, matching FakeTransport.
    `error`, when set, is raised from `headers()` instead.
    """

    def __init__(
        self,
        headers_by_call: Sequence[Mapping[str, str]],
        error: BaseException | None = None,
    ) -> None:
        self._headers = [dict(h) for h in headers_by_call] or [{}]
        self._index = 0
        self._error = error
        self.calls: list[SignalKind] = []
        self.invalidated: list[SignalKind] = []

    async def headers(self, kind: SignalKind) -> Mapping[str, str]:
        self.calls.append(kind)
        if self._error is not None:
            raise self._error
        headers = self._headers[min(self._index, len(self._headers) - 1)]
        self._index += 1
        return headers

    async def invalidate(self, kind: SignalKind) -> None:
        self.invalidated.append(kind)
```

- [ ] **Step 2: Run them and watch them fail**

Run: `.venv/bin/python -m pytest tests/test_client_credentials.py -q`
Expected: FAIL — `ImportError` for `FakeCredentials`, then `TypeError: __init__() got an unexpected keyword argument 'credentials'`.

- [ ] **Step 3: Write the protocol module**

Create `src/otlp_client/credentials.py`:

```python
"""Credentials resolved per request, rather than frozen into OTLPConfig.

The OTLP specification defines no authentication concept, so nothing here is
conformance work: a provider is consulted on every export attempt, which is
what makes a rotating token possible without rebuilding the client.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, runtime_checkable

from otlp_client.signals import SignalKind


@runtime_checkable
class CredentialProvider(Protocol):
    """Supplies credential headers for one export attempt.

    Both methods take the signal so a provider can hold a different credential
    per signal. The shipped helpers ignore it.
    """

    async def headers(self, kind: SignalKind) -> Mapping[str, str]:
        """Return the credential headers for this attempt.

        Merged over the configured static headers, winning any key collision.
        Awaited on every attempt, so an implementation that caches owns its own
        expiry.
        """
        ...

    async def invalidate(self, kind: SignalKind) -> None:
        """The collector rejected what `headers()` last returned.

        Drop any cached credential so the next `headers()` call mints a fresh
        one. A stateless provider implements this as `pass`.
        """
        ...
```

- [ ] **Step 4: Wire the client**

In `src/otlp_client/client.py`, add the import:

```python
from otlp_client.credentials import CredentialProvider
from otlp_client.outcomes import (
    ExportOutcome,
    PartialSuccess,
    Permanent,
    Retryable,
    Success,
    is_credential_rejection,
)
```

Add the parameter to `__init__` (after `encoder`, keyword-only like its neighbours) and store it:

```python
credentials: CredentialProvider | None = (None,)
```

```python
        self._credentials = credentials
```

Add it to `create()` too, and pass it through:

```python
    @classmethod
    async def create(
        cls,
        config: OTLPConfig,
        *,
        session: object | None = None,
        scope: InstrumentationScope | None = None,
        credentials: CredentialProvider | None = None,
    ) -> OTLPClient:
```

```python
return cls(config, transport=transport, encoder=encoder, scope=scope, credentials=credentials)
```

Add the header resolver as a method:

```python
    async def _headers(self, kind: SignalKind) -> Mapping[str, str]:
        """Static headers for the signal, with the provider's layered on top.

        The provider wins a key collision: a rotating `authorization` must be
        able to replace a stale configured one, while configured headers that
        the provider says nothing about survive.
        """
        static = self._config.headers_for(kind)
        if self._credentials is None:
            return static
        return {**static, **await self._credentials.headers(kind)}
```

Add `from collections.abc import Awaitable, Callable, Mapping, Sequence` to the imports.

Replace `attempt()` in `_export`:

```python
reauthed = False


async def attempt() -> ExportOutcome:
    nonlocal reauthed
    outcome = await self._transport.send(kind, payload, await self._headers(kind))
    if not reauthed and self._credentials is not None and is_credential_rejection(outcome):
        # One re-auth per export, outside the retry budget and without
        # backoff: covers a token revoked or expired between mint and
        # arrival, without turning a wrong secret into a token-endpoint
        # storm.
        reauthed = True
        await self._credentials.invalidate(kind)
        outcome = await self._transport.send(kind, payload, await self._headers(kind))
    return outcome
```

- [ ] **Step 5: Map provider exceptions**

A provider's errors must become *outcomes*, not escape the loop — that is what
lets a transport-shaped failure ride the existing backoff. Wrap the body of
`attempt()` (the `with_retry` call below it is left exactly as it is):

```python
async def attempt() -> ExportOutcome:
    nonlocal reauthed
    try:
        outcome = await self._transport.send(kind, payload, await self._headers(kind))
        if not reauthed and self._credentials is not None and is_credential_rejection(outcome):
            # One re-auth per export, outside the retry budget and
            # without backoff: covers a token revoked or expired between
            # mint and arrival, without turning a wrong secret into a
            # token-endpoint storm.
            reauthed = True
            await self._credentials.invalidate(kind)
            outcome = await self._transport.send(kind, payload, await self._headers(kind))
        return outcome
    except OTLPPermanentError as exc:
        # Only a provider can raise these here; transports return
        # outcomes. A rejected client secret must fail fast.
        return Permanent(message=f"credential provider failed: {exc}")
    except OTLPTransportError as exc:
        return Retryable(message=f"credential provider failed: {exc}")
```

This replaces the `attempt()` from Step 4 wholesale — write Step 4's version
first, watch its tests pass, then widen it here.

Any other exception from the provider propagates untouched, which is deliberate: a bug in someone's provider must stay visible rather than being absorbed by the retry loop.

- [ ] **Step 6: Run the tests**

Run: `.venv/bin/python -m pytest tests/test_client_credentials.py -q`
Expected: PASS, all eleven.

- [ ] **Step 7: Run the whole suite, typecheck, lint**

Run: `.venv/bin/python -m pytest -q && .venv/bin/python -m mypy && .venv/bin/python -m ruff check .`
Expected: all green.

- [ ] **Step 8: Commit**

```bash
git add src/otlp_client/credentials.py src/otlp_client/client.py tests/support/fakes.py tests/test_client_credentials.py
git commit -m "feat: consult a credential provider per export attempt

Adds the CredentialProvider protocol and wires it into OTLPClient: provider
headers merge over static ones, and a 401/403 buys exactly one re-auth per
export, outside the retry budget."
```

---

### Task 4: BearerToken and BasicAuth helpers

**Files:**
- Modify: `src/otlp_client/credentials.py`
- Modify: `src/otlp_client/__init__.py`
- Create: `tests/test_credentials.py`

**Interfaces:**
- Consumes: `CredentialProvider` (Task 3).
- Produces: `BearerToken(source: str | Callable[[], Awaitable[str]])` and `BasicAuth(username: str, password: str)`, both satisfying `CredentialProvider`; both exported from `otlp_client`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_credentials.py`:

```python
from __future__ import annotations

import base64

from otlp_client.credentials import BasicAuth, BearerToken, CredentialProvider
from otlp_client.signals import SignalKind

METRICS = SignalKind.METRICS


async def test_a_static_bearer_token_becomes_an_authorization_header() -> None:
    provider = BearerToken("abc123")
    assert await provider.headers(METRICS) == {"authorization": "Bearer abc123"}


async def test_a_callable_source_is_consulted_on_every_call() -> None:
    tokens = iter(["one", "two"])

    async def source() -> str:
        return next(tokens)

    provider = BearerToken(source)
    assert await provider.headers(METRICS) == {"authorization": "Bearer one"}
    assert await provider.headers(METRICS) == {"authorization": "Bearer two"}


async def test_invalidating_a_bearer_token_is_harmless() -> None:
    provider = BearerToken("abc123")
    await provider.invalidate(METRICS)
    assert await provider.headers(METRICS) == {"authorization": "Bearer abc123"}


async def test_basic_auth_matches_the_rfc_7617_vector() -> None:
    # RFC 7617 section 2: "Aladdin:open sesame" -> QWxhZGRpbjpvcGVuIHNlc2FtZQ==
    provider = BasicAuth("Aladdin", "open sesame")
    headers = await provider.headers(METRICS)
    assert headers == {"authorization": "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="}


async def test_basic_auth_encodes_non_ascii_as_utf8() -> None:
    provider = BasicAuth("user", "pässwörd")
    (value,) = (await provider.headers(METRICS)).values()
    encoded = value.removeprefix("Basic ")
    assert base64.b64decode(encoded).decode("utf-8") == "user:pässwörd"


def test_the_helpers_satisfy_the_protocol() -> None:
    assert isinstance(BearerToken("t"), CredentialProvider)
    assert isinstance(BasicAuth("u", "p"), CredentialProvider)


def test_the_helpers_are_exported_from_the_package() -> None:
    import otlp_client

    for name in ("CredentialProvider", "BearerToken", "BasicAuth"):
        assert name in otlp_client.__all__
        assert hasattr(otlp_client, name)
```

- [ ] **Step 2: Run them and watch them fail**

Run: `.venv/bin/python -m pytest tests/test_credentials.py -q`
Expected: FAIL — `ImportError: cannot import name 'BasicAuth'`.

- [ ] **Step 3: Implement both helpers**

Append to `src/otlp_client/credentials.py` (and add `base64`, `Awaitable`, `Callable` to its imports):

```python
class BearerToken:
    """An `authorization: Bearer ...` header, static or fetched per attempt.

    A `str` source is a token you already hold. A callable source is consulted
    on every attempt and its result is never cached here -- the caching belongs
    to whoever owns the token, and a cache this class did not agree to would be
    exactly what makes a rotated token look stale.
    """

    def __init__(self, source: str | Callable[[], Awaitable[str]]) -> None:
        self._source = source

    async def headers(self, kind: SignalKind) -> Mapping[str, str]:
        token = self._source if isinstance(self._source, str) else await self._source()
        return {"authorization": f"Bearer {token}"}

    async def invalidate(self, kind: SignalKind) -> None:
        """No cache to drop: a callable source is re-consulted next call anyway."""


class BasicAuth:
    """An RFC 7617 `authorization: Basic ...` header.

    Static, but it keeps the secret out of the config and environment surface,
    where it would otherwise sit inside an OTEL_EXPORTER_OTLP_HEADERS string.
    """

    def __init__(self, username: str, password: str) -> None:
        raw = f"{username}:{password}".encode()
        self._header = f"Basic {base64.b64encode(raw).decode('ascii')}"

    async def headers(self, kind: SignalKind) -> Mapping[str, str]:
        return {"authorization": self._header}

    async def invalidate(self, kind: SignalKind) -> None:
        """Nothing is cached; the credential cannot go stale."""
```

- [ ] **Step 4: Export them**

In `src/otlp_client/__init__.py`, add the import next to the other `from otlp_client.…` lines:

```python
from otlp_client.credentials import BasicAuth, BearerToken, CredentialProvider
```

and add `"BasicAuth"`, `"BearerToken"`, `"CredentialProvider"` to `__all__`, keeping its existing sort order.

- [ ] **Step 5: Run the tests**

Run: `.venv/bin/python -m pytest tests/test_credentials.py -q`
Expected: PASS.

- [ ] **Step 6: Typecheck, lint, commit**

```bash
.venv/bin/python -m pytest -q && .venv/bin/python -m mypy && .venv/bin/python -m ruff check .
git add src/otlp_client/credentials.py src/otlp_client/__init__.py tests/test_credentials.py
git commit -m "feat: add BearerToken and BasicAuth credential providers"
```

---

### Task 5: OAuth2ClientCredentials — minting, caching, expiry

Single-flight, invalidation, and error classification arrive in Task 6.

**Files:**
- Modify: `src/otlp_client/credentials.py`
- Modify: `src/otlp_client/__init__.py`
- Create: `tests/test_credentials_oauth2.py`

**Interfaces:**
- Consumes: `CredentialProvider` (Task 3).
- Produces:
  - `AuthStyle(StrEnum)` with members `POST = "post"` and `BASIC = "basic"`.
  - `OAuth2ClientCredentials(*, token_url: str, client_id: str, client_secret: str, scope: str | None = None, extra_params: Mapping[str, str] | None = None, auth_style: AuthStyle = AuthStyle.POST, session: aiohttp.ClientSession | None = None, expiry_skew: float = 30.0, default_ttl: float = 300.0, monotonic: Callable[[], float] = time.monotonic)` with `async def headers(kind)`, `async def invalidate(kind)`, `async def aclose()`.
  - Both exported from `otlp_client`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_credentials_oauth2.py`:

```python
from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer

from otlp_client.credentials import AuthStyle, CredentialProvider, OAuth2ClientCredentials
from otlp_client.signals import SignalKind
from tests.support.fakes import FakeClock

METRICS = SignalKind.METRICS


class TokenEndpoint:
    """A token endpoint that hands out a numbered token per request."""

    def __init__(
        self,
        status: int = 200,
        body: dict[str, object] | None = None,
        expires_in: int | None = 3600,
    ) -> None:
        self.status = status
        self.body = body
        self.expires_in = expires_in
        self.requests: list[tuple[dict[str, str], dict[str, str]]] = []

    async def handle(self, request: web.Request) -> web.Response:
        form = dict(await request.post())  # type: ignore[arg-type]
        self.requests.append((dict(request.headers), {k: str(v) for k, v in form.items()}))
        if self.body is not None:
            return web.json_response(self.body, status=self.status)
        payload: dict[str, object] = {
            "access_token": f"token-{len(self.requests)}",
            "token_type": "Bearer",
        }
        if self.expires_in is not None:
            payload["expires_in"] = self.expires_in
        return web.json_response(payload, status=self.status)


EndpointFactory = Callable[[TokenEndpoint], Awaitable[tuple[str, ClientSession]]]


@pytest.fixture
async def token_server() -> AsyncIterator[EndpointFactory]:
    servers: list[TestServer] = []
    sessions: list[ClientSession] = []

    async def make(endpoint: TokenEndpoint) -> tuple[str, ClientSession]:
        app = web.Application()
        app.router.add_route("POST", "/token", endpoint.handle)
        server = TestServer(app)
        await server.start_server()
        servers.append(server)
        session = ClientSession()
        sessions.append(session)
        return str(server.make_url("/token")), session

    yield make
    for s in sessions:
        await s.close()
    for srv in servers:
        await srv.close()


async def test_mints_a_token_and_returns_it_as_a_bearer_header(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint()
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="sh", session=session
    )
    assert await provider.headers(METRICS) == {"authorization": "Bearer token-1"}
    _, form = endpoint.requests[0]
    assert form["grant_type"] == "client_credentials"
    assert form["client_id"] == "id"
    assert form["client_secret"] == "sh"


async def test_the_token_is_cached_across_calls(token_server: EndpointFactory) -> None:
    endpoint = TokenEndpoint()
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="sh", session=session
    )
    await provider.headers(METRICS)
    await provider.headers(SignalKind.LOGS)
    assert len(endpoint.requests) == 1


async def test_the_token_refreshes_inside_the_expiry_skew(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint(expires_in=100)
    url, session = await token_server(endpoint)
    clock = FakeClock()
    provider = OAuth2ClientCredentials(
        token_url=url,
        client_id="id",
        client_secret="sh",
        session=session,
        expiry_skew=30.0,
        monotonic=clock.monotonic,
    )
    assert await provider.headers(METRICS) == {"authorization": "Bearer token-1"}
    await clock.sleep(69.0)  # 31s of life left: still outside the skew
    assert await provider.headers(METRICS) == {"authorization": "Bearer token-1"}
    await clock.sleep(2.0)  # 29s left: inside the skew, refresh
    assert await provider.headers(METRICS) == {"authorization": "Bearer token-2"}


async def test_a_response_without_expires_in_uses_the_default_ttl(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint(expires_in=None)
    url, session = await token_server(endpoint)
    clock = FakeClock()
    provider = OAuth2ClientCredentials(
        token_url=url,
        client_id="id",
        client_secret="sh",
        session=session,
        default_ttl=300.0,
        expiry_skew=0.0,
        monotonic=clock.monotonic,
    )
    await provider.headers(METRICS)
    await clock.sleep(299.0)
    assert len(endpoint.requests) == 1
    await clock.sleep(2.0)
    await provider.headers(METRICS)
    assert len(endpoint.requests) == 2


async def test_scope_and_extra_params_reach_the_form(token_server: EndpointFactory) -> None:
    endpoint = TokenEndpoint()
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url,
        client_id="id",
        client_secret="sh",
        scope="metrics:write",
        extra_params={"audience": "collector"},
        session=session,
    )
    await provider.headers(METRICS)
    _, form = endpoint.requests[0]
    assert form["scope"] == "metrics:write"
    assert form["audience"] == "collector"


async def test_basic_auth_style_sends_credentials_in_the_header(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint()
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url,
        client_id="id",
        client_secret="sh",
        auth_style=AuthStyle.BASIC,
        session=session,
    )
    await provider.headers(METRICS)
    headers, form = endpoint.requests[0]
    assert headers["Authorization"].startswith("Basic ")
    assert "client_secret" not in form


async def test_a_borrowed_session_is_not_closed(token_server: EndpointFactory) -> None:
    endpoint = TokenEndpoint()
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="sh", session=session
    )
    await provider.headers(METRICS)
    await provider.aclose()
    assert session.closed is False


async def test_an_owned_session_is_created_lazily_and_closed(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint()
    url, _ = await token_server(endpoint)
    provider = OAuth2ClientCredentials(token_url=url, client_id="id", client_secret="sh")
    await provider.headers(METRICS)
    owned = provider._session
    assert owned is not None
    await provider.aclose()
    assert owned.closed is True


def test_the_helper_satisfies_the_protocol() -> None:
    provider = OAuth2ClientCredentials(token_url="http://x/token", client_id="i", client_secret="s")
    assert isinstance(provider, CredentialProvider)


def test_the_helper_is_exported_from_the_package() -> None:
    import otlp_client

    for name in ("OAuth2ClientCredentials", "AuthStyle"):
        assert name in otlp_client.__all__
        assert hasattr(otlp_client, name)
```

- [ ] **Step 2: Run them and watch them fail**

Run: `.venv/bin/python -m pytest tests/test_credentials_oauth2.py -q`
Expected: FAIL — `ImportError: cannot import name 'AuthStyle'`.

- [ ] **Step 3: Implement the helper**

Append to `src/otlp_client/credentials.py`. Add `time`, `StrEnum`, and `TYPE_CHECKING` to its imports; `aiohttp` is imported **inside** the method, never at module level:

```python
if TYPE_CHECKING:
    import aiohttp


class AuthStyle(StrEnum):
    """How client credentials are presented, per RFC 6749 section 2.3.1."""

    POST = "post"
    BASIC = "basic"


class OAuth2ClientCredentials:
    """An OAuth2 client-credentials token, cached until it nears expiry.

    Pass a `session` whenever one exists; Home Assistant integrations must pass
    `async_get_clientsession(hass)` rather than letting this create one. A
    session this provider creates is owned by it and released by `aclose()`.

    The client never closes a provider -- one provider shared across several
    OTLPClient instances is a first-class pattern -- so the caller owns the
    lifetime of whatever it passes here.
    """

    def __init__(
        self,
        *,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: str | None = None,
        extra_params: Mapping[str, str] | None = None,
        auth_style: AuthStyle = AuthStyle.POST,
        session: aiohttp.ClientSession | None = None,
        expiry_skew: float = 30.0,
        default_ttl: float = 300.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._scope = scope
        self._extra_params = dict(extra_params or {})
        self._auth_style = auth_style
        self._session = session
        self._owns_session = False
        self._expiry_skew = expiry_skew
        self._default_ttl = default_ttl
        self._monotonic = monotonic
        self._token: str | None = None
        self._expires_at = 0.0

    async def headers(self, kind: SignalKind) -> Mapping[str, str]:
        if self._token is None or self._monotonic() >= self._expires_at - self._expiry_skew:
            await self._refresh()
        return {"authorization": f"Bearer {self._token}"}

    async def invalidate(self, kind: SignalKind) -> None:
        self._token = None

    async def aclose(self) -> None:
        """Close the session only if this provider created it."""
        if self._owns_session and self._session is not None:
            await self._session.close()

    def _form(self) -> dict[str, str]:
        form = {"grant_type": "client_credentials", **self._extra_params}
        if self._scope:
            form["scope"] = self._scope
        if self._auth_style is AuthStyle.POST:
            form["client_id"] = self._client_id
            form["client_secret"] = self._client_secret
        return form

    def _auth_header(self) -> dict[str, str]:
        if self._auth_style is not AuthStyle.BASIC:
            return {}
        raw = f"{self._client_id}:{self._client_secret}".encode()
        return {"Authorization": f"Basic {base64.b64encode(raw).decode('ascii')}"}

    async def _ensure_session(self) -> aiohttp.ClientSession:
        import aiohttp

        if self._session is None:
            self._session = aiohttp.ClientSession()
            self._owns_session = True
        return self._session

    async def _refresh(self) -> None:
        session = await self._ensure_session()
        async with session.post(
            self._token_url, data=self._form(), headers=self._auth_header()
        ) as response:
            body = await response.json(content_type=None)
        token = body.get("access_token")
        expires_in = body.get("expires_in")
        self._token = token
        ttl = float(expires_in) if expires_in is not None else self._default_ttl
        self._expires_at = self._monotonic() + ttl
```

Error handling is deliberately absent here; Task 6 adds it with its tests.

- [ ] **Step 4: Export it**

In `src/otlp_client/__init__.py`, extend the credentials import to
`from otlp_client.credentials import AuthStyle, BasicAuth, BearerToken, CredentialProvider, OAuth2ClientCredentials`
and add `"AuthStyle"` and `"OAuth2ClientCredentials"` to `__all__`.

- [ ] **Step 5: Run the tests**

Run: `.venv/bin/python -m pytest tests/test_credentials_oauth2.py -q`
Expected: PASS.

- [ ] **Step 6: Confirm the lazy aiohttp import survived**

Run: `.venv/bin/python -m pytest tests/test_core_only.py -q`
Expected: PASS. If it fails, `aiohttp` reached module level — move it back inside `_ensure_session`.

- [ ] **Step 7: Typecheck, lint, commit**

```bash
.venv/bin/python -m pytest -q && .venv/bin/python -m mypy && .venv/bin/python -m ruff check .
git add src/otlp_client/credentials.py src/otlp_client/__init__.py tests/test_credentials_oauth2.py
git commit -m "feat: add OAuth2 client-credentials provider with cached refresh"
```

---

### Task 6: OAuth2 single-flight, invalidation, and error classification

**Files:**
- Modify: `src/otlp_client/credentials.py`
- Modify: `tests/test_credentials_oauth2.py`

**Interfaces:**
- Consumes: Task 5's `OAuth2ClientCredentials`.
- Produces: no new names. `_refresh` raises `OTLPPermanentError` or `OTLPTransportError`, and concurrent `headers()` calls mint at most one token.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_credentials_oauth2.py`:

```python
async def test_concurrent_calls_mint_exactly_one_token(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint()
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="sh", session=session
    )
    results = await asyncio.gather(*(provider.headers(METRICS) for _ in range(10)))
    assert len(endpoint.requests) == 1
    assert {tuple(r.items()) for r in results} == {(("authorization", "Bearer token-1"),)}


async def test_invalidate_forces_a_fresh_mint(token_server: EndpointFactory) -> None:
    endpoint = TokenEndpoint()
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="sh", session=session
    )
    assert await provider.headers(METRICS) == {"authorization": "Bearer token-1"}
    await provider.invalidate(METRICS)
    assert await provider.headers(METRICS) == {"authorization": "Bearer token-2"}


async def test_concurrent_calls_after_an_invalidate_still_mint_once(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint()
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="sh", session=session
    )
    await provider.headers(METRICS)
    await provider.invalidate(METRICS)
    await asyncio.gather(*(provider.headers(METRICS) for _ in range(10)))
    assert len(endpoint.requests) == 2


@pytest.mark.parametrize("status", [400, 401])
async def test_an_rfc_6749_error_response_is_permanent(
    token_server: EndpointFactory, status: int
) -> None:
    endpoint = TokenEndpoint(
        status=status,
        body={"error": "invalid_client", "error_description": "bad secret"},
    )
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="hunter2", session=session
    )
    with pytest.raises(OTLPPermanentError) as caught:
        await provider.headers(METRICS)
    assert "invalid_client" in str(caught.value)
    assert "bad secret" in str(caught.value)


async def test_a_server_error_is_transport_shaped(token_server: EndpointFactory) -> None:
    endpoint = TokenEndpoint(status=503, body={"error": "temporarily_unavailable"})
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="sh", session=session
    )
    with pytest.raises(OTLPTransportError):
        await provider.headers(METRICS)


async def test_an_unreachable_token_endpoint_is_transport_shaped() -> None:
    provider = OAuth2ClientCredentials(
        token_url="http://127.0.0.1:1/token", client_id="id", client_secret="sh"
    )
    with pytest.raises(OTLPTransportError):
        await provider.headers(METRICS)
    await provider.aclose()


async def test_a_response_without_an_access_token_is_permanent(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint(body={"token_type": "Bearer"})
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="sh", session=session
    )
    with pytest.raises(OTLPPermanentError):
        await provider.headers(METRICS)


async def test_a_non_json_body_is_permanent(token_server: EndpointFactory) -> None:
    async def handle(request: web.Request) -> web.Response:
        return web.Response(status=200, body=b"<html>not json</html>")

    app = web.Application()
    app.router.add_route("POST", "/token", handle)
    server = TestServer(app)
    await server.start_server()
    session = ClientSession()
    provider = OAuth2ClientCredentials(
        token_url=str(server.make_url("/token")),
        client_id="id",
        client_secret="sh",
        session=session,
    )
    with pytest.raises(OTLPPermanentError):
        await provider.headers(METRICS)
    await session.close()
    await server.close()


async def test_the_client_secret_never_appears_in_an_error(
    token_server: EndpointFactory,
) -> None:
    endpoint = TokenEndpoint(status=401, body={"error": "invalid_client"})
    url, session = await token_server(endpoint)
    provider = OAuth2ClientCredentials(
        token_url=url, client_id="id", client_secret="hunter2", session=session
    )
    with pytest.raises(OTLPPermanentError) as caught:
        await provider.headers(METRICS)
    assert "hunter2" not in str(caught.value)
```

Add `import asyncio` and `from otlp_client.errors import OTLPPermanentError, OTLPTransportError` to the file's imports.

- [ ] **Step 2: Run them and watch them fail**

Run: `.venv/bin/python -m pytest tests/test_credentials_oauth2.py -q`
Expected: FAIL — 10 token requests instead of 1, and `TypeError`/`aiohttp.ClientConnectorError` where an `OTLPTransportError` was expected.

- [ ] **Step 3: Add the lock**

In `src/otlp_client/credentials.py`, add `asyncio` to the imports, create the lock in `__init__`:

```python
        self._lock = asyncio.Lock()
```

and rewrite `headers` so the refresh is single-flight:

```python
async def headers(self, kind: SignalKind) -> Mapping[str, str]:
    if self._usable():
        return {"authorization": f"Bearer {self._token}"}
    async with self._lock:
        # Re-check inside the lock: a burst of concurrent exports (or of
        # simultaneous 401s after an invalidate) collapses into one mint.
        if not self._usable():
            await self._refresh()
    return {"authorization": f"Bearer {self._token}"}


def _usable(self) -> bool:
    return self._token is not None and self._monotonic() < self._expires_at - self._expiry_skew
```

Note the lock cannot be created at class definition time; creating it in `__init__` is fine because `asyncio.Lock()` no longer binds a loop at construction on Python 3.10+.

- [ ] **Step 4: Classify the errors**

Replace `_refresh` with:

```python
    async def _refresh(self) -> None:
        import aiohttp

        session = await self._ensure_session()
        try:
            async with session.post(
                self._token_url, data=self._form(), headers=self._auth_header()
            ) as response:
                status = response.status
                body = await response.json(content_type=None)
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise OTLPTransportError(
                f"could not reach the token endpoint: {type(exc).__name__}: {exc}"
            ) from exc
        except ValueError as exc:
            # A body that is not JSON at all. Never echoed back: it may be an
            # upstream error page carrying anything.
            raise OTLPPermanentError("token endpoint returned a non-JSON body") from exc

        if status >= 500:
            raise OTLPTransportError(f"token endpoint returned status {status}")
        if status >= 400:
            # RFC 6749 section 5.2: `error` and `error_description` only. The
            # raw body never goes into the message -- it can carry the secret
            # back at us.
            detail = ""
            if isinstance(body, dict):
                error = str(body.get("error", "unknown_error"))
                description = body.get("error_description")
                detail = f"{error}: {description}" if description else error
            raise OTLPPermanentError(f"token endpoint rejected the credentials ({detail})")
        if not isinstance(body, dict) or not body.get("access_token"):
            raise OTLPPermanentError("token endpoint returned no access_token")

        self._token = str(body["access_token"])
        expires_in = body.get("expires_in")
        ttl = float(expires_in) if expires_in is not None else self._default_ttl
        self._expires_at = self._monotonic() + ttl
```

Add `from otlp_client.errors import OTLPPermanentError, OTLPTransportError` to the module imports.

- [ ] **Step 5: Run the tests**

Run: `.venv/bin/python -m pytest tests/test_credentials_oauth2.py -q`
Expected: PASS, including the Task 5 tests.

- [ ] **Step 6: Run the whole suite, typecheck, lint**

Run: `.venv/bin/python -m pytest -q && .venv/bin/python -m mypy && .venv/bin/python -m ruff check .`
Expected: all green.

- [ ] **Step 7: Commit**

```bash
git add src/otlp_client/credentials.py tests/test_credentials_oauth2.py
git commit -m "feat: single-flight refresh and error classification for OAuth2

Concurrent exports mint one token. Token-endpoint failures are classified so
the client can fail fast on a bad secret and retry a flaky endpoint."
```

---

### Task 7: End-to-end proof through a real transport

Every credential test so far uses a fake transport. This proves the whole path against a live aiohttp server.

**Files:**
- Modify: `tests/test_transport_http.py`

**Interfaces:**
- Consumes: everything from Tasks 1–6.
- Produces: nothing importable.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_transport_http.py`:

```python
async def test_a_rotating_credential_reaches_the_wire_and_survives_a_401(
    server_factory: ServerFactory,
) -> None:
    # The whole path, no fakes: a 401 makes the client invalidate, re-mint, and
    # resend, and the second request carries the new token.
    class Rotating:
        def __init__(self) -> None:
            self.minted = 0

        async def headers(self, kind: SignalKind) -> dict[str, str]:
            return {"authorization": f"Bearer token-{self.minted}"}

        async def invalidate(self, kind: SignalKind) -> None:
            self.minted += 1

    class RejectOnce:
        def __init__(self) -> None:
            self.requests: list[dict[str, str]] = []

        async def handle(self, request: web.Request) -> web.Response:
            self.requests.append(dict(request.headers))
            await request.read()
            if len(self.requests) == 1:
                return web.Response(status=401, body=b"token expired")
            return web.Response(status=200, body=b"{}")

    handler = RejectOnce()
    app = web.Application()
    app.router.add_route("POST", "/{tail:.*}", handler.handle)
    server = TestServer(app)
    await server.start_server()
    session = ClientSession()
    base = str(server.make_url("")).rstrip("/")

    config = OTLPConfig(endpoint=base, headers={"x-tenant": "acme"})
    credentials = Rotating()
    transport = HTTPTransport(config, JSONEncoder(), session=session)
    client = OTLPClient(config, transport=transport, encoder=JSONEncoder(), credentials=credentials)
    result = await client.export_metrics([gauge("t", 1.0, time_unix_nano=1)])

    assert isinstance(result, Success)
    assert [h["authorization"] for h in handler.requests] == ["Bearer token-0", "Bearer token-1"]
    assert handler.requests[1]["x-tenant"] == "acme"

    await session.close()
    await server.close()
```

Add these imports to the file: `from otlp_client.client import OTLPClient` and `from otlp_client.model.metrics import gauge`.

- [ ] **Step 2: Run it**

Run: `.venv/bin/python -m pytest tests/test_transport_http.py -k rotating_credential -v`
Expected: PASS on the first run — every piece it exercises already landed in Tasks 1–6. This test is a regression net, not a driver. If it fails, the failure is a real integration bug in the earlier tasks: fix it there rather than adjusting this test.

- [ ] **Step 3: Typecheck, lint, commit**

```bash
.venv/bin/python -m pytest -q && .venv/bin/python -m mypy && .venv/bin/python -m ruff check .
git add tests/test_transport_http.py
git commit -m "test: prove a rotating credential survives a 401 end to end"
```

---

### Task 8: Lock in the lazy aiohttp import

**Files:**
- Modify: `tests/test_core_only.py:125-161`

**Interfaces:**
- Consumes: `otlp_client.credentials` (Task 3).
- Produces: nothing importable.

- [ ] **Step 1: Extend the subprocess guard**

In `tests/test_core_only.py`, inside `SCRIPT`, add the credentials import next to the other public-surface imports:

```python
from otlp_client.credentials import BasicAuth, BearerToken, OAuth2ClientCredentials  # noqa: F401
```

and add this assertion just before the `leaked = sorted(...)` block:

```python
# aiohttp is a core dependency, so it is not FORBIDDEN -- but it must stay
# lazily imported. Home Assistant loads this module inside a running event
# loop and OAuth2ClientCredentials must not drag aiohttp in at import time.
assert "aiohttp" not in sys.modules, "aiohttp must not be imported at module level"
```

- [ ] **Step 2: Run it**

Run: `.venv/bin/python -m pytest tests/test_core_only.py -q`
Expected: PASS. (Verified before this plan was written: `aiohttp` is absent from `sys.modules` after importing the public surface today.)

- [ ] **Step 3: Prove the guard bites**

Temporarily add `import aiohttp` at the top of `src/otlp_client/credentials.py`, re-run the command from Step 2, and confirm it now FAILS with the new assertion message. Then remove that line and confirm it passes again.

- [ ] **Step 4: Commit**

```bash
git add tests/test_core_only.py
git commit -m "test: guard that aiohttp stays lazily imported"
```

---

### Task 9: Documentation and the version bump

**Files:**
- Modify: `README.md` (new section after `## Per-signal configuration`, ends at line 199)
- Modify: `docs/auth-audit.md:96-110`
- Modify: `pyproject.toml:3`
- Modify: `src/otlp_client/client.py:23`

**Interfaces:**
- Consumes: everything.
- Produces: nothing importable.

- [ ] **Step 1: Add the README section**

Insert after the `## Per-signal configuration` section and before `## Scope`:

````markdown
## Dynamic credentials

Headers in `OTLPConfig` are fixed for the life of the client. When a credential
rotates, pass a provider instead — it is awaited on every export attempt:

```python
from otlp_client import OAuth2ClientCredentials, OTLPClient, OTLPConfig

credentials = OAuth2ClientCredentials(
    token_url="https://auth.example.com/oauth2/token",
    client_id="collector-writer",
    client_secret=secret,
    scope="otlp:write",
    session=session,          # required under Home Assistant
)
client = await OTLPClient.create(config, session=session, credentials=credentials)
```

Three helpers ship with the library:

| Helper | Sends | Notes |
| --- | --- | --- |
| `BearerToken(source)` | `authorization: Bearer …` | `source` is a token string, or an async callable consulted per attempt |
| `BasicAuth(user, password)` | `authorization: Basic …` | RFC 7617, UTF-8 |
| `OAuth2ClientCredentials(...)` | `authorization: Bearer …` | Client-credentials grant, cached until it nears expiry |

Anything else implements the protocol directly:

```python
class VaultCredentials:
    async def headers(self, kind: SignalKind) -> Mapping[str, str]:
        return {"authorization": f"Bearer {await self.vault.read()}"}

    async def invalidate(self, kind: SignalKind) -> None:
        self.vault.clear()
```

**Provider headers merge over configured ones, and the provider wins a key
collision.** This is the opposite of per-signal headers, which *replace* the
general set. The two answer different questions: per-signal headers pick which
static set applies, and the provider layers credentials on top of it. A static
`x-tenant` alongside a rotating `authorization` therefore works as written.

**A rejected credential earns one retry.** On a 401, 403, or gRPC
`UNAUTHENTICATED`, the client calls `invalidate()` and re-sends once,
immediately, outside the retry budget. A second rejection is permanent — a
wrong secret fails fast rather than hammering the token endpoint.

**You own the provider's lifetime.** `OTLPClient.aclose()` never closes a
provider, because one provider shared across several clients (the recommended
pattern when signals need different protocols or TLS settings) must outlive any
one of them. Call `await credentials.aclose()` yourself if the provider owns a
session.

The OTLP specification defines no authentication concept at all, so there is no
`OTEL_EXPORTER_OTLP_*` variable for any of this and `OTLPConfig.from_env()`
cannot build a provider. Construct it in code.
````

- [ ] **Step 2: Mark the audit finding resolved**

In `docs/auth-audit.md`, change the finding 3 heading to:

```markdown
### 3. Headers are frozen at construction time — RESOLVED 2026-09-05
```

and insert directly beneath it, before the existing paragraphs:

```markdown
**Resolved 2026-09-05** by `docs/superpowers/specs/2026-09-05-credential-providers-design.md`.
`OTLPClient.create(..., credentials=...)` takes a provider awaited per export
attempt, with `BearerToken`, `BasicAuth` and `OAuth2ClientCredentials` helpers.
Unlike every other item in this audit, this is beyond-spec work rather than
conformance: the specification defines no authentication concept, so there is
nothing here to conform to. The paragraphs below describe the original gap.
```

Delete the final line of that finding ("Deliberately not designed here. Worth
its own brainstorm if we pursue it."), which is now false.

Also update the conformance table row for headers, adding a row beneath it:

```markdown
| Dynamic credentials (beyond spec) | — | Supported (2026-09-05) |
```

- [ ] **Step 3: Bump the version**

`pyproject.toml` line 3 and `src/otlp_client/client.py` line 23 both become `0.5.0`. They must match — `DEFAULT_SCOPE` reports `__version__` on every export.

```bash
sed -i '' 's/^version = "0.4.0"$/version = "0.5.0"/' pyproject.toml
sed -i '' 's/^__version__ = "0.4.0"$/__version__ = "0.5.0"/' src/otlp_client/client.py
grep -rn '0\.5\.0' pyproject.toml src/otlp_client/client.py
```

- [ ] **Step 4: Verify**

Run: `.venv/bin/python -m pytest -q && .venv/bin/python -m mypy && .venv/bin/python -m ruff check .`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add README.md docs/auth-audit.md pyproject.toml src/otlp_client/client.py
git commit -m "docs: document dynamic credentials and release 0.5.0

Transport.send takes resolved headers, which breaks any custom transport
passed to OTLPClient.__init__."
```

---

## Verification

After Task 9, the branch is complete when all of these pass from the repo root:

```bash
.venv/bin/python -m pytest -q          # full suite, integration excluded
.venv/bin/python -m mypy               # strict, src + tests
.venv/bin/python -m ruff check .
.venv/bin/python -m pytest tests/test_core_only.py -q
```

The gRPC tests need the `grpc` extra; they `importorskip` without it, so confirm
they actually ran (`-v` shows them) rather than silently skipping.
