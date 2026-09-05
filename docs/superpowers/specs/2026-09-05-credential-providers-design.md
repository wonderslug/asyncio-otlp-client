# Dynamic credential providers — Design

**Date:** 2026-09-05
**Status:** Approved, ready for implementation planning
**Extends:** `docs/superpowers/specs/2026-09-05-per-signal-config-design.md`
**Follows from:** `docs/auth-audit.md`, finding 3

## Problem

`OTLPConfig` is a frozen dataclass and both transports read their headers from
it at send time. A bearer token that rotates therefore cannot be refreshed: the
only way to change the `authorization` header is to build a new `OTLPConfig`,
tear down the client, and rebuild it — closing the gRPC channel or dropping the
HTTP session along with it.

This is not a defect against the OTLP specification, because the specification
has nothing to say here. It defines no authentication concept at all: headers,
a CA file, mTLS material, and a gRPC `insecure` toggle. Bearer tokens and API
keys are headers by convention and nothing more.

It is a known sore point everywhere else, though. opentelemetry-java#4590 and
opentelemetry-dotnet#2504 both track it, and the Collector routes around it
entirely with authenticator extensions — `bearertokenauth`, `oauth2clientauth`,
`basicauth`, `oidc`. An async-native client can offer directly what those
extensions offer out of process: a credential provider awaited per request.

Every other decision in this repository has been a conformance decision, argued
against the specification text. This one is not. The spec is silent, so the
design is judged on whether it is correct, small, and hard to misuse.

## Decisions

### 1. The provider is a collaborator on `create()`, not a config field

```python
client = await OTLPClient.create(config, credentials=provider, session=session)
```

`OTLPConfig` is documented as "every knob the client reads. The only source of
settings" — frozen, comparable, and buildable from the environment. A
credential provider is none of those things: it holds a cache, performs I/O,
and may own an `aiohttp.ClientSession`. It belongs with `transport` and
`encoder`, which are also collaborators passed in rather than configured.

The rejected alternative was a `credentials` field plus `metrics_credentials` /
`logs_credentials` / `traces_credentials`, resolved by a `credentials_for(kind)`
joining the existing resolver family. It is more symmetric with how headers,
timeout, and compression already work, and that symmetry is a genuine loss.
It was rejected because it puts a stateful, closeable object inside a frozen
dataclass, and because the per-signal design closed that field set at nine
deliberately.

The cost, stated plainly: `from_env()` can never produce a provider, so
environment-only configuration cannot express dynamic auth. This is acceptable
because the specification defines no variable for it, so there is nothing to be
non-conformant with.

### 2. The provider contributes headers

```python
@runtime_checkable
class CredentialProvider(Protocol):
    async def headers(self, kind: SignalKind) -> Mapping[str, str]:
        """Credentials for one export. Awaited per attempt."""

    async def invalidate(self, kind: SignalKind) -> None:
        """The collector rejected what headers() last returned. Drop any cache."""
```

Narrow on purpose. The provider adds headers, and may override a statically
configured one by returning the same key (decision 3), but it cannot *remove*
one or see what the static headers were.

The rejected alternative, `apply(kind, headers) -> Mapping`, hands the provider
the resolved static headers and lets it return the final map. It is strictly
more powerful — a provider could implement replace semantics itself — and
strictly more dangerous: a provider that returns `{}` silently drops every
configured header, and every helper has to remember to thread the input
through. A `token(kind) -> str` form was also rejected as too narrow; basic
auth and multi-header API keys (`x-api-key` plus `x-tenant`) need an escape
hatch immediately.

Both methods are mandatory rather than `invalidate` being optional and
duck-typed. The repository is `mypy --strict`; a `hasattr` probe would be
unverifiable, and a stateless provider implements `invalidate` as a single
`pass`.

### 3. Provider values merge over static headers, and the provider wins

```python
headers = {**config.headers_for(kind), **await provider.headers(kind)}
```

Deliberately the opposite of per-signal headers, which *replace* the general
ones. The two answer different questions: a per-signal header set answers
"which static headers for this signal", and the provider answers "what to layer
on top of them". Static `x-tenant` alongside a rotating `authorization` is the
case that must work, and replace semantics would make the provider silently
drop `x-tenant`.

Documented explicitly in the README, because the repository now contains both
semantics and the difference is not guessable.

### 4. Per-signal variation lives in the provider

`headers()` and `invalidate()` both take a `SignalKind`. A provider that needs
a different token per signal switches on it; the three shipped helpers ignore
it.

This keeps the per-signal rule intact — a credential is a per-request option,
so it may vary per signal — while expressing that variation through one object
instead of four more config fields.

### 5. A rejected credential earns exactly one re-auth attempt per export

On a 401, 403, or gRPC `UNAUTHENTICATED`, the client calls `invalidate(kind)`
and re-sends once, immediately, outside the retry budget and with no backoff.
If the second attempt is also rejected, the outcome is `Permanent`, as it is
today.

This covers the cases that actually occur — clock skew, server-side revocation,
a token rotated between mint and use — without turning a wrong API key into a
storm against a token endpoint.

Two alternatives were rejected. Doing nothing (the provider defends itself with
its own expiry check) leaves a token revoked mid-flight as a dropped export
that the next export silently fixes. Making rejection fully `Retryable` gives a
genuinely wrong credential a 90-second `max_elapsed` budget and a burst of
token traffic per export, when a loud, fast failure is exactly what an operator
with a bad secret needs.

The budget is scoped to the `_export` call, not to the attempt. An export that
retries a 503 and then meets a 401 still gets one re-auth in total.

### 6. Rejection is detected through the existing `Permanent.status`

`GRPCTransport._classify` maps `UNAUTHENTICATED` to 401 and `PERMISSION_DENIED`
to 403, on the `status` field it leaves `None` today. One predicate in
`outcomes.py` then serves both transports:

```python
def is_credential_rejection(outcome: ExportOutcome) -> bool:
    return isinstance(outcome, Permanent) and outcome.status in (401, 403)
```

No new public type, so no `isinstance` chain in the client, retry loop,
processor, or tests has to grow a branch, and no downstream exhaustive match
breaks. It also incidentally fixes the `collector rejected traces (status
None)` message the gRPC path produces now.

Rejected: a third `Unauthenticated` outcome alongside `Permanent` and
`Retryable` — more explicit, but it widens the public `ExportOutcome` union and
breaks exhaustive matching downstream. Also rejected: a boolean flag on
`Permanent`, which encodes one fact in two fields that can disagree.

### 7. The client owns the seam; `Transport.send` takes headers

```python
async def send(
    self, kind: SignalKind, payload: bytes, headers: Mapping[str, str]
) -> ExportOutcome: ...
```

The transports stop calling `config.headers_for()` themselves. The client
resolves static-plus-dynamic headers and runs the single re-auth branch in
`_export`, so all credential logic exists in one place and is testable against
a fake transport with no wire involved.

Rejected: leaving each transport to consult the provider. It avoids the
protocol change, but the invalidate-and-resend branch would then be written
twice, in two modules with different error shapes. Also rejected: an
`AuthenticatedTransport` wrapper, which is elegant but still requires `send()`
to accept headers for the inner transport to honour them — the same protocol
change plus another object in the graph.

The cost is real: `headers_for` moves out of the transports, where the
per-signal work put it a day ago, and `Transport.send` is a breaking signature
change (see Versioning).

### 8. The caller owns the provider's lifetime

`OTLPClient.aclose()` does not close the provider.

This repository's answer to per-signal protocol and TLS settings is "use
separate `OTLPClient` instances, one per signal". One provider shared across
three such clients is therefore a first-class pattern, and a client closing it
on `aclose()` would break its siblings.

`aclose()` is not part of `CredentialProvider`. Only a provider that owns
resources needs one, which among the shipped helpers means
`OAuth2ClientCredentials` alone; requiring a third no-op method from every
hand-rolled provider buys nothing when the client never calls it.

### 9. The OAuth2 helper uses `aiohttp`, with an injectable session

`aiohttp` is already the sole core dependency, so the helper adds nothing to
the install and needs no extra. It mirrors `HTTPTransport.create`: pass a
session and the helper borrows it; omit one and it creates and owns one,
released by its own `aclose()`.

The constraint that actually binds here is Home Assistant, which requires
`async_get_clientsession(hass)` rather than a library creating its own session.
Injection satisfies it.

Rejected: gating the helper behind an `oauth2` extra, which is ceremony
protecting a dependency the core install already requires, and a fourth install
permutation to document and test. Also rejected: a caller-supplied
`fetch` callable, which would ship the caching and leave the RFC 6749 form POST
— the tedious part — to the user.

`aiohttp` is imported lazily inside the helper's methods, matching the existing
pattern in `client.py`, so `import otlp_client` stays as cheap as it is now.

## Public surface

New module `src/otlp_client/credentials.py`, exporting `CredentialProvider`,
`BearerToken`, `BasicAuth`, `OAuth2ClientCredentials`, and `AuthStyle`. All five
are re-exported from `otlp_client/__init__.py`.

### `BearerToken`

```python
class BearerToken:
    def __init__(self, source: str | Callable[[], Awaitable[str]]) -> None: ...
```

A plain `str` covers "I have a token now". A callable covers "ask me each
time" — a rotating file on disk, a Kubernetes projected service-account token,
another library's async accessor. The callable is consulted per attempt and its
result is never cached: the caching belongs to whoever owns the token, and a
cache the user did not ask for is exactly what makes a rotated token look
stale. `invalidate` is a no-op in both forms, since the next call already
re-asks.

### `BasicAuth`

```python
class BasicAuth:
    def __init__(self, username: str, password: str) -> None: ...
```

Static, but it earns its place: it gets RFC 7617 right (UTF-8, base64 of
`user:password`) and keeps the secret out of the config and environment
surface, where it would otherwise live inside an `OTEL_EXPORTER_OTLP_HEADERS`
string. `invalidate` is a no-op.

### `OAuth2ClientCredentials`

```python
class OAuth2ClientCredentials:
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
    ) -> None: ...

    async def aclose(self) -> None: ...
```

`AuthStyle` is a `StrEnum` with `POST` (credentials in the form body) and
`BASIC` (credentials in an `Authorization` header), the two styles RFC 6749
§2.3.1 permits. `monotonic` is injectable for the same reason `OTLPClient`
injects it: expiry behaviour must be testable without sleeping.

**Caching.** The helper holds the token and a monotonic `expires_at`, and
refreshes when `now >= expires_at - expiry_skew`. A response that omits
`expires_in` is cached for `default_ttl` rather than minting per request; a
token endpoint hit on every export is a worse failure than a conservative TTL.

**Single-flight.** An `asyncio.Lock` guards the mint, with a re-check of the
cache after acquiring it. A burst of concurrent exports across three signals
produces one token request, not N. The lock covers the refresh only, never the
use of a cached token.

`invalidate()` clears the cache and nothing more. The same re-check then
collapses a burst of simultaneous 401s into a single re-mint. One race
survives: a mint completing just as an invalidate lands discards a good token.
It costs one extra token request and is not worth a generation counter.

**Errors.** Classified so that the client-side mapping in the next section
does the right thing:

| Token endpoint | Helper raises |
| --- | --- |
| 400 or 401 with an RFC 6749 error body (`invalid_client`, `invalid_scope`, …) | `OTLPPermanentError` |
| 5xx, timeout, connection error | `OTLPTransportError` |
| Malformed body, or no `access_token` | `OTLPPermanentError` |

**Secrets.** Exception messages carry the response's `error` and
`error_description` fields only — never the raw body, never the client secret,
never the token. The same rule governs every log line the module emits.

## Client and transport changes

`OTLPClient.create()` and `__init__` gain `credentials: CredentialProvider |
None = None`. `_export` becomes:

```python
reauthed = False


async def attempt() -> ExportOutcome:
    nonlocal reauthed
    outcome = await self._transport.send(kind, payload, await self._headers(kind))
    if not reauthed and self._credentials and is_credential_rejection(outcome):
        reauthed = True
        await self._credentials.invalidate(kind)
        outcome = await self._transport.send(kind, payload, await self._headers(kind))
    return outcome
```

`_headers(kind)` merges `config.headers_for(kind)` with the provider's result
per decision 3, and returns `config.headers_for(kind)` unchanged when no
provider is configured.

Exceptions raised by a provider are mapped rather than escaping into the retry
loop unclassified:

| Provider raises | Client returns |
| --- | --- |
| `OTLPPermanentError` | `Permanent` — fails fast |
| `OTLPTransportError` | `Retryable` — rides the existing backoff |
| anything else | propagates uncaught |

Anything else propagating is deliberate: a `KeyError` in someone's provider is
a bug, and swallowing it into a retry loop would hide it permanently.

`HTTPTransport.send` and `GRPCTransport.send` take the resolved headers instead
of reading `headers_for`. HTTP layers `Content-Type` and `Content-Encoding` on
top exactly as it does today; gRPC turns the mapping into its metadata tuple.
`GRPCTransport._classify` gains the two status mappings from decision 6.
Neither transport's channel, session, or TLS handling changes.

## Testing

**Re-auth budget** — the cases where it could leak:

- 401 then success: exactly one `invalidate`, one resend, `Success`.
- 401 twice: `Permanent`, and exactly one `invalidate`.
- 503 → retry → 401 → resend → 401: still exactly one `invalidate` for the
  whole export, proving the budget is per-export rather than per-attempt.
- No provider configured: a 401 stays `Permanent` and nothing raises.

**Merge** — the provider wins over a colliding static header; the provider
layers over a per-signal static set; the provider observes the correct
`SignalKind`.

**Provider failures** — the mapping table above, including that an unrelated
exception propagates.

**Transports** — both honour the passed headers; HTTP still appends
`Content-Type` and `Content-Encoding`; gRPC passes them as metadata; `_classify`
maps `UNAUTHENTICATED` to 401 and `PERMISSION_DENIED` to 403.

**Helpers** — `BearerToken` in both forms, including that the callable is
re-consulted per attempt. `BasicAuth` against the RFC 7617 vector, plus a
non-ASCII password to pin the UTF-8 encoding. `OAuth2ClientCredentials` against
a live aiohttp test server, following the existing `EchoHandler` pattern in
`tests/test_transport_http.py`: mint once and cache; refresh at the skew
boundary under an injected `monotonic`; N concurrent exports produce exactly one
token request, asserted against the server's request count; `invalidate` forces
a re-mint; the full error table; a missing `expires_in` falls back to
`default_ttl`; and the client secret appears in no exception message.

**Core-only** — the AST scan in `tests/test_core_only.py` globs `src/`, so
`credentials.py` is covered from the moment it exists. The subprocess guard is
extended to assert that `aiohttp` never reaches `sys.modules` on a bare `import
otlp_client`, locking in the lazy import decision 9 depends on. This must be
verified to hold before the guard is added; if it does not, the guard is
dropped rather than unrelated modules restructured to satisfy it.

Every existing test stays green.

## Documentation

The README gains a dynamic-credentials section covering the protocol, the three
helpers, merge-over-static semantics with the contrast against per-signal
replace semantics, the one-re-auth-per-export behaviour, and provider ownership
including the Home Assistant `async_get_clientsession` guidance.

`docs/auth-audit.md` finding 3 is marked resolved, labelled explicitly as
beyond-spec rather than conformance work.

## Versioning

Version 0.5.0. `Transport.send` gains a required parameter, which breaks any
caller passing a custom transport to `OTLPClient.__init__`. Called out in the
release notes; no deprecation shim, since the parameter is required for the
feature to function and a shim would leave two code paths for headers.

## Out of scope

- **Environment configuration of credentials.** `from_env()` cannot build a
  provider, and the specification defines no variable to read, so there is
  nothing new to reject either.
- **Other OAuth2 flows.** Client credentials only — no refresh-token,
  authorization-code, or device flows.
- **gRPC native `CallCredentials`.** Metadata works on plaintext channels too;
  one code path beats two.
- **Background refresh timers.** Refresh happens on demand at the skew
  boundary. A timer means an owned task with its own lifecycle for no gain.
- **Per-signal credential config fields.** Decision 1 and decision 4.
