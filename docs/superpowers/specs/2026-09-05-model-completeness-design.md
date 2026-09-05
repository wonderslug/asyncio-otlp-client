# Model Completeness: trace context, flags, and exemplars — Design

**Date:** 2026-09-05
**Status:** Approved, ready for implementation planning
**Extends:** `docs/superpowers/specs/2026-09-04-asyncio-otlp-client-design.md`

## Problem

The v0.1.0 data model covers the fields the client sets, not the full OTLP
proto tree. A review against the installed `opentelemetry-proto` descriptors
found the gaps. Three of them are real capability limits rather than
bookkeeping:

- **No `trace_state`** on `Span` or `SpanLink`. W3C `tracestate` cannot be
  propagated, so a trace cannot carry vendor context across a boundary.
- **No `exemplars`** on any data point. Nothing links a metric data point to
  the trace that produced it, which is the metrics-to-traces correlation
  most backends key on.
- **No `flags`** anywhere. W3C trace flags (the sampled bit, remote-parent
  bits) are lost, and a data point cannot be marked `NO_RECORDED_VALUE` —
  the distinction between "measured zero" and "no measurement", which a
  Home Assistant entity going `unavailable` produces routinely.

Smaller: `LogRecord.event_name` is missing, and `HistogramDataPoint` lacks
the `min`/`max` that `ExponentialHistogramDataPoint` already has — an
unexplained asymmetry between two sibling types.

## Scope

**In:** `trace_state`, `flags` (all three families), `event_name`,
`HistogramDataPoint.min`/`max`, and exemplars.

**Out:** the `dropped_attributes_count` / `dropped_events_count` /
`dropped_links_count` family. This client enforces no limits and drops
nothing, so those fields would be hardcoded zero. They stay in Known
Limitations until something in the library actually truncates.

## Decisions

1. **`flags` are plain `int` fields**, faithful to the wire, accompanied by
   named constants and two helpers for the cases callers actually hit.
   Not `IntFlag`: `SPAN_FLAGS_TRACE_FLAGS_MASK = 255` is a mask over eight
   bits, not a flag to set, and an `IntFlag` member of that name invites
   exactly the wrong usage.

2. **The model becomes `kw_only=True`,** and new fields are declared in
   proto order. Field order then stops being an API concern permanently, no
   matter how much OTLP grows. Free now — nothing is published to PyPI and
   there are no consumers — and a major version later. These are wide record
   types where positional construction is unreadable regardless.

3. **`Exemplar.value` mirrors `NumberDataPoint.value`:** one `int | float`
   field over the proto's `as_int`/`as_double` oneof, with the same
   `isinstance(v, int)` branch and the same rejection of `bool`.

4. **`filtered_attributes` keeps its proto name.** It means "attributes not
   already present on the parent data point"; shortening it to `attributes`
   would invite callers to duplicate what the data point already carries.

5. **Version becomes 0.2.0.**

## Model changes

`kw_only=True` on every model dataclass. Then, in proto order:

| Type | Added |
|---|---|
| `Span` | `trace_state: str = ""`, `flags: int = 0` |
| `SpanLink` | `trace_state: str = ""`, `flags: int = 0` |
| `LogRecord` | `event_name: str = ""` (`flags` already exists from v0.1.0) |
| `HistogramDataPoint` | `min: float \| None = None`, `max: float \| None = None`, `flags: int = 0`, `exemplars: Sequence[Exemplar] = ()` |
| `NumberDataPoint` | `flags: int = 0`, `exemplars: Sequence[Exemplar] = ()` |
| `ExponentialHistogramDataPoint` | `flags: int = 0`, `exemplars: Sequence[Exemplar] = ()` |
| `SummaryDataPoint` | `flags: int = 0` |
| `Exemplar` (new) | `filtered_attributes`, `time_unix_nano`, `value: int \| float`, `span_id: bytes \| None`, `trace_id: bytes \| None` |

`SummaryDataPoint` gets `flags` but **not** exemplars — the proto defines
none there. `HistogramDataPoint` gains `exemplars` alongside the other two.

Constants: `SPAN_FLAGS_TRACE_FLAGS_MASK`, `SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE`,
`SPAN_FLAGS_CONTEXT_IS_REMOTE`, `LOG_RECORD_FLAGS_TRACE_FLAGS_MASK`,
`DATA_POINT_FLAGS_NO_RECORDED_VALUE`.

Helpers: one building span flags from `sampled` and `is_remote` booleans,
one marking a data point as having no recorded value.

## Encoding

**`flags` are 32-bit and encode as JSON numbers, not decimal strings.**
`Span.flags` and `SpanLink.flags` are `fixed32`; `LogRecord.flags` and the
data-point flags are `fixed32`/`uint32`. The project's standing rule that
64-bit fields render as decimal strings must not be over-applied here. Only
`Exemplar.time_unix_nano` (`fixed64`) and `Exemplar.as_int` (`sfixed64`) are
64-bit and take the string form.

Presence rules, which is where every encoder defect in this project has
originated:

| Field | Presence | JSON treatment |
|---|---|---|
| `trace_state`, `event_name` | implicit | `or None`; empty string omitted |
| all `flags` | implicit | `or None`; zero omitted; **number**, not string |
| `min`, `max` | **explicit** | emitted unconditionally inside `omit_empty`; only `None` omitted |
| `Exemplar.value` | oneof | `asInt` as a string / `asDouble` as a number; `bool` rejected |
| `Exemplar.trace_id`, `span_id` | implicit | **hex**, omitted when empty |

`min` and `max` having explicit presence is what keeps them clear of the
`-0.0` defect that hit `Summary.sum`. Because they are emitted
unconditionally and only `None` is dropped, a `-0.0` survives identically on
both sides. `Summary.sum` broke precisely because it is *implicit*-presence
and used a truthiness check, which treats `-0.0` as absent while protobuf's
bit-pattern presence check keeps it. Do not "simplify" `min`/`max` into that
pattern.

The protobuf encoder omits a nested message that carries no information, as
it already does for `status`, `resource`, `scope` and `Buckets`. Exemplars
sit in a repeated field and therefore carry no presence bit of their own.

Exemplar trace and span ids are picked up by the oracle's existing
hex-to-base64 adapter with no change, because that adapter was written as a
generic recursive walk keyed on `traceId`/`spanId` rather than a fixed list
of paths.

## Testing

The Hypothesis strategies extend to generate every new field: `trace_state`
text, `flags` as small integers, `event_name`, `min`/`max` including `None`,
`0.0` and `-0.0`, and exemplars with both `int` and `float` values and with
ids both present and absent. The encoder oracle then cross-checks all of it
against the canonical protobuf schema — the same mechanism that caught the
`-0.0` divergence no hand-written test would have found.

Explicit unit tests cover the three most likely failures:

1. `flags` emitted as a JSON number rather than a decimal string.
2. `min`/`max` round-tripping `-0.0` and `0.0` distinctly from `None`.
3. Exemplar ids emitted as hex rather than base64.

## Sequencing

Three tasks:

1. Scalar fields — `trace_state`, the three `flags` families, `event_name`,
   `min`/`max` — plus the `kw_only` conversion, which touches every model
   file once and so goes first.
2. Exemplars: the new message, its encoders, and its strategies. Separate
   because it is a nested message carrying both an `int`/`double` oneof and
   hex ids — the two rules behind every encoder bug this project has had.
3. Documentation: README, the Home Assistant guide, the version bump, and
   trimming Known Limitations to the Tier 3 family.

## Out of scope

- The `dropped_*_count` family (see Scope).
- Any instrument, aggregation, or sampling API. This remains a
  transport-and-encoding client.
