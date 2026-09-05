# Using asyncio-otlp-client in a Home Assistant integration

This library never imports `homeassistant`. This page is the glue.

## Why the core install works in HA

Home Assistant ships `aiohttp` and `orjson` but **neither `protobuf` nor
`grpcio`**. A core install of this library adds no new packages inside HA, and
because it is pure Python it publishes a `py3-none-any` wheel — no musllinux
tags, no native compilation, no involvement from the HA wheel builder.

Add it to `manifest.json` with no extras:

```json
{
  "domain": "my_integration",
  "requirements": ["asyncio-otlp-client==0.2.0"]
}
```

Installing the `[grpc]` extra pulls a native `grpcio` wheel into the HA
environment. That works on aarch64 and x86_64 but forfeits the property above;
prefer OTLP/HTTP inside HA.

## Wiring it into a config entry

Two HA rules shape this code, and both are requirements, not suggestions:

1. **Never let the client create its own `ClientSession`.** Pass
   `async_get_clientsession(hass)` to `OTLPClient.create()`. HA owns one shared
   session per instance; a component that opens its own leaks a connector and
   is flagged by HA's integration quality checks.
2. **The config entry must own the processor's background task.** Start the
   `BatchProcessor` in `async_setup_entry` and stop it in `async_unload_entry`
   (or an `async_on_unload` callback), so reloading the integration cannot leak
   the flush task from a previous load.

```python
import time

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import Event, EventStateChangedData, HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_track_state_change_event
from otlp_client import BatchProcessor, OTLPClient, OTLPConfig, Resource, gauge

type MyConfigEntry = ConfigEntry[BatchProcessor]


async def async_setup_entry(hass: HomeAssistant, entry: MyConfigEntry) -> bool:
    config = OTLPConfig(
        endpoint=entry.data["endpoint"],
        headers={"api-key": entry.data["api_key"]},
        resource=Resource(
            attributes={
                "service.name": "home-assistant",
                "service.instance.id": hass.config.location_name,
            }
        ),
    )
    client = await OTLPClient.create(config, session=async_get_clientsession(hass))
    processor = BatchProcessor(client, flush_interval=10.0, max_queue=4096)
    await processor.__aenter__()
    entry.runtime_data = processor

    def _handle(event: Event[EventStateChangedData]) -> None:
        new_state = event.data["new_state"]
        if new_state is None:
            return
        try:
            value = float(new_state.state)
        except ValueError:
            return  # non-numeric states are not metrics
        processor.submit_metrics(
            [
                gauge(
                    "homeassistant.state",
                    value,
                    time_unix_nano=time.time_ns(),
                    unit=new_state.attributes.get("unit_of_measurement", ""),
                    attributes={
                        "entity_id": new_state.entity_id,
                        "domain": new_state.domain,
                    },
                )
            ]
        )

    entry.async_on_unload(async_track_state_change_event(hass, ["sensor.living_room"], _handle))

    async def _stop_processor() -> None:
        await processor.__aexit__(None, None, None)

    entry.async_on_unload(_stop_processor)
    return True
```

`OTLPClient.create(config, session=...)` only opens its own `aiohttp.ClientSession`
when `session` is omitted (or `None`); passed one, it uses it as-is and never
closes it, so `async_get_clientsession(hass)`'s lifetime stays HA's to manage.
`BatchProcessor.__aenter__`/`__aexit__` start and stop the background flush
task — `async with BatchProcessor(...) as proc:` is equivalent for code that
does not need to hold the processor across callback boundaries, but a config
entry does, so it drives the same two methods directly instead.

`_handle` and the final `entry.async_on_unload(_stop_processor)` are nested
inside `async_setup_entry` on purpose: nesting is what puts both `processor`
and `entry` in `_handle`'s closure, so there is nothing to look up through
`hass.data` and nothing that can go stale across a reload. `_stop_processor`
is a real `async def`, not a lambda wrapping `hass.async_create_task(...)` —
`ConfigEntry.async_on_unload` accepts a callable that may itself return a
coroutine to await, and an `async def` closure is awaited unambiguously either
way, whereas a lambda that returns a `Task` depends on unload behavior this
guide should not ask a reader to reason about.

## Feeding state changes as metrics

`submit_metrics` never blocks and never raises, which is what makes it safe to
call from `_handle` above: a state-change listener has nowhere to handle an
exception, and `submit_metrics` returns `False` (queue full, or the processor
is already closed) rather than raising — safe for a listener to ignore.

### The `unavailable` / `unknown` case

`_handle` above skips any state that fails `float(new_state.state)`, which is
exactly what happens when an entity goes `unavailable` or `unknown` — HA
reports those as sentinel strings, not numbers. Skipping is defensible, but it
means a collector-side chart just stops updating with no signal as to why. If
you want that transition visible, don't skip it: submit a data point with
`data_point_flags(no_recorded_value=True)`, which records "no reading was
taken" as distinct from "the sensor read zero". `gauge()` doesn't expose
`flags`, so build the point directly:

```python
from otlp_client import Gauge, Metric, NumberDataPoint, data_point_flags


def _handle(event: Event[EventStateChangedData]) -> None:
    new_state = event.data["new_state"]
    if new_state is None:
        return
    attributes = {"entity_id": new_state.entity_id, "domain": new_state.domain}
    if new_state.state in ("unavailable", "unknown"):
        point = NumberDataPoint(
            time_unix_nano=time.time_ns(),
            value=0.0,
            attributes=attributes,
            flags=data_point_flags(no_recorded_value=True),
        )
        processor.submit_metrics(
            [Metric(name="homeassistant.state", data=Gauge(data_points=[point]))]
        )
        return
    try:
        value = float(new_state.state)
    except ValueError:
        return  # some other non-numeric state; not a metric
    processor.submit_metrics(
        [
            gauge(
                "homeassistant.state",
                value,
                time_unix_nano=time.time_ns(),
                unit=new_state.attributes.get("unit_of_measurement", ""),
                attributes=attributes,
            )
        ]
    )
```

## Surfacing client health

`processor.stats` is a plain frozen dataclass, which makes a natural diagnostic
sensor. A sensor platform lives in its own module, so it fetches the processor
back off the config entry rather than relying on a closure:

```python
processor = my_config_entry.runtime_data
stats = processor.stats
attributes = {
    "submitted": stats.submitted,
    "exported": stats.exported,
    "dropped": stats.dropped,
    "consecutive_failures": stats.consecutive_failures,
    "last_error": stats.last_error,
}
```

`dropped` rises for two different reasons: the bounded queue evicting the
oldest record because nothing is draining it fast enough, or a whole batch
failing to export (collector unreachable, rejected the payload, etc.) and
being discarded rather than requeued. Either way, the record is gone —
telemetry here is best-effort with no on-disk durability, so `dropped` means
"never reached the collector," full stop. `last_error` carries the most recent
failure's message so a diagnostic sensor can explain *why* the count moved,
and `consecutive_failures` distinguishes a blip from a sustained outage.

You do not have to poll `stats` to learn a collector went down: the first
failed export in a run is logged at `WARNING` (`otlp_client.processor`), and
every consecutive failure after that logs at `DEBUG` so a long outage does not
flood the HA log.

## What this library does not do

It is a transport-and-encoding client, not an SDK. There is no `Tracer` /
`Meter` / `Logger` API to instrument HA code with, no metric aggregation, and
no automatic instrumentation — you build `Metric`, `LogRecord`, and `Span`
values yourself (or use the `gauge()` / `sum_()` / `log_record()` / `span()`
helpers) and hand them to `submit_*` or `export_*`. Profiles are a defined
seam — `SignalKind.PROFILES` exists and carries its `/v1development/profiles`
path — but no encoder implements it and no public method on `OTLPClient` or
`BatchProcessor` accepts it yet; the OTLP profiles format is still in
development upstream, so there is nothing to send.
