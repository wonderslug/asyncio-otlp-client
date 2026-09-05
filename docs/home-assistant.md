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
  "requirements": ["asyncio-otlp-client==0.1.0"]
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
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from otlp_client import BatchProcessor, OTLPClient, OTLPConfig, Resource

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

    entry.async_on_unload(lambda: hass.async_create_task(processor.__aexit__(None, None, None)))
    return True
```

`OTLPClient.create(config, session=...)` only opens its own `aiohttp.ClientSession`
when `session` is omitted (or `None`); passed one, it uses it as-is and never
closes it, so `async_get_clientsession(hass)`'s lifetime stays HA's to manage.
`BatchProcessor.__aenter__`/`__aexit__` start and stop the background flush
task — `async with BatchProcessor(...) as proc:` is equivalent for code that
does not need to hold the processor across callback boundaries, but a config
entry does, so it drives the same two methods directly instead.

## Feeding state changes as metrics

`submit_metrics` never blocks and never raises, which is what makes it safe to
call from a state-change listener. Register the listener inside
`async_setup_entry` (continuing the function above) so `processor` is captured
by closure — no lookup through `hass.data` needed:

```python
import time

from homeassistant.core import Event, EventStateChangedData
from homeassistant.helpers.event import async_track_state_change_event
from otlp_client import gauge


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
```

`submit_metrics` returns `False` when a record was dropped (queue full) or the
processor is already closed — safe to ignore in a listener that has nowhere to
report failure, which is exactly why `submit_*` is built this way instead of
raising.

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
helpers) and hand them to `submit_*` or `export_*`. Profiles are a defined seam
(`SignalKind.PROFILES`) but calling `export_*` for that signal raises
`NotImplementedError` — the OTLP profiles format is still in development
upstream, so there is nothing to send yet.
