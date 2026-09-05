"""Exercise the client against an already-running OTLP collector.

Expects a collector listening at the endpoint from OTEL_EXPORTER_OTLP_* env
vars, defaulting to http://localhost:4318 (http/json) if unset. Start one
with `make collector-up`, or point OTEL_EXPORTER_OTLP_ENDPOINT elsewhere.

Run with: uv run python examples/basic_usage.py
"""

import asyncio
import dataclasses
import time

from aiohttp import ClientSession

from otlp_client import (
    OTLPClient,
    OTLPConfig,
    OTLPPermanentError,
    OTLPTransportError,
    Resource,
    SeverityNumber,
    gauge,
    log_record,
    span,
)

RESOURCE = Resource(attributes={"service.name": "asyncio-otlp-client-example"})


async def main() -> None:
    config = dataclasses.replace(OTLPConfig.from_env(), resource=RESOURCE)

    now = time.time_ns()
    trace_id = bytes.fromhex("0102030405060708090a0b0c0d0e0f10")
    span_id = bytes.fromhex("1112131415161718")

    async with ClientSession() as session:
        client = await OTLPClient.create(config, session=session)
        try:
            metrics_outcome = await client.export_metrics(
                [gauge("example.temperature", 21.5, unit="Cel", time_unix_nano=now)]
            )
            print(f"metrics: {metrics_outcome}")

            logs_outcome = await client.export_logs(
                [
                    log_record(
                        "hello from asyncio-otlp-client",
                        time_unix_nano=now,
                        severity=SeverityNumber.INFO,
                    )
                ]
            )
            print(f"logs:    {logs_outcome}")

            traces_outcome = await client.export_traces(
                [
                    span(
                        "example-span",
                        trace_id=trace_id,
                        span_id=span_id,
                        start_time_unix_nano=now,
                        end_time_unix_nano=now + 1_000_000,
                    )
                ]
            )
            print(f"traces:  {traces_outcome}")
        except (OTLPTransportError, OTLPPermanentError) as exc:
            print(f"export failed against {config.endpoint}: {exc}")
        finally:
            await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
