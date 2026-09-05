"""OTLP/HTTP transport built on aiohttp."""

from __future__ import annotations

import asyncio
import gzip
import ssl
import time

import aiohttp

from otlp_client.config import Compression, OTLPConfig
from otlp_client.encoding.base import Encoder
from otlp_client.outcomes import ExportOutcome, Permanent, Retryable, Success
from otlp_client.retry import RETRYABLE_STATUSES, parse_retry_after
from otlp_client.signals import SignalKind


def _build_ssl_context(config: OTLPConfig) -> ssl.SSLContext | None:
    """Build the TLS context. Blocking: only call via asyncio.to_thread."""
    if not (
        config.certificate_file or config.client_certificate_file or config.insecure_skip_verify
    ):
        return None
    context = ssl.create_default_context(cafile=config.certificate_file)
    if config.insecure_skip_verify:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    if config.client_certificate_file:
        context.load_cert_chain(config.client_certificate_file, config.client_key_file)
    return context


class HTTPTransport:
    """Ships encoded bytes over OTLP/HTTP and classifies the response."""

    def __init__(
        self,
        config: OTLPConfig,
        encoder: Encoder,
        *,
        session: aiohttp.ClientSession,
        owns_session: bool = False,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self._config = config
        self._encoder = encoder
        self._session = session
        self._owns_session = owns_session
        self._ssl = ssl_context
        self._timeout = aiohttp.ClientTimeout(total=config.timeout)

    @classmethod
    async def create(
        cls,
        config: OTLPConfig,
        encoder: Encoder,
        *,
        session: aiohttp.ClientSession | None = None,
    ) -> HTTPTransport:
        """Build a transport, doing all blocking setup off the event loop.

        Pass `session` whenever one exists. Home Assistant integrations must
        pass `async_get_clientsession(hass)` rather than letting this create one.
        """
        ssl_context = await asyncio.to_thread(_build_ssl_context, config)
        owns = session is None
        return cls(
            config,
            encoder,
            session=session or aiohttp.ClientSession(),
            owns_session=owns,
            ssl_context=ssl_context,
        )

    async def _compress(self, payload: bytes) -> bytes:
        """gzip the payload, off-loop when it is large enough to matter."""
        if len(payload) >= self._config.gzip_threshold:
            return await asyncio.to_thread(gzip.compress, payload)
        return gzip.compress(payload)

    async def send(self, kind: SignalKind, payload: bytes) -> ExportOutcome:
        headers = {**self._config.headers, "Content-Type": self._encoder.content_type}
        body = payload
        if self._config.compression is Compression.GZIP:
            body = await self._compress(payload)
            headers["Content-Encoding"] = "gzip"

        try:
            async with self._session.post(
                self._config.endpoint_for(kind),
                data=body,
                headers=headers,
                timeout=self._timeout,
                ssl=self._ssl if self._ssl is not None else True,
            ) as response:
                raw = await response.read()
                if 200 <= response.status < 300:
                    partial = self._encoder.decode_response(kind, raw)
                    return partial if partial is not None else Success()
                message = raw.decode("utf-8", "replace")[:512]
                if response.status in RETRYABLE_STATUSES:
                    header = response.headers.get("Retry-After", "")
                    return Retryable(
                        status=response.status,
                        message=message,
                        retry_after=parse_retry_after(header, now_wall=time.time())
                        if header
                        else None,
                    )
                return Permanent(status=response.status, message=message)
        except (aiohttp.ClientError, TimeoutError) as exc:
            return Retryable(message=f"{type(exc).__name__}: {exc}")

    async def aclose(self) -> None:
        """Close the session only if this transport created it."""
        if self._owns_session:
            await self._session.close()
