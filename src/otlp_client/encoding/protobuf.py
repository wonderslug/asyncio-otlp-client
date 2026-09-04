"""OTLP/protobuf encoding. Requires the `protobuf` extra.

Placeholder: the real encoder lands in a later task. Every `opentelemetry.proto`
import must stay inside a function so that importing this module remains free
for a core-only install.
"""

from __future__ import annotations

from otlp_client.errors import OTLPConfigError

_MISSING = (
    "the protobuf encoder needs the optional extra: "
    "pip install 'asyncio-otlp-client[protobuf]'"
)


def build_protobuf_encoder() -> object:
    """Not yet implemented; raises with the install hint."""
    raise OTLPConfigError(_MISSING)
