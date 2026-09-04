"""OTLP/JSON primitive encoding rules.

These are the rules most hand-written OTLP JSON gets wrong:
64-bit fields are decimal strings, trace and span ids are hex rather than
base64, and enums are integers.
"""

from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from typing import Any

from otlp_client.model.common import AnyValue


def u64(value: int) -> str:
    """Render a 64-bit field as the decimal string the spec requires."""
    return str(value)


def hex_id(value: bytes) -> str:
    """Render a traceId or spanId as hex. These are never base64."""
    return value.hex()


def b64(value: bytes) -> str:
    """Render any other bytes field as base64."""
    return base64.b64encode(value).decode("ascii")


def encode_any_value(value: AnyValue) -> dict[str, Any]:
    """Encode one AnyValue.

    `bool` is checked before `int` deliberately: `bool` is a subclass of `int`,
    so the reverse order would encode `True` as `{"intValue": "1"}`.
    """
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": u64(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, str):
        return {"stringValue": value}
    if isinstance(value, bytes):
        return {"bytesValue": b64(value)}
    if isinstance(value, Mapping):
        return {"kvlistValue": {"values": encode_attributes(value)}}
    if isinstance(value, Sequence):
        return {"arrayValue": {"values": [encode_any_value(v) for v in value]}}
    raise TypeError(f"unsupported attribute value type: {type(value)!r}")


def encode_attributes(attributes: Mapping[str, AnyValue]) -> list[dict[str, Any]]:
    """Encode an attribute mapping as an OTLP KeyValue list."""
    return [{"key": key, "value": encode_any_value(value)} for key, value in attributes.items()]


def omit_empty(obj: dict[str, Any]) -> dict[str, Any]:
    """Drop absent fields to keep payloads small.

    Removes None, empty strings, and empty containers. Zero and False are kept
    so that an explicitly-zero measurement is never confused with an absent one.
    """
    return {k: v for k, v in obj.items() if v is not None and v != "" and v != [] and v != {}}
