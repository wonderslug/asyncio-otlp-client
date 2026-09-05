"""Tests for the fields added in the model-completeness work."""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from otlp_client.model.common import InstrumentationScope, Resource
from otlp_client.model.logs import LogRecord
from otlp_client.model.metrics import (
    ExponentialHistogramDataPoint,
    HistogramDataPoint,
    NumberDataPoint,
    SummaryDataPoint,
)
from otlp_client.model.traces import Span, SpanEvent, SpanLink

KW_ONLY_TYPES = [
    Resource,
    InstrumentationScope,
    LogRecord,
    NumberDataPoint,
    HistogramDataPoint,
    ExponentialHistogramDataPoint,
    SummaryDataPoint,
    Span,
    SpanEvent,
    SpanLink,
]


@pytest.mark.parametrize("cls", KW_ONLY_TYPES, ids=lambda c: c.__name__)
def test_model_types_are_keyword_only(cls: type) -> None:
    """Every field is kw_only, so field order can never break a caller."""
    assert all(f.kw_only for f in dataclasses.fields(cls)), f"{cls.__name__} has positional fields"


def test_positional_construction_is_rejected() -> None:
    # Routed through an Any-typed alias so mypy does not analyse the call:
    # a `# type: ignore` here would become an `unused-ignore` error instead.
    ctor: Any = Resource
    with pytest.raises(TypeError):
        ctor({"a": "b"})
