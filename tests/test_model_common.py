import dataclasses

import pytest

from otlp_client.model.common import InstrumentationScope, Resource


def test_resource_holds_attributes() -> None:
    r = Resource(attributes={"service.name": "hass", "port": 8123})
    assert r.attributes["service.name"] == "hass"
    assert r.dropped_attributes_count == 0


def test_resource_is_frozen() -> None:
    r = Resource(attributes={})
    with pytest.raises(dataclasses.FrozenInstanceError):
        r.dropped_attributes_count = 5  # type: ignore[misc]


def test_scope_defaults() -> None:
    s = InstrumentationScope(name="otlp_client")
    assert s.name == "otlp_client"
    assert s.version is None
    assert s.attributes == {}
