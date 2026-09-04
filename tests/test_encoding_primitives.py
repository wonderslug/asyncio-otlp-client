from otlp_client.encoding.primitives import (
    b64,
    encode_any_value,
    encode_attributes,
    hex_id,
    omit_empty,
    u64,
)


def test_u64_renders_as_decimal_string() -> None:
    assert u64(1700000000000000000) == "1700000000000000000"
    assert u64(0) == "0"


def test_hex_id_is_lowercase_hex_not_base64() -> None:
    assert hex_id(bytes.fromhex("0102030405060708")) == "0102030405060708"


def test_other_bytes_use_base64() -> None:
    assert b64(b"\x01\x02") == "AQI="


def test_bool_encodes_as_bool_not_int() -> None:
    # bool is a subclass of int; checking int first would silently corrupt this.
    assert encode_any_value(True) == {"boolValue": True}
    assert encode_any_value(1) == {"intValue": "1"}


def test_int_value_is_a_string_and_double_is_a_number() -> None:
    assert encode_any_value(7) == {"intValue": "7"}
    assert encode_any_value(7.5) == {"doubleValue": 7.5}


def test_string_and_bytes_values() -> None:
    assert encode_any_value("hi") == {"stringValue": "hi"}
    assert encode_any_value(b"\x01\x02") == {"bytesValue": "AQI="}


def test_array_and_kvlist_values() -> None:
    assert encode_any_value([1, "a"]) == {
        "arrayValue": {"values": [{"intValue": "1"}, {"stringValue": "a"}]}
    }
    assert encode_any_value({"k": 2}) == {
        "kvlistValue": {"values": [{"key": "k", "value": {"intValue": "2"}}]}
    }


def test_encode_attributes_produces_key_value_list() -> None:
    assert encode_attributes({"service.name": "hass"}) == [
        {"key": "service.name", "value": {"stringValue": "hass"}}
    ]


def test_encode_attributes_of_empty_mapping_is_empty_list() -> None:
    assert encode_attributes({}) == []


def test_omit_empty_drops_none_and_empty_containers_but_keeps_zero() -> None:
    assert omit_empty({"a": None, "b": "", "c": [], "d": {}, "e": 0, "f": "x"}) == {
        "e": 0,
        "f": "x",
    }
