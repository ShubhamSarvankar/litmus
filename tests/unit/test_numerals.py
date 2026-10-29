"""parse_decimal and parse_signed_decimal (server/numerals.py, decision 37)."""

import pytest

from server.numerals import (
    SATURATED,
    numeral_key,
    numeral_text,
    parse_decimal,
    parse_signed_decimal,
)


def test_saturated_is_10_to_the_18th():
    assert SATURATED == 10**18


@pytest.mark.parametrize(
    "raw, value",
    [
        pytest.param("0", 0, id="zero"),
        pytest.param("1", 1, id="one"),
        pytest.param("000", 0, id="zeros"),
        pytest.param("0001", 1, id="leading-zeros"),
        pytest.param("9" * 18, 10**18 - 1, id="18-nines"),
        pytest.param("9" * 19, SATURATED, id="19-nines"),
        pytest.param("1" + "0" * 18, SATURATED, id="exactly-10-to-the-18th"),
        pytest.param("9" * 4300, SATURATED, id="4300-nines"),
        pytest.param("9" * 4301, SATURATED, id="4301-nines"),
        pytest.param("9" * 5000, SATURATED, id="5000-nines"),
        pytest.param("0" * 4999 + "1", 1, id="4999-zeros-then-1"),
        pytest.param("0" * 5000, 0, id="5000-zeros"),
        pytest.param("0" * 4990 + "9" * 18, 10**18 - 1, id="padded-18-nines"),
    ],
)
def test_digits_are_read_by_value(raw, value):
    assert parse_decimal(raw) == value


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param("", id="empty"),
        pytest.param("+1", id="plus"),
        pytest.param("-1", id="minus"),
        pytest.param(" 1", id="leading-space"),
        pytest.param("1 ", id="trailing-space"),
        pytest.param("1_0", id="underscore"),
        pytest.param("١", id="arabic-indic-digit"),
        pytest.param("1.0", id="fraction"),
        pytest.param("1e3", id="exponent"),
        pytest.param("0x10", id="hex"),
        pytest.param("1\n", id="trailing-newline"),
        pytest.param("²", id="superscript-two"),
        pytest.param("9" * 5000 + "x", id="long-then-letter"),
    ],
)
def test_anything_but_ascii_digits_is_none(raw):
    assert parse_decimal(raw) is None


@pytest.mark.parametrize(
    "raw, value",
    [
        pytest.param("1", 1, id="digits"),
        pytest.param("-1", -1, id="minus-one"),
        pytest.param("-0", 0, id="minus-zero"),
        pytest.param("-" + "0" * 4999 + "1", -1, id="padded-minus-one"),
        pytest.param("-" + "9" * 5000, -SATURATED, id="long-negative"),
        pytest.param("9" * 5000, SATURATED, id="long-positive"),
    ],
)
def test_signed_decimal_takes_one_minus(raw, value):
    assert parse_signed_decimal(raw) == value


@pytest.mark.parametrize("raw", ["", "-", "--1", "+1", "-+1", "- 1", "1-", "-١", "-1.0"])
def test_signed_decimal_refuses_anything_else(raw):
    assert parse_signed_decimal(raw) is None


@pytest.mark.parametrize(
    "raw, text",
    [
        ("7", "7"),
        ("0007", "7"),
        ("0", "0"),
        ("-0", "0"),
        ("-000", "0"),
        ("-0012", "-12"),
        ("9" * 5000, "9" * 5000),
        ("-" + "0" * 4300 + "5", "-5"),
    ],
)
def test_numeral_text_is_the_digits_without_leading_zeros(raw, text):
    assert numeral_text(raw) == text


def test_numeral_key_orders_true_signed_values_however_long():
    ascending = [
        "-" + "9" * 5000,
        "-" + "9" * 4999,
        "-10002",
        "-10001",
        "-1",
        "0",
        "1",
        "10001",
        "10002",
        "9" * 4999,
        "9" * 5000,
    ]
    keys = [numeral_key(t) for t in ascending]
    assert keys == sorted(keys)
    assert len(set(keys)) == len(keys)
    assert numeral_key(numeral_text("-0")) == numeral_key(numeral_text("0"))
    assert numeral_key("-98") < numeral_key("-97") < numeral_key("-9")
