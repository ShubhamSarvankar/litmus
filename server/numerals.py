"""The one parser for a decimal number taken from a request (docs/design/storage-layout.md,
decision 37).

Python 3.12's int() refuses a decimal string of more than 4300 digits with ValueError, so a
request that carries a long numeral must never reach it unchecked. parse_decimal reads the
digits itself and never raises. A value of more than 18 digits once its leading zeros are
stripped is returned as SATURATED, 10**18, which is above every size, count and part number
Litmus compares a request value against (at most 5 GiB, 1000 keys, 10000 parts)."""

import re

SATURATED = 10**18
_MAX_SIGNIFICANT_DIGITS = 18
_DIGITS = re.compile(r"[0-9]+")


def parse_decimal(raw: str) -> int | None:
    """The value of raw when it is one or more ASCII digits 0 to 9 and nothing else (no
    sign, no whitespace, no underscore, no other Unicode digit); None otherwise. Leading
    zeros are stripped first, then a value of more than 18 digits is SATURATED."""
    if _DIGITS.fullmatch(raw) is None:
        return None
    digits = raw.lstrip("0")
    if len(digits) > _MAX_SIGNIFICANT_DIGITS:
        return SATURATED
    return int(digits or "0")


def parse_signed_decimal(raw: str) -> int | None:
    """parse_decimal for ASCII digits with at most one leading "-"; a negative value is
    -SATURATED when it is saturated. None for anything else, a "+" included."""
    magnitude = parse_decimal(raw[1:] if raw.startswith("-") else raw)
    if magnitude is None:
        return None
    return -magnitude if raw.startswith("-") else magnitude


def numeral_text(raw: str) -> str:
    """The canonical text of a numeral that parse_signed_decimal accepted: the digits
    without leading zeros, "0" for zero (never "-0"), and a leading "-" kept. Nothing is
    converted, so the length of the digits does not matter."""
    digits = (raw[1:] if raw.startswith("-") else raw).lstrip("0")
    if not digits:
        return "0"
    return "-" + digits if raw.startswith("-") else digits


def numeral_key(text: str) -> tuple:
    """A sort key for numeral_text's output that orders the true signed values, however
    long the digits: sign first, then length, then the digits, all reversed for a negative
    value. Equal values have equal keys."""
    if text.startswith("-"):
        magnitude = text[1:]
        return (-1, -len(magnitude), tuple(-ord(c) for c in magnitude))
    return (0 if text == "0" else 1, len(text), text)
