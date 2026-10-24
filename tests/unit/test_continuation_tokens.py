import base64
import json

import pytest

from server.api.continuation_tokens import decode_token, encode_token
from server.errors.s3errors import InvalidArgument

KEYS = ["", "a", "a/b", "é", "日本語", "\U0001f600", "\U0010ffff", "", "\x00", '"\\', "%2F"]


@pytest.mark.parametrize("key", KEYS)
def test_after_round_trips(key):
    assert decode_token(encode_token(key, None)) == (key, None)


@pytest.mark.parametrize("key", KEYS)
def test_at_round_trips(key):
    assert decode_token(encode_token(None, key)) == (None, key)


def test_token_is_url_safe_ascii():
    token = encode_token("\U0010ffff/?+&=", None)
    assert token.isascii()
    assert not set(token) & set("+/?&")


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode()


@pytest.mark.parametrize(
    "token",
    [
        "x",
        "!!!!",
        "é",
        _b64(b"not json"),
        _b64(b"\xff\xfe"),
        _b64(b"[]"),
        _b64(b'"after"'),
        _b64(b'{"v":1}'),
        _b64(b'{"v":2,"after":"a"}'),
        _b64(b'{"v":true,"after":"a"}'),
        _b64(b'{"v":1.0,"after":"a"}'),
        _b64(b'{"v":1,"after":1}'),
        _b64(b'{"v":1,"after":null}'),
        _b64(b'{"v":1,"before":"a"}'),
        _b64(b'{"v":1,"after":"a","at":"b"}'),
        _b64(b'{"v":1,"after":"\\ud800"}'),
        _b64(b'{"v": 1, "after": "a"}'),
        _b64(b'{"after":"a","v":1}'),
        # The right content in the standard alphabet or without padding.
        base64.b64encode(json.dumps({"v": 1, "after": "a?>"}).replace(" ", "").encode()).decode(),
        encode_token("a", None).rstrip("="),
        encode_token("a", None) + "\n",
    ],
)
def test_malformed_token_is_invalid_argument(token):
    with pytest.raises(InvalidArgument) as info:
        decode_token(token)
    assert info.value.code == "InvalidArgument"
    assert info.value.message == "The continuation token provided is incorrect"
    assert info.value.extra == {"ArgumentName": "continuation-token", "ArgumentValue": token}
