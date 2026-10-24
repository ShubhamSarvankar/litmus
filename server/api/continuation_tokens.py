"""ListObjectsV2 continuation tokens.

A token is opaque to the client: URL-safe base64 of a small JSON object that records
where the next page starts, either after a key ({"v": 1, "after": key}) or at a key,
inclusive ({"v": 1, "at": key}), the second being the successor of the common prefix
a page ended on (docs/design/storage-layout.md §13). JSON carries any key, including
non-ASCII ones, and the decoder accepts nothing else: a malformed token is an
InvalidArgument error, never a server error.
"""

import base64
import binascii
import json

from server.errors.s3errors import InvalidContinuationToken

_VERSION = 1


def encode_token(after: str | None, at: str | None) -> str:
    position = {"after": after} if at is None else {"at": at}
    raw = json.dumps({"v": _VERSION, **position}, ensure_ascii=False, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def decode_token(token: str) -> tuple[str | None, str | None]:
    """(after, at) from a token; exactly one is a string."""
    try:
        raw = base64.b64decode(token.encode("ascii"), altchars=b"-_", validate=True)
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeError, binascii.Error, ValueError):
        raise _invalid(token) from None
    if not isinstance(value, dict) or value.get("v") != _VERSION or len(value) != 2:
        raise _invalid(token)
    ((name, key),) = ((k, v) for k, v in value.items() if k != "v")
    if name not in ("after", "at") or not isinstance(key, str):
        raise _invalid(token)
    try:
        # JSON can spell lone surrogates ("\ud800"), which no stored key contains and
        # SQLite cannot bind.
        key.encode("utf-8")
    except UnicodeEncodeError:
        raise _invalid(token) from None
    position = (key, None) if name == "after" else (None, key)
    # Only the exact encoding this server produces: no "v": true or 1.0, no other
    # spacing or key order, no standard base64 alphabet or missing padding.
    if encode_token(*position) != token:
        raise _invalid(token)
    return position


def _invalid(token: str) -> InvalidContinuationToken:
    return InvalidContinuationToken(
        extra={"ArgumentName": "continuation-token", "ArgumentValue": token}
    )
