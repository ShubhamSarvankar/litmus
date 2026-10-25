"""User-defined object metadata: the x-amz-meta-* headers of a PutObject, as S3 stores
and returns them (S3 User Guide, "Working with object metadata", "User-defined
metadata"; docs/compatibility.md, "User metadata").

Names are stored lower case, without the prefix. A value is stored as text: RFC 2047
encoded words are decoded when it arrives, and a value that is not printable ASCII is
encoded as RFC 2047 when it is returned, which also keeps a control character out of a
response header.
"""

import base64
import binascii
import quopri
import re

from starlette.datastructures import Headers

from server.errors.s3errors import MetadataTooLarge

PREFIX = "x-amz-meta-"
# The documented limit on the user-defined metadata of one PUT request.
MAX_SIZE = 2048

# Charsets an encoded word may name, compared without regard to case. S3 documents
# only "RFC 2047"; its example is UTF-8. US-ASCII and ISO-8859-1 are the other two
# the RFC's own examples and most mail software use.
_CHARSETS = {"utf-8": "utf-8", "us-ascii": "ascii", "iso-8859-1": "latin-1"}
_WORD_RE = r"=\?[^?\s]+\?[BbQq]\?[^?\s]*\?="
_WORD = re.compile(r"=\?([^?\s]+)\?([BbQq])\?([^?\s]*)\?=")
# White space between two encoded words is not part of the text (RFC 2047 6.2).
_BETWEEN_WORDS = re.compile(rf"({_WORD_RE})\s+(?={_WORD_RE})")


def _decode_word(match: re.Match) -> str:
    """The text of one encoded word, or the word itself when it cannot be decoded: an
    unknown charset, an invalid base64 or quoted-printable payload, or bytes that are
    not valid in the charset."""
    charset, encoding, payload = match.groups()
    codec = _CHARSETS.get(charset.lower())
    if codec is None:
        return match.group(0)
    try:
        if encoding in "Bb":
            data = base64.b64decode(payload, validate=True)
        else:
            data = quopri.decodestring(payload.encode("ascii"), header=True)
        return data.decode(codec)
    except (binascii.Error, ValueError):
        return match.group(0)


def decode_value(raw: str) -> str:
    """The text of a request header value. raw is the header read as ISO-8859-1, which
    is how the server reads header bytes.

    Raw non-ASCII bytes that are not RFC 2047 are read as UTF-8 when they are valid
    UTF-8 and as ISO-8859-1 otherwise; RFC 2047 encoded words are decoded."""
    try:
        text = raw.encode("latin-1").decode("utf-8")
    except UnicodeError:
        text = raw
    return _WORD.sub(_decode_word, _BETWEEN_WORDS.sub(r"\1", text))


def encode_value(text: str) -> str:
    """The header value S3 returns: printable ASCII as it is, anything else as one
    RFC 2047 encoded word in UTF-8 and base64."""
    if all(" " <= c <= "~" for c in text):
        return text
    return "=?UTF-8?B?" + base64.b64encode(text.encode("utf-8")).decode("ascii") + "?="


def parse_user_metadata(headers: Headers) -> dict[str, str]:
    """The user metadata of a request: name (lower case, without the prefix) to text.

    A name sent more than once has its values joined with ", ". An empty value is
    kept. A header named only by the prefix has no name and is ignored. Raises
    MetadataTooLarge when the UTF-8 bytes of the names (without the prefix) and the
    values together exceed MAX_SIZE."""
    values: dict[str, list[str]] = {}
    for header, value in headers.items():
        header = header.lower()
        if header.startswith(PREFIX) and len(header) > len(PREFIX):
            values.setdefault(header[len(PREFIX) :], []).append(value)
    metadata = {name: decode_value(", ".join(vs)) for name, vs in values.items()}
    size = sum(len(n.encode("utf-8")) + len(v.encode("utf-8")) for n, v in metadata.items())
    if size > MAX_SIZE:
        raise MetadataTooLarge()
    return metadata


def response_headers(metadata: dict[str, str]) -> dict[str, str]:
    """The headers GET and HEAD return for stored user metadata."""
    return {PREFIX + name: encode_value(value) for name, value in metadata.items()}
