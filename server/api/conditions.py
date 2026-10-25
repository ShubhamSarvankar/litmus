"""Read preconditions on GET and HEAD (RFC 9110 13.1 and 13.2.2), and If-Range.

evaluate_read_conditions() takes the request headers and the one row the response will
be built from, and answers whether the response is 304 Not Modified. It raises
PreconditionFailed (412) when If-Match or If-Unmodified-Since fails. if_range_holds()
is step 5, for a GET that carries a Range.

The conditions are evaluated in the order RFC 9110 13.2.2 gives:

1. If-Match; false is 412.
2. If-Unmodified-Since, only when there is no If-Match; false is 412.
3. If-None-Match; false is 304.
4. If-Modified-Since, only when there is no If-None-Match; false is 304.

The S3 API reference documents two combinations, and both follow from this order:
If-Match true with If-Unmodified-Since false is 200, and If-None-Match false with
If-Modified-Since true is 304. A missing bucket or key is answered before any condition
(RFC 9110 13.2.1), by the callers.

5. If-Range, only on GET and only with a Range (13.2.2 step 5): true applies the Range
   (206, or 416 when no byte satisfies it), false ignores it and sends the whole object.
"""

import re
from datetime import UTC, datetime, timedelta

from server.errors.s3errors import PreconditionFailed

# One member of an ETag list: "*", an entity-tag (RFC 9110 8.8.3: an optional W/ and a
# quoted opaque tag, which may hold a comma), or a bare token, which is read as an
# opaque tag sent without its quotes. Clients send both forms.
_MEMBER = re.compile(r'\s*(\*|(?:W/)?"[^"]*"|[^\s,]+)\s*(?:,|$)', re.ASCII)

_IMF_FIXDATE = "%a, %d %b %Y %H:%M:%S GMT"
_RFC850_DATE = "%A, %d-%b-%y %H:%M:%S GMT"
_ASCTIME_DATE = "%a %b %d %H:%M:%S %Y"


def _entity_tags(headers, name: str) -> list[tuple[bool, str, bool]] | None:
    """The members of an If-Match or If-None-Match header (repeated lines are one list,
    RFC 9110 5.3) as (weak, opaque tag, is "*"). None when the header is absent or
    blank, which is no condition."""
    values = [v for v in headers.getlist(name) if v.strip()]
    if not values:
        return None
    tags = []
    for member in _MEMBER.findall(",".join(values)):
        weak = member.startswith("W/")
        tag = member[2:] if weak else member
        if len(tag) >= 2 and tag.startswith('"') and tag.endswith('"'):
            tag = tag[1:-1]
        tags.append((weak, tag, member == "*"))
    return tags


def _matches(tags, etag: str, strong: bool) -> bool:
    """RFC 9110 8.8.3.2. Strong comparison: neither tag is weak and the opaque tags are
    equal character by character. Weak comparison: the opaque tags are equal. The
    stored ETag is never weak. "*" is a member of its own only when it is alone."""
    if len(tags) == 1 and tags[0][2]:
        return True
    opaque = etag.strip('"')
    return any(opaque == tag and not (strong and weak) for weak, tag, _ in tags)


def parse_http_date(headers, name: str) -> datetime | None:
    """The header as a naive UTC datetime, or None when it is absent, repeated, a list of
    dates or not a valid HTTP-date, so the condition is ignored (RFC 9110 13.1.3,
    13.1.4). All three formats of RFC 9110 5.6.7 are accepted; the zone must be GMT."""
    values = headers.getlist(name)
    if len(values) != 1 or not values[0].isascii():
        return None
    value = values[0].strip()
    for fmt in (_IMF_FIXDATE, _RFC850_DATE, _ASCTIME_DATE):
        try:
            parsed = datetime.strptime(value, fmt)
        except ValueError:
            continue
        if fmt == _RFC850_DATE:
            parsed = _expand_two_digit_year(parsed)
        return parsed
    return None


def _expand_two_digit_year(parsed: datetime) -> datetime:
    """RFC 9110 5.6.7: a two-digit year that would be more than 50 years in the future
    is read as the most recent past year with the same last two digits."""
    now = datetime.now(UTC).replace(tzinfo=None)
    year = parsed.year  # strptime put 69-99 in the 1900s and 0-68 in the 2000s
    if year > now.year + 50:
        year -= 100
    elif year < now.year - 50 and year + 100 <= now.year + 50:
        year += 100
    return parsed.replace(year=year)


def evaluate_read_conditions(headers, etag: str, last_modified: datetime) -> bool:
    """True when the response is 304 Not Modified. Raises PreconditionFailed.

    last_modified is compared at the second the Last-Modified header carries, not at
    the microseconds the row stores."""
    modified = last_modified.replace(microsecond=0)

    if_match = _entity_tags(headers, "if-match")
    if if_match is not None:
        if not _matches(if_match, etag, strong=True):
            raise PreconditionFailed()
    else:
        unmodified_since = parse_http_date(headers, "if-unmodified-since")
        if unmodified_since is not None and modified > unmodified_since:
            raise PreconditionFailed()

    if_none_match = _entity_tags(headers, "if-none-match")
    if if_none_match is not None:
        return _matches(if_none_match, etag, strong=False)
    modified_since = parse_http_date(headers, "if-modified-since")
    return modified_since is not None and modified <= modified_since


def if_range_holds(
    headers, etag: str, last_modified: datetime, now: datetime | None = None
) -> bool:
    """RFC 9110 13.1.5: whether the Range of this request is to be applied. True when
    there is no If-Range. The caller asks only for a GET that has a Range: "a server
    MUST ignore an If-Range header field received in a request that does not contain a
    Range header field", and 14.2 defines range handling for GET alone.

    The value is an entity tag or an HTTP-date. An entity tag holds when it is not weak
    and equals the ETag character by character (strong comparison, 8.8.3.2). A date
    holds when it equals Last-Modified exactly, at the second the header carries, and
    Last-Modified is a strong validator: "the condition is false if the date is not a
    strong validator". RFC 9110 8.8.2.2 calls a modification date weak unless it can
    be deduced to be strong; here it is strong once it is at least one second older than
    now, since any later change then gets a different second.

    The RFC does not say what a value is that is neither a valid entity tag nor a valid
    HTTP-date. It does not hold (the whole object is sent), the safe answer when the
    validator cannot be checked. That covers a blank value, a list, "*" and a repeated
    header."""
    values = headers.getlist("if-range")
    if not values:
        return True
    if len(values) != 1:
        return False
    date = parse_http_date(headers, "if-range")
    if date is not None:
        now = now or datetime.now(UTC).replace(tzinfo=None)
        strong = now - last_modified >= timedelta(seconds=1)
        return strong and date == last_modified.replace(microsecond=0)
    tags = _entity_tags(headers, "if-range")
    if tags is None or len(tags) != 1 or tags[0][2]:
        return False
    return _matches(tags, etag, strong=True)
