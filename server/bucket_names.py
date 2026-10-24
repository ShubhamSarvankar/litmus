"""General purpose bucket naming rules, as enforced on CreateBucket.

Source: "General purpose bucket naming rules", AWS S3 User Guide,
https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html

Enforced: length 3 to 63; only lowercase letters, digits, "." and "-"; begin and end with
a letter or digit; no two adjacent periods; not formatted as an IP address; no
reserved prefix (xn--, sthree-, amzn-s3-demo-) or suffix (-s3alias, --ol-s3, .mrap,
--x-s3, --table-s3, -an).

Not enforced: the Transfer Acceleration rule (no periods), because Litmus has no
acceleration and a period is otherwise valid; global uniqueness across accounts, since
Litmus has one account (a name already in use is BucketAlreadyExists). "-an" is
rejected because it is allowed only in an account regional namespace, which needs
the x-amz-bucket-namespace header that Litmus does not implement.

Only CreateBucket applies these rules. A bucket already in a store is reachable under
whatever name it has (AWS: buckets created before March 1, 2018 in us-east-1 may have
uppercase letters and underscores), so no other operation validates the name.
"""

import re

MIN_LENGTH = 3
MAX_LENGTH = 63

RESERVED_PREFIXES = ("xn--", "sthree-", "amzn-s3-demo-")
RESERVED_SUFFIXES = ("-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3", "-an")

_CHARACTERS = re.compile(r"[a-z0-9.-]+")
_EDGE = re.compile(r"[a-z0-9]")
# Four dot-separated runs of digits, the shape of 192.168.5.4. Octet values are not
# checked: 999.999.999.999 is refused too, as botocore's own bucket name check does.
_IP_SHAPE = re.compile(r"[0-9]+(\.[0-9]+){3}")


def bucket_name_violation(name: str) -> str | None:
    """The first naming rule `name` breaks, as a short description, or None if the
    name is acceptable for CreateBucket."""
    if not MIN_LENGTH <= len(name) <= MAX_LENGTH:
        return f"length must be {MIN_LENGTH} to {MAX_LENGTH}"
    if not _CHARACTERS.fullmatch(name):
        return "only lowercase letters, digits, periods and hyphens are allowed"
    if not (_EDGE.fullmatch(name[0]) and _EDGE.fullmatch(name[-1])):
        return "must begin and end with a letter or digit"
    if ".." in name:
        return "must not contain two adjacent periods"
    if _IP_SHAPE.fullmatch(name):
        return "must not be formatted as an IP address"
    for prefix in RESERVED_PREFIXES:
        if name.startswith(prefix):
            return f"must not start with {prefix}"
    for suffix in RESERVED_SUFFIXES:
        if name.endswith(suffix):
            return f"must not end with {suffix}"
    return None
