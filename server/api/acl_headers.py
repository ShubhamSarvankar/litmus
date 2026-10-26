"""ACL and grant request headers, for a Litmus whose buckets all use the Object Ownership
setting BucketOwnerEnforced: ACLs are disabled, the owner has full control and nothing
else is granted.

S3 User Guide, "Controlling ownership of objects and disabling ACLs for your bucket":
BucketOwnerEnforced is the default for a new bucket. S3 API reference, PutObject and
CopyObject: a bucket with that setting "only accept[s] PUT requests that don't specify an
ACL or PUT requests that specify bucket owner full control ACLs"; PutObject adds that
others "fail and return a 400 error with the error code AccessControlListNotSupported".
CreateBucket: setting an ACL needs an Object Ownership other than BucketOwnerEnforced.
User Guide, "Troubleshooting": both error codes and their messages; neither is in the S3
API error code list. docs/compatibility.md records each choice.

A value that is empty after stripping whitespace counts as absent, and values are
compared exactly, case sensitive.
"""

from server.errors.s3errors import (
    AccessControlListNotSupported,
    InvalidBucketAclWithObjectOwnership,
)

ACL_HEADER = "x-amz-acl"
OBJECT_OWNERSHIP_HEADER = "x-amz-object-ownership"
BUCKET_OWNER_ENFORCED = "BucketOwnerEnforced"

# Every grant header is refused, even one that names only the owner: a grant is an ACL.
# PutObject, CreateMultipartUpload and CopyObject define four; CreateBucket adds
# x-amz-grant-write.
_OBJECT_GRANT_HEADERS = (
    "x-amz-grant-full-control",
    "x-amz-grant-read",
    "x-amz-grant-read-acp",
    "x-amz-grant-write-acp",
)
_BUCKET_GRANT_HEADERS = (*_OBJECT_GRANT_HEADERS, "x-amz-grant-write")

# What a BucketOwnerEnforced bucket accepts on an object write. "private" is not named
# by the API reference (a literal reading refuses it); it is accepted because the owner
# having full control and no one else any is exactly what private grants.
_OBJECT_ACLS = frozenset({"private", "bucket-owner-full-control"})
_BUCKET_ACLS = frozenset({"private"})


def _values(headers, name: str):
    return [v for v in (v.strip() for v in headers.getlist(name)) if v]


def _asks_for_acl(headers, grant_headers, accepted) -> bool:
    return any(v not in accepted for v in _values(headers, ACL_HEADER)) or any(
        _values(headers, name) for name in grant_headers
    )


def check_object_write(headers) -> None:
    """PutObject, CreateMultipartUpload and CopyObject, after the bucket is known to
    exist and before anything else about the request is read or parsed."""
    if _asks_for_acl(headers, _OBJECT_GRANT_HEADERS, _OBJECT_ACLS):
        raise AccessControlListNotSupported


def check_create_bucket(headers) -> None:
    """CreateBucket, after the name is validated and before the existence check."""
    if _asks_for_acl(headers, _BUCKET_GRANT_HEADERS, _BUCKET_ACLS):
        raise InvalidBucketAclWithObjectOwnership
