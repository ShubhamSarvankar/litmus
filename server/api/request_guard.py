"""Reject requests for S3 operations Litmus does not implement, before any handler runs.

Without this, an unimplemented subresource request falls through to whatever plain
handler matches the path: PUT /{bucket}/{key}?acl became a zero-byte PutObject that
destroyed the object, DELETE /{bucket}?cors deleted the bucket, GET ?acl returned the
object body. Every such request now gets 501 NotImplemented and changes nothing.

Two layers:
  1. Subresource lists, taken from the S3 API reference (each operation's
     identifying query parameter). A listed subresource is rejected for every
     method unless Litmus implements that exact method + subresource.
  2. For methods that change state, an allow-list of the query parameters Litmus
     implements. A subresource missing from the lists above still cannot reach a
     mutating handler.

The same guard rejects bucket names and keys that are path traversal ("." or ".."
segments), since those map outside their directory in the current on-disk layout.
"""

from fastapi import Request
from fastapi.responses import Response

from server.errors.s3errors import (
    InvalidArgument,
    InvalidBucketName,
    NotImplementedOperation,
    S3Error,
    render_error,
)

# Bucket-level operations, by identifying query parameter (S3 API reference).
BUCKET_SUBRESOURCES = frozenset(
    {
        "accelerate",  # Get/PutBucketAccelerateConfiguration
        "acl",  # Get/PutBucketAcl
        "analytics",  # Get/Put/Delete/ListBucketAnalyticsConfiguration(s)
        "cors",  # Get/Put/DeleteBucketCors
        "delete",  # DeleteObjects (POST, implemented)
        "encryption",  # Get/Put/DeleteBucketEncryption
        "intelligent-tiering",  # Get/Put/Delete/ListBucketIntelligentTieringConfiguration(s)
        "inventory",  # Get/Put/Delete/ListBucketInventoryConfiguration(s)
        "lifecycle",  # Get/Put/DeleteBucketLifecycle(Configuration)
        "location",  # GetBucketLocation
        "logging",  # Get/PutBucketLogging
        "metadataConfiguration",  # Create/Get/DeleteBucketMetadataConfiguration
        "metadataInventoryTable",  # UpdateBucketMetadataInventoryTableConfiguration
        "metadataJournalTable",  # UpdateBucketMetadataJournalTableConfiguration
        "metadataTable",  # Create/Get/DeleteBucketMetadataTableConfiguration
        "metrics",  # Get/Put/Delete/ListBucketMetricsConfiguration(s)
        "notification",  # Get/PutBucketNotification(Configuration)
        "object-lock",  # Get/PutObjectLockConfiguration
        "ownershipControls",  # Get/Put/DeleteBucketOwnershipControls
        "policy",  # Get/Put/DeleteBucketPolicy
        "policyStatus",  # GetBucketPolicyStatus
        "publicAccessBlock",  # Get/Put/DeletePublicAccessBlock
        "replication",  # Get/Put/DeleteBucketReplication
        "requestPayment",  # Get/PutBucketRequestPayment
        "session",  # CreateSession
        "tagging",  # Get/Put/DeleteBucketTagging
        "uploads",  # ListMultipartUploads (GET)
        "versioning",  # Get/PutBucketVersioning
        "versions",  # ListObjectVersions (GET, implemented for unversioned buckets)
        "website",  # Get/Put/DeleteBucketWebsite
    }
)

# Object-level operations, by identifying query parameter (S3 API reference).
OBJECT_SUBRESOURCES = frozenset(
    {
        "acl",  # Get/PutObjectAcl
        "attributes",  # GetObjectAttributes
        "legal-hold",  # Get/PutObjectLegalHold
        "renameObject",  # RenameObject
        "restore",  # RestoreObject (POST)
        "retention",  # Get/PutObjectRetention
        "select",  # SelectObjectContent (POST, with select-type)
        "tagging",  # Get/Put/DeleteObjectTagging
        "torrent",  # GetObjectTorrent
        "uploadId",  # UploadPart / Complete / Abort (implemented), ListParts (GET, not)
        "uploads",  # CreateMultipartUpload (POST, implemented)
    }
)

# Subresource requests Litmus implements, as (method, subresource).
_IMPLEMENTED_BUCKET = frozenset({("GET", "versions"), ("POST", "delete")})
_IMPLEMENTED_OBJECT = frozenset(
    {("PUT", "uploadId"), ("POST", "uploadId"), ("DELETE", "uploadId"), ("POST", "uploads")}
)

# Query-parameter sets each mutating method accepts. "x-id" is an operation-name hint
# some SDKs append to every request; it never selects an operation by itself.
_IGNORED = frozenset({"x-id"})
_ALLOWED_MUTATING_BUCKET = {
    "PUT": [frozenset()],
    "DELETE": [frozenset()],
    "POST": [frozenset({"delete"})],
}
_ALLOWED_MUTATING_OBJECT = {
    "PUT": [frozenset(), frozenset({"partNumber", "uploadId"})],
    "POST": [frozenset({"uploads"}), frozenset({"uploadId"})],
    "DELETE": [frozenset(), frozenset({"uploadId"})],
}

# Headers that select an operation Litmus does not implement on an otherwise
# supported request: CopyObject and UploadPartCopy.
_UNIMPLEMENTED_HEADERS = ("x-amz-copy-source",)

_DOT_SEGMENTS = frozenset({".", ".."})


def unsafe_key(key: str) -> bool:
    """A key with a "." or ".." path segment maps outside, or onto another path
    inside, its bucket directory in the current filesystem layout."""
    return any(segment in _DOT_SEGMENTS for segment in key.split("/"))


def _check(method: str, path: str, query_keys: set[str], headers) -> None:
    if path in ("", "/") or path == "/health":
        return
    bucket, _, key = path.lstrip("/").partition("/")
    if bucket in _DOT_SEGMENTS:
        raise InvalidBucketName(extra={"BucketName": bucket})
    if key and unsafe_key(key):
        raise InvalidArgument(extra={"ArgumentName": "key", "ArgumentValue": key, "Key": key})

    if key:
        subresources, implemented, allowed = (
            OBJECT_SUBRESOURCES,
            _IMPLEMENTED_OBJECT,
            _ALLOWED_MUTATING_OBJECT,
        )
        if any(h in headers for h in _UNIMPLEMENTED_HEADERS):
            raise NotImplementedOperation(extra={"Header": "x-amz-copy-source"})
    else:
        subresources, implemented, allowed = (
            BUCKET_SUBRESOURCES,
            _IMPLEMENTED_BUCKET,
            _ALLOWED_MUTATING_BUCKET,
        )

    for name in sorted(query_keys & subresources):
        if (method, name) not in implemented:
            raise NotImplementedOperation(extra={"Subresource": name, "Method": method})

    if method in allowed and (query_keys - _IGNORED) not in allowed[method]:
        raise NotImplementedOperation(
            extra={"Method": method, "QueryParameters": ",".join(sorted(query_keys))}
        )


async def reject_unimplemented(request: Request, call_next):
    """HTTP middleware: answer unimplemented or unsafe requests before routing."""
    try:
        _check(
            request.method,
            request.url.path,
            set(request.query_params.keys()),
            request.headers,
        )
    except S3Error as exc:
        return Response(
            content=render_error(exc), status_code=exc.status_code, media_type="application/xml"
        )
    return await call_next(request)
