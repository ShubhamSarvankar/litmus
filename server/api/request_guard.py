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

The same guard rejects the bucket names "." and "..", which S3's bucket naming
rules forbid. Keys are opaque: a key with "." or ".." segments is stored as it is,
since no key is ever part of a filesystem path (docs/design/storage-layout.md R16).
"""

from fastapi import Request
from fastapi.responses import Response

from server.errors.s3errors import (
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

# versionId. Litmus has no versioning, so every bucket is unversioned, and S3 gives
# every object in an unversioned bucket the version ID "null" (S3 User Guide, "How S3
# Versioning works": "If S3 Versioning is not enabled, Amazon S3 sets the value of the
# version ID to null"). versionId=null therefore names the current object, and GET,
# HEAD and DELETE accept it as if it were absent. Any other version ID cannot name an
# existing version; the API reference does not say which error S3 returns for it on
# an unversioned bucket, so it gets 501, as does versionId on any other method.
NULL_VERSION_ID = "null"
_NULL_VERSION_METHODS = frozenset({"GET", "HEAD", "DELETE"})

# Conditional DELETE headers. Litmus does not evaluate them, and ignoring one would
# delete an object (or abort an upload) the client asked to keep. From the S3 API
# reference: DeleteObject takes If-Match, x-amz-if-match-last-modified-time and
# x-amz-if-match-size; AbortMultipartUpload takes x-amz-if-match-initiated-time. The
# reference lists no other conditional headers for these operations; If-None-Match,
# If-Modified-Since and If-Unmodified-Since (RFC 7232 preconditions) are refused too,
# so no precondition on a DELETE is ever silently dropped.
CONDITIONAL_DELETE_HEADERS = (
    "if-match",
    "x-amz-if-match-last-modified-time",
    "x-amz-if-match-size",
    "x-amz-if-match-initiated-time",
    "if-none-match",
    "if-modified-since",
    "if-unmodified-since",
)

# Conditional write headers, from the S3 API reference: PutObject takes If-Match,
# If-None-Match and x-amz-write-offset-bytes (append at an offset that must equal the
# current size); CompleteMultipartUpload takes If-Match and If-None-Match. Litmus does
# not evaluate them, and ignoring one would overwrite an object the client asked to
# keep (or replace it with only the appended bytes). The storage layout now writes new
# bytes to a fresh path and checks nothing in the commit yet; evaluating preconditions
# in that commit (docs/design/storage-layout.md §10) is later work.
CONDITIONAL_PUT_OBJECT_HEADERS = ("if-match", "if-none-match", "x-amz-write-offset-bytes")
CONDITIONAL_COMPLETE_HEADERS = ("if-match", "if-none-match")

# Headers that select an operation Litmus does not implement on an otherwise
# supported request: CopyObject and UploadPartCopy.
_UNIMPLEMENTED_HEADERS = ("x-amz-copy-source",)

# A chunk-framed (aws-chunked) body, as S3 identifies one: x-amz-content-sha256 names a
# STREAMING- payload, and x-amz-decoded-content-length gives the size without the
# framing (S3 User Guide, "Checking object integrity for data uploads", trailing
# checksum headers). Content-Encoding: aws-chunked is optional there, so it is not a
# marker. botocore 1.36+ and AWS CLI 2.23+ send such bodies for PutObject and
# UploadPart to https endpoints. Litmus does not decode the framing; storing the body
# as it arrives would keep the chunk sizes and trailer as object bytes.
_STREAMING_PAYLOAD_PREFIX = "STREAMING-"
_DECODED_LENGTH_HEADER = "x-amz-decoded-content-length"

# Bucket names S3's naming rules forbid and that would read as path segments.
_DOT_SEGMENTS = frozenset({".", ".."})


def _check(method: str, path: str, query, headers) -> None:
    query_keys = set(query.keys())
    if path in ("", "/") or path == "/health":
        return
    bucket, _, key = path.lstrip("/").partition("/")
    if bucket in _DOT_SEGMENTS:
        raise InvalidBucketName(extra={"BucketName": bucket})

    if key:
        subresources, implemented, allowed = (
            OBJECT_SUBRESOURCES,
            _IMPLEMENTED_OBJECT,
            _ALLOWED_MUTATING_OBJECT,
        )
        if any(h in headers for h in _UNIMPLEMENTED_HEADERS):
            raise NotImplementedOperation(extra={"Header": "x-amz-copy-source"})
        if method == "PUT":  # PutObject and UploadPart
            content_sha256 = headers.get("x-amz-content-sha256", "")
            if content_sha256.upper().startswith(_STREAMING_PAYLOAD_PREFIX):
                raise NotImplementedOperation(
                    extra={"Header": "x-amz-content-sha256", "Value": content_sha256}
                )
            if _DECODED_LENGTH_HEADER in headers:
                raise NotImplementedOperation(extra={"Header": _DECODED_LENGTH_HEADER})
        if method == "DELETE":
            conditional = CONDITIONAL_DELETE_HEADERS
        elif method == "PUT" and "uploadId" not in query_keys:  # PutObject, not UploadPart
            conditional = CONDITIONAL_PUT_OBJECT_HEADERS
        elif method == "POST" and "uploadId" in query_keys:  # CompleteMultipartUpload
            conditional = CONDITIONAL_COMPLETE_HEADERS
        else:
            conditional = ()
        for h in conditional:
            if h in headers:
                raise NotImplementedOperation(extra={"Header": h, "Method": method})
        if "versionId" in query_keys:
            if method not in _NULL_VERSION_METHODS or any(
                v != NULL_VERSION_ID for v in query.getlist("versionId")
            ):
                raise NotImplementedOperation(extra={"Parameter": "versionId", "Method": method})
            query_keys.discard("versionId")
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
            request.query_params,
            request.headers,
        )
    except S3Error as exc:
        return Response(
            content=render_error(exc), status_code=exc.status_code, media_type="application/xml"
        )
    return await call_next(request)
