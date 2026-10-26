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

from server.api.aws_chunked import (
    CONTENT_SHA256_HEADER,
    DECODED_LENGTH_HEADER,
    TRAILER_HEADER,
    is_framed,
    trailer_is_supported,
)
from server.api.copy_source import COPY_SOURCE_HEADER, parse_copy_source
from server.checksums import unsupported_value_header
from server.config import settings
from server.errors.s3errors import (
    AccessDenied,
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

# Subresource requests Litmus implements, as (method, subresource). GET ?tagging is
# GetObjectTagging, which Litmus answers with an empty tag set: it stores no tags, so
# that is always true. It is not tagging support; Put/DeleteObjectTagging and the bucket
# tagging operations stay refused.
_IMPLEMENTED_BUCKET = frozenset({("GET", "versions"), ("POST", "delete")})
_IMPLEMENTED_OBJECT = frozenset(
    {
        ("PUT", "uploadId"),
        ("POST", "uploadId"),
        ("DELETE", "uploadId"),
        ("POST", "uploads"),
        ("GET", "tagging"),
    }
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

# Conditional DELETE headers. Litmus does not evaluate them on every DELETE, and
# ignoring one would delete an object (or abort an upload) the client asked to keep. From
# the S3 API reference: DeleteObject takes If-Match, x-amz-if-match-last-modified-time
# and x-amz-if-match-size; AbortMultipartUpload takes x-amz-if-match-initiated-time. The
# reference lists no other conditional headers for these operations; If-None-Match,
# If-Modified-Since and If-Unmodified-Since (RFC 7232 preconditions) are refused too,
# so no precondition on a DELETE is ever silently dropped. All of them are refused on
# AbortMultipartUpload.
CONDITIONAL_DELETE_HEADERS = (
    "if-match",
    "x-amz-if-match-last-modified-time",
    "x-amz-if-match-size",
    "x-amz-if-match-initiated-time",
    "if-none-match",
    "if-modified-since",
    "if-unmodified-since",
)

# DeleteObject evaluates If-Match in its commit (docs/design/storage-layout.md §10), so
# it refuses the rest. AWS supports only If-Match on DeleteObject in general purpose
# buckets; x-amz-if-match-last-modified-time and x-amz-if-match-size are documented for
# directory buckets only, and Litmus has none, so refusing them is deliberate.
UNEVALUATED_DELETE_OBJECT_HEADERS = tuple(h for h in CONDITIONAL_DELETE_HEADERS if h != "if-match")

# Conditional write headers, from the S3 API reference: PutObject takes If-Match,
# If-None-Match and x-amz-write-offset-bytes (append at an offset that must equal the
# current size); CompleteMultipartUpload takes If-Match and If-None-Match. Ignoring one
# would overwrite an object the client asked to keep (or replace it with only the
# appended bytes), so those Litmus does not evaluate are refused. PutObject and
# CompleteMultipartUpload evaluate If-Match and If-None-Match in their commit
# (docs/design/storage-layout.md §10), so only x-amz-write-offset-bytes, on PutObject,
# is refused.
UNEVALUATED_PUT_OBJECT_HEADERS = ("x-amz-write-offset-bytes",)

# x-amz-copy-source selects CopyObject on a PUT with no query parameters and
# UploadPartCopy on a PUT with partNumber and uploadId. On any other request it selects
# an operation Litmus does not implement (a header on a method that takes none, or on
# a query that is neither), and is refused. A source Litmus cannot name (an ARN, a
# version ID other than "null") is refused too (server/api/copy_source.py), and so is a
# CopyObject that would replace the tag set with tags: Litmus stores none, so they would
# be dropped. x-amz-tagging-directive: REPLACE with a blank x-amz-tagging (an empty tag
# set) and COPY proceed.
_TAGGING_DIRECTIVE = "x-amz-tagging-directive"
_TAGGING = "x-amz-tagging"
_UPLOAD_PART_QUERY = frozenset({"partNumber", "uploadId"})

# x-amz-tagging on PutObject and CreateMultipartUpload sets the new object's tags
# (S3 API reference: "The tag-set must be encoded as URL Query parameters"). Litmus
# stores none, so a non-empty value is refused, unparsed, rather than dropped. An empty
# value asks for no tags and is accepted. A PUT with x-amz-copy-source is CopyObject,
# which decides the header with its directive (_check_copy_object); UploadPart and
# UploadPartCopy take no such header.

# The SSE-C headers that unlock a source encrypted with a customer-provided key, on
# CopyObject and UploadPartCopy. Litmus stores nothing encrypted, so a request that
# supplies a key for the source asks for something it cannot do; ignoring the headers
# would copy a source the client believes is encrypted without saying so.
_COPY_SOURCE_SSE_C_PREFIX = "x-amz-copy-source-server-side-encryption-customer-"

# A chunk-framed (aws-chunked) body, as S3 identifies one: x-amz-content-sha256 names a
# STREAMING- payload, and x-amz-decoded-content-length gives the size without the
# framing (S3 User Guide, "Checking object integrity for data uploads", trailing
# checksum headers). Content-Encoding: aws-chunked is optional there, so it is not a
# marker. botocore 1.36+ and AWS CLI 2.23+ send such bodies for PutObject and
# UploadPart. Litmus decodes exactly one framing (server/api/aws_chunked.py):
# STREAMING-UNSIGNED-PAYLOAD-TRAILER with the single trailer x-amz-checksum-crc64nvme.
# Everything else is refused, because storing such a body as it arrives would keep the
# chunk sizes (and signatures, and trailer) as object bytes: the signed
# STREAMING-AWS4-HMAC-SHA256-* variants, x-amz-decoded-content-length under any other
# x-amz-content-sha256, the streaming value without a supported x-amz-trailer.
_STREAMING_PAYLOAD_PREFIX = "STREAMING-"
_DECODED_LENGTH_HEADER = DECODED_LENGTH_HEADER

# Bucket owner condition (S3 User Guide, "Verifying bucket ownership with bucket owner
# condition"). x-amz-expected-bucket-owner names the owner of the target bucket (the
# destination, on CopyObject and UploadPartCopy); x-amz-source-expected-bucket-owner
# names the source bucket's owner and exists on those two operations only. CreateBucket
# and ListBuckets ignore both. A value that differs from S3_OWNER_ID is AccessDenied.
EXPECTED_OWNER_HEADER = "x-amz-expected-bucket-owner"
SOURCE_EXPECTED_OWNER_HEADER = "x-amz-source-expected-bucket-owner"

# Bucket names S3's naming rules forbid and that would read as path segments.
_DOT_SEGMENTS = frozenset({".", ".."})


def _check_framing(headers, decodable: bool) -> None:
    """Refuse every chunk-framed request except the one framing Litmus decodes, and that
    one only where it is decoded: decodable is true for PutObject and UploadPart alone. A
    framed CreateBucket, DeleteObjects, CreateMultipartUpload or CompleteMultipartUpload,
    or any other request, is refused like an unsupported framing, since its body would
    otherwise be ignored, parsed as plain XML or stored with the framing in it."""
    content_sha256 = headers.get(CONTENT_SHA256_HEADER, "")
    if decodable and is_framed(headers):
        if not trailer_is_supported(headers):
            raise NotImplementedOperation(
                extra={"Header": TRAILER_HEADER, "Value": ",".join(headers.getlist(TRAILER_HEADER))}
            )
        return
    if content_sha256.upper().startswith(_STREAMING_PAYLOAD_PREFIX):
        raise NotImplementedOperation(
            extra={"Header": CONTENT_SHA256_HEADER, "Value": content_sha256}
        )
    if _DECODED_LENGTH_HEADER in headers:
        raise NotImplementedOperation(extra={"Header": _DECODED_LENGTH_HEADER})


def _check_copy_source_encryption(headers) -> None:
    for name in headers.keys():
        if name.lower().startswith(_COPY_SOURCE_SSE_C_PREFIX):
            raise NotImplementedOperation(extra={"Header": name.lower()})


def _check_copy_source(headers) -> None:
    """Refuse a source Litmus cannot name. A malformed source is not decided here: the
    handler answers it, after the destination bucket."""
    try:
        parse_copy_source(", ".join(headers.getlist(COPY_SOURCE_HEADER)))
    except InvalidArgument:
        pass


def _check_copy_object(headers) -> None:
    """Refuse the CopyObject requests Litmus cannot honor."""
    _check_copy_source(headers)
    if headers.get(_TAGGING_DIRECTIVE) == "REPLACE" and any(
        v.strip() for v in headers.getlist(_TAGGING)
    ):
        raise NotImplementedOperation(extra={"Header": _TAGGING})


def _check_new_object_tags(headers) -> None:
    """Refuse a PutObject or CreateMultipartUpload that asks for tags."""
    if any(v.strip() for v in headers.getlist(_TAGGING)):
        raise NotImplementedOperation(extra={"Header": _TAGGING})


def _takes_owner_condition(method: str, key: str, query_keys: set[str]) -> bool:
    """True for the 17 operations that define x-amz-expected-bucket-owner. The caller
    has already refused everything Litmus does not implement, so a GET or HEAD that
    reaches here is a read, and the other methods are matched by their exact query.
    CreateBucket (PUT /{bucket} with no key and no query) and anything else, such as an
    unknown method, is not one of them."""
    query_keys = query_keys - _IGNORED
    if method in ("GET", "HEAD"):
        return True
    if key:
        return query_keys in _ALLOWED_MUTATING_OBJECT.get(method, [])
    if method == "PUT":  # CreateBucket
        return False
    return query_keys in _ALLOWED_MUTATING_BUCKET.get(method, [])


def _owner_mismatch(headers, name: str) -> bool:
    """A present, non-blank header whose stripped value is not S3_OWNER_ID. The
    comparison is exact: case sensitive, and the value is not validated as an account
    ID. An empty or whitespace-only value counts as absent."""
    value = ", ".join(headers.getlist(name)).strip()
    return bool(value) and value != settings.owner_id


def _check_expected_owner(method: str, key: str, query_keys: set[str], headers) -> None:
    if not _takes_owner_condition(method, key, query_keys):
        return
    if _owner_mismatch(headers, EXPECTED_OWNER_HEADER):
        raise AccessDenied
    if (
        method == "PUT"
        and COPY_SOURCE_HEADER in headers
        and _owner_mismatch(headers, SOURCE_EXPECTED_OWNER_HEADER)
    ):
        raise AccessDenied


def _check(method: str, path: str, query, headers) -> None:
    query_keys = set(query.keys())
    if path == "/health":
        return
    bucket, _, key = path.lstrip("/").partition("/")
    if bucket in _DOT_SEGMENTS:
        raise InvalidBucketName(extra={"BucketName": bucket})

    # PutObject (no subresource) and UploadPart (partNumber and uploadId) are the only
    # requests whose framed body is decoded.
    is_object_write = (
        method == "PUT" and bool(key) and (query_keys - _IGNORED) in _ALLOWED_MUTATING_OBJECT["PUT"]
    )
    _check_framing(headers, decodable=is_object_write)
    if path in ("", "/"):
        return
    if is_object_write and (name := unsupported_value_header(headers)):
        raise NotImplementedOperation(extra={"Header": name})

    if method == "POST" and not key and (query_keys - _IGNORED) == {"delete"}:
        # DeleteObjects: checksum claims over the XML body, the same algorithms.
        if name := unsupported_value_header(headers):
            raise NotImplementedOperation(extra={"Header": name})

    if key:
        subresources, implemented, allowed = (
            OBJECT_SUBRESOURCES,
            _IMPLEMENTED_OBJECT,
            _ALLOWED_MUTATING_OBJECT,
        )
        if COPY_SOURCE_HEADER in headers:
            copy_query = query_keys - _IGNORED
            if method != "PUT" or copy_query not in (set(), _UPLOAD_PART_QUERY):
                raise NotImplementedOperation(extra={"Header": COPY_SOURCE_HEADER})
            _check_copy_source_encryption(headers)
            if copy_query:
                _check_copy_source(headers)
            else:
                _check_copy_object(headers)
        elif (method == "PUT" and not (query_keys - _IGNORED)) or (
            method == "POST" and (query_keys - _IGNORED) == {"uploads"}
        ):
            _check_new_object_tags(headers)
        if method == "DELETE":
            conditional = (
                CONDITIONAL_DELETE_HEADERS
                if "uploadId" in query_keys
                else UNEVALUATED_DELETE_OBJECT_HEADERS
            )
        elif method == "PUT" and "uploadId" not in query_keys:  # PutObject, not UploadPart
            conditional = UNEVALUATED_PUT_OBJECT_HEADERS
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

    # GetObjectTagging takes ?tagging and versionId (already checked above) and nothing
    # else: partNumber, response-* and the like belong to GetObject, so they are refused
    # rather than ignored.
    if (
        key
        and method == "GET"
        and "tagging" in query_keys
        and (query_keys - _IGNORED) != {"tagging"}
    ):
        raise NotImplementedOperation(
            extra={"Method": method, "QueryParameters": ",".join(sorted(query_keys))}
        )

    if method in allowed and (query_keys - _IGNORED) not in allowed[method]:
        raise NotImplementedOperation(
            extra={"Method": method, "QueryParameters": ",".join(sorted(query_keys))}
        )

    # Last, so every 501 above beats it. Still before routing, so before any body read.
    _check_expected_owner(method, key, query_keys, headers)


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
