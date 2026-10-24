import xml.etree.ElementTree as ET
from collections.abc import Callable
from datetime import UTC, datetime
from urllib.parse import quote

from fastapi import APIRouter, Request
from fastapi.responses import Response

from server.api.continuation_tokens import decode_token, encode_token
from server.api.request_guard import NULL_VERSION_ID
from server.api.request_xml import parse_request_xml
from server.bucket_names import bucket_name_violation
from server.config import settings
from server.crash_points import (
    BUCKET_DELETE_AFTER_COMMIT,
    DELETE_OBJECTS_AFTER_COMMIT,
    crash_point,
)
from server.errors.s3errors import (
    BucketAlreadyExists,
    BucketNotEmpty,
    InvalidArgument,
    InvalidBucketName,
    InvalidEncodingType,
    NoSuchBucket,
    NotImplementedOperation,
)
from server.metadata.base import BucketHasObjects, BucketMissing, ListPage
from server.storage.lifecycle import collect_garbage

router = APIRouter()

# S3's default and largest max-keys.
MAX_KEYS = 1000

# Per-key conditions a DeleteObjects <Object> may carry (ObjectIdentifier).
_CONDITIONAL_DELETE_ELEMENTS = ("ETag", "LastModifiedTime", "Size")

# The one encoding-type value S3 documents (ListObjects, ListObjectsV2 and
# ListObjectVersions references: "Valid Values: url").
URL_ENCODING = "url"


def _url_encode(value: str) -> str:
    """S3's url encoding type: every byte of the UTF-8 form is %XX except the
    unreserved characters (A-Z a-z 0-9 - _ . ~) and "/". A space is %20 and "+" is
    %2B, never a bare "+": botocore decodes with unquote_plus, which turns a bare "+"
    into a space, so this is the only encoding it round trips."""
    return quote(value, safe="/")


def _identity(value: str) -> str:
    return value


def _encoder(params) -> Callable[[str], str] | None:
    """The key encoder for a listing, or None without encoding-type. Only "url" is
    valid; an empty value is treated as absent, like an empty delimiter."""
    raw = params.get("encoding-type", "")
    if raw == "":
        return None
    if raw != URL_ENCODING:
        raise InvalidEncodingType(extra={"ArgumentName": "encoding-type", "ArgumentValue": raw})
    return _url_encode


def _owner_element(parent: ET.Element) -> None:
    owner = ET.SubElement(parent, "Owner")
    ET.SubElement(owner, "ID").text = settings.owner_id
    ET.SubElement(owner, "DisplayName").text = settings.owner_display_name


def _list_buckets_xml(buckets) -> str:
    root = ET.Element("ListAllMyBucketsResult")
    _owner_element(root)
    buckets_el = ET.SubElement(root, "Buckets")
    for b in buckets:
        bucket_el = ET.SubElement(buckets_el, "Bucket")
        ET.SubElement(bucket_el, "Name").text = b.name
        ET.SubElement(bucket_el, "CreationDate").text = b.created_at.strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def _list_objects_xml(
    bucket: str,
    page: ListPage,
    prefix: str,
    delimiter: str,
    marker: str,
    max_keys: int,
    encode: Callable[[str], str] | None,
) -> str:
    """With encoding-type=url, the encoded elements are Marker, NextMarker,
    Delimiter, Key and CommonPrefixes/Prefix: the elements botocore decodes for
    ListObjects. Prefix is not encoded (docs/compatibility.md, listing choices)."""
    enc = encode or _identity
    root = ET.Element("ListBucketResult")
    ET.SubElement(root, "Name").text = bucket
    ET.SubElement(root, "Prefix").text = prefix
    ET.SubElement(root, "Marker").text = enc(marker)
    ET.SubElement(root, "MaxKeys").text = str(max_keys)
    if delimiter:
        ET.SubElement(root, "Delimiter").text = enc(delimiter)
    ET.SubElement(root, "IsTruncated").text = "true" if page.is_truncated else "false"
    # S3 returns NextMarker only with a delimiter; without one, a client continues
    # from the last Key.
    if delimiter and page.is_truncated:
        ET.SubElement(root, "NextMarker").text = enc(page.next_marker)
    if encode:
        ET.SubElement(root, "EncodingType").text = URL_ENCODING
    _page_entries(root, page, owner=True, enc=enc)
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def _page_entries(root: ET.Element, page: ListPage, owner: bool, enc: Callable[[str], str]) -> None:
    for obj in page.objects:
        contents = ET.SubElement(root, "Contents")
        ET.SubElement(contents, "Key").text = enc(obj.key)
        ET.SubElement(contents, "ETag").text = obj.etag
        ET.SubElement(contents, "Size").text = str(obj.size)
        ET.SubElement(contents, "LastModified").text = obj.last_modified.strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )
        ET.SubElement(contents, "StorageClass").text = "STANDARD"
        if owner:
            _owner_element(contents)
    for common in page.common_prefixes:
        ET.SubElement(ET.SubElement(root, "CommonPrefixes"), "Prefix").text = enc(common)


def _max_keys(params) -> int:
    """max-keys: default 1000, larger values capped at 1000, 0 allowed. Anything but
    a decimal integer is InvalidArgument."""
    raw = params.get("max-keys")
    if raw is None:
        return MAX_KEYS
    if not raw.isascii() or not raw.isdigit():
        raise InvalidArgument(extra={"ArgumentName": "max-keys", "ArgumentValue": raw})
    return min(int(raw), MAX_KEYS)


def _list_objects_v2_xml(
    bucket: str,
    page: ListPage,
    params,
    prefix: str,
    delimiter: str,
    max_keys: int,
    fetch_owner: bool,
    encode: Callable[[str], str] | None,
) -> str:
    """With encoding-type=url, the encoded elements are Delimiter, Prefix, Key,
    StartAfter (the ListObjectsV2 reference) and CommonPrefixes/Prefix. The tokens
    are opaque and never encoded."""
    enc = encode or _identity
    root = ET.Element("ListBucketResult")
    ET.SubElement(root, "Name").text = bucket
    ET.SubElement(root, "Prefix").text = enc(prefix)
    # StartAfter and ContinuationToken are echoed when sent, even empty.
    if "start-after" in params:
        ET.SubElement(root, "StartAfter").text = enc(params["start-after"])
    if "continuation-token" in params:
        ET.SubElement(root, "ContinuationToken").text = params["continuation-token"]
    # KeyCount counts keys and common prefixes, as max-keys does.
    ET.SubElement(root, "KeyCount").text = str(page.count)
    ET.SubElement(root, "MaxKeys").text = str(max_keys)
    if delimiter:
        ET.SubElement(root, "Delimiter").text = enc(delimiter)
    ET.SubElement(root, "IsTruncated").text = "true" if page.is_truncated else "false"
    if page.is_truncated:
        ET.SubElement(root, "NextContinuationToken").text = encode_token(
            page.restart_after, page.restart_at
        )
    if encode:
        ET.SubElement(root, "EncodingType").text = URL_ENCODING
    _page_entries(root, page, owner=fetch_owner, enc=enc)
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def _list_versions_xml(
    bucket: str,
    page: ListPage,
    prefix: str,
    delimiter: str,
    key_marker: str,
    version_id_marker: str,
    max_keys: int,
    encode: Callable[[str], str] | None,
) -> str:
    """Every bucket is unversioned: each key has one version, "null", which is the
    latest. With encoding-type=url, the encoded elements are KeyMarker,
    NextKeyMarker, Prefix, Key, Delimiter (the ListObjectVersions reference) and
    CommonPrefixes/Prefix; version IDs are never encoded."""
    enc = encode or _identity
    root = ET.Element("ListVersionsResult")
    ET.SubElement(root, "Name").text = bucket
    ET.SubElement(root, "Prefix").text = enc(prefix)
    ET.SubElement(root, "KeyMarker").text = enc(key_marker)
    ET.SubElement(root, "VersionIdMarker").text = version_id_marker
    ET.SubElement(root, "MaxKeys").text = str(max_keys)
    if delimiter:
        ET.SubElement(root, "Delimiter").text = enc(delimiter)
    ET.SubElement(root, "IsTruncated").text = "true" if page.is_truncated else "false"
    if page.is_truncated:
        # The last key or common prefix returned, and the last version returned; a
        # page that ended on a common prefix returned no version after it.
        ET.SubElement(root, "NextKeyMarker").text = enc(page.next_marker)
        if page.restart_after is not None:
            ET.SubElement(root, "NextVersionIdMarker").text = NULL_VERSION_ID
    if encode:
        ET.SubElement(root, "EncodingType").text = URL_ENCODING
    for obj in page.objects:
        ver = ET.SubElement(root, "Version")
        ET.SubElement(ver, "Key").text = enc(obj.key)
        ET.SubElement(ver, "VersionId").text = NULL_VERSION_ID
        ET.SubElement(ver, "IsLatest").text = "true"
        ET.SubElement(ver, "ETag").text = obj.etag
        ET.SubElement(ver, "Size").text = str(obj.size)
        ET.SubElement(ver, "LastModified").text = obj.last_modified.strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )
        ET.SubElement(ver, "StorageClass").text = "STANDARD"
        _owner_element(ver)
    for common in page.common_prefixes:
        ET.SubElement(ET.SubElement(root, "CommonPrefixes"), "Prefix").text = enc(common)
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


@router.get("/")
async def list_buckets(request: Request) -> Response:
    metadata = request.app.state.metadata
    buckets = metadata.list_buckets()
    return Response(
        content=_list_buckets_xml(buckets),
        status_code=200,
        media_type="application/xml",
    )


@router.get("/{bucket}")
@router.get("/{bucket}/")
async def list_objects(bucket: str, request: Request) -> Response:
    metadata = request.app.state.metadata
    params = dict(request.query_params)

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})

    prefix = params.get("prefix", "")
    # An empty delimiter is no delimiter.
    max_keys = _max_keys(params)
    delimiter = params.get("delimiter", "")
    encode = _encoder(params)

    # ListObjectVersions. Each key's only version is "null", so a page resumes after
    # (key-marker, version-id-marker) by starting after key-marker, with V1's marker
    # rule for common prefixes. Any other version ID names a version that cannot
    # exist in an unversioned bucket: 501, as for versionId elsewhere.
    if "versions" in params:
        key_marker = params.get("key-marker", "")
        version_id_marker = params.get("version-id-marker", "")
        if version_id_marker and not key_marker:
            raise InvalidArgument(
                extra={"ArgumentName": "version-id-marker", "ArgumentValue": version_id_marker}
            )
        if version_id_marker and version_id_marker != NULL_VERSION_ID:
            raise NotImplementedOperation(
                extra={"Parameter": "version-id-marker", "Value": version_id_marker}
            )
        page = metadata.list_page(bucket, prefix, delimiter, key_marker, max_keys)
        return Response(
            content=_list_versions_xml(
                bucket, page, prefix, delimiter, key_marker, version_id_marker, max_keys, encode
            ),
            status_code=200,
            media_type="application/xml",
        )

    # ListObjectsV2 (list-type=2). A continuation token holds where the previous page
    # ended and takes precedence over start-after, which is only echoed then. An
    # empty token is no token.
    if params.get("list-type") == "2":
        token = params.get("continuation-token", "")
        if token:
            after, at = decode_token(token)
            page = metadata.list_page(bucket, prefix, delimiter, after or "", max_keys, at)
        else:
            start_after = params.get("start-after", "")
            page = metadata.list_page(bucket, prefix, delimiter, start_after, max_keys)
        fetch_owner = params.get("fetch-owner", "").lower() == "true"
        return Response(
            content=_list_objects_v2_xml(
                bucket, page, params, prefix, delimiter, max_keys, fetch_owner, encode
            ),
            status_code=200,
            media_type="application/xml",
        )

    # ListObjects V1
    marker = params.get("marker", "")
    page = metadata.list_page(bucket, prefix, delimiter, marker, max_keys)
    return Response(
        content=_list_objects_xml(bucket, page, prefix, delimiter, marker, max_keys, encode),
        status_code=200,
        media_type="application/xml",
    )


@router.post("/{bucket}")
@router.post("/{bucket}/")
async def post_bucket(bucket: str, request: Request) -> Response:
    """Dispatch bucket-level POSTs — currently only ?delete (DeleteObjects)."""
    params = dict(request.query_params)
    if "delete" in params:
        return await _delete_objects(bucket, request)
    # Unreachable through the request guard; kept so this handler can never act
    # on a POST it does not implement (e.g. browser-form POST Object).
    raise NotImplementedOperation(extra={"Method": "POST"})


async def _delete_objects(bucket: str, request: Request) -> Response:
    """POST /{bucket}?delete — bulk delete objects (S3 DeleteObjects API)."""
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})

    body = await request.body()
    root = parse_request_xml(body)
    # boto3 sends XML with the S3 namespace — strip it for robust findall
    ns = "http://s3.amazonaws.com/doc/2006-03-01/"

    def _find(tag):
        # try with namespace first, then without (for clients that omit it)
        results = root.findall(f"{{{ns}}}{tag}")
        return results if results else root.findall(tag)

    def _text(obj, tag):
        # "is not None", not "or": an empty element (e.g. <VersionId/>) must stay "".
        value = obj.findtext(f"{{{ns}}}{tag}")
        return value if value is not None else obj.findtext(tag)

    def _has(obj, tag):
        return obj.find(f"{{{ns}}}{tag}") is not None or obj.find(tag) is not None

    def _error(key, version_id, code, message):
        e = ET.Element("Error")
        ET.SubElement(e, "Key").text = key
        if version_id is not None:
            ET.SubElement(e, "VersionId").text = version_id
        ET.SubElement(e, "Code").text = code
        ET.SubElement(e, "Message").text = message
        return e

    # Entries in request order; keys to delete are collected and deleted together in
    # one transaction after every entry is checked (docs/design/storage-layout.md §6.4).
    entries = []
    to_delete = []
    for obj in _find("Object"):
        key, version_id = _text(obj, "Key"), _text(obj, "VersionId")
        # Version IDs arrive in the body, so the request guard cannot see them.
        # Same rule as DELETE /{bucket}/{key}?versionId=: every bucket is unversioned,
        # so "null" is the current object and any other version ID is not implemented.
        if version_id is not None and version_id != NULL_VERSION_ID:
            entries.append(
                _error(key, version_id, "NotImplemented", NotImplementedOperation.message)
            )
            continue
        # Per-key delete conditions (ObjectIdentifier in the S3 API reference). Litmus
        # does not evaluate them; deleting anyway could delete an object the client
        # asked to keep, so the key gets an error and is kept.
        if any(_has(obj, tag) for tag in _CONDITIONAL_DELETE_ELEMENTS):
            entries.append(
                _error(key, version_id, "NotImplemented", NotImplementedOperation.message)
            )
            continue
        to_delete.append(key)
        d = ET.Element("Deleted")
        ET.SubElement(d, "Key").text = key
        if version_id is not None:
            ET.SubElement(d, "VersionId").text = version_id
        entries.append(d)

    garbage = metadata.delete_objects(bucket, to_delete)
    crash_point(DELETE_OBJECTS_AFTER_COMMIT)
    collect_garbage(storage, metadata, garbage)

    result = ET.Element("DeleteResult")
    for entry in entries:
        result.append(entry)

    return Response(
        content=ET.tostring(result, encoding="unicode", xml_declaration=True),
        status_code=200,
        media_type="application/xml",
    )


@router.head("/{bucket}")
@router.head("/{bucket}/")
async def head_bucket(bucket: str, request: Request) -> Response:
    metadata = request.app.state.metadata
    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})
    return Response(status_code=200)


@router.put("/{bucket}")
@router.put("/{bucket}/")
async def create_or_configure_bucket(bucket: str, request: Request) -> Response:
    metadata = request.app.state.metadata

    # Configuration subresources (?versioning, ?cors, ...) never reach this handler:
    # server/api/request_guard.py answers them with 501 NotImplemented.
    # Only CreateBucket validates the name (server/bucket_names.py), and before the
    # existence check, so an invalid name that is somehow in the store is still refused.
    if bucket_name_violation(bucket) is not None:
        raise InvalidBucketName(extra={"BucketName": bucket})
    if metadata.bucket_exists(bucket):
        raise BucketAlreadyExists(extra={"BucketName": bucket})
    metadata.create_bucket(bucket, datetime.now(UTC).replace(tzinfo=None))
    return Response(status_code=200)


@router.delete("/{bucket}")
@router.delete("/{bucket}/")
async def delete_bucket(bucket: str, request: Request) -> Response:
    """DeleteBucket (docs/design/storage-layout.md §6.9): open multipart uploads are
    aborted in the same transaction (decision 7)."""
    metadata = request.app.state.metadata
    storage = request.app.state.storage
    try:
        garbage = metadata.delete_bucket(bucket)
    except BucketMissing:
        raise NoSuchBucket(extra={"BucketName": bucket}) from None
    except BucketHasObjects:
        raise BucketNotEmpty(extra={"BucketName": bucket}) from None
    crash_point(BUCKET_DELETE_AFTER_COMMIT)
    collect_garbage(storage, metadata, garbage)
    return Response(status_code=204)
