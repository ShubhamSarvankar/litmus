import xml.etree.ElementTree as ET
from datetime import UTC, datetime

from fastapi import APIRouter, Request
from fastapi.responses import Response

from server.api.request_guard import NULL_VERSION_ID, unsafe_key
from server.crash_points import (
    BUCKET_DELETE_AFTER_COMMIT,
    DELETE_OBJECTS_AFTER_COMMIT,
    crash_point,
)
from server.errors.s3errors import (
    BucketAlreadyExists,
    BucketNotEmpty,
    NoSuchBucket,
    NotImplementedOperation,
)
from server.metadata.base import BucketHasObjects, BucketMissing
from server.storage.lifecycle import collect_garbage

router = APIRouter()

# Per-key conditions a DeleteObjects <Object> may carry (ObjectIdentifier).
_CONDITIONAL_DELETE_ELEMENTS = ("ETag", "LastModifiedTime", "Size")


def _list_buckets_xml(buckets) -> str:
    root = ET.Element("ListAllMyBucketsResult")
    buckets_el = ET.SubElement(root, "Buckets")
    for b in buckets:
        bucket_el = ET.SubElement(buckets_el, "Bucket")
        ET.SubElement(bucket_el, "Name").text = b.name
        ET.SubElement(bucket_el, "CreationDate").text = b.created_at.strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def _list_objects_xml(bucket: str, objects: list, prefix: str = "") -> str:
    root = ET.Element("ListBucketResult")
    ET.SubElement(root, "Name").text = bucket
    ET.SubElement(root, "Prefix").text = prefix
    ET.SubElement(root, "MaxKeys").text = "1000"
    ET.SubElement(root, "IsTruncated").text = "false"
    for obj in objects:
        contents = ET.SubElement(root, "Contents")
        ET.SubElement(contents, "Key").text = obj.key
        ET.SubElement(contents, "ETag").text = obj.etag
        ET.SubElement(contents, "Size").text = str(obj.size)
        ET.SubElement(contents, "LastModified").text = obj.last_modified.strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )
        ET.SubElement(contents, "StorageClass").text = "STANDARD"
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def _list_objects_v2_xml(bucket: str, objects: list, prefix: str = "") -> str:
    root = ET.Element("ListBucketResult")
    ET.SubElement(root, "Name").text = bucket
    ET.SubElement(root, "Prefix").text = prefix
    ET.SubElement(root, "KeyCount").text = str(len(objects))
    ET.SubElement(root, "MaxKeys").text = "1000"
    ET.SubElement(root, "IsTruncated").text = "false"
    for obj in objects:
        contents = ET.SubElement(root, "Contents")
        ET.SubElement(contents, "Key").text = obj.key
        ET.SubElement(contents, "ETag").text = obj.etag
        ET.SubElement(contents, "Size").text = str(obj.size)
        ET.SubElement(contents, "LastModified").text = obj.last_modified.strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )
        ET.SubElement(contents, "StorageClass").text = "STANDARD"
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def _list_versions_xml(bucket: str, objects: list) -> str:
    """Versioning not implemented — return objects as Version elements so cleanup can delete them."""
    root = ET.Element("ListVersionsResult")
    ET.SubElement(root, "Name").text = bucket
    ET.SubElement(root, "Prefix").text = ""
    ET.SubElement(root, "MaxKeys").text = "1000"
    ET.SubElement(root, "IsTruncated").text = "false"
    for obj in objects:
        ver = ET.SubElement(root, "Version")
        ET.SubElement(ver, "Key").text = obj.key
        ET.SubElement(ver, "VersionId").text = "null"
        ET.SubElement(ver, "IsLatest").text = "true"
        ET.SubElement(ver, "ETag").text = obj.etag
        ET.SubElement(ver, "Size").text = str(obj.size)
        ET.SubElement(ver, "LastModified").text = obj.last_modified.strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )
        ET.SubElement(ver, "StorageClass").text = "STANDARD"
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
async def list_objects(bucket: str, request: Request) -> Response:
    metadata = request.app.state.metadata
    params = dict(request.query_params)

    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})

    # ?versions → return objects as Version elements (versioning not implemented,
    # but cleanup code in ceph/s3-tests calls this to find objects before deleting)
    if "versions" in params:
        prefix = params.get("prefix", "")
        objects = metadata.list_objects(bucket, prefix=prefix or None)
        return Response(
            content=_list_versions_xml(bucket, objects),
            status_code=200,
            media_type="application/xml",
        )

    prefix = params.get("prefix", "")
    objects = metadata.list_objects(bucket, prefix=prefix or None)

    # ListObjectsV2 (list-type=2)
    if params.get("list-type") == "2":
        return Response(
            content=_list_objects_v2_xml(bucket, objects, prefix),
            status_code=200,
            media_type="application/xml",
        )

    # ListObjects V1
    return Response(
        content=_list_objects_xml(bucket, objects, prefix),
        status_code=200,
        media_type="application/xml",
    )


@router.post("/{bucket}")
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
    root = ET.fromstring(body.decode())
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
        # Keys and version IDs arrive in the body, so the request guard cannot see them.
        if unsafe_key(key):
            entries.append(
                _error(
                    key, version_id, "InvalidArgument", "Key contains a '.' or '..' path segment"
                )
            )
            continue
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
async def head_bucket(bucket: str, request: Request) -> Response:
    metadata = request.app.state.metadata
    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})
    return Response(status_code=200)


@router.put("/{bucket}")
async def create_or_configure_bucket(bucket: str, request: Request) -> Response:
    metadata = request.app.state.metadata

    # Configuration subresources (?versioning, ?cors, ...) never reach this handler:
    # server/api/request_guard.py answers them with 501 NotImplemented.
    if metadata.bucket_exists(bucket):
        raise BucketAlreadyExists(extra={"BucketName": bucket})
    metadata.create_bucket(bucket, datetime.now(UTC).replace(tzinfo=None))
    return Response(status_code=200)


@router.delete("/{bucket}")
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
