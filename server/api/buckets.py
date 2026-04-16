import xml.etree.ElementTree as ET
from datetime import UTC, datetime

from fastapi import APIRouter, Request
from fastapi.responses import Response

from server.errors.s3errors import BucketAlreadyExists, BucketNotEmpty, NoSuchBucket

router = APIRouter()


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
    return Response(status_code=400)


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

    keys_to_delete = [
        obj.findtext(f"{{{ns}}}Key") or obj.findtext("Key") for obj in _find("Object")
    ]

    deleted = []
    for key in keys_to_delete:
        storage.delete_object(bucket, key)
        metadata.delete_object_meta(bucket, key)
        d = ET.Element("Deleted")
        ET.SubElement(d, "Key").text = key
        deleted.append(d)

    result = ET.Element("DeleteResult")
    for d in deleted:
        result.append(d)

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
    params = dict(request.query_params)

    # Sub-resource configuration PUTs (versioning, ownership, etc.) — accept silently
    # since these features are out of scope. Return 200 if bucket exists, 404 if not.
    sub_resources = {
        "versioning",
        "ownershipControls",
        "accelerate",
        "cors",
        "lifecycle",
        "logging",
        "metrics",
        "notification",
        "policy",
        "replication",
        "requestPayment",
        "website",
        "encryption",
        "publicAccessBlock",
        "intelligentTieringConfiguration",
        "inventoryConfiguration",
        "analyticsConfiguration",
    }
    if any(k in params for k in sub_resources):
        if not metadata.bucket_exists(bucket):
            raise NoSuchBucket(extra={"BucketName": bucket})
        return Response(status_code=200)

    # Plain bucket creation
    if metadata.bucket_exists(bucket):
        raise BucketAlreadyExists(extra={"BucketName": bucket})
    metadata.create_bucket(bucket, datetime.now(UTC).replace(tzinfo=None))
    return Response(status_code=200)


@router.delete("/{bucket}")
async def delete_bucket(bucket: str, request: Request) -> Response:
    metadata = request.app.state.metadata
    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})
    if metadata.object_count(bucket) > 0:
        raise BucketNotEmpty(extra={"BucketName": bucket})
    metadata.delete_bucket(bucket)
    return Response(status_code=204)
