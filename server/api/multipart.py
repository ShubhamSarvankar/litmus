import uuid
import xml.etree.ElementTree as ET
from datetime import UTC, datetime

from fastapi import APIRouter, Request
from fastapi.responses import Response

from server.errors.s3errors import InvalidPart, NoSuchBucket, NoSuchUpload
from server.metadata.base import PartMeta
from server.multipart.state_machine import complete_multipart_upload

router = APIRouter()


def _initiate_xml(bucket: str, key: str, upload_id: str) -> str:
    root = ET.Element("InitiateMultipartUploadResult")
    ET.SubElement(root, "Bucket").text = bucket
    ET.SubElement(root, "Key").text = key
    ET.SubElement(root, "UploadId").text = upload_id
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def _complete_xml(bucket: str, key: str, etag: str) -> str:
    root = ET.Element("CompleteMultipartUploadResult")
    ET.SubElement(root, "Bucket").text = bucket
    ET.SubElement(root, "Key").text = key
    ET.SubElement(root, "ETag").text = etag
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def _parse_complete_body(body: bytes) -> list[dict]:
    root = ET.fromstring(body.decode())
    # boto3 sends XML with the S3 namespace — handle both namespaced and plain
    ns = "http://s3.amazonaws.com/doc/2006-03-01/"

    def _find_parts():
        result = root.findall(f"{{{ns}}}Part")
        return result if result else root.findall("Part")

    def _text(el, tag):
        return el.findtext(f"{{{ns}}}{tag}") or el.findtext(tag)

    parts = []
    for part_el in _find_parts():
        parts.append(
            {
                "PartNumber": int(_text(part_el, "PartNumber")),
                "ETag": _text(part_el, "ETag"),
            }
        )
    return parts


@router.post("/{bucket}/{key:path}")
async def post_object(bucket: str, key: str, request: Request) -> Response:
    """Dispatch: ?uploads → initiate, ?uploadId → complete."""
    params = dict(request.query_params)

    if "uploads" in params:
        return await _initiate(bucket, key, request)
    if "uploadId" in params:
        return await _complete(bucket, key, params["uploadId"], request)

    return Response(status_code=400)


@router.delete("/{bucket}/{key:path}")
async def delete_object_or_abort(bucket: str, key: str, request: Request) -> Response:
    """Dispatch: ?uploadId → abort multipart, otherwise → delete object."""
    params = dict(request.query_params)

    if "uploadId" in params:
        return await _abort(bucket, key, params["uploadId"], request)

    # Plain object delete
    from server.api.objects import _delete_object

    return await _delete_object(bucket, key, request)


@router.put("/{bucket}/{key:path}")
async def put_object_or_part(bucket: str, key: str, request: Request) -> Response:
    """Dispatch: ?partNumber&uploadId → upload part, otherwise → put object."""
    params = dict(request.query_params)

    if "partNumber" in params and "uploadId" in params:
        return await _upload_part(bucket, key, params, request)

    # Plain object put
    from server.api.objects import _put_object

    return await _put_object(bucket, key, request)


@router.get("/{bucket}/{key:path}")
async def get_object_proxy(bucket: str, key: str, request: Request) -> Response:
    """Proxy to objects router — multipart has no GET of its own."""
    from server.api.objects import _get_object

    return await _get_object(bucket, key, request)


@router.head("/{bucket}/{key:path}")
async def head_object_proxy(bucket: str, key: str, request: Request) -> Response:
    from server.api.objects import _head_object

    return await _head_object(bucket, key, request)


# --- private handlers ---


async def _initiate(bucket: str, key: str, request: Request) -> Response:
    metadata = request.app.state.metadata
    if not metadata.bucket_exists(bucket):
        raise NoSuchBucket(extra={"BucketName": bucket})
    upload_id = uuid.uuid4().hex
    metadata.create_upload(upload_id, bucket, key, datetime.now(UTC).replace(tzinfo=None))
    return Response(
        content=_initiate_xml(bucket, key, upload_id),
        status_code=200,
        media_type="application/xml",
    )


async def _upload_part(bucket: str, key: str, params: dict, request: Request) -> Response:
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    upload_id = params["uploadId"]
    try:
        part_number = int(params["partNumber"])
    except ValueError:
        raise InvalidPart()

    if not metadata.upload_exists(upload_id):
        raise NoSuchUpload(extra={"UploadId": upload_id})

    if not (1 <= part_number <= 10000):
        raise InvalidPart(extra={"PartNumber": str(part_number)})

    import io

    body = await request.body()
    part_etag = storage.write_part(upload_id, part_number, io.BytesIO(body))
    metadata.record_part(
        upload_id,
        PartMeta(upload_id=upload_id, part_number=part_number, etag=part_etag, size=len(body)),
    )
    return Response(status_code=200, headers={"ETag": part_etag})


async def _complete(bucket: str, key: str, upload_id: str, request: Request) -> Response:
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    if not metadata.upload_exists(upload_id):
        raise NoSuchUpload(extra={"UploadId": upload_id})

    body = await request.body()
    requested_parts = _parse_complete_body(body)
    if not requested_parts:
        raise InvalidPart()

    composite_etag = complete_multipart_upload(
        bucket=bucket,
        key=key,
        upload_id=upload_id,
        requested_parts=requested_parts,
        storage=storage,
        metadata=metadata,
    )
    return Response(
        content=_complete_xml(bucket, key, composite_etag),
        status_code=200,
        media_type="application/xml",
    )


async def _abort(bucket: str, key: str, upload_id: str, request: Request) -> Response:
    metadata = request.app.state.metadata
    storage = request.app.state.storage

    if not metadata.upload_exists(upload_id):
        raise NoSuchUpload(extra={"UploadId": upload_id})

    storage.delete_parts(upload_id)
    metadata.abort_upload(upload_id)
    return Response(status_code=204)
