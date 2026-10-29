"""ListParts driven by boto3, against the real app under uvicorn: the paginator pages
through the parts with max-parts, part-number-marker and NextPartNumberMarker
(docs/design/storage-layout.md §21)."""

import base64
import hashlib
import socket
import threading
import time
import zlib
from pathlib import Path

import boto3
import pytest
import uvicorn
from botocore.config import Config

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "boto3-list-parts"
KEY = "dir/multipart.bin"
PARTS = 25


@pytest.fixture(scope="module")
def s3(tmp_path_factory):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    tmp: Path = tmp_path_factory.mktemp("boto3-list-parts")
    app = create_app(
        storage=BlobStore(tmp / "blobs"), metadata=SQLiteMetadataStore(db_path=tmp / "meta.db")
    )
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, ws="none", log_level="warning")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("uvicorn did not start")
        time.sleep(0.02)
    client = boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{port}",
        aws_access_key_id="key",
        aws_secret_access_key="secret",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 0}),
    )
    client.create_bucket(Bucket=BUCKET)
    yield client
    server.should_exit = True
    thread.join(timeout=10)


def _body(number: int) -> bytes:
    return bytes([number]) * (number * 7)


def _upload(s3, **create) -> tuple[str, dict[int, str]]:
    upload_id = s3.create_multipart_upload(Bucket=BUCKET, Key=KEY, **create)["UploadId"]
    etags = {}
    for number in range(1, PARTS + 1):
        etags[number] = s3.upload_part(
            Bucket=BUCKET,
            Key=KEY,
            UploadId=upload_id,
            PartNumber=number,
            Body=_body(number),
            ChecksumAlgorithm=create.get("ChecksumAlgorithm", "CRC64NVME"),
        )["ETag"]
    return upload_id, etags


def test_the_paginator_pages_through_every_part(s3):
    upload_id, etags = _upload(s3)

    pages = list(
        s3.get_paginator("list_parts").paginate(
            Bucket=BUCKET, Key=KEY, UploadId=upload_id, PaginationConfig={"PageSize": 10}
        )
    )

    assert [len(page["Parts"]) for page in pages] == [10, 10, 5]
    assert [page["IsTruncated"] for page in pages] == [True, True, False]
    assert [page["PartNumberMarker"] for page in pages] == [0, 10, 20]
    assert [page.get("NextPartNumberMarker") for page in pages] == [10, 20, None]
    parts = [part for page in pages for part in page["Parts"]]
    assert [p["PartNumber"] for p in parts] == list(range(1, PARTS + 1))
    assert [p["ETag"] for p in parts] == [etags[n] for n in range(1, PARTS + 1)]
    assert [p["Size"] for p in parts] == [n * 7 for n in range(1, PARTS + 1)]
    assert all(p["LastModified"].tzinfo is not None for p in parts)
    for page in pages:
        assert page["Bucket"] == BUCKET
        assert page["Key"] == KEY
        assert page["UploadId"] == upload_id
        assert page["StorageClass"] == "STANDARD"
        assert page["Owner"]["ID"] and page["Initiator"]["ID"]
        assert "ChecksumAlgorithm" not in page
    s3.abort_multipart_upload(Bucket=BUCKET, Key=KEY, UploadId=upload_id)


def test_a_single_call_with_a_marker_lists_the_rest(s3):
    upload_id, _ = _upload(s3)
    page = s3.list_parts(
        Bucket=BUCKET, Key=KEY, UploadId=upload_id, PartNumberMarker=22, MaxParts=100
    )
    assert [p["PartNumber"] for p in page["Parts"]] == [23, 24, 25]
    assert page["MaxParts"] == 100
    assert page["IsTruncated"] is False
    s3.abort_multipart_upload(Bucket=BUCKET, Key=KEY, UploadId=upload_id)


def test_a_named_upload_lists_its_checksums(s3):
    upload_id, _ = _upload(s3, ChecksumAlgorithm="CRC32")

    page = s3.list_parts(Bucket=BUCKET, Key=KEY, UploadId=upload_id, MaxParts=3)

    assert page["ChecksumAlgorithm"] == "CRC32"
    assert page["ChecksumType"] == "COMPOSITE"
    for part in page["Parts"]:
        want = base64.b64encode(zlib.crc32(_body(part["PartNumber"])).to_bytes(4, "big")).decode()
        assert part["ChecksumCRC32"] == want
        assert part["ETag"] == '"' + hashlib.md5(_body(part["PartNumber"])).hexdigest() + '"'
    s3.abort_multipart_upload(Bucket=BUCKET, Key=KEY, UploadId=upload_id)


def test_a_completed_upload_is_no_such_upload(s3):
    upload_id = s3.create_multipart_upload(Bucket=BUCKET, Key="done")["UploadId"]
    etag = s3.upload_part(Bucket=BUCKET, Key="done", UploadId=upload_id, PartNumber=1, Body=b"x")[
        "ETag"
    ]
    s3.complete_multipart_upload(
        Bucket=BUCKET,
        Key="done",
        UploadId=upload_id,
        MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": etag}]},
    )
    with pytest.raises(s3.exceptions.NoSuchUpload) as error:
        s3.list_parts(Bucket=BUCKET, Key="done", UploadId=upload_id)
    assert error.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
