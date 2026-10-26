"""UploadPartCopy driven by boto3's managed copy, against the real app under uvicorn.

client.copy() sends a HEAD of the source and, above the multipart threshold (8 MiB), a
CreateMultipartUpload, one UploadPartCopy per 8 MiB range and a CompleteMultipartUpload
(docs/design/storage-layout.md §11). Litmus must put the same bytes in the destination
and list it.
"""

import hashlib
import os
import socket
import threading
import time
from pathlib import Path

import boto3
import pytest
import uvicorn
from botocore.config import Config

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "boto3-copy"
MIB = 1024 * 1024
SIZE = 20 * MIB
CHUNK = 8 * MIB  # boto3's multipart_chunksize


@pytest.fixture(scope="module")
def s3(tmp_path_factory):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    tmp: Path = tmp_path_factory.mktemp("boto3-copy")
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


def test_managed_copy_of_a_20_mib_object_has_the_source_bytes_and_is_listed(s3):
    data = os.urandom(SIZE)
    s3.put_object(Bucket=BUCKET, Key="source.bin", Body=data)

    s3.copy({"Bucket": BUCKET, "Key": "source.bin"}, BUCKET, "copy.bin")

    got = s3.get_object(Bucket=BUCKET, Key="copy.bin")
    body = got["Body"].read()
    assert len(body) == SIZE
    assert hashlib.sha256(body).digest() == hashlib.sha256(data).digest()
    # Three parts (8 + 8 + 4 MiB), so the destination has a composite ETag over their MD5s.
    pieces = [data[i : i + CHUNK] for i in range(0, SIZE, CHUNK)]
    digest = hashlib.md5(b"".join(hashlib.md5(p).digest() for p in pieces)).hexdigest()
    assert got["ETag"] == f'"{digest}-{len(pieces)}"'
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET)["Contents"]]
    assert "copy.bin" in keys and "source.bin" in keys
    # The source is as it was.
    assert s3.get_object(Bucket=BUCKET, Key="source.bin")["Body"].read() == data
