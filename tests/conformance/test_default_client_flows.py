"""What the default clients send, over a live server (docs/design/storage-layout.md §20).

Every case runs against uvicorn, so the requests are those of the client and not of a
TestClient. Each asserts the checksum the row stores and what the response returns.

b1 to b5 are boto3 1.42 at its default config: PutObject, upload_file over the 8 MiB
threshold, GetObject whole and ranged, HeadObject without the mode, and CopyObject. a1 to a6
replay what aws-cli 2.38.5 sends at its default config, shaped as captured through a logging
proxy against a scratch server:

  a1  PutObject, Content-Encoding aws-chunked, STREAMING-UNSIGNED-PAYLOAD-TRAILER, an
      x-amz-checksum-crc64nvme trailer
  a2  CreateMultipartUpload with x-amz-checksum-algorithm CRC64NVME, each UploadPart framed
      with a crc64nvme trailer, CompleteMultipartUpload with x-amz-mp-object-size and a
      ChecksumCRC64NVME in each Part
  a3  the same with SHA256 (a COMPOSITE upload)
  a4  a GET with Range and x-amz-checksum-mode: ENABLED
  a5  s3 to s3 above the multipart threshold: an unnamed CreateMultipartUpload,
      UploadPartCopy with x-amz-copy-source-range and x-amz-copy-source-if-match, and a
      Complete whose Part elements hold the ETag and PartNumber only
  a6  s3 to s3 under the threshold: one CopyObject with x-amz-copy-source and
      x-amz-object-annotation-directive: EXCLUDE
"""

import hashlib
import socket
import threading
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

import boto3
import httpx
import pytest
import uvicorn
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from test_upload_checksums import b64, digest

from chaos.framework.framed import frame, framed_headers
from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "default-flows"
MIB = 1024 * 1024
NS = "http://s3.amazonaws.com/doc/2006-03-01/"
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
MODE = {"x-amz-checksum-mode": "enabled"}


@dataclass
class Live:
    s3: object  # a boto3 client with its default checksum behavior
    http: httpx.Client
    metadata: SQLiteMetadataStore

    def row(self, key: str):
        return self.metadata.get_object_meta(BUCKET, key)

    def columns(self, key: str) -> tuple:
        meta = self.row(key)
        return meta.checksum, meta.checksum_algorithm, meta.checksum_type


@pytest.fixture(scope="module")
def live(tmp_path_factory):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    tmp: Path = tmp_path_factory.mktemp("default-client-flows")
    metadata = SQLiteMetadataStore(db_path=tmp / "meta.db")
    app = create_app(storage=BlobStore(tmp / "blobs"), metadata=metadata)
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
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{port}",
        aws_access_key_id="key",
        aws_secret_access_key="secret",
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 0}),
    )
    s3.create_bucket(Bucket=BUCKET)
    with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=30.0) as http:
        yield Live(s3, http, metadata)
    server.should_exit = True
    thread.join(timeout=10)


def body_of(size: int, seed: int) -> bytes:
    return bytes((i * seed + i // 251) % 256 for i in range(size))


def children(resp: httpx.Response) -> dict[str, str]:
    """The child elements of a response document, namespace stripped."""
    return {e.tag.rpartition("}")[2]: e.text for e in ET.fromstring(resp.text)}


def cli_put(live: Live, key: str, data: bytes) -> httpx.Response:
    """a1's request: an unsigned aws-chunked PutObject with a crc64nvme trailer."""
    body = frame(data, trailer_line=f"x-amz-checksum-crc64nvme:{b64(digest('crc64nvme', data))}")
    headers = framed_headers(body, len(data), **{"content-type": "application/octet-stream"})
    return live.http.put(f"/{BUCKET}/{key}", headers=headers, content=body)


def cli_part(live: Live, key: str, uid: str, number: int, data: bytes, algorithm: str):
    name = algorithm.lower()
    trailer = f"x-amz-checksum-{name}"
    body = frame(data, trailer_line=f"{trailer}:{b64(digest(name, data))}")
    headers = framed_headers(body, len(data), **{"x-amz-trailer": trailer})
    return live.http.put(
        f"/{BUCKET}/{key}?partNumber={number}&uploadId={uid}", headers=headers, content=body
    )


def create_upload(live: Live, key: str, algorithm: str | None) -> str:
    headers = {
        "content-type": "application/octet-stream",
        "x-amz-content-sha256": "UNSIGNED-PAYLOAD",
    }
    if algorithm:
        headers["x-amz-checksum-algorithm"] = algorithm
    resp = live.http.post(f"/{BUCKET}/{key}?uploads", headers=headers)
    assert resp.status_code == 200, resp.text
    assert resp.headers.get("x-amz-checksum-algorithm") == algorithm
    return children(resp)["UploadId"]


def complete(live: Live, key: str, uid: str, parts: list[tuple[str, int, str | None]], size=None):
    """The CLI's Complete: the Part elements in an xmlns document."""
    items = ""
    for etag, number, checksum in parts:
        items += (
            f"<Part><ETag>{etag}</ETag><PartNumber>{number}</PartNumber>{checksum or ''}</Part>"
        )
    headers = {"x-amz-content-sha256": "UNSIGNED-PAYLOAD"}
    if size is not None:
        headers["x-amz-mp-object-size"] = str(size)
    return live.http.post(
        f"/{BUCKET}/{key}?uploadId={uid}",
        headers=headers,
        content=f'<CompleteMultipartUpload xmlns="{NS}">{items}</CompleteMultipartUpload>',
    )


# --- boto3 at its default config ---


def test_b1_boto3_put_object_then_get_object_validates_the_checksum(live):
    data = body_of(5000, 3)
    put = live.s3.put_object(Bucket=BUCKET, Key="b1", Body=data)
    value = b64(digest("crc32", data))
    assert put["ChecksumCRC32"] == value
    assert live.columns("b1") == (value, "CRC32", "FULL_OBJECT")
    # botocore sends the mode on every GetObject and checks the body against the header.
    got = live.s3.get_object(Bucket=BUCKET, Key="b1")
    assert got["ChecksumCRC32"] == value and got["ChecksumType"] == "FULL_OBJECT"
    assert got["Body"].read() == data


def test_b2_boto3_upload_file_over_the_threshold_completes_a_crc32_composite(live, tmp_path):
    data = body_of(8 * MIB, 5) + body_of(MIB, 7)
    source = tmp_path / "big.bin"
    source.write_bytes(data)
    live.s3.upload_file(str(source), BUCKET, "b2", Config=TransferConfig())
    parts = [data[: 8 * MIB], data[8 * MIB :]]
    raw = b"".join(digest("crc32", p) for p in parts)
    value = b64(digest("crc32", raw)) + "-2"
    assert live.columns("b2") == (value, "CRC32", "COMPOSITE")
    head = live.s3.head_object(Bucket=BUCKET, Key="b2", ChecksumMode="ENABLED")
    assert (head["ChecksumCRC32"], head["ChecksumType"]) == (value, "COMPOSITE")
    # A composite value is not the checksum of the whole body, and botocore skips it.
    got = live.s3.get_object(Bucket=BUCKET, Key="b2")
    assert got["ChecksumCRC32"] == value
    assert got["Body"].read() == data


def test_b3_boto3_get_object_whole_carries_the_checksum_and_ranged_carries_none(live):
    data = body_of(3000, 11)
    live.s3.put_object(Bucket=BUCKET, Key="b3", Body=data, ChecksumAlgorithm="SHA256")
    value = b64(digest("sha256", data))
    assert live.columns("b3") == (value, "SHA256", "FULL_OBJECT")
    whole = live.s3.get_object(Bucket=BUCKET, Key="b3", ChecksumMode="ENABLED")
    assert whole["ChecksumSHA256"] == value
    assert whole["Body"].read() == data
    ranged = live.s3.get_object(
        Bucket=BUCKET, Key="b3", Range="bytes=10-99", ChecksumMode="ENABLED"
    )
    assert not [k for k in ranged if k.startswith("Checksum")]
    assert ranged["Body"].read() == data[10:100]
    assert live.columns("b3") == (value, "SHA256", "FULL_OBJECT")


def test_b4_boto3_head_object_without_the_mode_returns_no_checksum(live):
    data = body_of(700, 13)
    live.s3.put_object(Bucket=BUCKET, Key="b4", Body=data)
    head = live.s3.head_object(Bucket=BUCKET, Key="b4")
    assert not [k for k in head if k.startswith("Checksum")]
    assert head["ContentLength"] == len(data)
    assert live.columns("b4") == (b64(digest("crc32", data)), "CRC32", "FULL_OBJECT")


def test_b5_boto3_copy_object_stores_the_sources_algorithm_and_get_validates_it(live):
    data = body_of(4000, 17)
    live.s3.put_object(Bucket=BUCKET, Key="b5-src", Body=data, ChecksumAlgorithm="SHA1")
    done = live.s3.copy_object(
        Bucket=BUCKET, Key="b5-dst", CopySource={"Bucket": BUCKET, "Key": "b5-src"}
    )
    value = b64(digest("sha1", data))
    assert done["CopyObjectResult"]["ChecksumSHA1"] == value
    assert done["CopyObjectResult"]["ChecksumType"] == "FULL_OBJECT"
    assert live.columns("b5-dst") == (value, "SHA1", "FULL_OBJECT")
    got = live.s3.get_object(Bucket=BUCKET, Key="b5-dst")
    assert got["ChecksumSHA1"] == value
    assert got["Body"].read() == data


# --- what the AWS CLI sends ---


def test_a1_cli_put_object_with_a_crc64nvme_trailer(live):
    data = body_of(1024, 19)
    resp = cli_put(live, "a1", data)
    value = b64(digest("crc64nvme", data))
    assert resp.status_code == 200, resp.text
    assert resp.headers["x-amz-checksum-crc64nvme"] == value
    assert resp.headers["x-amz-checksum-type"] == "FULL_OBJECT"
    assert live.columns("a1") == (value, "CRC64NVME", "FULL_OBJECT")
    got = live.http.get(f"/{BUCKET}/a1", headers=MODE)
    assert got.content == data and got.headers["x-amz-checksum-crc64nvme"] == value


@pytest.mark.parametrize(
    "algorithm, key, type_",
    [("CRC64NVME", "a2", "FULL_OBJECT"), ("SHA256", "a3", "COMPOSITE")],
    ids=["a2-crc64nvme", "a3-sha256"],
)
def test_a2_a3_cli_multipart_upload_with_trailers_and_part_checksums(live, algorithm, key, type_):
    name = algorithm.lower()
    chunks = [body_of(5000, 23), body_of(5000, 29), body_of(123, 31)]
    uid = create_upload(live, key, algorithm)
    parts = []
    for number, data in enumerate(chunks, 1):
        resp = cli_part(live, key, uid, number, data, algorithm)
        assert resp.status_code == 200, resp.text
        value = b64(digest(name, data))
        assert resp.headers[f"x-amz-checksum-{name}"] == value
        parts.append(
            (
                resp.headers["etag"].strip('"'),
                number,
                f"<Checksum{algorithm}>{value}</Checksum{algorithm}>",
            )
        )
    done = complete(live, key, uid, parts, size=sum(map(len, chunks)))
    assert done.status_code == 200, done.text
    if type_ == "FULL_OBJECT":
        value = b64(digest(name, b"".join(chunks)))
    else:
        value = b64(digest(name, b"".join(digest(name, c) for c in chunks))) + f"-{len(chunks)}"
    assert live.columns(key) == (value, algorithm, type_)
    result = children(done)
    assert (result[f"Checksum{algorithm}"], result["ChecksumType"]) == (value, type_)
    got = live.http.get(f"/{BUCKET}/{key}", headers=MODE)
    assert got.content == b"".join(chunks)
    assert got.headers[f"x-amz-checksum-{name}"] == value
    assert got.headers["x-amz-checksum-type"] == type_


def test_a4_cli_ranged_get_with_the_checksum_mode_carries_no_checksum(live):
    data = body_of(1024, 37)
    assert cli_put(live, "a4", data).status_code == 200
    stored = live.columns("a4")
    resp = live.http.get(f"/{BUCKET}/a4", headers={"range": "bytes=0-99", **MODE})
    assert resp.status_code == 206
    assert resp.headers["content-range"] == "bytes 0-99/1024"
    assert resp.content == data[:100]
    assert not [h for h in resp.headers if h.startswith("x-amz-checksum")]
    assert (
        live.columns("a4") == stored == (b64(digest("crc64nvme", data)), "CRC64NVME", "FULL_OBJECT")
    )


def test_a5_cli_s3_to_s3_multipart_copy_of_an_unnamed_upload(live):
    chunks = [body_of(4000, 41), body_of(4000, 43), body_of(77, 47)]
    source = b"".join(chunks)
    assert cli_put(live, "a5-src", source).status_code == 200
    etag = live.http.head(f"/{BUCKET}/a5-src").headers["etag"]
    uid = create_upload(live, "a5-dst", None)
    parts, first = [], 0
    for number, chunk in enumerate(chunks, 1):
        resp = live.http.put(
            f"/{BUCKET}/a5-dst?uploadId={uid}&partNumber={number}",
            headers={
                "x-amz-copy-source": f"{BUCKET}/a5-src",
                "x-amz-copy-source-range": f"bytes={first}-{first + len(chunk) - 1}",
                "x-amz-copy-source-if-match": etag,
            },
        )
        assert resp.status_code == 200, resp.text
        # An unnamed upload returns no checksum element for a copied part.
        assert not [t for t in children(resp) if t.startswith("Checksum")]
        parts.append((children(resp)["ETag"], number, None))
        first += len(chunk)
    done = complete(live, "a5-dst", uid, parts)
    assert done.status_code == 200, done.text
    # CRC64NVME is what an unnamed upload's parts are stored in, combined into the object's.
    value = b64(digest("crc64nvme", source))
    assert live.columns("a5-dst") == (value, "CRC64NVME", "FULL_OBJECT")
    assert children(done)["ChecksumCRC64NVME"] == value
    got = live.http.get(f"/{BUCKET}/a5-dst", headers=MODE)
    assert got.content == source and got.headers["x-amz-checksum-crc64nvme"] == value


def test_a6_cli_s3_to_s3_copy_object_under_the_threshold(live):
    data = body_of(1024, 53)
    assert cli_put(live, "a6-src", data).status_code == 200
    resp = live.http.put(
        f"/{BUCKET}/a6-dst",
        headers={
            "x-amz-copy-source": f"{BUCKET}/a6-src",
            "x-amz-object-annotation-directive": "EXCLUDE",
            "x-amz-content-sha256": EMPTY_SHA256,
            "content-length": "0",
        },
    )
    value = b64(digest("crc64nvme", data))
    assert resp.status_code == 200, resp.text
    assert list(children(resp)) == ["ETag", "LastModified", "ChecksumType", "ChecksumCRC64NVME"]
    assert children(resp)["ChecksumCRC64NVME"] == value
    assert live.columns("a6-dst") == (value, "CRC64NVME", "FULL_OBJECT")
    got = live.http.get(f"/{BUCKET}/a6-dst", headers=MODE)
    assert got.content == data and got.headers["x-amz-checksum-crc64nvme"] == value
