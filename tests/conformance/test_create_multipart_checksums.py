"""CreateMultipartUpload with x-amz-checksum-algorithm and x-amz-checksum-type
(server/api/multipart.py, server/api/checksum_headers.py, docs/design/storage-layout.md §20).

The allowed algorithm and type pairs are the S3 User Guide's, "Checking object integrity
for data uploads in Amazon S3". The default types, the InvalidRequest and InvalidArgument
answers and the echo of nothing for an upload that names no algorithm are Litmus's
choices (docs/compatibility.md). A named upload answers with both headers; every
refusal creates no upload row.
"""

import socket
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import boto3
import pytest
import uvicorn
from botocore.config import Config
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "create-checksums"
KEY = "obj.bin"
URL = f"/{BUCKET}/{KEY}?uploads"
ALGORITHM = "x-amz-checksum-algorithm"
TYPE = "x-amz-checksum-type"


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata), raise_server_exceptions=False
    )
    assert client.put(f"/{BUCKET}").status_code == 200
    return client, metadata


def _code(resp) -> str:
    return ET.fromstring(resp.text).findtext("Code")


def _upload_id(resp) -> str:
    return ET.fromstring(resp.text).findtext("UploadId")


def _checksum_headers(resp) -> dict[str, str]:
    return {k: v for k, v in resp.headers.items() if k.startswith("x-amz-checksum")}


def _assert_refused(client, metadata, headers, status, code):
    resp = client.post(URL, headers=headers)
    assert (resp.status_code, _code(resp)) == (status, code)
    assert metadata.list_incomplete_uploads() == []
    assert _checksum_headers(resp) == {}


# Each algorithm with three forms of an allowed type: the type left out, a type written
# out (the one that is not the default where the algorithm allows two), and the default
# type in lower case with spaces around it.
_FORMS = {
    "CRC64NVME": ("FULL_OBJECT", "FULL_OBJECT", " full_object "),
    "CRC32": ("COMPOSITE", "FULL_OBJECT", " composite "),
    "CRC32C": ("COMPOSITE", "FULL_OBJECT", " composite "),
    "SHA1": ("COMPOSITE", "COMPOSITE", " composite "),
    "SHA256": ("COMPOSITE", "COMPOSITE", " composite "),
}
_CASES = [
    pytest.param(algorithm, form, id=f"{algorithm}-{form}")
    for algorithm in _FORMS
    for form in ("absent", "written", "lower-spaced")
]


@pytest.mark.parametrize(("algorithm", "form"), _CASES)
def test_a_named_upload_echoes_both_headers_and_stores_them(env, algorithm, form):
    client, metadata = env
    default, written, lower = _FORMS[algorithm]
    sent = {"absent": None, "written": written, "lower-spaced": lower}[form]
    expected = default if form == "absent" else sent.strip().upper()
    headers = {ALGORITHM: algorithm.lower() if form == "lower-spaced" else algorithm}
    if sent is not None:
        headers[TYPE] = sent

    resp = client.post(URL, headers=headers)

    assert resp.status_code == 200
    assert _checksum_headers(resp) == {ALGORITHM: algorithm, TYPE: expected}
    upload = metadata.get_upload(_upload_id(resp))
    assert (upload.checksum_algorithm, upload.checksum_type) == (algorithm, expected)
    assert upload.state == "open"


def test_an_upload_that_names_nothing_stores_null_and_echoes_nothing(env):
    client, metadata = env
    resp = client.post(URL, headers={ALGORITHM: "  ", TYPE: " "})
    assert resp.status_code == 200
    assert _checksum_headers(resp) == {}
    upload = metadata.get_upload(_upload_id(resp))
    assert (upload.checksum_algorithm, upload.checksum_type) == (None, None)


@pytest.mark.parametrize("algorithm", ["SHA512", "MD5", "XXHASH64", "XXHASH3", "XXHASH128"])
def test_an_algorithm_litmus_does_not_compute_is_not_implemented(env, algorithm):
    client, metadata = env
    _assert_refused(client, metadata, {ALGORITHM: algorithm}, 501, "NotImplemented")


def test_an_algorithm_litmus_does_not_compute_is_refused_in_lower_case(env):
    client, metadata = env
    _assert_refused(client, metadata, {ALGORITHM: " sha512 "}, 501, "NotImplemented")


@pytest.mark.parametrize(
    ("algorithm", "type_"),
    [("CRC64NVME", "COMPOSITE"), ("SHA1", "FULL_OBJECT"), ("SHA256", "FULL_OBJECT")],
)
def test_a_pair_the_user_guide_does_not_allow_is_invalid_request(env, algorithm, type_):
    client, metadata = env
    _assert_refused(client, metadata, {ALGORITHM: algorithm, TYPE: type_}, 400, "InvalidRequest")


@pytest.mark.parametrize("type_", ["COMPOSITE", "FULL_OBJECT"])
def test_a_type_without_an_algorithm_is_invalid_request(env, type_):
    client, metadata = env
    _assert_refused(client, metadata, {TYPE: type_}, 400, "InvalidRequest")


@pytest.mark.parametrize(
    ("headers", "name"),
    [
        ({ALGORITHM: "CRC16"}, ALGORITHM),
        ({ALGORITHM: "CRC32", TYPE: "SEGMENTED"}, TYPE),
    ],
    ids=["algorithm", "type"],
)
def test_a_value_that_is_not_an_aws_name_is_invalid_argument(env, headers, name):
    client, metadata = env
    resp = client.post(URL, headers=headers)
    assert (resp.status_code, _code(resp)) == (400, "InvalidArgument")
    assert ET.fromstring(resp.text).findtext("ArgumentName") == name
    assert metadata.list_incomplete_uploads() == []


# --- boto3 reads the response ---


@pytest.fixture(scope="module")
def s3(tmp_path_factory):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    tmp: Path = tmp_path_factory.mktemp("create-checksums")
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


@pytest.mark.parametrize(
    ("kwargs", "algorithm", "type_"),
    [
        ({"ChecksumAlgorithm": "CRC32"}, "CRC32", "COMPOSITE"),
        ({"ChecksumAlgorithm": "CRC32C", "ChecksumType": "FULL_OBJECT"}, "CRC32C", "FULL_OBJECT"),
        ({"ChecksumAlgorithm": "SHA256"}, "SHA256", "COMPOSITE"),
    ],
    ids=["crc32", "crc32c-full-object", "sha256"],
)
def test_boto3_parses_the_algorithm_and_type(s3, kwargs, algorithm, type_):
    resp = s3.create_multipart_upload(Bucket=BUCKET, Key=KEY, **kwargs)
    assert resp["ChecksumAlgorithm"] == algorithm
    assert resp["ChecksumType"] == type_
    s3.abort_multipart_upload(Bucket=BUCKET, Key=KEY, UploadId=resp["UploadId"])
