"""CompleteMultipartUpload enforces the minimum part size (docs/design/storage-layout.md §6.7).

Every listed part but the last listed one must be at least S3_MIN_PART_SIZE bytes (5 MiB
by default), measured by its stored size; otherwise 400 EntityTooSmall. The check uses
the listed parts only, a single part upload has no minimum, and UploadPart and
UploadPartCopy are not checked. The errors come in this order: NoSuchUpload, the
x-amz-mp-object-size syntax check, MalformedXML, InvalidPartOrder, InvalidPart, then
EntityTooSmall, then the x-amz-mp-object-size mismatch, then the If-Match and
If-None-Match conditions. A refusal leaves the upload open with all its parts and
registers no blob.

Every test names the minimum it needs through create_app, so none depends on the value
the test session runs with.
"""

import os
import socket
import subprocess
import sys
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import boto3
import pytest
import uvicorn
from botocore.config import Config
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient

from server.app import create_app
from server.config import Settings
from server.consistency import run_consistency_sweep
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

BUCKET = "too-small"
KEY = "obj.bin"
MIN = 1024
ROOT = Path(__file__).resolve().parents[2]


def build(tmp_path: Path, min_part_size: int):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata, min_part_size=min_part_size),
        raise_server_exceptions=False,
    )
    assert client.put(f"/{BUCKET}").status_code == 200
    return client, storage, metadata


@pytest.fixture
def env(tmp_path: Path):
    return build(tmp_path, MIN)


def code(resp) -> str | None:
    try:
        return ET.fromstring(resp.text).findtext("Code")
    except ET.ParseError:
        return None


def initiate(client) -> str:
    return ET.fromstring(client.post(f"/{BUCKET}/{KEY}?uploads").text).findtext("UploadId")


def part(client, uid: str, number: int, data: bytes) -> str:
    r = client.put(f"/{BUCKET}/{KEY}?partNumber={number}&uploadId={uid}", content=data)
    assert r.status_code == 200
    return r.headers["etag"]


def upload(client, sizes: list[int]) -> tuple[str, list[str], list[bytes]]:
    """An open upload with one part per size, numbered from 1; each part's bytes differ."""
    uid = initiate(client)
    datas = [bytes([65 + n]) * size for n, size in enumerate(sizes)]
    etags = [part(client, uid, n, data) for n, data in enumerate(datas, start=1)]
    return uid, etags, datas


def body(entries: list[tuple[int, str]]) -> bytes:
    items = "".join(
        f"<Part><PartNumber>{n}</PartNumber><ETag>{e}</ETag></Part>" for n, e in entries
    )
    return f"<CompleteMultipartUpload>{items}</CompleteMultipartUpload>".encode()


def complete(client, uid: str, entries: list[tuple[int, str]], headers: dict | None = None):
    return client.post(
        f"/{BUCKET}/{KEY}?uploadId={uid}", content=body(entries), headers=headers or {}
    )


def numbered(etags: list[str]) -> list[tuple[int, str]]:
    return list(enumerate(etags, start=1))


def state(storage, metadata, uid: str) -> tuple:
    """Everything a refused Complete must leave alone."""
    return (
        metadata.list_objects(BUCKET),
        metadata.list_blob_records(),
        storage.temp_entries(),
        sorted(storage.scan()),
        metadata.get_upload(uid, bucket=BUCKET, key=KEY),
        metadata.get_parts(uid),
    )


def assert_refused(env, uid: str, etags: list[str], resp, expected: str, before: tuple) -> None:
    client, storage, metadata = env
    assert (resp.status_code, code(resp)) == (400, expected)
    assert state(storage, metadata, uid) == before
    assert metadata.upload_exists(uid, bucket=BUCKET, key=KEY)
    assert [(p.part_number, p.etag) for p in metadata.get_parts(uid)] == numbered(etags)
    assert all(p.blob_id is not None for p in metadata.get_parts(uid))
    assert client.get(f"/{BUCKET}/{KEY}").status_code == 404
    assert run_consistency_sweep(metadata, storage).issues == []


# --- the setting ---


def test_default_is_five_mebibytes_when_unset(monkeypatch):
    monkeypatch.delenv("S3_MIN_PART_SIZE", raising=False)
    assert Settings().min_part_size == 5242880 == 5 * 1024 * 1024


def test_default_applies_to_a_server_started_without_the_variable():
    env = {k: v for k, v in os.environ.items() if k != "S3_MIN_PART_SIZE"}
    out = subprocess.run(
        [sys.executable, "-c", "from server.config import settings; print(settings.min_part_size)"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout.strip() == "5242880"


@pytest.mark.parametrize("value", ["0", "1", "1024", " 2048 ", "5242880"])
def test_valid_settings_are_read_from_the_environment(monkeypatch, value):
    monkeypatch.setenv("S3_MIN_PART_SIZE", value)
    assert Settings().min_part_size == int(value)


@pytest.mark.parametrize("value", ["-1", "1.5", "abc", "", "1e3", "0x10", "+5"])
def test_invalid_setting_is_refused(monkeypatch, value):
    monkeypatch.setenv("S3_MIN_PART_SIZE", value)
    with pytest.raises(ValueError, match="S3_MIN_PART_SIZE"):
        Settings()


@pytest.mark.parametrize("value", ["-1", "1.5", "abc"])
def test_invalid_setting_stops_the_server_from_starting(value):
    out = subprocess.run(
        [sys.executable, "-c", "import server.app"],
        cwd=ROOT,
        env={**os.environ, "S3_MIN_PART_SIZE": value},
        capture_output=True,
        text=True,
    )
    assert out.returncode != 0
    assert "S3_MIN_PART_SIZE" in out.stderr


@pytest.mark.parametrize("value", [-1, 1.5, "1024", True])
def test_invalid_minimum_given_to_the_app_factory_is_refused(tmp_path, value):
    with pytest.raises(ValueError, match="min_part_size"):
        create_app(
            storage=BlobStore(tmp_path / "blobs"),
            metadata=SQLiteMetadataStore(db_path=tmp_path / "meta.db"),
            min_part_size=value,
        )


# --- the minimum ---


def test_a_non_last_part_one_byte_under_the_minimum_is_entity_too_small_and_the_upload_stays_open(
    env,
):
    client, storage, metadata = env
    uid, etags, _ = upload(client, [MIN - 1, MIN])
    before = state(storage, metadata, uid)

    resp = complete(client, uid, numbered(etags))

    assert_refused(env, uid, etags, resp, "EntityTooSmall", before)
    assert resp.headers["content-type"].startswith("application/xml")


def test_a_refused_complete_can_be_retried_after_the_small_part_is_replaced(env):
    client, storage, metadata = env
    uid, etags, datas = upload(client, [MIN - 1, 5])
    assert complete(client, uid, numbered(etags)).status_code == 400

    etags[0] = part(client, uid, 1, b"Z" * MIN)
    resp = complete(client, uid, numbered(etags))

    assert resp.status_code == 200
    assert client.get(f"/{BUCKET}/{KEY}").content == b"Z" * MIN + datas[1]


def test_a_non_last_part_of_exactly_the_minimum_completes(env):
    client, _, _ = env
    uid, etags, datas = upload(client, [MIN, MIN])

    resp = complete(client, uid, numbered(etags))

    assert resp.status_code == 200
    assert client.get(f"/{BUCKET}/{KEY}").content == b"".join(datas)


def test_a_last_part_of_one_byte_completes(env):
    client, _, _ = env
    uid, etags, datas = upload(client, [MIN, 1])

    assert complete(client, uid, numbered(etags)).status_code == 200
    assert client.get(f"/{BUCKET}/{KEY}").content == b"".join(datas)


def test_a_single_part_upload_of_one_byte_completes(env):
    client, _, _ = env
    uid, etags, datas = upload(client, [1])

    assert complete(client, uid, numbered(etags)).status_code == 200
    assert client.get(f"/{BUCKET}/{KEY}").content == datas[0]


def test_a_zero_byte_last_part_completes_as_it_does_without_a_minimum(tmp_path):
    objects = {}
    for minimum in (MIN, 0):
        (tmp_path / str(minimum)).mkdir()
        client, _, _ = build(tmp_path / str(minimum), minimum)
        uid, etags, datas = upload(client, [MIN, 0])
        assert complete(client, uid, numbered(etags)).status_code == 200
        objects[minimum] = client.get(f"/{BUCKET}/{KEY}").content
    assert objects[MIN] == objects[0] == datas[0]


def test_a_zero_byte_non_last_part_is_too_small(env):
    client, storage, metadata = env
    uid, etags, _ = upload(client, [0, MIN])
    before = state(storage, metadata, uid)

    resp = complete(client, uid, numbered(etags))

    assert_refused(env, uid, etags, resp, "EntityTooSmall", before)


def test_only_the_last_listed_part_is_exempt(env):
    client, storage, metadata = env
    uid, etags, _ = upload(client, [MIN, 1, 1])
    before = state(storage, metadata, uid)

    resp = complete(client, uid, numbered(etags))

    assert_refused(env, uid, etags, resp, "EntityTooSmall", before)


# --- the listed parts only ---


def test_a_small_last_listed_part_is_allowed_when_a_further_part_is_uploaded_but_not_listed(env):
    client, _, _ = env
    uid, etags, datas = upload(client, [MIN, 1, MIN])

    resp = complete(client, uid, numbered(etags)[:2])

    assert resp.status_code == 200
    assert client.get(f"/{BUCKET}/{KEY}").content == datas[0] + datas[1]


def test_a_small_listed_part_is_too_small_when_it_is_not_the_last_listed(env):
    client, storage, metadata = env
    uid, etags, _ = upload(client, [MIN, 1, MIN])
    before = state(storage, metadata, uid)

    resp = complete(client, uid, numbered(etags)[1:])

    assert_refused(env, uid, etags, resp, "EntityTooSmall", before)


def test_a_small_part_that_is_not_listed_does_not_count(env):
    client, _, _ = env
    uid, etags, datas = upload(client, [MIN, 1, MIN])

    resp = complete(client, uid, [(1, etags[0]), (3, etags[2])])

    assert resp.status_code == 200
    assert client.get(f"/{BUCKET}/{KEY}").content == datas[0] + datas[2]


# --- the error order ---


def test_no_such_upload_comes_before_everything(env):
    client, _, _ = env
    uid, etags, _ = upload(client, [1, 1])
    resp = complete(client, "nosuchupload", numbered(etags), {"x-amz-mp-object-size": "abc"})
    assert (resp.status_code, code(resp)) == (404, "NoSuchUpload")


def test_the_object_size_syntax_check_comes_before_entity_too_small(env):
    client, storage, metadata = env
    uid, etags, _ = upload(client, [1, 1])
    before = state(storage, metadata, uid)

    resp = complete(client, uid, numbered(etags), {"x-amz-mp-object-size": "abc"})

    assert_refused(env, uid, etags, resp, "InvalidArgument", before)


def test_malformed_xml_comes_before_entity_too_small(env):
    client, storage, metadata = env
    uid, etags, _ = upload(client, [1, 1])
    before = state(storage, metadata, uid)

    resp = client.post(f"/{BUCKET}/{KEY}?uploadId={uid}", content=b"<CompleteMultipartUpload>")

    assert_refused(env, uid, etags, resp, "MalformedXML", before)


def test_invalid_part_order_comes_before_entity_too_small(env):
    client, storage, metadata = env
    uid, etags, _ = upload(client, [1, 1])
    before = state(storage, metadata, uid)

    resp = complete(client, uid, [(2, etags[1]), (1, etags[0])])

    assert_refused(env, uid, etags, resp, "InvalidPartOrder", before)


def test_a_small_part_with_an_invalid_part_is_invalid_part(env):
    client, storage, metadata = env
    uid, etags, _ = upload(client, [1, MIN])
    before = state(storage, metadata, uid)

    resp = complete(client, uid, [(1, etags[0]), (2, '"0123456789abcdef0123456789abcdef"')])

    assert_refused(env, uid, etags, resp, "InvalidPart", before)


def test_a_small_part_with_a_wrong_object_size_is_entity_too_small(env):
    client, storage, metadata = env
    uid, etags, _ = upload(client, [1, MIN])
    before = state(storage, metadata, uid)

    resp = complete(client, uid, numbered(etags), {"x-amz-mp-object-size": str(MIN + 2)})

    assert_refused(env, uid, etags, resp, "EntityTooSmall", before)


def test_the_object_size_mismatch_is_still_reported_when_no_part_is_too_small(env):
    client, storage, metadata = env
    uid, etags, _ = upload(client, [MIN, 1])
    before = state(storage, metadata, uid)

    resp = complete(client, uid, numbered(etags), {"x-amz-mp-object-size": "7"})

    assert_refused(env, uid, etags, resp, "InvalidRequest", before)


def test_a_small_part_with_a_failing_if_none_match_is_entity_too_small(env):
    client, storage, metadata = env
    assert client.put(f"/{BUCKET}/{KEY}", content=b"existing").status_code == 200
    uid, etags, _ = upload(client, [1, MIN])
    before = state(storage, metadata, uid)

    resp = complete(client, uid, numbered(etags), {"If-None-Match": "*"})

    assert (resp.status_code, code(resp)) == (400, "EntityTooSmall")
    assert state(storage, metadata, uid) == before
    assert client.get(f"/{BUCKET}/{KEY}").content == b"existing"


def test_without_a_small_part_the_failing_if_none_match_is_still_412(env):
    client, _, _ = env
    assert client.put(f"/{BUCKET}/{KEY}", content=b"existing").status_code == 200
    uid, etags, _ = upload(client, [MIN, 1])

    resp = complete(client, uid, numbered(etags), {"If-None-Match": "*"})

    assert (resp.status_code, code(resp)) == (412, "PreconditionFailed")


# --- parts made with UploadPartCopy ---


def copy_part(client, uid: str, number: int, first: int, last: int) -> str:
    r = client.put(
        f"/{BUCKET}/{KEY}?partNumber={number}&uploadId={uid}",
        headers={
            "x-amz-copy-source": f"{BUCKET}/source.bin",
            "x-amz-copy-source-range": f"bytes={first}-{last}",
        },
    )
    assert r.status_code == 200
    return ET.fromstring(r.text).findtext("ETag")


def test_parts_made_with_upload_part_copy_are_checked_the_same_way(env):
    client, storage, metadata = env
    source = bytes(range(256)) * 16
    assert client.put(f"/{BUCKET}/source.bin", content=source).status_code == 200
    uid = initiate(client)
    # UploadPartCopy accepts a copied part of 1023 bytes: the check is at Complete.
    etags = [copy_part(client, uid, 1, 0, MIN - 2), copy_part(client, uid, 2, MIN - 1, MIN + 3)]
    before = state(storage, metadata, uid)

    resp = complete(client, uid, numbered(etags))

    assert_refused(env, uid, etags, resp, "EntityTooSmall", before)

    etags[0] = copy_part(client, uid, 1, 0, MIN - 1)
    resp = complete(client, uid, numbered(etags))
    assert resp.status_code == 200
    assert client.get(f"/{BUCKET}/{KEY}").content == source[:MIN] + source[MIN - 1 : MIN + 4]


# --- other minimums ---


def test_with_a_minimum_of_zero_nothing_is_too_small(tmp_path):
    client, _, _ = build(tmp_path, 0)
    uid, etags, datas = upload(client, [0, 1, 1, 0, 1])

    assert complete(client, uid, numbered(etags)).status_code == 200
    assert client.get(f"/{BUCKET}/{KEY}").content == b"".join(datas)


def test_the_minimum_given_to_the_app_is_the_one_applied(tmp_path):
    client, storage, metadata = build(tmp_path, 2048)
    uid, etags, _ = upload(client, [MIN, MIN])
    before = state(storage, metadata, uid)

    resp = complete(client, uid, numbered(etags))

    assert (resp.status_code, code(resp)) == (400, "EntityTooSmall")
    assert state(storage, metadata, uid) == before


def test_upload_part_and_upload_part_copy_are_not_checked(env):
    client, _, _ = env
    uid = initiate(client)
    assert client.put(f"/{BUCKET}/source.bin", content=b"abc").status_code == 200
    assert part(client, uid, 1, b"x")
    assert copy_part(client, uid, 2, 0, 0)


# --- boto3 ---


@pytest.fixture(scope="module")
def s3(tmp_path_factory):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    tmp: Path = tmp_path_factory.mktemp("entity-too-small")
    app = create_app(
        storage=BlobStore(tmp / "blobs"),
        metadata=SQLiteMetadataStore(db_path=tmp / "meta.db"),
        min_part_size=MIN,
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


def test_boto3_complete_with_a_small_non_last_part_raises_entity_too_small(s3):
    uid = s3.create_multipart_upload(Bucket=BUCKET, Key="boto.bin")["UploadId"]
    parts = []
    for number, size in ((1, MIN - 1), (2, 10)):
        r = s3.upload_part(
            Bucket=BUCKET, Key="boto.bin", UploadId=uid, PartNumber=number, Body=b"b" * size
        )
        parts.append({"ETag": r["ETag"], "PartNumber": number})

    with pytest.raises(ClientError) as raised:
        s3.complete_multipart_upload(
            Bucket=BUCKET, Key="boto.bin", UploadId=uid, MultipartUpload={"Parts": parts}
        )

    assert raised.value.response["Error"]["Code"] == "EntityTooSmall"
    assert raised.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    # The upload is still open: a second Complete answers the same, and Abort finds it.
    with pytest.raises(ClientError) as again:
        s3.complete_multipart_upload(
            Bucket=BUCKET, Key="boto.bin", UploadId=uid, MultipartUpload={"Parts": parts}
        )
    assert again.value.response["Error"]["Code"] == "EntityTooSmall"
    s3.abort_multipart_upload(Bucket=BUCKET, Key="boto.bin", UploadId=uid)
