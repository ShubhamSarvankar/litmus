"""The checksum and time of an UploadPartCopy (server/api/upload_part_copy.py,
docs/design/storage-layout.md §20).

The copied bytes are hashed in E, the effective algorithm of the upload: the one
CreateMultipartUpload named, else CRC64NVME. The part row holds E, the digest of the bytes
copied, and the time of the commit. CopyPartResult returns that time as LastModified, and
Checksum<E> only when the upload named an algorithm (API_CopyPartResult: each element is
"present if the multipart upload request was created with" that algorithm). The expected
digests come from the independent implementations of test_upload_checksums.py.
"""

import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from time import sleep

import pytest
from fastapi.testclient import TestClient
from test_upload_checksums import b64, digest

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.blobs import BlobStore

SRC_BUCKET = "src-bucket"
DST_BUCKET = "dst-bucket"
SRC = "src.bin"
DST = "dest.bin"
BODY = bytes(range(256)) * 4  # 1024 bytes
ALGOS = ["crc32", "crc32c", "crc64nvme", "sha1", "sha256"]
TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"


@pytest.fixture
def env(tmp_path: Path):
    storage = BlobStore(tmp_path / "blobs")
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    client = TestClient(
        create_app(storage=storage, metadata=metadata), raise_server_exceptions=False
    )
    for bucket in (SRC_BUCKET, DST_BUCKET):
        assert client.put(f"/{bucket}").status_code == 200
    assert client.put(f"/{SRC_BUCKET}/{SRC}", content=BODY).status_code == 200
    return client, storage, metadata


def _create(client, algorithm: str | None = None) -> str:
    headers = {"x-amz-checksum-algorithm": algorithm.upper()} if algorithm else {}
    resp = client.post(f"/{DST_BUCKET}/{DST}?uploads", headers=headers)
    assert resp.status_code == 200
    return ET.fromstring(resp.text).findtext("UploadId")


def _copy(client, upload, src=f"{SRC_BUCKET}/{SRC}", byte_range=None):
    headers = {"x-amz-copy-source": src}
    if byte_range:
        headers["x-amz-copy-source-range"] = byte_range
    return client.put(f"/{DST_BUCKET}/{DST}?partNumber=1&uploadId={upload}", headers=headers)


def _elements(resp) -> dict[str, str]:
    root = ET.fromstring(resp.text)
    assert root.tag == "CopyPartResult"
    return {el.tag: el.text for el in root}


@pytest.mark.parametrize("byte_range", [None, "bytes=100-899"], ids=["whole", "range"])
@pytest.mark.parametrize("name", ALGOS)
def test_named_upload_hashes_the_copied_bytes_in_its_algorithm(env, name, byte_range):
    client, storage, metadata = env
    upload = _create(client, name)
    copied = BODY if byte_range is None else BODY[100:900]

    resp = _copy(client, upload, byte_range=byte_range)

    assert resp.status_code == 200
    want = b64(digest(name, copied))
    elements = _elements(resp)
    assert set(elements) == {"ETag", "LastModified", f"Checksum{name.upper()}"}
    assert elements[f"Checksum{name.upper()}"] == want
    (part,) = metadata.get_parts(upload)
    assert (part.checksum_algorithm, part.checksum) == (name.upper(), want)
    assert part.size == len(copied)
    assert [h for h in resp.headers if h.startswith("x-amz-checksum")] == []


def test_unnamed_upload_returns_no_checksum_element(env):
    client, storage, metadata = env
    resp = _copy(client, _create(client))
    assert resp.status_code == 200
    assert set(_elements(resp)) == {"ETag", "LastModified"}


def test_unnamed_upload_stores_crc64nvme_of_the_copied_bytes(env):
    client, storage, metadata = env
    upload = _create(client)
    assert _copy(client, upload, byte_range="bytes=0-499").status_code == 200
    (part,) = metadata.get_parts(upload)
    assert (part.checksum_algorithm, part.checksum) == (
        "CRC64NVME",
        b64(digest("crc64nvme", BODY[:500])),
    )


def test_last_modified_is_the_stored_part_time(env):
    client, storage, metadata = env
    upload = _create(client, "crc32")
    resp = _copy(client, upload)
    (part,) = metadata.get_parts(upload)
    assert _elements(resp)["LastModified"] == part.last_modified.strftime(TIME_FORMAT)
    shown = datetime.strptime(_elements(resp)["LastModified"], TIME_FORMAT)
    assert abs((part.last_modified - shown).total_seconds()) < 1


def test_last_modified_does_not_move_when_the_part_is_read_again(env):
    client, storage, metadata = env
    upload = _create(client, "sha1")
    resp = _copy(client, upload)
    (first,) = metadata.get_parts(upload)
    sleep(1.1)  # past the one second the response shows

    (again,) = metadata.get_parts(upload)

    assert again.last_modified == first.last_modified
    assert _elements(resp)["LastModified"] == again.last_modified.strftime(TIME_FORMAT)


def test_a_source_with_no_stored_checksum_is_hashed_over_the_copied_bytes(env, tmp_path):
    client, storage, metadata = env
    source = ET.fromstring(client.post(f"/{SRC_BUCKET}/mp.bin?uploads").text).findtext("UploadId")
    put = client.put(f"/{SRC_BUCKET}/mp.bin?partNumber=1&uploadId={source}", content=BODY)
    done = client.post(
        f"/{SRC_BUCKET}/mp.bin?uploadId={source}",
        content=f"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>"
        f"<ETag>{put.headers['etag']}</ETag></Part></CompleteMultipartUpload>",
    )
    assert done.status_code == 200
    # Complete stores a checksum now. The NULL columns stand for an object migrated from
    # schema version 2, the only way an object without a checksum arises.
    db = sqlite3.connect(tmp_path / "meta.db")
    try:
        with db:
            db.execute(
                "UPDATE objects SET checksum = NULL, checksum_algorithm = NULL, "
                "checksum_type = NULL WHERE bucket = ? AND key = 'mp.bin'",
                (SRC_BUCKET,),
            )
    finally:
        db.close()
    source_meta = metadata.get_object_meta(SRC_BUCKET, "mp.bin")
    assert (source_meta.checksum, source_meta.checksum_algorithm, source_meta.checksum_type) == (
        None,
        None,
        None,
    )
    upload = _create(client, "crc32c")

    resp = _copy(client, upload, src=f"{SRC_BUCKET}/mp.bin", byte_range="bytes=10-209")

    assert resp.status_code == 200
    assert _elements(resp)["ChecksumCRC32C"] == b64(digest("crc32c", BODY[10:210]))
    (part,) = metadata.get_parts(upload)
    assert part.checksum == b64(digest("crc32c", BODY[10:210]))


def test_a_source_with_a_checksum_in_another_algorithm_is_hashed_over_the_copied_bytes(env):
    client, storage, metadata = env
    claimed = {"x-amz-checksum-sha256": b64(digest("sha256", BODY))}
    assert client.put(f"/{SRC_BUCKET}/sha.bin", content=BODY, headers=claimed).status_code == 200
    assert metadata.get_object_meta(SRC_BUCKET, "sha.bin").checksum_algorithm == "SHA256"
    upload = _create(client, "crc32")

    resp = _copy(client, upload, src=f"{SRC_BUCKET}/sha.bin")

    assert resp.status_code == 200
    elements = _elements(resp)
    assert "ChecksumSHA256" not in elements
    assert elements["ChecksumCRC32"] == b64(digest("crc32", BODY))
    (part,) = metadata.get_parts(upload)
    assert (part.checksum_algorithm, part.checksum) == ("CRC32", b64(digest("crc32", BODY)))
