import hashlib
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.filesystem import FilesystemBackend


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    storage = FilesystemBackend(
        data_dir=tmp_path / "objects",
        parts_dir=tmp_path / "parts",
    )
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    app = create_app(storage=storage, metadata=metadata)
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def bucket(client) -> str:
    client.put("/mp-bucket")
    return "mp-bucket"


# --- XML helpers ---


def _xml(text: str) -> ET.Element:
    return ET.fromstring(text)


def _error_code(response) -> str:
    return _xml(response.text).findtext("Code")


def _complete_xml(parts: list[tuple[int, str]]) -> bytes:
    """Build CompleteMultipartUpload XML body from [(part_number, etag), ...]."""
    root = ET.Element("CompleteMultipartUpload")
    for num, etag in parts:
        part = ET.SubElement(root, "Part")
        ET.SubElement(part, "PartNumber").text = str(num)
        ET.SubElement(part, "ETag").text = etag
    return ET.tostring(root)


def _composite_etag(part_bodies: list[bytes]) -> str:
    raw = b"".join(bytes.fromhex(hashlib.md5(d).hexdigest()) for d in part_bodies)
    return '"' + hashlib.md5(raw).hexdigest() + f'-{len(part_bodies)}"'


# --- initiate ---


def test_initiate_returns_200(client, bucket):
    resp = client.post(f"/{bucket}/mykey?uploads")
    assert resp.status_code == 200


def test_initiate_returns_upload_id(client, bucket):
    resp = client.post(f"/{bucket}/mykey?uploads")
    upload_id = _xml(resp.text).findtext("UploadId")
    assert upload_id is not None and len(upload_id) > 0


def test_initiate_returns_bucket_and_key(client, bucket):
    resp = client.post(f"/{bucket}/mykey?uploads")
    root = _xml(resp.text)
    assert root.findtext("Bucket") == bucket
    assert root.findtext("Key") == "mykey"


def test_initiate_nonexistent_bucket_returns_404(client):
    resp = client.post("/no-bucket/key?uploads")
    assert resp.status_code == 404
    assert _error_code(resp) == "NoSuchBucket"


# --- upload part ---


def test_upload_part_returns_200(client, bucket):
    uid = _xml(client.post(f"/{bucket}/k?uploads").text).findtext("UploadId")
    resp = client.put(f"/{bucket}/k?partNumber=1&uploadId={uid}", content=b"part-data")
    assert resp.status_code == 200


def test_upload_part_returns_etag(client, bucket):
    uid = _xml(client.post(f"/{bucket}/k?uploads").text).findtext("UploadId")
    resp = client.put(f"/{bucket}/k?partNumber=1&uploadId={uid}", content=b"part-data")
    assert "etag" in resp.headers


def test_upload_part_etag_is_md5(client, bucket):
    body = b"part-data"
    uid = _xml(client.post(f"/{bucket}/k?uploads").text).findtext("UploadId")
    resp = client.put(f"/{bucket}/k?partNumber=1&uploadId={uid}", content=body)
    expected = '"' + hashlib.md5(body).hexdigest() + '"'
    assert resp.headers["etag"] == expected


def test_upload_part_invalid_upload_id(client, bucket):
    resp = client.put(f"/{bucket}/k?partNumber=1&uploadId=no-such-id", content=b"data")
    assert resp.status_code == 404
    assert _error_code(resp) == "NoSuchUpload"


# --- complete ---


def test_complete_full_lifecycle(client, bucket):
    """Initiate → 3 parts → complete → GET assembled object matches original."""
    parts_data = [b"first-part--", b"second-part-", b"third-part--"]
    uid = _xml(client.post(f"/{bucket}/assembled?uploads").text).findtext("UploadId")

    part_etags = []
    for i, data in enumerate(parts_data, start=1):
        resp = client.put(f"/{bucket}/assembled?partNumber={i}&uploadId={uid}", content=data)
        part_etags.append(resp.headers["etag"])

    complete_body = _complete_xml(list(enumerate(part_etags, start=1)))
    resp = client.post(f"/{bucket}/assembled?uploadId={uid}", content=complete_body)
    assert resp.status_code == 200

    get_resp = client.get(f"/{bucket}/assembled")
    assert get_resp.status_code == 200
    assert get_resp.content == b"".join(parts_data)


def test_complete_composite_etag(client, bucket):
    parts_data = [b"aaaa", b"bbbb", b"cccc"]
    uid = _xml(client.post(f"/{bucket}/etag-check?uploads").text).findtext("UploadId")

    part_etags = []
    for i, data in enumerate(parts_data, start=1):
        resp = client.put(f"/{bucket}/etag-check?partNumber={i}&uploadId={uid}", content=data)
        part_etags.append(resp.headers["etag"])

    complete_body = _complete_xml(list(enumerate(part_etags, start=1)))
    resp = client.post(f"/{bucket}/etag-check?uploadId={uid}", content=complete_body)
    returned_etag = _xml(resp.text).findtext("ETag")
    assert returned_etag == _composite_etag(parts_data)


def test_complete_object_visible_after_completion(client, bucket):
    uid = _xml(client.post(f"/{bucket}/vis?uploads").text).findtext("UploadId")
    resp = client.put(f"/{bucket}/vis?partNumber=1&uploadId={uid}", content=b"data")
    etag = resp.headers["etag"]
    client.post(f"/{bucket}/vis?uploadId={uid}", content=_complete_xml([(1, etag)]))
    assert client.get(f"/{bucket}/vis").status_code == 200


def test_complete_part_overwrite(client, bucket):
    """Upload part 2 twice; complete with second ETag; verify first bytes are gone."""
    uid = _xml(client.post(f"/{bucket}/overwrite?uploads").text).findtext("UploadId")
    client.put(f"/{bucket}/overwrite?partNumber=1&uploadId={uid}", content=b"part-one-")
    # Upload part 2 twice
    client.put(f"/{bucket}/overwrite?partNumber=2&uploadId={uid}", content=b"ORIGINAL-")
    resp2 = client.put(f"/{bucket}/overwrite?partNumber=2&uploadId={uid}", content=b"REPLACED-")
    etag1 = client.put(
        f"/{bucket}/overwrite?partNumber=1&uploadId={uid}", content=b"part-one-"
    ).headers["etag"]
    etag2 = resp2.headers["etag"]

    complete_body = _complete_xml([(1, etag1), (2, etag2)])
    client.post(f"/{bucket}/overwrite?uploadId={uid}", content=complete_body)

    body = client.get(f"/{bucket}/overwrite").content
    assert b"REPLACED-" in body
    assert b"ORIGINAL-" not in body


def test_complete_wrong_etag_returns_400(client, bucket):
    uid = _xml(client.post(f"/{bucket}/bad-etag?uploads").text).findtext("UploadId")
    client.put(f"/{bucket}/bad-etag?partNumber=1&uploadId={uid}", content=b"data")
    complete_body = _complete_xml([(1, '"wrongetag"')])
    resp = client.post(f"/{bucket}/bad-etag?uploadId={uid}", content=complete_body)
    assert resp.status_code == 400
    assert _error_code(resp) == "InvalidPart"


def test_complete_missing_part_returns_400(client, bucket):
    uid = _xml(client.post(f"/{bucket}/missing?uploads").text).findtext("UploadId")
    client.put(f"/{bucket}/missing?partNumber=1&uploadId={uid}", content=b"data")
    # Reference part 2 which was never uploaded
    complete_body = _complete_xml([(2, '"someetag"')])
    resp = client.post(f"/{bucket}/missing?uploadId={uid}", content=complete_body)
    assert resp.status_code == 400
    assert _error_code(resp) == "InvalidPart"


def test_complete_invalid_upload_id_returns_404(client, bucket):
    complete_body = _complete_xml([(1, '"etag"')])
    resp = client.post(f"/{bucket}/k?uploadId=no-such-id", content=complete_body)
    assert resp.status_code == 404
    assert _error_code(resp) == "NoSuchUpload"


# --- abort ---


def test_abort_returns_204(client, bucket):
    uid = _xml(client.post(f"/{bucket}/abort-key?uploads").text).findtext("UploadId")
    resp = client.delete(f"/{bucket}/abort-key?uploadId={uid}")
    assert resp.status_code == 204


def test_abort_object_not_visible(client, bucket):
    uid = _xml(client.post(f"/{bucket}/abort-vis?uploads").text).findtext("UploadId")
    client.put(f"/{bucket}/abort-vis?partNumber=1&uploadId={uid}", content=b"data")
    client.delete(f"/{bucket}/abort-vis?uploadId={uid}")
    assert client.get(f"/{bucket}/abort-vis").status_code == 404


def test_abort_invalid_upload_id_returns_404(client, bucket):
    resp = client.delete(f"/{bucket}/k?uploadId=no-such-id")
    assert resp.status_code == 404
    assert _error_code(resp) == "NoSuchUpload"


# --- plain object operations still work alongside multipart ---


def test_plain_put_get_unaffected(client, bucket):
    client.put(f"/{bucket}/plain.txt", content=b"plain body")
    resp = client.get(f"/{bucket}/plain.txt")
    assert resp.status_code == 200
    assert resp.content == b"plain body"


def test_plain_delete_unaffected(client, bucket):
    client.put(f"/{bucket}/del.txt", content=b"data")
    assert client.delete(f"/{bucket}/del.txt").status_code == 204
    assert client.get(f"/{bucket}/del.txt").status_code == 404


# --- large file (simulates boto3 managed multipart path) ---


def test_large_file_multipart(client, bucket):
    """15 MB assembled from 3 x 5 MB parts — exercises the full streaming path."""
    part_size = 5 * 1024 * 1024
    parts_data = [bytes([i % 256] * part_size) for i in range(3)]

    uid = _xml(client.post(f"/{bucket}/large?uploads").text).findtext("UploadId")
    part_etags = []
    for i, data in enumerate(parts_data, start=1):
        resp = client.put(f"/{bucket}/large?partNumber={i}&uploadId={uid}", content=data)
        part_etags.append(resp.headers["etag"])

    complete_body = _complete_xml(list(enumerate(part_etags, start=1)))
    resp = client.post(f"/{bucket}/large?uploadId={uid}", content=complete_body)
    assert resp.status_code == 200

    get_resp = client.get(f"/{bucket}/large")
    assert get_resp.content == b"".join(parts_data)
