import hashlib
import io

import pytest

from server.multipart.etag import composite_etag
from server.storage.base import PartSpec
from server.storage.blobs import BlobStore, new_blob_id


@pytest.fixture
def backend(tmp_path):
    store = BlobStore(tmp_path / "blobs")
    store.create_tree()
    return store


def _write(backend, data: bytes) -> str:
    etag, _size = backend.write_blob(new_blob_id(), io.BytesIO(data))
    return etag


def _write_parts(backend, part_data: list[bytes]) -> list[str]:
    ids = []
    for d in part_data:
        blob_id = new_blob_id()
        backend.write_blob(blob_id, io.BytesIO(d))
        ids.append(blob_id)
    return ids


def _assemble(backend, ids: list[str], parts: list[PartSpec]) -> str:
    """Assemble the part blobs and return the composite ETag Complete computes."""
    backend.assemble_blob(new_blob_id(), ids)
    return composite_etag([p.etag for p in parts])


# --- Single-part ETag ---


def test_single_part_etag_format(backend):
    data = b"hello world"
    etag = _write(backend, data)
    assert etag.startswith('"')
    assert etag.endswith('"')


def test_single_part_etag_is_md5(backend):
    data = b"hello world"
    expected = '"' + hashlib.md5(data).hexdigest() + '"'
    etag = _write(backend, data)
    assert etag == expected


def test_single_part_etag_empty_body(backend):
    data = b""
    expected = '"' + hashlib.md5(data).hexdigest() + '"'
    etag = _write(backend, data)
    assert etag == expected


def test_single_part_etag_large_body(backend):
    data = b"x" * (1024 * 1024)  # 1 MB
    expected = '"' + hashlib.md5(data).hexdigest() + '"'
    etag = _write(backend, data)
    assert etag == expected


def test_etag_changes_with_different_content(backend):
    etag1 = _write(backend, b"aaa")
    etag2 = _write(backend, b"bbb")
    assert etag1 != etag2


# --- Multipart composite ETag ---


def _part_etag(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


def _composite_etag(part_data_list: list[bytes]) -> str:
    raw = b"".join(bytes.fromhex(hashlib.md5(d).hexdigest()) for d in part_data_list)
    return '"' + hashlib.md5(raw).hexdigest() + f'-{len(part_data_list)}"'


def test_multipart_etag_1_part(backend):
    part_data = b"part-one-data"
    ids = _write_parts(backend, [part_data])
    parts = [PartSpec(part_number=1, etag=_part_etag(part_data))]
    etag = _assemble(backend, ids, parts)
    assert etag == _composite_etag([part_data])


def test_multipart_etag_3_parts(backend):
    part_data = [b"part-one", b"part-two", b"part-three"]
    ids = _write_parts(backend, part_data)
    parts = [PartSpec(part_number=i + 1, etag=_part_etag(d)) for i, d in enumerate(part_data)]
    etag = _assemble(backend, ids, parts)
    assert etag == _composite_etag(part_data)


def test_multipart_etag_10_parts(backend):
    part_data = [f"part-{i:02d}-data".encode() for i in range(1, 11)]
    ids = _write_parts(backend, part_data)
    parts = [PartSpec(part_number=i + 1, etag=_part_etag(d)) for i, d in enumerate(part_data)]
    etag = _assemble(backend, ids, parts)
    assert etag == _composite_etag(part_data)


def test_multipart_etag_is_quoted(backend):
    part_data = b"data"
    ids = _write_parts(backend, [part_data])
    parts = [PartSpec(part_number=1, etag=_part_etag(part_data))]
    etag = _assemble(backend, ids, parts)
    assert etag.startswith('"')
    assert etag.endswith('"')


def test_multipart_etag_suffix_contains_part_count(backend):
    part_data = [b"a", b"b", b"c"]
    ids = _write_parts(backend, part_data)
    parts = [PartSpec(part_number=i + 1, etag=_part_etag(d)) for i, d in enumerate(part_data)]
    etag = _assemble(backend, ids, parts)
    assert etag.endswith('-3"')


def test_multipart_assembled_body_correct(backend, tmp_path):
    """Assembled file bytes must equal concatenation of all part bytes."""
    part_data = [b"hello-", b"world-", b"from-parts"]
    ids = _write_parts(backend, part_data)
    assembled = new_blob_id()
    backend.assemble_blob(assembled, ids)
    with backend.open_blob(assembled) as f:
        result = f.read()
    assert result == b"hello-world-from-parts"


# --- Single-part ETag matches quoted MD5 hex string invariant ---


@pytest.mark.parametrize(
    "data",
    [
        b"",
        b"a",
        b"the quick brown fox",
        bytes(range(256)),
    ],
)
def test_etag_always_quoted_md5(backend, data):
    expected = '"' + hashlib.md5(data).hexdigest() + '"'
    etag = _write(backend, data)
    assert etag == expected
