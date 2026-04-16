import hashlib
import io

import pytest

from server.storage.base import PartSpec
from server.storage.filesystem import FilesystemBackend


@pytest.fixture
def backend(tmp_path):
    return FilesystemBackend(
        data_dir=tmp_path / "objects",
        parts_dir=tmp_path / "parts",
    )


# --- Single-part ETag ---


def test_single_part_etag_format(backend):
    data = b"hello world"
    etag = backend.write_object("b", "k", io.BytesIO(data), len(data))
    assert etag.startswith('"')
    assert etag.endswith('"')


def test_single_part_etag_is_md5(backend):
    data = b"hello world"
    expected = '"' + hashlib.md5(data).hexdigest() + '"'
    etag = backend.write_object("b", "k", io.BytesIO(data), len(data))
    assert etag == expected


def test_single_part_etag_empty_body(backend):
    data = b""
    expected = '"' + hashlib.md5(data).hexdigest() + '"'
    etag = backend.write_object("b", "empty", io.BytesIO(data), 0)
    assert etag == expected


def test_single_part_etag_large_body(backend):
    data = b"x" * (1024 * 1024)  # 1 MB
    expected = '"' + hashlib.md5(data).hexdigest() + '"'
    etag = backend.write_object("b", "large", io.BytesIO(data), len(data))
    assert etag == expected


def test_etag_changes_with_different_content(backend):
    etag1 = backend.write_object("b", "k1", io.BytesIO(b"aaa"), 3)
    etag2 = backend.write_object("b", "k2", io.BytesIO(b"bbb"), 3)
    assert etag1 != etag2


# --- Multipart composite ETag ---


def _part_etag(data: bytes) -> str:
    return '"' + hashlib.md5(data).hexdigest() + '"'


def _composite_etag(part_data_list: list[bytes]) -> str:
    raw = b"".join(bytes.fromhex(hashlib.md5(d).hexdigest()) for d in part_data_list)
    return '"' + hashlib.md5(raw).hexdigest() + f'-{len(part_data_list)}"'


def test_multipart_etag_1_part(backend):
    part_data = b"part-one-data"
    backend.write_part("uid-1", 1, io.BytesIO(part_data))
    parts = [PartSpec(part_number=1, etag=_part_etag(part_data))]
    etag = backend.assemble_parts("b", "k", "uid-1", parts)
    assert etag == _composite_etag([part_data])


def test_multipart_etag_3_parts(backend):
    part_data = [b"part-one", b"part-two", b"part-three"]
    for i, d in enumerate(part_data, start=1):
        backend.write_part("uid-1", i, io.BytesIO(d))
    parts = [PartSpec(part_number=i + 1, etag=_part_etag(d)) for i, d in enumerate(part_data)]
    etag = backend.assemble_parts("b", "k", "uid-1", parts)
    assert etag == _composite_etag(part_data)


def test_multipart_etag_10_parts(backend):
    part_data = [f"part-{i:02d}-data".encode() for i in range(1, 11)]
    for i, d in enumerate(part_data, start=1):
        backend.write_part("uid-1", i, io.BytesIO(d))
    parts = [PartSpec(part_number=i + 1, etag=_part_etag(d)) for i, d in enumerate(part_data)]
    etag = backend.assemble_parts("b", "k", "uid-1", parts)
    assert etag == _composite_etag(part_data)


def test_multipart_etag_is_quoted(backend):
    part_data = b"data"
    backend.write_part("uid-1", 1, io.BytesIO(part_data))
    parts = [PartSpec(part_number=1, etag=_part_etag(part_data))]
    etag = backend.assemble_parts("b", "k", "uid-1", parts)
    assert etag.startswith('"')
    assert etag.endswith('"')


def test_multipart_etag_suffix_contains_part_count(backend):
    part_data = [b"a", b"b", b"c"]
    for i, d in enumerate(part_data, start=1):
        backend.write_part("uid-2", i, io.BytesIO(d))
    parts = [PartSpec(part_number=i + 1, etag=_part_etag(d)) for i, d in enumerate(part_data)]
    etag = backend.assemble_parts("b", "k2", "uid-2", parts)
    assert etag.endswith('-3"')


def test_multipart_assembled_body_correct(backend, tmp_path):
    """Assembled file bytes must equal concatenation of all part bytes."""
    part_data = [b"hello-", b"world-", b"from-parts"]
    for i, d in enumerate(part_data, start=1):
        backend.write_part("uid-3", i, io.BytesIO(d))
    parts = [PartSpec(part_number=i + 1, etag=_part_etag(d)) for i, d in enumerate(part_data)]
    backend.assemble_parts("b", "assembled", "uid-3", parts)
    with backend.read_object("b", "assembled") as f:
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
    etag = backend.write_object("b", "key", io.BytesIO(data), len(data))
    assert etag == expected
