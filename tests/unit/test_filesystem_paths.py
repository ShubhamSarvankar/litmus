"""The storage layer refuses any bucket, key or upload ID that would leave its
directory, independently of the HTTP-level checks."""

import io
from pathlib import Path

import pytest

from server.storage.base import PartSpec
from server.storage.filesystem import FilesystemBackend, UnsafePathError

UNSAFE_KEYS = ["../escape", "../../escape", "a/../../escape", "..", ".", "../other/k"]


@pytest.fixture
def backend(tmp_path: Path) -> FilesystemBackend:
    return FilesystemBackend(data_dir=tmp_path / "objects", parts_dir=tmp_path / "parts")


@pytest.mark.parametrize("key", UNSAFE_KEYS)
def test_write_object_refuses_unsafe_key_and_writes_nothing(backend, tmp_path, key):
    with pytest.raises(UnsafePathError):
        backend.write_object("b", key, io.BytesIO(b"x"), 1)
    assert not [p for p in tmp_path.rglob("*") if p.is_file()]


@pytest.mark.parametrize("key", UNSAFE_KEYS)
def test_read_and_delete_refuse_unsafe_key(backend, key):
    with pytest.raises(UnsafePathError):
        backend.read_object("b", key)
    with pytest.raises(UnsafePathError):
        backend.delete_object("b", key)


@pytest.mark.parametrize("key", UNSAFE_KEYS)
def test_object_exists_is_false_for_unsafe_key(backend, key):
    assert backend.object_exists("b", key) is False


@pytest.mark.parametrize("bucket", ["..", ".", "../x"])
def test_unsafe_bucket_is_refused(backend, bucket):
    with pytest.raises(UnsafePathError):
        backend.write_object(bucket, "k", io.BytesIO(b"x"), 1)


@pytest.mark.parametrize("upload_id", ["..", "../..", "../objects"])
def test_unsafe_upload_id_is_refused(backend, tmp_path, upload_id):
    with pytest.raises(UnsafePathError):
        backend.write_part(upload_id, 1, io.BytesIO(b"x"))
    with pytest.raises(UnsafePathError):
        backend.delete_parts(upload_id)
    with pytest.raises(UnsafePathError):
        backend.assemble_parts("b", "k", upload_id, [PartSpec(1, '"' + "0" * 32 + '"')])
    assert (tmp_path / "objects").is_dir() and (tmp_path / "parts").is_dir()


def test_safe_paths_still_work(backend):
    backend.write_object("b", "dir/sub/file..txt", io.BytesIO(b"ok"), 2)
    assert backend.object_exists("b", "dir/sub/file..txt")
    with backend.read_object("b", "dir/sub/file..txt") as f:
        assert f.read() == b"ok"
