from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from server.app import create_app
from server.metadata.sqlite import SQLiteMetadataStore
from server.storage.filesystem import FilesystemBackend


@pytest.fixture
def tmp_deps(tmp_path: Path):
    """Fresh storage + metadata backends in a temp directory."""
    storage = FilesystemBackend(
        data_dir=tmp_path / "objects",
        parts_dir=tmp_path / "parts",
    )
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    return storage, metadata, tmp_path


@pytest.fixture
def s3_server(tmp_deps):
    """TestClient wired to a fresh in-process server. Yields (client, storage, metadata)."""
    storage, metadata, tmp_path = tmp_deps
    app = create_app(storage=storage, metadata=metadata)
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client, storage, metadata


def make_faulty_server(tmp_path: Path, injector):
    """Build a TestClient whose storage backend is wrapped with the given FaultInjector."""
    storage = FilesystemBackend(
        data_dir=tmp_path / "objects",
        parts_dir=tmp_path / "parts",
    )
    metadata = SQLiteMetadataStore(db_path=tmp_path / "meta.db")
    faulty_storage = injector.wrap_backend(storage)
    app = create_app(storage=faulty_storage, metadata=metadata)
    client = TestClient(app, raise_server_exceptions=False)
    return client, faulty_storage, metadata, storage
