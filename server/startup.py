"""Startup checks of docs/design/storage-layout.md §7: the old layout's variables,
the blob directory lock, and the store identity that ties one database to one blob
directory (I6, I7).

Every refusal raises StartupRefused before anything in blobs/ is deleted, so the
sweep never judges one store's files against another store's registry.
"""

import logging
import os

from server.crash_points import STORE_INIT_AFTER_COMMIT, crash_point
from server.storage.blobs import StoreInUse, new_blob_id

logger = logging.getLogger(__name__)

# Environment variables of the old storage layout (decisions 4 and 6): refused,
# never silently ignored.
OLD_LAYOUT_VARIABLES = ("S3_DATA_DIR", "S3_PARTS_DIR")


class StartupRefused(RuntimeError):
    """The server must not start on this configuration or data."""


def _refuse(message: str) -> None:
    logger.error("startup refused: %s", message)
    raise StartupRefused(message)


def refuse_old_environment() -> None:
    """§7 step 2."""
    found = [name for name in OLD_LAYOUT_VARIABLES if name in os.environ]
    if found:
        _refuse(
            f"{', '.join(found)} belong to the old storage layout and are no longer used; "
            "unset them and set S3_BLOB_DIR (and S3_DB_PATH) instead. Data written under "
            "the old layout is not migrated."
        )


def refuse_missing_blob_dir(metadata, storage) -> None:
    """§7 step 4: the database belongs to a blob directory that is not there."""
    store = metadata.get_store()
    if store is not None and not storage.root.is_dir():
        _refuse(
            f"blob directory {storage.root} does not exist, but the database already "
            f"belongs to store {store.store_id} (blob directory {store.blob_dir})"
        )


def lock_blob_dir(storage) -> int:
    """§7 step 5: create blobs/ if missing and take its exclusive lock (I6)."""
    storage.create_root()
    try:
        return storage.acquire_lock()
    except StoreInUse:
        _refuse(f"blob directory {storage.root} is already in use by another server")


def establish_store_identity(metadata, storage) -> str:
    """§7 step 6: check the store ID on both sides, or finish a first start.

    Decision 16: a one-sided store (an 'initializing' row) is finished only by the
    blob directory recorded with it, and only if it never held data: no row in any
    table but store and schema_version, and nothing under blobs/ but LOCK, empty
    fanout directories, a tmp/ that is empty or holds only tmp/STORE_ID.tmp, and
    STORE_ID if it already holds this store's ID.
    """
    blob_dir = os.path.realpath(storage.root)
    while True:
        store = metadata.get_store()
        marker = storage.read_marker()

        if store is not None and store.state == "ready":
            if marker == store.store_id:
                return store.store_id
            if marker is None:
                _refuse(
                    f"blob directory {blob_dir} has no STORE_ID marker, but the database "
                    f"belongs to store {store.store_id}"
                )
            _refuse(
                f"blob directory {blob_dir} belongs to store {marker!r}, the database to "
                f"store {store.store_id}"
            )

        if store is None:
            if marker is not None:
                _refuse(
                    f"the database has no store ID, but blob directory {blob_dir} belongs "
                    f"to store {marker!r}"
                )
            if metadata.holds_data():
                # Binding this database to a new blob directory would make the right
                # one refused for good.
                _refuse(
                    "the database has rows but no store ID, so it was not created for "
                    f"blob directory {blob_dir}; refusing to bind it to that directory"
                )
            obstacles = storage.unexpected_entries(tmp_holds_only_marker_temp=False, marker=None)
            if obstacles:
                _refuse(
                    f"blob directory {blob_dir} holds data but no STORE_ID marker: "
                    f"{[str(p) for p in obstacles[:5]]}"
                )
            store_id = new_blob_id()
            if not metadata.create_store(store_id, blob_dir):
                continue  # another server initialized the database meanwhile: re-read
            logger.info("first start: store %s, blob directory %s", store_id, blob_dir)
            crash_point(STORE_INIT_AFTER_COMMIT)
            _finish(metadata, storage, store_id)
            return store_id

        # store.state == "initializing": a first start that was cut short.
        if store.blob_dir != blob_dir:
            _refuse(
                f"the database's store {store.store_id} was initialized for blob directory "
                f"{store.blob_dir}, not {blob_dir}"
            )
        if marker is not None and marker != store.store_id:
            _refuse(
                f"blob directory {blob_dir} belongs to store {marker!r}, the database to "
                f"store {store.store_id}"
            )
        if metadata.holds_data():
            _refuse(
                f"the database's store {store.store_id} never finished its first start but "
                "has held data; refusing to finish it"
            )
        obstacles = storage.unexpected_entries(
            tmp_holds_only_marker_temp=True, marker=store.store_id
        )
        if obstacles:
            _refuse(
                f"store {store.store_id} never finished its first start but blob directory "
                f"{blob_dir} has held data: {[str(p) for p in obstacles[:5]]}"
            )
        logger.info("finishing the first start of store %s", store.store_id)
        _finish(metadata, storage, store.store_id)
        return store.store_id


def _finish(metadata, storage, store_id: str) -> None:
    """First start, steps c and d: write the marker, then mark the store ready."""
    if storage.read_marker() != store_id:
        storage.write_marker(store_id)
    metadata.mark_store_ready(store_id)
