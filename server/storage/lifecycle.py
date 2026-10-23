"""Write-path steps shared by the handlers: RELEASE and GC from
docs/design/storage-layout.md §6.1.

Both are best effort. A file that cannot be removed, or a row that cannot be
dropped, is left for the startup sweep, which finds it through the registry.
"""

import logging

logger = logging.getLogger(__name__)


def discard_new_blob(storage, metadata, blob_id: str) -> None:
    """A registered blob that will never be referenced (failed write, refused
    commit): unlink its file and release its pending row.

    Does nothing unless the row is still pending, so it can never unlink a blob a
    commit made live. If any file of the blob may remain, the row stays pending so
    the sweep removes the file. Never raises: it runs on error paths, where the
    original exception must be the one that propagates.
    """
    try:
        if metadata.blob_state(blob_id) != "pending":
            return
        storage.remove_blob(blob_id)
        if storage.has_files(blob_id):
            return
        metadata.release_blob(blob_id)
    except Exception:
        logger.warning(
            "could not discard new blob %s; the startup sweep will", blob_id, exc_info=True
        )


def collect_garbage(storage, metadata, blob_ids: list[str]) -> None:
    """GC: unlink blobs a committed transaction made garbage, then drop the rows of
    the ones that are gone."""
    unlinked = []
    for blob_id in blob_ids:
        try:
            storage.remove_blob(blob_id)
        except OSError:
            # Windows refuses to unlink a file a reader has open; the row stays
            # garbage and the next sweep removes the file.
            logger.warning("could not unlink garbage blob %s", blob_id, exc_info=True)
            continue
        unlinked.append(blob_id)
    if unlinked:
        try:
            metadata.purge_garbage(unlinked)
        except Exception:
            logger.warning("could not purge garbage rows %s", unlinked, exc_info=True)
