"""Startup consistency sweep (docs/design/storage-layout.md §7).

Runs before the server accepts requests. It deletes temp files and the files of
pending and garbage registry rows, which the registry proves are the server's own
leftovers, and reports everything else without touching it. It never stops the
server from starting.

Every action removes a file before its row, so a kill during the sweep leaves rows
without files, which the next sweep purges.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class OrphanedMetadata:
    """A live object row whose blob file is missing. Reported only."""

    bucket: str
    key: str


@dataclass
class MissingPart:
    """A part of an open upload whose blob file is missing. Reported only."""

    upload_id: str
    part_number: int


@dataclass
class LeftoverTemp:
    """A file in tmp/ from a write that never finished. Deleted."""

    path: Path


@dataclass
class InterruptedWrite:
    """A pending registry row: a write that died before its commit. Its file, if
    any, and its row are deleted."""

    blob_id: str
    kind: str
    bucket: str
    key: str
    upload_id: str | None
    part_number: int | None
    bytes_durable: bool


@dataclass
class InterruptedCleanup:
    """A garbage registry row whose file is still present: a GC that was cut short.
    The file and the row are deleted."""

    blob_id: str
    kind: str
    bucket: str
    key: str
    upload_id: str | None
    part_number: int | None


@dataclass
class SizeMismatch:
    """A live blob whose file size differs from its row. Reported only."""

    blob_id: str
    bucket: str
    key: str
    expected: int
    actual: int


@dataclass
class RegistryMismatch:
    """A registry row and the rows referring to it disagree. Reported only."""

    blob_id: str
    detail: str


@dataclass
class UntrackedFile:
    """An entry under blobs/ with no registry row, or that cannot be a blob.
    Reported only, never touched."""

    path: Path


Issue = (
    OrphanedMetadata
    | MissingPart
    | LeftoverTemp
    | InterruptedWrite
    | InterruptedCleanup
    | SizeMismatch
    | RegistryMismatch
    | UntrackedFile
)


@dataclass
class ConsistencyReport:
    issues: list[Issue] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return len(self.issues) == 0


def run_consistency_sweep(metadata, storage) -> ConsistencyReport:
    """Clean up after a previous crash and report what cannot be cleaned up."""
    issues: list[Issue] = []

    def report(issue: Issue, level: int, message: str, *args) -> None:
        issues.append(issue)
        logger.log(level, "consistency: " + message, *args)

    # 1. tmp/: every entry belonged to a request of a process that is gone.
    for path in storage.temp_entries():
        if not path.is_file():
            report(UntrackedFile(path), logging.WARNING, "untracked entry in tmp/ — %s", path)
            continue
        try:
            path.unlink()
        except OSError as e:
            logger.warning("consistency: could not delete temp file %s: %s", path, e)
        report(LeftoverTemp(path), logging.INFO, "deleted leftover temp file %s", path)

    # 2. pending and garbage rows: no committed row refers to these blobs.
    for rec in metadata.list_blob_records():
        if rec.state == "live":
            continue
        if metadata.blob_is_referenced(rec.blob_id):
            report(
                RegistryMismatch(rec.blob_id, f"{rec.state} blob is referenced"),
                logging.WARNING,
                "%s blob %s is still referenced; left alone",
                rec.state,
                rec.blob_id,
            )
            continue
        had_file = storage.blob_size(rec.blob_id) is not None
        try:
            storage.remove_blob(rec.blob_id)
        except OSError as e:
            logger.warning("consistency: could not delete blob %s: %s", rec.blob_id, e)
            had_file = True
            removed = False
        else:
            removed = True
        names = (rec.blob_id, rec.kind, rec.bucket, rec.key, rec.upload_id, rec.part_number)
        if rec.state == "pending":
            report(
                InterruptedWrite(*names, bytes_durable=had_file),
                logging.WARNING,
                "interrupted write — %s blob %s for bucket=%s key=%s upload_id=%s part=%s "
                "(bytes durable: %s); removed",
                rec.kind,
                rec.blob_id,
                rec.bucket,
                rec.key,
                rec.upload_id,
                rec.part_number,
                had_file,
            )
        elif had_file:
            report(
                InterruptedCleanup(*names),
                logging.INFO,
                "interrupted cleanup — garbage %s blob %s (bucket=%s key=%s upload_id=%s "
                "part=%s); removed",
                rec.kind,
                rec.blob_id,
                rec.bucket,
                rec.key,
                rec.upload_id,
                rec.part_number,
            )
        if removed:
            metadata.delete_blob_record(rec.blob_id, rec.state)

    # 3. live rows and the rows that refer to them.
    referenced = set()
    for ref in metadata.object_references():
        referenced.add(ref["blob_id"])
        if ref["blob_state"] != "live":
            report(
                RegistryMismatch(ref["blob_id"], f"object row names a {ref['blob_state']} blob"),
                logging.WARNING,
                "object bucket=%s key=%s names blob %s in state %s",
                ref["bucket"],
                ref["key"],
                ref["blob_id"],
                ref["blob_state"],
            )
            continue
        size = storage.blob_size(ref["blob_id"])
        if size is None:
            report(
                OrphanedMetadata(bucket=ref["bucket"], key=ref["key"]),
                logging.WARNING,
                "orphaned metadata — no file on disk for bucket=%s key=%s",
                ref["bucket"],
                ref["key"],
            )
        elif size != ref["size"]:
            report(
                SizeMismatch(ref["blob_id"], ref["bucket"], ref["key"], ref["size"], size),
                logging.WARNING,
                "size mismatch — bucket=%s key=%s blob %s is %d bytes, row says %d",
                ref["bucket"],
                ref["key"],
                ref["blob_id"],
                size,
                ref["size"],
            )
    for ref in metadata.part_references():
        referenced.add(ref["blob_id"])
        if ref["blob_state"] != "live" or ref["upload_state"] != "open":
            report(
                RegistryMismatch(
                    ref["blob_id"],
                    f"part of a {ref['upload_state']} upload names a {ref['blob_state']} blob",
                ),
                logging.WARNING,
                "part upload_id=%s part=%d names blob %s in state %s (upload %s)",
                ref["upload_id"],
                ref["part_number"],
                ref["blob_id"],
                ref["blob_state"],
                ref["upload_state"],
            )
            continue
        size = storage.blob_size(ref["blob_id"])
        if size is None:
            report(
                MissingPart(upload_id=ref["upload_id"], part_number=ref["part_number"]),
                logging.WARNING,
                "missing part file — upload_id=%s part_number=%d",
                ref["upload_id"],
                ref["part_number"],
            )
        elif size != ref["size"]:
            report(
                SizeMismatch(ref["blob_id"], "", "", ref["size"], size),
                logging.WARNING,
                "size mismatch — upload_id=%s part=%d blob %s is %d bytes, row says %d",
                ref["upload_id"],
                ref["part_number"],
                ref["blob_id"],
                size,
                ref["size"],
            )

    registry = {rec.blob_id: rec for rec in metadata.list_blob_records()}
    for blob_id, rec in registry.items():
        if rec.state == "live" and blob_id not in referenced:
            report(
                RegistryMismatch(blob_id, "live blob has no reference"),
                logging.WARNING,
                "live blob %s has no reference",
                blob_id,
            )

    # 4. Files the registry does not know: reported, never touched.
    for path, blob_id in storage.scan():
        if blob_id is None or blob_id not in registry:
            report(UntrackedFile(path), logging.WARNING, "untracked file — %s", path)

    if issues:
        logger.warning("consistency sweep complete: %d issue(s) found", len(issues))
    else:
        logger.info("consistency sweep complete: no issues found")

    return ConsistencyReport(issues=issues)
