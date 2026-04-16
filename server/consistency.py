import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class OrphanedMetadata:
    bucket: str
    key: str


@dataclass
class MissingPart:
    upload_id: str
    part_number: int


@dataclass
class ConsistencyReport:
    issues: list[OrphanedMetadata | MissingPart] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return len(self.issues) == 0


def run_consistency_sweep(metadata, storage) -> ConsistencyReport:
    """Scan for metadata/data mismatches left by a previous crash.

    Logs each issue as WARNING. Does not auto-repair. Does not prevent startup.
    """
    issues: list[OrphanedMetadata | MissingPart] = []

    # Check 1: every object in metadata must have a file on disk
    for bucket in metadata.list_buckets():
        for obj in metadata.list_objects(bucket.name):
            if not storage.object_exists(bucket.name, obj.key):
                issue = OrphanedMetadata(bucket=bucket.name, key=obj.key)
                issues.append(issue)
                logger.warning(
                    "consistency: orphaned metadata — no file on disk for bucket=%s key=%s",
                    bucket.name,
                    obj.key,
                )

    # Check 2: every recorded part in an incomplete upload must have a file on disk
    for upload in metadata.list_incomplete_uploads():
        for part in metadata.get_parts(upload.upload_id):
            if not storage.part_exists(upload.upload_id, part.part_number):
                issue = MissingPart(upload_id=upload.upload_id, part_number=part.part_number)
                issues.append(issue)
                logger.warning(
                    "consistency: missing part file — upload_id=%s part_number=%d",
                    upload.upload_id,
                    part.part_number,
                )

    if issues:
        logger.warning("consistency sweep complete: %d issue(s) found", len(issues))
    else:
        logger.info("consistency sweep complete: no issues found")

    return ConsistencyReport(issues=issues)
