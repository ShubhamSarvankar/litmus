# Crash model

## Scope

Litmus is tested against one failure: the server process being killed with SIGKILL
and started again on the same data. The crash tests run on POSIX systems only; they
are skipped on Windows, where Litmus also skips directory fsync.

Not covered and not tested:

- OS crash or kernel panic.
- Power loss and storage hardware failure.
- Silent corruption of stored bytes. GET does not re-hash a blob before serving it.
- More than one server on the same data. Litmus is a single process, and a second
  server on the same blob directory refuses to start (see "Startup" below).

## Invariants after a kill and restart

1. An object whose PutObject received 200 is readable with the same bytes and ETag.
   The 200 is sent only after the commit that makes the object visible.
2. An object whose CompleteMultipartUpload received 200 is readable with the
   assembled bytes and the composite ETag.
3. A write killed before its commit changes nothing visible. A new key stays absent;
   an existing object keeps its previous bytes and ETag, because new bytes are always
   written to a fresh file.
4. A DeleteObject or AbortMultipartUpload killed before its commit changes nothing.
   A DeleteObject, DeleteObjects, AbortMultipartUpload or DeleteBucket killed after
   its commit has its full effect.
5. A multipart upload is never left open while its object is committed: the object
   row and the upload completion are one transaction.
6. A GET returns one whole version of an object, with that version's ETag and
   length, or an error status. It never mixes two versions, and never returns an
   empty or partial body under a success status.

## What the tests prove, and what they do not

The Tier B tests in `chaos/scenarios/test_crash_points.py` start a real server with
`S3_TEST_CRASH_POINT` set to a named step. The server kills itself with SIGKILL when a
request reaches that step. Each test then checks the files and database rows left
while the server is down, the startup sweep's report, what clients see after the
restart, and what the client's retry does. These tests prove the order of the steps:
what is visible, and what is left behind, after a kill at each step of PutObject,
DeleteObject, DeleteObjects, UploadPart, CompleteMultipartUpload,
AbortMultipartUpload, DeleteBucket, the startup sweep and the first start.

Under SIGKILL the operating system's page cache survives, so data that was written
but not fsynced is still there after the restart. The fsync calls on files and
directories, and SQLite's `synchronous=FULL`, therefore make no difference to any
Tier B test. They only matter for an OS crash or power loss, which are out of scope.

The barrier order tests in `chaos/scenarios/test_barrier_order.py` record the calls
made by PutObject, UploadPart and CompleteMultipartUpload. They check that the fsync
of the new blob's file, its rename into place, the fsync of its directory and the
commit that makes it visible all happen, in that order, and that the blob it
replaces is unlinked only after that commit. They do not cover the fsync inside
SQLite's commit, the fsyncs made at startup, or the fsyncs of the store marker. No
test checks that any fsync makes data durable across a power loss.

## Startup

Before the server accepts requests it runs these steps in order:

1. Refuse to start if `S3_DATA_DIR` or `S3_PARTS_DIR` is set. Both belong to the
   storage layout used before this one.
2. Open the database. Refuse a database with schema version 1. Data written under
   the earlier layout is not migrated.
3. Refuse to start if the blob directory does not exist while the database already
   has a store ID.
4. Create the blob directory if needed and take an exclusive lock on `blobs/LOCK`. A
   second server using the same blob directory refuses to start.
5. Check the store ID (below).
6. Create the fanout directories and `blobs/tmp/`.
7. Run the startup sweep (below).

Every refusal happens before the sweep, so a refused configuration loses nothing:
the refusal tests check that every file is unchanged apart from `LOCK`.

### Store ID

The first start generates a store ID and writes it to the database's `store` table
and to `blobs/STORE_ID`. Every later start requires both to hold the same ID. The
server refuses to start when:

- the two IDs differ;
- only one side has an ID;
- on a first start, the database already has rows;
- on a first start, the blob directory holds data but no `STORE_ID`.

A first start killed after writing the database row and before writing the marker is
finished by the next start, but only with the same blob directory and only if the
store never held data. This keeps a database from being bound to the wrong blob
directory, after which the right one would be refused.

## Startup sweep

Every blob file is recorded in a registry table from before it is created until
after it is unlinked, in one of three states: `pending` (being written), `live`
(referenced by an object or part) or `garbage` (no longer referenced). The sweep uses
that record to decide what it may delete.

| Found | Action | Test |
|---|---|---|
| A temp file in `blobs/tmp/` | Deleted; reported as `LeftoverTemp` | `test_put_killed_mid_write` |
| A `pending` blob: a write that died before its commit | Its file, if any, and its row deleted; reported as `InterruptedWrite` | `test_put_killed_after_register`, `test_put_killed_after_bytes_before_metadata` |
| A `garbage` blob whose file is still present: cleanup cut short | File and row deleted; reported as `InterruptedCleanup` | `test_overwrite_killed_after_commit`, `test_complete_killed_after_commit_before_part_cleanup` |
| A `garbage` blob whose file is gone | Row deleted; not reported | `test_overwrite_killed_after_gc_unlink` |
| An object row whose blob file is missing | Reported as `OrphanedMetadata` | `test_sweep_detects_orphaned_metadata` |
| A part of an open upload whose blob file is missing | Reported as `MissingPart` | `test_sweep_detects_missing_part` |
| A live blob whose size differs from its row | Reported as `SizeMismatch` | none |
| A registry row that disagrees with the rows referring to it | Reported as `RegistryMismatch`; nothing deleted | none |
| A file under `blobs/` with no registry row, or whose name is not a blob ID | Reported as `UntrackedFile`; never touched | none |

The last three rows describe `server/consistency.py`; no test covers them. The two
report-only rows that have tests check the report; no test checks that the sweep
leaves those rows in place.

The sweep never stops the server from starting. If the server is killed during the
sweep, the next start finishes it.

Deleting is limited to files the registry proves are leftovers. A `pending` blob was
never referenced by a committed row, and no 200 was sent for it. A `garbage` blob lost
its last reference in a committed transaction. Before deleting either, the sweep
checks that no object or part row refers to it. The lock and the store ID make sure
the registry belongs to the blob directory being swept.

## SQLite

Every connection sets `journal_mode=WAL`, `synchronous=FULL` and `foreign_keys=ON`
(`server/metadata/sqlite.py`). `synchronous=FULL` makes SQLite fsync its log at every
commit; like the file and directory fsyncs, that only matters for an OS crash or
power loss.
