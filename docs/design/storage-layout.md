# Storage layout redesign

Status: steps (a) and (b) implemented. The design follows the requirements below;
the decisions are recorded in §19.

## Requirements

Items marked **(asked)** were requested explicitly for this design. Items marked
**(noted)** were found during the investigation and compatibility work and belong to
the same redesign or constrain it. File paths are as of commit 2026-10-23 (after the
`Expect: 100-continue` fix).

Ground rule that every sequence must keep: bytes before metadata (temp file, fsync,
rename, fsync of the parent directory, then the SQLite commit).

### Bugs the layout must fix

1. **(asked) `k` vs `k/` collision, and file vs directory collision.** Objects are
   stored at `objects/{bucket}/{key}`, so `k` and `k/` map to the same file (a PUT of
   `k/` silently overwrote the committed object `k`, leaving its metadata pointing at
   the wrong bytes), and `a/b` vs `a/b/c` collide as file vs directory (PUT returns 500
   `FileExistsError`). Path derivation: `FilesystemBackend._object_path`,
   `server/storage/filesystem.py`.

2. **(asked) Overwrite crash window.** `_durable_write` renames the new bytes over the
   old file before `put_object_meta` commits, so a kill between the two leaves new
   bytes under the old metadata (size and ETag mismatch: a GET returns a truncated or
   wrong body). This breaks crash-model invariant 3. Sequence:
   `server/api/objects.py:_put_object`, `server/storage/filesystem.py:_durable_write`.
   Assembly in Complete has the same window (`assemble_parts`).

3. **(asked) DELETE unlinks before the metadata delete.** `_delete_object`
   (`server/api/objects.py`) and DeleteObjects (`server/api/buckets.py:_delete_objects`)
   remove the file first, so a kill in between leaves a metadata row with no file
   (GET returns 500 InternalError until the sweep reports it). AbortMultipartUpload
   has the same inversion: `storage.delete_parts` runs before `metadata.abort_upload`
   (`server/api/multipart.py:_abort`).

4. **(asked) Unfsynced key subdirectories.** `final_path.parent.mkdir(parents=True)` in
   `_durable_write` and `assemble_parts` creates intermediate directories without
   fsyncing their parents; only the final file's directory is fsynced, so the
   directory entries leading to it are not durable.

5. **(asked) Complete as one transaction (now fixed; keep it).** Object metadata and
   upload completion used to be separate SQLite transactions. Since 8210a63 they are
   one (`SQLiteMetadataStore.commit_completed_upload`, `server/metadata/sqlite.py`;
   used in `server/multipart/state_machine.py`). The new Complete sequence must keep
   both in a single transaction, and the design must show it explicitly.

6. **(asked) A refused Complete commit can change an object's bytes without its
   metadata.** `commit_completed_upload` returns False (upload not open) after
   `assemble_parts` has already renamed the assembled file over the object's final
   path, so the object's bytes change while its metadata does not. Complete must only
   ever write to a fresh path, so that a refused or failed commit leaves the existing
   object untouched (`server/multipart/state_machine.py`).

7. **(asked) The sweep does not detect orphaned files.** `run_consistency_sweep`
   (`server/consistency.py`) only checks metadata to file (`OrphanedMetadata`) and
   open-upload parts (`MissingPart`). It does not report: object files with no
   metadata row (left by a kill between bytes and commit), part files of completed
   uploads (left by a kill between the Complete commit and part cleanup), or temp
   files (`tmp*` from `mkstemp` in the object's final directory). The first two are
   recorded as strict xfail tests:
   `test_gap_sweep_reports_orphaned_file_after_put_crash` and
   `test_gap_sweep_reports_part_files_of_completed_upload`
   (`chaos/scenarios/test_crash_points.py`). They will XPASS, and fail the run, once
   fixed. The design must say what the sweep reports and whether it repairs.

8. **(asked) Deleting a bucket with multipart uploads in progress leaves orphaned
   upload rows.** `delete_bucket` (`server/api/buckets.py`) only checks
   `object_count`, so it deletes a bucket whose uploads are still open; their
   `multipart_uploads` / `multipart_parts` rows and part files remain and point at a
   bucket that no longer exists (seen in ceph `test_multipart_upload_empty`). Check
   what S3 does: the AbortMultipartUpload reference states that for directory buckets
   "you can't delete the bucket until all the in-progress multipart uploads are
   aborted or completed"; the general purpose bucket behavior still has to be
   confirmed. Design the correct behavior and its sequencing (including crash points).

### Design scope

9. **(asked) Evaluate the proposed layout against alternatives.** The proposal: each
   object version stored under an opaque generated ID in a fixed directory layout,
   metadata mapping key to ID; new bytes become durable at a new path before the
   metadata commit points at them, and the old file is removed only after the commit.

10. **(asked) Exact sequences** for PUT (new key), overwrite, DELETE (single and
    DeleteObjects), CompleteMultipartUpload and AbortMultipartUpload.

11. **(asked) Kill at every step:** the state each step leaves behind after SIGKILL and
    restart, and what the sweep does about it.

12. **(asked) Schema change** and migration from the current layout and schema
    (`_DDL` and `_SCHEMA_VERSION` in `server/metadata/sqlite.py`; existing data dirs
    under `data/objects` and `data/parts`, `server/config.py`).

13. **(asked) CopyObject and UploadPartCopy.** How they fit the layout (a new ID with
    copied bytes, or a shared ID with reference counting). Both returned 501 via
    `x-amz-copy-source` in `server/api/request_guard.py` when this was asked; both are
    implemented now (§6.10, §6.6A, §11).

14. **(asked) Concurrent same-key PUTs.** What happens under the new design. Today they
    are safe only by accident: handlers do blocking file and SQLite I/O inside async
    functions, so the event loop serializes them. Moving I/O to a thread pool would
    reintroduce a race in the current layout.

15. **(asked) Conditional writes evaluated atomically with the metadata commit.**
    If-Match / If-None-Match on PutObject and CompleteMultipartUpload (and later
    CopyObject) must be checked in the same transaction as the metadata commit, after
    the new bytes are durable at a fresh path. A failed precondition must leave the
    old object's bytes and metadata untouched. PutObject and CompleteMultipartUpload
    now do this (§10, §6.7 C4.c), and so does If-Match on DeleteObject (§10, D2.c).
    The other conditional deletes (still 501) should follow the same model.

16. **(asked) Lift the `.` / `..` key segment rejection.** S3 keys are opaque, but keys
    with `.` or `..` segments are rejected with 400 InvalidArgument (and bucket names
    `.` / `..` with InvalidBucketName) because paths are derived from keys. This is a
    documented temporary deviation (`docs/compatibility.md`, "Known gaps"). Once paths
    no longer derive from keys, remove the check (`unsafe_key` in
    `server/api/request_guard.py`, its use in `server/api/buckets.py`) and the tests
    that pin it (`tests/conformance/test_path_safety.py`). Keep the storage-level
    containment check (`_contained` in `server/storage/filesystem.py`) for IDs.

17. **(asked) Tier B tests that prove each crash window.** Use the crash point
    mechanism (`server/crash_points.py`, `S3_TEST_CRASH_POINT`; current points
    `put-after-bytes`, `complete-after-assemble`, `complete-after-commit`). Overwrite
    and DELETE crash tests were deferred from the Tier B work until this design
    (`chaos/scenarios/test_crash_points.py`).

18. **(asked) Which ceph tests it should flip.** Baseline 99 of 330
    (`docs/s3-tests-results.txt` at 8f9c4a9, 2026-10-23). Known direct candidates:
    `test_bucket_list_delimiter_basic`, `test_bucket_listv2_delimiter_basic` and
    `test_bucket_list_delimiter_not_skip_special` fail first on the file vs directory
    collision; they also need listing parameters (category c) to pass.

### Noted along the way

19. **(noted) Path traversal should become structurally impossible.** It is blocked
    today by the key check (item 16) and by `_contained`. With opaque IDs, no client
    string should ever be part of a filesystem path, including upload IDs
    (`_upload_dir` / `_part_path`).

20. **(noted) Bucket directories are never removed.** `delete_bucket` deletes only the
    metadata row; `objects/{bucket}/` (with any leftover subdirectories or temp files)
    stays, and a later bucket with the same name inherits it.

21. **(noted) Completed uploads are never cleaned from metadata.** `multipart_uploads`
    rows (with `completed = 1`) and their `multipart_parts` rows are deleted only on
    abort. Decide what Complete keeps. Part sizes and ETags of completed objects are
    needed for GetObject/HeadObject `partNumber` (j8), GetObjectAttributes `ObjectParts`
    (j7) and re-completing a completed upload, which S3 accepts (j3); the schema should
    allow these. Decision 35 supersedes the j3 claim: a retried Complete is NoSuchUpload.

22. **(noted) Plan the schema migration once.** User metadata (`x-amz-meta-*`,
    category d) and stored system headers (Cache-Control, Expires, Content-Encoding,
    Content-Disposition, Content-Type on multipart, j1) need new object columns. Fold
    them into the same schema change to avoid two migrations. Content-Type passed to
    CreateMultipartUpload is currently dropped (`_initiate` in
    `server/api/multipart.py`; `complete_multipart_upload` uses a default).

23. **(noted) Listing over the new schema.** Prefix listing uses
    `LIKE prefix || '%'` (`list_objects` in `server/metadata/sqlite.py`), where `_`
    and `%` are wildcards and matching ignores ASCII case. Delimiters, markers and
    continuation tokens (category c) need ordered, binary-collated key ranges. The key
    column and indexes should support that. GET `?versions` shares the same gaps and
    must honor `max-keys` before DeleteObjects enforces the 1000-key limit (j9),
    because the ceph harness cleanup pages it with `MaxKeys=128`.

24. **(noted) Temp file placement.** `mkstemp` creates temp files inside the object's
    final directory (bucket namespace), with mode 0600. Decide where temp files live,
    how the sweep finds them, and the file mode.

25. **(noted) Crash-model and publication-barrier docs to rewrite after the redesign.**
    Known inaccuracies:
    - `docs/crash-model.md`: says the sweep detects orphaned files (it does not);
      invariant 3 ("never a body that doesn't match the ETag") is broken by the
      overwrite window (item 2) and the `k` / `k/` collision (item 1); the assembly
      paragraph implies a rollback that only covers the SQLite part.
    - `docs/publication-barrier.md`: step 7 says the sweep detects the orphaned file
      (it does not); "killed between steps 9 and 10, part files linger; the sweep
      detects this as MissingPart" is wrong (MissingPart means a missing file, and
      completed uploads are not swept); "a second PUT of the same key is handled
      correctly (idempotent overwrite)" ignores the overwrite window; the
      concurrent-writer paragraph relies on rename plus commit ordering, but writers
      are safe only because the event loop serializes them (item 14); step 6 does not
      cover new intermediate directories (item 4). Steps 7 and 8 being one transaction
      is now true (item 5).
    - The README's crash and publication-barrier sections repeat some of these claims.


## Design proposal

Status: steps (a) and (b) implemented. Reviewed on 2026-10-23; the decisions are
recorded in §19 and the open questions marked in §18. Requirement numbers are written R1 to R25
(the numbered items above); sections of this design are written §1 to §19.

Notation used in the sequences:

- **[C]** a SQLite commit (`journal_mode=WAL`, `synchronous=FULL`, so one WAL fsync).
- **[Ff]** `fsync` of a file descriptor. **[Fd]** `fsync` of a directory.
- `⚡name` a crash point: the place where `S3_TEST_CRASH_POINT=name` SIGKILLs the server.
- "BEGIN IMMEDIATE ... COMMIT" is one transaction that takes SQLite's write lock at
  `BEGIN`, so every row it reads stays current until it commits.

### §1. Summary

Accepted: **opaque blob IDs with a blob registry** (option A in §2).

- Every object version and every uploaded part is an immutable file
  `blobs/<xx>/<id>`, where `<id>` is 32 random lowercase hex characters and `<xx>` its
  first two. No client string (bucket, key, upload ID, part number) is ever part of a
  path.
- A new table, `blobs`, has one row per blob file, from before the file is created
  until after it is removed. The row's state is `pending` (being written; nothing
  refers to it), `live` (an object or part row refers to it) or `garbage` (a committed
  transaction removed the last reference; the file is waiting to be unlinked).
- Writes: register the new ID **[C]**, write the bytes to a temp file **[Ff]**, rename
  it to its fresh blob path **[Fd]**, then one transaction **[C]** that points the
  object (or part) row at the new blob, marks it `live` and marks the blob it replaced
  `garbage`. The replaced file is unlinked only after that commit.
- Deletes: one transaction **[C]** removes the row and marks its blob `garbage`; the
  file is unlinked after the commit.
- The startup sweep deletes temp files and the files of `pending` and `garbage` rows,
  reports everything else, and never touches a file that has no registry row.
- GET opens the blob named by the row it read. If that file was already unlinked
  because a later commit replaced it, GET reads the row again. An open file
  descriptor pins the bytes of one immutable blob, so a body is never a mix of
  versions.
- A store ID, generated at first start and kept both in the database and in
  `blobs/STORE_ID`, ties one database to one blob directory. The server refuses to
  start, before the sweep, if the two disagree or only one side has it (§7).

Cost compared with today: one more commit per PutObject, UploadPart and Complete (the
registration), plus one commit after each overwrite, delete, Complete and Abort that
removes `garbage` rows once their files are unlinked. Correctness never depends on that
last commit (§6.1, GC).

### §2. Options considered

| | A. Opaque IDs + registry (accepted) | B. Key-derived encoded paths | C. Content-addressed blobs | D. Bytes in SQLite |
|---|---|---|---|---|
| Path | `blobs/<xx>/<random id>` | `objects/<h(bucket)>/<h(key)>` (hash or escaped key, fixed depth) | `blobs/<xx>/<sha256 of bytes>` | none; rows of byte chunks |
| R1 `k` vs `k/`, file vs dir | fixed (no key in path) | fixed (one path component per key) | fixed | fixed |
| R2 overwrite window | fixed (fresh path, old unlinked after commit) | **not fixed**: rename still replaces committed bytes before the commit | fixed (new content, new path) | fixed (one transaction) |
| R3 delete order | fixed (unlink after commit) | fixable by reordering | needs refcount in the commit | fixed |
| R4 dir fsync | fixed (fanout dirs made at startup, never on the write path) | fixed (fixed depth) | fixed | n/a |
| R6 refused Complete | fixed (writes only a fresh path) | **not fixed** | fixed | fixed |
| R7 orphan detection | precise: registry says which files are leftovers, with bucket and key | scan, by key | scan + refcounts | n/a |
| R13 copy | copy bytes to a new ID; refcounted sharing possible later | copy bytes | free (same hash), needs refcounts | copy rows |
| R15 conditional writes | yes, checked in the commit after bytes are durable | **no** (same reason as R2) | yes | yes |
| R16/R19 no client string in paths | yes | only if every component is a hash | yes | yes |
| GET racing overwrite | safe: row names an immutable file; retry on ENOENT (§8) | **unsafe**: GET can read the old row, then open the new bytes (body and ETag from different versions) | safe if refcounts keep files alive | safe |
| New machinery | registry table, sweep rules | path encoding | refcounts, pending state, SHA-256 pass | chunking, range reads over rows |

**B. Key-derived encoded paths.** Encoding each key into one fixed-depth path
component (for example SHA-256 of the key, since S3 keys can be 1024 bytes and file
names 255) removes the collisions and path traversal. It keeps one file per key,
replaced by rename, so the bytes of a committed object still change before the
metadata commit (R2, R6), a precondition found false at commit time has already
overwritten the object (R15), and a GET that reads the row and then opens the path can
get the new bytes with the old size and ETag. Making B safe means giving every write
its own path and pointing the row at it, which is option A with a key-derived prefix.
Rejected.

**C. Content-addressed blobs.** Naming blobs by a hash of their bytes gives fresh paths
for new content and free CopyObject, but two keys with identical bytes share one file.
Deleting one key must not unlink a file the other still uses, so every object row needs
a reference count changed in its commit. A PUT that finds the blob already present
cannot skip the write either: a concurrent delete can drop the count to zero and unlink
the file between the PUT's check and its commit, so C needs A's `pending` state as
well. It also needs a SHA-256 pass besides MD5 (the multipart ETag is not a content
hash). Deduplication is not a goal here. Rejected; A can add refcounted sharing for
CopyObject later (§11) if copying bytes turns out too slow.

**D. Bytes in SQLite.** Object bytes stored as BLOB rows commit atomically with the
metadata and need no filesystem ordering. But SQLite caps a value at about 1 GB by
default (`SQLITE_MAX_LENGTH`), so 5 GiB parts need a chunk table and range reads
over it; with WAL every byte is written twice (WAL, then checkpoint); a 5 GiB
`synchronous=FULL` transaction holds the write lock for its whole length, blocking
every other writer. It also replaces the publication-barrier protocol this project
exists to demonstrate. Rejected.

**A variant, not chosen now: objects as a manifest of part blobs.** Complete
could commit a list of the part blobs instead of assembling a new file (no copy, no
assembly crash window, partNumber GET for free). The cost is on reads: a GET streaming
segment after segment can find a later segment already unlinked by an overwrite that
committed mid-stream. Avoiding that means opening every segment before the first byte
is sent (up to 10,000 file descriptors) or deferring unlinks while readers exist. The
registry would support it later; for now Complete assembles one blob (§6.7).

**A without the `pending` state (scan-based sweep).** Dropping the registration commit
saves one commit per write, and the sweep would treat any file in `blobs/` that no
object or part row references as orphaned. That orphaned file has no bucket or key
anywhere, so the sweep cannot report which write died (the strict xfail
`test_gap_sweep_reports_orphaned_file_after_put_crash` asserts bucket and key; see
§17), and deleting "anything unreferenced" would delete every blob if the server is
started with the wrong `S3_DB_PATH`. With a registry, only files the server itself
recorded as leftovers are deleted. Decision 2 in §19.

### §3. Invariants of the accepted design

- **I1. Immutable blobs.** Bytes are written only to `tmp/<id>.tmp`. After its one
  rename to `blobs/<xx>/<id>` a blob file is never opened for writing, renamed or
  truncated; it is only read and, at the end, unlinked.
- **I2. Registry coverage.** Every blob file has a `blobs` row from before its temp file
  is created (the registration commits first) until after the file is unlinked (the
  row is deleted only after the unlink). The `blob_id` primary key therefore covers
  every ID that is on disk or about to be, so registration fails on an ID already in
  use, and a rename can never replace an existing blob.
- **I3. References point at live blobs.** `objects.blob_id` and `multipart_parts.blob_id`
  are foreign keys into `blobs`. A blob becomes `live` in the same transaction that
  creates its reference, and `garbage` in the same transaction that removes its last
  reference. Each blob is superseded exactly once, because the transaction that
  replaces a reference reads the old ID under the write lock.
- **I4. Unlink only when nothing refers to the file.** A blob file is unlinked only by
  (a) GC, after the transaction that made it `garbage` committed, (b) its writer's
  failure path, while its row is still `pending`, or (c) the startup sweep, for
  `pending` and `garbage` rows.
- **I5. Bytes before metadata.** The transaction that makes a blob `live` runs only after
  the blob file **[Ff]** and its directory entry **[Fd]** are durable.
- **I6. One server per blob directory.** The server holds an exclusive lock on
  `blobs/LOCK` from before the sweep until it exits (§7). The sweep's deletions rely on
  no other process writing the same blobs.
- **I7. One database per blob directory.** `S3_DB_PATH` and `S3_BLOB_DIR` are configured
  separately, so I6 alone does not catch two servers sharing one database with
  different blob directories, or the right database paired with the wrong blob
  directory. A store ID, generated once at first start, is kept in the database
  (`store` table, §5) and in the marker file `blobs/STORE_ID`. The server starts only
  if both sides hold the same ID. A mismatch, or an ID on only one side, stops startup
  before the sweep, so the sweep never judges one store's files against another
  store's registry (§7). The single exception is a first start that was killed
  between its two writes, which the database records as `initializing` (§7,
  decision 16).

### §4. On-disk layout and ID generation

```
data/
  litmus.db, litmus.db-wal, litmus.db-shm   S3_DB_PATH (unchanged)
  blobs/                                    S3_BLOB_DIR (new; replaces S3_DATA_DIR and S3_PARTS_DIR)
    LOCK                                    exclusive flock, held for the server's lifetime
    STORE_ID                                the store ID (32 hex characters and a newline), written once at first start
    00/ 01/ ... ff/                         256 fanout dirs, created at startup, never removed
      3fa9c1d2e4b5f60718293a4b5c6d7e8f      one immutable file per object version or uploaded part
    tmp/                                    in-flight writes, <blob_id>.tmp; everything here is garbage after a restart
```

**IDs.** `secrets.token_hex(16)`: 128 random bits as 32 lowercase hex characters,
generated per object version and per part upload attempt, never derived from the
bucket, key, upload ID or content. The storage layer accepts only
`^[0-9a-f]{32}$` when building a path and keeps `_contained` as a second check (R16,
R19). Upload IDs stay client-visible identifiers only; they no longer name
directories. Random IDs spread files evenly over the fanout; time-ordered IDs would
put a burst of writes into one directory.

**Fanout.** One level of 256 directories keyed by the first two hex characters. At one
million blobs that is about 4,000 entries per directory, which ext4's hashed
directories handle without trouble. Two levels (65,536 directories) would only matter
at hundreds of millions of blobs and make startup creation slower. Decision 5 in §19.

**Which directories are fsynced, and when.**

- At startup, before the sweep: create `blobs/` if missing and **[Fd]** its parent;
  create each missing fanout directory and `tmp/`, then **[Fd]** `blobs/` once. Running
  this at every startup also repairs a set left half-created by a kill during the
  first start.
- At first start only: the store marker is written as `tmp/STORE_ID.tmp` **[Ff]**,
  renamed to `blobs/STORE_ID`, then **[Fd]** `blobs/` (§7).
- On every write: only the destination fanout directory, **[Fd]** `blobs/<xx>/`, after
  the rename. The write path never creates a directory, so R4 cannot recur. If a
  fanout directory is missing at run time the rename fails and the request gets 500;
  it is not recreated on the fly.
- Not fsynced: `tmp/` after creating or renaming a temp file, and fanout directories
  after an unlink. A temp entry that comes back, or an unlinked blob that comes back,
  is in a state the sweep already cleans up (`tmp/` is always emptied; a resurrected
  `garbage` file is unlinked again). Under the process-kill model the page cache
  survives the kill anyway; these fsyncs would only matter for OS crashes, which are
  out of scope.

**Same filesystem.** `tmp/` is inside `blobs/`, so the rename from `tmp/` to a fanout
directory never crosses filesystems. The database may live elsewhere; nothing is ever
renamed between the two.

**Modes.** Files 0600 (as `mkstemp` creates them today), directories 0700. Nothing
outside the server process needs to read them (R24).

**What disappears.** No per-bucket directories exist, so a deleted bucket leaves no
directory behind and a new bucket with the same name inherits nothing (R20). The
`objects/` and `parts/` trees, and the `k` / `k/` and file-versus-directory collisions,
go with them (R1).

### §5. Schema version 2

```sql
CREATE TABLE schema_version (version INTEGER NOT NULL);           -- 2

-- Store identity (I7). At most one row; created at first start, never changed after
-- state becomes 'ready'.
CREATE TABLE store (
    singleton   INTEGER PRIMARY KEY CHECK (singleton = 1),
    store_id    TEXT NOT NULL CHECK (length(store_id) = 32),
    state       TEXT NOT NULL CHECK (state IN ('initializing', 'ready')),
    blob_dir    TEXT NOT NULL,   -- realpath of S3_BLOB_DIR at first start; consulted
                                 -- only while state = 'initializing'
    created_at  TEXT NOT NULL
);

CREATE TABLE buckets (
    name        TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL
);

-- One row per file under blobs/, from before the file exists until after it is unlinked.
CREATE TABLE blobs (
    blob_id      TEXT PRIMARY KEY CHECK (length(blob_id) = 32),
    state        TEXT NOT NULL CHECK (state IN ('pending', 'live', 'garbage')),
    kind         TEXT NOT NULL CHECK (kind IN ('object', 'part')),
    bucket       TEXT NOT NULL,   -- for sweep reports only: no foreign key, so an
    key          TEXT NOT NULL,   -- in-flight write never blocks DeleteBucket
    upload_id    TEXT,            -- parts, and objects written by Complete
    part_number  INTEGER,
    created_at   TEXT NOT NULL
) WITHOUT ROWID;
CREATE INDEX blobs_by_state ON blobs (state);

CREATE TABLE multipart_uploads (
    upload_id            TEXT PRIMARY KEY,
    bucket               TEXT NOT NULL REFERENCES buckets (name),
    key                  TEXT NOT NULL,
    initiated            TEXT NOT NULL,
    state                TEXT NOT NULL CHECK (state IN ('open', 'completed')),
    -- System headers and user metadata given to CreateMultipartUpload (R22, j1);
    -- copied onto the object row by Complete.
    content_type         TEXT NOT NULL DEFAULT 'binary/octet-stream',
    cache_control        TEXT,
    content_disposition  TEXT,
    content_encoding     TEXT,
    content_language     TEXT,
    expires              TEXT,
    user_metadata        TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX uploads_by_bucket ON multipart_uploads (bucket, state);

CREATE TABLE multipart_parts (
    upload_id    TEXT NOT NULL REFERENCES multipart_uploads (upload_id),
    part_number  INTEGER NOT NULL CHECK (part_number BETWEEN 1 AND 10000),
    blob_id      TEXT UNIQUE REFERENCES blobs (blob_id),   -- NULL once Complete released the bytes
    etag         TEXT NOT NULL,
    size         INTEGER NOT NULL,
    PRIMARY KEY (upload_id, part_number)
);

CREATE TABLE objects (
    bucket               TEXT NOT NULL REFERENCES buckets (name),
    key                  TEXT NOT NULL,          -- BINARY collation: byte order of the UTF-8 key
    blob_id              TEXT NOT NULL UNIQUE REFERENCES blobs (blob_id),
    size                 INTEGER NOT NULL,
    etag                 TEXT NOT NULL,
    last_modified        TEXT NOT NULL,
    content_type         TEXT NOT NULL DEFAULT 'binary/octet-stream',
    cache_control        TEXT,
    content_disposition  TEXT,
    content_encoding     TEXT,
    content_language     TEXT,
    expires              TEXT,
    user_metadata        TEXT NOT NULL DEFAULT '{}',  -- JSON object of x-amz-meta-* (names lowercased)
    upload_id            TEXT UNIQUE REFERENCES multipart_uploads (upload_id),  -- set if written by Complete
    PRIMARY KEY (bucket, key)
) WITHOUT ROWID;
```

Notes:

- **Foreign keys do structural work.** `objects.bucket` and `multipart_uploads.bucket`
  make it impossible to delete a bucket row while objects or upload rows still name it
  (R8), and make a commit into a bucket deleted mid-request fail with an integrity
  error, which the handler maps to NoSuchBucket. `objects.blob_id` and
  `multipart_parts.blob_id` make it impossible to purge a registry row that is still
  referenced. "Referenced implies `live`" cannot be a foreign key; the sweep checks it
  (§7).
- **What Complete keeps (R21).** The upload row stays, with `state = 'completed'`. The
  rows of the parts listed in the Complete request stay with their ETag and size, and
  `blob_id` set to NULL because their bytes now live in the assembled blob. Rows of
  unlisted parts are deleted. The object row points at the upload (`objects.upload_id`).
  This is what GetObject/HeadObject `partNumber` (j8), GetObjectAttributes
  `ObjectParts` (j7) and re-completing a completed upload (j3) need. Those rows live
  exactly as long as the object version: the transaction that overwrites or deletes
  the object deletes them. Decision 35 supersedes the j3 reference: a retried Complete is NoSuchUpload.
- **Columns for R22.** User metadata and the stored system headers are folded into this
  one schema change, so category d and j1 need no second migration. User metadata is
  one JSON column (S3 caps it at 2 KB, so a side table buys nothing). Decision 13.
- **Keys and ordering (R23).** `key` keeps SQLite's default BINARY collation, which
  compares the UTF-8 bytes, the order S3 lists keys in. The `(bucket, key)` primary
  key is the ordered index that delimiter, marker and continuation-token listing need
  (§13).
- **No version columns yet.** See §12 and decision 11.

**Data written under the current layout.** Decided (decision 4): **no migration**. At
startup, if the database reports schema version 1, or `S3_DATA_DIR` or
`S3_PARTS_DIR` is set, the server refuses to start with a message that the storage
layout changed and that the data directory must be emptied or a new one used. It never
reads, converts, ignores or deletes the old trees. Reasons: Litmus is documented as
not a production store, the test suites and CI always start from empty directories, and
version-1 data is not trustworthy anyway. The `k` / `k/` bug may have left metadata
pointing at the wrong bytes, so a migration would carry over corrupt objects unless it
re-hashed every single-part object and reported mismatches (multipart ETags cannot
be checked from the assembled bytes alone).

Not chosen, kept for the record: a migration could be made restartable: (1) one transaction
creates the version-2 tables and inserts a `pending` registry row with a new ID for
every object row and every part of an open upload; (2) for each, hard-link the old file
to its blob path (copy and **[Ff]** if `S3_DATA_DIR` is on another filesystem), with
**[Fd]** per fanout directory; (3) one transaction sets `objects.blob_id` and
`multipart_parts.blob_id`, marks the rows `live` and writes version 2; (4) unlink the
old trees. A kill before (3) leaves version 1 intact plus registry rows the next
attempt reuses; a kill after (3) leaves old files the next start removes. It would also
need its own crash points.

### §6. Write sequences

Every handler below follows the same shape: register, write bytes, one commit,
garbage collection, response. The client's 200 always comes after the commit, so a
kill before the commit leaves nothing the client was told about.

#### §6.1 Shared procedures

**REGISTER(kind, bucket, key, upload_id, part_number) → id**

1. `id = token_hex(16)`.
2. `INSERT INTO blobs (blob_id, state='pending', kind, bucket, key, upload_id,
   part_number, created_at)` **[C]**. On a primary key conflict, generate a new ID and
   retry. A conflict means a 2^-128 coincidence, but handling it keeps I2 structural
   instead of probabilistic.

**WRITE_BLOB(id, source) → (md5, size)**

1. `fd = open(tmp/<id>.tmp, O_WRONLY | O_CREAT | O_EXCL, 0o600)`. `O_EXCL` cannot fail
   here: the ID is registered and `tmp/` was emptied at startup. If it does fail,
   that is a bug, and the request gets 500 rather than overwriting anything.
2. Stream the source into `fd`, computing MD5 and size as it goes. `⚡<op>-mid-write`
   fires after the first chunk.
3. `fsync(fd)` **[Ff]**; close.
4. `rename(tmp/<id>.tmp, blobs/<id[:2]>/<id>)`. This can never replace a file (I2).
5. `fsync(blobs/<id[:2]>)` **[Fd]**.

On any exception in steps 1 to 5: unlink the temp file and, if step 4 ran, the blob
file (ENOENT is ignored), then RELEASE(id) and re-raise.

**ASSEMBLE_BLOB(id, part_blob_ids) → size**: as WRITE_BLOB, except that step 2 opens
`blobs/<p[:2]>/<p>` for each part in order and copies it. `⚡complete-mid-assemble`
fires after the first part. If a part file is missing (ENOENT), assembly stops, the
failure path runs, and Complete decides the error (§6.7, C3).

**RELEASE(id)**: used by a writer whose blob will never be referenced (failed
precondition, NoSuchUpload, NoSuchBucket, exception). Its files are already unlinked.
`DELETE FROM blobs WHERE blob_id = ? AND state = 'pending'` **[C]**. If the server dies
first, the sweep finishes the job.

**GC(ids)**: run after a commit that marked `ids` as `garbage`.

1. For each id: `unlink(blobs/<id[:2]>/<id>)`. ENOENT is ignored. Any other error (for
   example Windows refusing to unlink a file a reader has open) is logged, and the id
   stays `garbage` for the next sweep. `⚡<op>-after-gc-unlink` fires after the first
   unlink.
2. `DELETE FROM blobs WHERE blob_id IN (<ids unlinked in step 1>) AND state =
   'garbage'` **[C]**.

Step 2 matters only for tidiness: a `garbage` row whose file is gone is purged
silently by the sweep. It could run at `synchronous=NORMAL` or be batched into the next
write, but `docs/crash-model.md` pins FULL for every connection, so the proposal keeps
FULL and one commit per GC call.

#### §6.2 PutObject (new key and overwrite)

P1. Read-only checks: the bucket exists (else NoSuchBucket). An early check of
    `If-Match` / `If-None-Match` against the current row may refuse before any bytes
    are written (§10); it is an optimization, never the decision. PutObject has none:
    the bytes of a refused request are written and discarded.

P2. `id = REGISTER('object', bucket, key)` **[C]**. `⚡put-after-register`

P3. `WRITE_BLOB(id, body)` **[Ff] [Fd]** (`⚡put-mid-write` inside). `⚡put-after-bytes`

P4. BEGIN IMMEDIATE

    a. old = SELECT blob_id, etag, upload_id FROM objects WHERE bucket = ? AND key = ?
    b. Evaluate If-Match / If-None-Match against old's ETag (§10). On failure: ROLLBACK,
       unlink the blob, RELEASE(id), answer 412 PreconditionFailed or 404 NoSuchKey.
    c. now = the clock, read here, under the write lock (§9).
       INSERT INTO objects (..., blob_id = id, size, etag, last_modified = now,
       headers, user_metadata, upload_id = NULL)
       ON CONFLICT (bucket, key) DO UPDATE SET <all of these>.
       A foreign key failure on bucket (deleted mid-request): ROLLBACK, unlink,
       RELEASE(id), NoSuchBucket.
    d. UPDATE blobs SET state = 'live' WHERE blob_id = id AND state = 'pending'
       (exactly one row, else ROLLBACK and 500).
    e. If old: UPDATE blobs SET state = 'garbage' WHERE blob_id = old.blob_id;
       if old.upload_id: DELETE FROM multipart_parts WHERE upload_id = old.upload_id;
       DELETE FROM multipart_uploads WHERE upload_id = old.upload_id.
       (Step c already cleared objects.upload_id, so the foreign key allows this.)

COMMIT **[C]**. `⚡put-after-commit`

P5. If old: `GC([old.blob_id])` (`⚡put-after-gc-unlink` inside) **[C]**.

P6. 200 with the ETag.

New key and overwrite are the same code. The only difference is whether step P4.a
finds a row.

| Kill at | State left behind | Sweep at restart | Clients see after restart |
|---|---|---|---|
| before P2's commit | nothing new | nothing | previous state (old object, or 404) |
| `put-after-register` | `pending` row, no file | delete row; report `InterruptedWrite` (bytes_durable = false) | previous state |
| `put-mid-write` | `pending` row, partial `tmp/<id>.tmp` | delete temp (`LeftoverTemp`), then row (`InterruptedWrite`, false) | previous state |
| after WRITE_BLOB step 3, before its rename | `pending` row, complete temp | same as `put-mid-write` | previous state |
| `put-after-bytes` | `pending` row, complete blob | unlink blob, delete row; report `InterruptedWrite` (bucket, key, bytes_durable = true) | previous state, with the old bytes and ETag (the R2 window is gone) |
| inside P4, before COMMIT | as `put-after-bytes` (WAL drops the uncommitted transaction) | same | previous state |
| `put-after-commit` | row points at new `live` blob; old blob `garbage`, file present | unlink old file, delete row; report `InterruptedCleanup` | new object |
| `put-after-gc-unlink` | old blob `garbage`, file gone | delete row silently | new object |
| after P5, before the 200 | final state | nothing | new object; a client retry is a harmless overwrite |

#### §6.3 DeleteObject

D1. The bucket exists (else NoSuchBucket). `⚡delete-before-commit`

D2. BEGIN IMMEDIATE

    a. old = SELECT blob_id, etag, size, last_modified, upload_id FROM objects WHERE ...
    b. No row: COMMIT (nothing changed); answer 204.
    c. Evaluate conditional-delete headers against old, when supported (§10).
    d. DELETE FROM objects WHERE bucket = ? AND key = ?
    e. UPDATE blobs SET state = 'garbage' WHERE blob_id = old.blob_id
    f. If old.upload_id: delete its multipart_parts and multipart_uploads rows.

COMMIT **[C]**. `⚡delete-after-commit`

D3. `GC([old.blob_id])` **[C]**.

D4. 204.

`versionId=null` takes the same path (§12).

| Kill at | State left behind | Sweep at restart | Clients see after restart |
|---|---|---|---|
| `delete-before-commit` (or anywhere before the COMMIT) | unchanged | nothing | the object, with its bytes (today this is a 500, R3) |
| `delete-after-commit` | row gone; blob `garbage`, file present | unlink, delete row; report `InterruptedCleanup` | 404 |
| inside GC, after the unlink | `garbage` row, no file | delete row silently | 404 |

There is never a row without a file: the row goes in the commit, the file afterwards.

#### §6.4 DeleteObjects

X1. Parse the body. Per-entry checks that need no database (unsupported version IDs,
    `LastModifiedTime` and `Size`, the 1000-key limit) produce per-key errors as today.
    An entry's `ETag` is not decided here (§10). `⚡delete-objects-before-commit`

X2. BEGIN IMMEDIATE; for each remaining entry, in request order: steps D2.a to D2.f,
    recording a `Deleted` or per-key `Error` entry. An entry with an `ETag` condition
    has it decided after D2.a, against that key's row as the earlier entries left it
    (§10); a refused entry writes nothing. COMMIT **[C]** once for the whole request.
    `⚡delete-objects-after-commit`

X3. `GC(<every old blob_id>)` **[C]**.

X4. 200 with the DeleteResult.

One transaction per request instead of one per key: S3 promises no atomicity across
keys, but one transaction means one fsync instead of up to 1000, and a kill leaves
either none or all of the request's deletes committed. Either way the client got no
response and retries. A key listed twice is deleted once and reported `Deleted` twice,
as S3 reports missing keys. Decision 12.

| Kill at | State left behind | Sweep at restart | Clients see after restart |
|---|---|---|---|
| `delete-objects-before-commit` (before X2) | unchanged | nothing | every object still present |
| `delete-objects-after-commit` | rows gone; their blobs `garbage`, files present | unlink each, delete rows; one `InterruptedCleanup` per file | every deleted key 404; keys that got per-key errors untouched |

#### §6.5 CreateMultipartUpload

One transaction **[C]**: INSERT INTO multipart_uploads (state = 'open', plus
Content-Type, the system headers and user metadata from the request, R22). No files.
A foreign key failure means NoSuchBucket. A kill before the commit leaves nothing;
after it, an open upload with no parts.

#### §6.6 UploadPart, including re-uploading a part number

U1. Read-only checks: the upload is open and belongs to this bucket and key (else
    NoSuchUpload); the part number is between 1 and 10000. Decision 17: the bucket and
    key check stays in the design but comes with the multipart work, not step (a).
    Decision 34 implements it: a missing bucket is NoSuchBucket before U1, and an upload
    of another bucket or key is NoSuchUpload.

U2. `id = REGISTER('part', bucket, key, upload_id, part_number)` **[C]**.
    `⚡part-after-register`

U3. `WRITE_BLOB(id, body)` **[Ff] [Fd]** (`⚡part-mid-write` inside). `⚡part-after-bytes`

U4. BEGIN IMMEDIATE

    a. SELECT state FROM multipart_uploads WHERE upload_id = ?; anything but 'open'
       (aborted, completed): ROLLBACK, unlink, RELEASE(id), NoSuchUpload.
    b. old = SELECT blob_id FROM multipart_parts WHERE upload_id = ? AND part_number = ?
    c. INSERT INTO multipart_parts (upload_id, part_number, blob_id = id, etag, size)
       ON CONFLICT DO UPDATE SET blob_id, etag, size
    d. UPDATE blobs SET state = 'live' WHERE blob_id = id AND state = 'pending'
    e. If old: UPDATE blobs SET state = 'garbage' WHERE blob_id = old.blob_id

COMMIT **[C]**. `⚡part-after-commit`

U5. If old: `GC([old.blob_id])` **[C]**.

U6. 200 with the part ETag.

Re-uploading a part number writes a new blob. The previous part stays recorded and
intact until the commit that replaces it, so a kill at any point before that commit
leaves the old part usable by Complete with its old ETag. Two concurrent uploads of the
same part number both succeed; the later commit wins (§9).

| Kill at | State left behind | Sweep at restart | Clients see after restart |
|---|---|---|---|
| `part-after-register` | `pending` part row, no file | delete row; `InterruptedWrite` (upload_id, part_number) | part not recorded (re-upload: old part still recorded) |
| `part-mid-write` | `pending` row, partial temp | delete temp and row | same |
| `part-after-bytes` | `pending` row, complete blob | unlink, delete row; `InterruptedWrite` (bytes_durable = true) | same; Complete with the new ETag gets InvalidPart, with the old ETag works |
| `part-after-commit` | part row points at new blob; old part blob `garbage`, file present | unlink old, delete row; `InterruptedCleanup` (upload_id, part_number) | new part recorded |
| inside GC, after the unlink | `garbage` row, no file | delete row silently | new part recorded |

#### §6.6A UploadPartCopy

UploadPartCopy is UploadPart (§6.6) whose bytes come from a byte range of another
object's blob (§11). It is PUT with `x-amz-copy-source`, `partNumber` and `uploadId`; the
guard lets the header through on exactly that query.

P1. Read-only checks, in this order: the destination bucket exists (NoSuchBucket); the
    part number is an integer (else 400 InvalidPart, as UploadPart); the upload is open
    (NoSuchUpload); the part number is between 1 and 10000 (InvalidPart); the
    `x-amz-copy-source` value parses (400 InvalidArgument; 501 for an ARN or a version ID
    other than `null`, already in the guard); `x-amz-copy-source-range`, when present, is
    `bytes=<first>-<last>` with first not greater than last (400 InvalidArgument naming
    the header); the source bucket exists (NoSuchBucket) and the source key has a row
    (NoSuchKey). The `x-amz-copy-source-server-side-encryption-customer-*` headers are
    501 on this operation and on CopyObject, in the guard.

P2. Read the source row, decide its `x-amz-copy-source-if-*` conditions against that row
    (412 PreconditionFailed), and open its blob: K2 of §6.10, the same code. The size of
    the open file must equal the row's size, else 500. Then the range is checked against
    that row's size: a `last` at or beyond it is 416 InvalidRange (so is every range of
    an empty source). With no range header the part is the whole source. Nothing is
    registered before this point, so a refused or failed read leaves nothing behind.

P3. `id = REGISTER('part', bucket, key, upload_id, part_number)` **[C]**.
    `⚡part-copy-after-register`

P4. `WRITE_BLOB(id, source descriptor, [first, last])` **[Ff] [Fd]** (`⚡part-copy-mid-write`
    inside). Before the fsync, a verify step checks that the length written equals the
    range's and, only when the range is the whole of a source that was not assembled from
    parts, that the MD5 written equals the row's ETag; otherwise 500 and the blob is
    discarded. A range has no stored digest to compare with, so for a range, and for a
    multipart source, the length is the only check. `⚡part-copy-after-bytes`

P5. The commit of U4 (§6.6): the upload is still open (else NoSuchUpload, the new blob is
    discarded), the row of the part number is inserted or replaced, the new blob becomes
    `live` and a replaced part blob `garbage`. COMMIT **[C]**. `⚡part-copy-after-commit`

P6. If the commit replaced a part: `GC([old.blob_id])` **[C]**.

P7. 200 with a `CopyPartResult` body holding the ETag (the MD5 of the copied bytes,
    always a plain MD5) and LastModified. A part has no timestamp of its own, so
    LastModified is the time of the response.

Kills leave what a killed UploadPart leaves, with the same sweep reports; the source is
never touched.

| Kill at | State left behind | Sweep at restart | Clients see after restart |
|---|---|---|---|
| `part-copy-after-register` | `pending` part row, no file | delete row; `InterruptedWrite` (bytes_durable = false) | part not recorded (a part of that number uploaded before stays recorded) |
| `part-copy-mid-write` | `pending` row, partial `tmp/<id>.tmp` holding the start of the range | delete temp (`LeftoverTemp`), then row (`InterruptedWrite`, false) | same |
| `part-copy-after-bytes` | `pending` row, complete blob | unlink, delete row; `InterruptedWrite` (bytes_durable = true) | same; Complete with the copy's ETag gets InvalidPart, with the old part's ETag works |
| `part-copy-after-commit` | part row points at the copy's `live` blob; a replaced part blob `garbage`, file present | unlink it, delete row; `InterruptedCleanup`; nothing when no part was replaced | the copied part |

A replaced part has no unlink point of its own, as for UploadPart (U5).

#### §6.7 CompleteMultipartUpload

C1. Read-only: the upload exists (else NoSuchUpload; the bucket and key part comes
    with the multipart work, decision 17). Decision 34 adds NoSuchBucket before this
    check and makes it find only the upload of this bucket and key. Then the `x-amz-mp-object-size` syntax
    (InvalidArgument, decision 26), then parse and validate the body, and nothing is
    registered until all of it has passed. The errors come in this order:
    1. MalformedXML: the body is empty, not UTF-8, not well-formed XML, its root is
       not CompleteMultipartUpload, it has no Part, or a Part has no ETag or a
       PartNumber that is not an integer.
    2. InvalidPartOrder: the part numbers are not strictly ascending, so a repeated
       number is refused too. The list is never sorted.
    3. InvalidPart: a part number outside 1 to 10000, or a part that is not recorded
       or whose ETag does not match. Read the part rows for this. Remember each
       requested part's `blob_id` and size.
    4. EntityTooSmall: a listed part other than the last listed one is stored smaller
       than `S3_MIN_PART_SIZE` bytes (5 MiB by default), judged by the remembered sizes
       and by the listed parts only (decision 27).
    5. InvalidRequest: `x-amz-mp-object-size` is not the total of the remembered sizes
       of exactly the listed parts (decision 26).
    6. The condition (C4.c): there is no early conditional check (§10), so a refused
       Complete costs a full assembly.
    The checksum checks and the full order are in §20 (decision 32).

C2. `id = REGISTER('object', bucket, key, upload_id)` **[C]**. `⚡complete-after-register`

C3. `ASSEMBLE_BLOB(id, <the remembered part blob_ids>)` **[Ff] [Fd]**
    (`⚡complete-mid-assemble` inside). `⚡complete-after-assemble`

    If a part file is missing during assembly: the failure path runs (unlink,
    RELEASE), then the part row is read again. If it now names a different blob, the
    part was re-uploaded during assembly: InvalidPart. If the upload is gone or
    completed: NoSuchUpload. If it still names the same blob, the bytes are lost: 500
    (the sweep reports it as MissingPart).

C4. BEGIN IMMEDIATE

    a. UPDATE multipart_uploads SET state = 'completed'
       WHERE upload_id = ? AND state = 'open'. No row updated (the upload is aborted,
       completed or unknown): ROLLBACK, unlink, RELEASE(id), NoSuchUpload. The
       existing object is untouched, because nothing outside the fresh blob was
       written (R6). This compare-and-set on the upload state runs first, so every
       later failure in the transaction is rolled back together with it (recorded in
       step (a); the proposal listed it last).
    b. Each requested part's current multipart_parts.blob_id must equal the blob that
       was assembled; otherwise a re-upload committed during assembly: ROLLBACK,
       unlink, RELEASE(id), InvalidPart.
    c. old = the current object row; evaluate If-Match / If-None-Match against it
       (§10). On failure: ROLLBACK, unlink, RELEASE(id), 412 or 404. The upload stays
       open, with every part row and part blob as before. The precondition is
       `commit_completed_upload`'s `precondition` argument, called after (a) and (b),
       so NoSuchUpload and InvalidPart come first; the ROLLBACK undoes (a), so the upload
       is `open` again, and there is no state to restore by hand.
    d. INSERT INTO objects (..., blob_id = id, size = sum of part sizes, etag =
       composite ETag, last_modified = the upload's `initiated` time (not the time of
       this commit), headers and user_metadata copied from the
       upload row, upload_id = this upload) ON CONFLICT (bucket, key) DO UPDATE ...
    e. UPDATE blobs SET state = 'live' WHERE blob_id = id AND state = 'pending'
    f. If old: its blob becomes 'garbage'; if old.upload_id (an earlier completed
       upload): delete that upload's part and upload rows.
    g. Listed parts: their blobs become 'garbage'; UPDATE multipart_parts SET
       blob_id = NULL for them (ETag and size stay, R21).
    h. Unlisted parts: their blobs become 'garbage'; delete their part rows.

COMMIT **[C]**. This one transaction carries both the object metadata (d) and the
upload completion (a), as `commit_completed_upload` does today (R5).
`⚡complete-after-commit`

C5. `GC(<old object blob, if any> + <every part blob>)` (`⚡complete-after-gc-unlink`
    inside) **[C]**.

C6. 200 with the CompleteMultipartUploadResult.

| Kill at | State left behind | Sweep at restart | Clients see after restart |
|---|---|---|---|
| `complete-after-register` | `pending` object blob row, no file; upload open, parts `live` | delete row; `InterruptedWrite` (bucket, key, upload_id) | previous object (or 404); upload open; a retried Complete works |
| `complete-mid-assemble` | `pending` row, partial temp | delete temp and row | same |
| `complete-after-assemble` | `pending` row, complete assembled blob | unlink, delete row; `InterruptedWrite` (bytes_durable = true) | same; the previous object keeps its bytes (the R2 assembly window is gone) |
| inside C4, before COMMIT | as `complete-after-assemble` | same | same |
| `complete-after-commit` | object row points at assembled blob; upload completed; listed part rows kept with NULL blob_id; part blobs and old object blob `garbage`, files present | unlink each, delete rows; one `InterruptedCleanup` per file (parts carry upload_id and part_number) | new object; UploadPart and Abort on the upload get NoSuchUpload |
| `complete-after-gc-unlink` | some `garbage` files gone, others present | unlink the rest and report them; purge rows of the gone ones silently | new object |

#### §6.8 AbortMultipartUpload

A1. Read-only: the upload exists and is open (else NoSuchUpload).
    `⚡abort-before-commit`

A2. BEGIN IMMEDIATE

    a. SELECT state ...; anything but 'open': ROLLBACK, NoSuchUpload.
    b. Evaluate `x-amz-if-match-initiated-time` when supported (§10).
    c. ids = SELECT blob_id FROM multipart_parts WHERE upload_id = ? AND blob_id IS NOT NULL
    d. UPDATE blobs SET state = 'garbage' WHERE blob_id IN ids
    e. DELETE FROM multipart_parts WHERE upload_id = ?
    f. DELETE FROM multipart_uploads WHERE upload_id = ?

COMMIT **[C]**. `⚡abort-after-commit`

A3. `GC(ids)` **[C]**.

A4. 204.

Part uploads still in flight have `pending` blobs, not `garbage` ones. They fail at
U4.a, release their own blobs and get NoSuchUpload; if the server is killed first, the
sweep removes them.

| Kill at | State left behind | Sweep at restart | Clients see after restart |
|---|---|---|---|
| `abort-before-commit` (or anywhere before the COMMIT) | unchanged | nothing | upload open, parts intact, Complete works (today the parts are already gone, R3) |
| `abort-after-commit` | upload and part rows gone; part blobs `garbage`, files present | unlink each, delete rows; `InterruptedCleanup` (upload_id) per file | NoSuchUpload; no MissingPart is possible |

#### §6.9 DeleteBucket (R8, R20)

Decided (decision 7): DeleteBucket aborts the bucket's open multipart uploads inside its
own transaction.

B1. BEGIN IMMEDIATE

    a. The bucket exists (else NoSuchBucket); no object rows (else BucketNotEmpty).
    b. For each open upload in the bucket: steps A2.c to A2.f (part blobs become
       'garbage', part rows and the upload row are deleted). Completed upload rows
       cannot exist here: they only exist while their object does.
    c. DELETE FROM buckets WHERE name = ?. The foreign keys on objects and
       multipart_uploads guarantee that this fails if anything still names the
       bucket.

COMMIT **[C]**. `⚡bucket-delete-after-commit`

B2. `GC(<part blobs of the aborted uploads>)` **[C]**.

B3. 204.

| Kill at | State left behind | Sweep at restart | Clients see after restart |
|---|---|---|---|
| before B1's COMMIT | unchanged | nothing | bucket, uploads and parts intact |
| `bucket-delete-after-commit` | bucket, upload and part rows gone; part blobs `garbage`, files present | unlink each, delete rows; `InterruptedCleanup` (upload_id) per file | NoSuchBucket; the uploads get NoSuchUpload |

There is no directory to remove (R20). A PutObject, UploadPart or
CreateMultipartUpload in flight against the bucket fails its commit (on the foreign
key, or at U4.a because the upload is gone) and gets NoSuchBucket or NoSuchUpload; a
blob it already wrote is released, or removed by the sweep.

Effect on the ceph harness: the pinned s3-tests
(`5b3987116eca0b4b8fa57d6b10f3db57bab196b6`) cleans up around every test with an
autouse fixture (`setup_teardown` in `s3tests/functional/__init__.py`) that calls
`nuke_prefixed_buckets`, whose `nuke_bucket` pages `list_object_versions` with
`MaxKeys=128`, deletes the listed versions with DeleteObjects, then calls
DeleteBucket. It never lists or aborts multipart uploads. So a test that leaves an
upload open (for example `test_multipart_upload_empty`, or each failing
`failing_conditional_multipart_upload` call) reaches DeleteBucket with the upload
still open. With this decision that bucket is deleted and its uploads aborted, which
from the harness's side is what happens today (minus today's orphaned rows and part
files). Refusing would have made those buckets fail cleanup: `nuke_prefixed_buckets`
re-raises the first cleanup error, which pytest would report as a teardown error.

#### §6.10 CopyObject

CopyObject is PutObject (§6.2) whose bytes come from another object's blob (§11).

K1. Read-only checks, in this order: the destination bucket exists (NoSuchBucket); the
    `x-amz-copy-source` value parses (400 InvalidArgument; 501 for an ARN or a version
    ID other than `null`); `x-amz-metadata-directive` and `x-amz-tagging-directive` are
    COPY or REPLACE (400 InvalidArgument); under REPLACE the request's headers are
    parsed (400 MetadataTooLarge); the source bucket exists (NoSuchBucket) and the
    source key has a row (NoSuchKey); a copy of an object onto itself without REPLACE
    is refused (400 InvalidRequest). The `x-amz-copy-source-server-side-encryption-
    customer-*` headers are 501, in the guard, before any of this.

K2. Read the source row, decide its `x-amz-copy-source-if-*` conditions against that
    row (412 PreconditionFailed), and open its blob: GET's sequence (§8 steps 1 to 3),
    with its ENOENT retry (the conditions are decided again on every re-read row), so
    the conditions, the headers and the bytes belong to one row. The size of the open
    file must equal the row's size, else 500. This happens before anything is
    registered, so a refused or failed read leaves nothing behind.

K3. `id = REGISTER('object', dst_bucket, dst_key)` **[C]**. `⚡copy-after-register`

K4. `WRITE_BLOB(id, source descriptor)` **[Ff] [Fd]** (`⚡copy-mid-write` inside). Before
    the fsync, a verify step checks that the size written equals the row's and, for a
    source that was not assembled from parts, that the MD5 written equals the row's
    ETag; otherwise 500 and the blob is discarded. A multipart source's ETag is not the
    MD5 of its bytes, so only its size is checked. `⚡copy-after-bytes`

K5. The P4 transaction for the destination key, with the request's `If-Match` and
    `If-None-Match` as the destination's conditions (§10): the headers are the
    source's under COPY and the request's under REPLACE; `last_modified` is stamped in
    the commit; the ETag is the MD5 of the copied bytes. COMMIT **[C]**.
    `⚡copy-after-commit`

K6. If the commit replaced a row: `GC([old.blob_id])` (`⚡copy-after-gc-unlink` inside)
    **[C]**.

K7. 200 with a `CopyObjectResult` body holding the ETag and LastModified.

Any failure in K3 to K5 discards the new blob as PutObject does. Kills leave what
PutObject's leave, with the same sweep reports:

| Kill at | State left behind | Sweep at restart | Clients see after restart |
|---|---|---|---|
| `copy-after-register` | `pending` row, no file | delete row; `InterruptedWrite` (bytes_durable = false) | destination as before; source untouched |
| `copy-mid-write` | `pending` row, partial `tmp/<id>.tmp` | delete temp (`LeftoverTemp`), then row (`InterruptedWrite`, false) | destination as before |
| `copy-after-bytes` | `pending` row, complete blob | unlink blob, delete row; `InterruptedWrite` (bytes_durable = true) | destination as before, with its old bytes and ETag |
| `copy-after-commit` | row points at the copy's `live` blob; a replaced destination blob is `garbage`, file present | unlink it, delete row; `InterruptedCleanup`; nothing when there was no old destination | the copy |
| `copy-after-gc-unlink` | replaced blob `garbage`, file gone | delete row silently | the copy |

### §7. Startup sequence and consistency sweep

Startup, before any request is accepted. Every refusal exits non-zero with a message
naming the cause, before anything in `blobs/` is deleted.

1. Validate the crash point name (unchanged).
2. Refuse the old environment variables `S3_DATA_DIR` and `S3_PARTS_DIR` (§5).
3. Open the database. Refuse schema version 1 (§5). On an empty database, create the
   version-2 schema in one transaction **[C]**. The `store` table starts empty.
4. If `S3_BLOB_DIR` does not exist and the database already has a `store` row, refuse
   (the database belongs to a blob directory that is not there). Nothing is created.
5. Create `blobs/` if missing (**[Fd]** its parent), open (creating if needed)
   `blobs/LOCK` and take an exclusive non-blocking `flock` (`msvcrt.locking` on
   Windows), held until exit. If another server holds it, refuse to start. Without
   this, a second server on the same directory would delete the first one's in-flight
   blobs as `pending` leftovers (I6). Today nothing prevents two servers sharing a
   data directory.
6. Check the store identity (I7). D is the database's `store` row; M is the content of
   `blobs/STORE_ID`, if the file exists.

   | D | M | Action |
   |---|---|---|
   | `ready`, id X | X | continue |
   | `ready`, id X | missing, or not X | refuse: this blob directory does not belong to this database |
   | none | present | refuse: this blob directory belongs to another database |
   | none | missing | first start (below) |
   | `initializing`, id X, `blob_dir` B | missing or X, realpath(`S3_BLOB_DIR`) is B, and the store never held data (decision 16, below) | finish the first start (below, from step c) |
   | `initializing` | anything else | refuse |

   First start:

   a. Refuse unless `blobs/` holds nothing but `LOCK`, `tmp/` and empty fanout
      directories, and no table other than `store` and `schema_version` has a row.
      Blob files without a marker belong to some other store. A database with rows
      but no store ID was not created for this blob directory; binding the two would
      make the right blob directory refused for good.
   b. Generate `store_id = token_hex(16)`; INSERT the `store` row with
      `state = 'initializing'` and `blob_dir = realpath(S3_BLOB_DIR)` **[C]**. If the
      insert fails because a row appeared (a second server initializing the same
      database at the same moment), read the row again and go back to the table.
      `⚡store-init-after-commit`
   c. Write `tmp/STORE_ID.tmp` (creating `tmp/` first if it is missing, with
      **[Fd]** `blobs/`, and removing a leftover temp file) **[Ff]**, rename it to
      `blobs/STORE_ID`, **[Fd]** `blobs/`.
   d. UPDATE the row to `state = 'ready'` **[C]**.

   The database is written first, so a kill at any point leaves a state the table
   recognizes. A kill after b and before d leaves an `initializing` row and possibly the
   marker, and the next start with the same blob directory finishes c and d. That
   `initializing` row is the one case where only one side holds the ID and the server
   still starts; refusing it would leave a store that can never start after a kill
   during its first start (decision 16). Recording `blob_dir` means that only the blob
   directory chosen at first start can adopt an `initializing` row.

   Decision 16, as accepted: an `initializing` store is finished only if it never held
   data. No table other than `store` and `schema_version` may have a row (bucket rows
   count), and `blobs/` may contain nothing except `LOCK`, empty fanout directories, a
   `tmp/` that is empty or holds only `tmp/STORE_ID.tmp`, and `STORE_ID` when it holds
   this store's own ID (left by a kill between c and d). Otherwise the server refuses
   to start.

   The schema migration of §20 runs at this point, after the store identity check and before step 7.
7. Create the fanout directories and `tmp/` and fsync them (§4).
8. Run the sweep.

What each check catches:

| Misconfiguration | Caught by |
|---|---|
| two servers, same blob directory (whatever their databases) | the lock (step 5) |
| two servers, same database, different blob directories | the second server finds a `ready` row whose ID is not in its blob directory (step 6) |
| right database, wrong blob directory (empty, missing or another store's) | step 4 or 6 |
| wrong or fresh database, right blob directory | step 6: marker present, no matching row |
| version-1 data | step 3 |

The sweep, in order. Every action removes a file before its row, so a kill during the
sweep leaves rows without files, which the next sweep purges (`⚡sweep-after-unlink`).

| Check | Issue reported | Action | Why the action is safe |
|---|---|---|---|
| every entry in `tmp/` | `LeftoverTemp(path)` | **delete** | Nothing references `tmp/`. Every temp file belonged to a request of a process that is gone, and it is either incomplete or not yet renamed. |
| `pending` row, file present | `InterruptedWrite(blob_id, kind, bucket, key, upload_id, part_number, bytes_durable=True)` | **delete** file, then row | `pending` means no commit referenced the blob (the reference and the switch to `live` are one transaction). The writer is dead (I6), so nothing can reference it later. No 200 was sent for it (the 200 follows the commit), so crash invariants 1 and 2 promise nothing about these bytes. Before deleting, the sweep confirms no `objects` or `multipart_parts` row names the ID; if one does, it reports `RegistryMismatch` and leaves both alone. |
| `pending` row, no file | `InterruptedWrite(..., bytes_durable=False)` | delete row | No file. |
| `garbage` row, file present | `InterruptedCleanup(blob_id, kind, bucket, key, upload_id, part_number)` | **delete** file, then row | `garbage` means a committed transaction removed the last reference (I3). The same reference check as above applies. |
| `garbage` row, no file | none (normal: GC unlinked the file and was stopped before its commit, or batched it) | delete row | No file. |
| `live` object blob, no file | `OrphanedMetadata(bucket, key)` (existing type) | report only | The object is lost. Deleting its row would silently turn a 500 into a 404; repair stays an operator decision. |
| `live` part blob of an open upload, no file | `MissingPart(upload_id, part_number)` (existing type) | report only | as above |
| `live` blob whose size differs from its row | `SizeMismatch(blob_id, bucket, key, expected, actual)` | report only | Truncation or outside corruption; cheap (`stat`) and catches what invariant 3 forbids. |
| `live` row with no reference, or a reference to a non-`live` row | `RegistryMismatch(blob_id, detail)` | report only | Should be impossible; never guess. |
| file in a fanout directory with no registry row or a name that is not an ID, or a top-level entry of `blobs/` other than `LOCK`, `STORE_ID`, `tmp/` and the 256 fanout directories | `UntrackedFile(path)` | **report only, never touched** | The registry is the only proof that a file is the server's own leftover. The store ID check (I7) already stops a wrong database from reaching the sweep; this rule is the second line of defense if that check is ever bypassed. |

`InterruptedWrite` and `InterruptedCleanup` carry the bucket, key and, for parts, the
upload ID and part number recorded at registration, which is what the two strict
xfail tests ask for (§17). Log levels: WARNING for `InterruptedWrite` and every
report-only issue; INFO for `LeftoverTemp` and `InterruptedCleanup` (an interrupted
cleanup is harmless). Every issue still goes into the report, so `report.clean` is
false after any crash trace. As today, the sweep never stops the server from starting.

This changes the documented policy in `docs/crash-model.md` ("does not auto-delete or
auto-repair"): it deletes temp files and the files of `pending` and `garbage` registry
rows, and repairs nothing else. Decided (decision 3): delete, not quarantine. You
rewrite `docs/crash-model.md` to match after implementation (decision 15).

One harmless inaccuracy: if a kill lands between a sweep's unlink of a `pending` blob
and the deletion of its row, the next sweep reports the same write again, with
`bytes_durable=False`.

### §8. Reads racing an overwrite or delete

GET (and range GET):

1. Read the object row (`blob_id`, size, ETag, headers). No row: 404.
2. Evaluate the GET conditions (If-Match and so on) against that row.
3. `fd = open(blobs/<id[:2]>/<id>)`.
   - ENOENT: read the row again. If it now names a different blob, go back to step 2
     with the new row; if it is gone, 404; if it still names the same blob, the bytes
     are lost: 500 InternalError (reported by the sweep as `OrphanedMetadata`). Three
     such retries are allowed; the fourth failed open answers 503 SlowDown, so one GET
     opens at most four times. Three commits racing one GET are served on the fourth
     open; four get 503. Decision 14.
4. `fstat(fd).st_size` must equal the row's size, else 500 (never send a truncated
   body).
5. Send the headers from the row from step 1 (or its retry), then stream from `fd`
   (`seek` for a range), and close `fd` when the stream ends or the client goes away.

Today the file is opened inside the streaming generator after the headers are built,
after a separate `object_exists` check. Both move: the file is opened before the
response is constructed, and the generator receives the open descriptor.

Why a GET can never return a missing file or a mix of versions:

- **Immutable bytes (I1).** A blob file never changes after its rename, so whatever
  descriptor GET holds reads exactly the bytes committed with the row that named it,
  which are the bytes the row's size and ETag describe.
- **A descriptor pins the inode.** On POSIX, unlinking an open file removes the name,
  not the data; GET keeps reading the old version to the end even if an overwrite or
  delete commits and GC unlinks it mid-stream. This is what keeps ceph's
  `test_atomic_read_*` passing today, and it holds without the event loop's help.
- **Unlink only after a later commit (I4).** ENOENT at step 3 can only mean that a
  transaction that committed after GET's row read replaced or deleted the key. The
  retry then serves that newer state or 404, both of which are states that really
  existed after GET started.
- **Headers and bytes come from one row.** ETag, Content-Length, Content-Range and
  Last-Modified are taken from the row that named the opened blob, never from a
  separate read.

A range GET is the same procedure on one descriptor. Separate requests (a client
fetching ranges in parallel) can see different versions if a write commits between
them, as on S3; clients guard against that with If-Match. HEAD reads only the row,
so it is always one consistent version. Listing reads only rows.

Windows: an open file cannot be unlinked there, so GC's unlink fails while a reader
holds the blob; the row stays `garbage` and the next sweep removes it. Reads are still
correct. The crash model is POSIX-only anyway.

### §9. Concurrent writers

**Concurrent PUTs to the same key.** Each registers its own ID and writes its own blob,
so they share no file. Their commits serialize on SQLite's write lock (BEGIN
IMMEDIATE). Each commit reads the current row under that lock, replaces it and marks
the blob it replaced `garbage`, so every blob is superseded exactly once and no
write is lost or unlinked twice. **The winner is the request whose commit takes the
write lock last**, not the one that started last or finished sending its body last.
Both clients get 200 (unless a precondition fails, §10), and a GET afterwards sees one
complete body with its ETag. For PutObject, `last_modified` is assigned inside the
commit, after the write lock is taken (the handler builds the row with
`last_modified = None` and `put_object_meta` stamps it), so among writes stamped at
commit the winner also has the latest Last-Modified. It used to be assigned in the
handler before the commit, which let a request that took the lock later carry an
earlier timestamp; moving it into the commit made this paragraph true rather than
weakening the paragraph, because the cost is one clock read under a lock that is
already held.

Two limits on that claim. It does not cover CompleteMultipartUpload, which does not
stamp the time of its commit: an assembled object's date is the date the upload was
initiated (`last_modified=upload.initiated`, `server/multipart/state_machine.py`), so a
Complete that commits after a PutObject can carry an earlier Last-Modified than the
PutObject it replaced. And it assumes the wall clock never steps backward: Litmus reads
the wall clock and does not clamp it to the replaced row's time, so a backward step
can give the winner an earlier Last-Modified (open question 5).

**No reliance on the event loop.** Today handlers do blocking file and SQLite I/O
inside `async` functions, so the event loop runs them one at a time (R14). Nothing in
this design depends on that: bytes are written to private paths without locks, and
every decision that depends on the current state (old row, upload state, part
identity, preconditions) happens inside one BEGIN IMMEDIATE transaction. File I/O and
SQLite calls can therefore move to a thread pool without reintroducing a race. That
move is not part of this change. Implementation notes: connections use
`isolation_level=None` so BEGIN IMMEDIATE is issued explicitly, and an explicit
`timeout=` (Python's default is 5 seconds) makes a writer wait for the lock rather
than fail with "database is locked".

Other races, each decided by commit order:

| Race | Outcome |
|---|---|
| PUT vs DeleteObject, same key | whichever commits last wins: the object exists with the PUT's bytes, or is gone |
| UploadPart vs UploadPart, same part number | both 200; the later commit's part is recorded; the other blob is `garbage` |
| UploadPart vs Complete | if Complete commits first, the part's commit sees `completed` and gets NoSuchUpload; if the part commits during assembly, Complete's check C4.b (or a missing file in C3) gives InvalidPart |
| UploadPart vs Abort | if Abort commits first, the part gets NoSuchUpload and releases its blob |
| Complete vs Complete, same upload | the second sees `completed` and gets NoSuchUpload (re-completing, j3, is separate work that the retained rows allow) (decision 35: it stays NoSuchUpload) |
| Complete vs Abort | the first commit wins; the other gets NoSuchUpload |
| PUT or CreateMultipartUpload vs DeleteBucket | if the bucket delete commits first, the other's commit fails on the foreign key: NoSuchBucket |

### §10. Conditional writes and deletes (R15)

Status: `If-Match` and `If-None-Match` on PutObject and on CompleteMultipartUpload are
implemented as below, and so are `If-Match` on DeleteObject and the per key `ETag` of
DeleteObjects (end of this section).
`x-amz-write-offset-bytes` and the other conditional deletes are not; they still return
501 (docs/compatibility.md).

The authoritative check always runs inside the commit transaction (P4.b, C4.c), against
the row as it is at that moment, after the new bytes are durable at their fresh path.
`put_object_meta` takes an optional `precondition` function; it is called inside BEGIN
IMMEDIATE, after the write lock is taken, with the ETag of the row being replaced (None
when there is none). What it raises (PreconditionFailed, NoSuchKey) rolls the
transaction back, and the handler discards the new blob through the same path as any
failed commit (unlink, RELEASE). The old object's bytes and row are untouched. For
Complete the upload stays open, so the client can retry without re-uploading parts.

CompleteMultipartUpload takes the same function: `commit_completed_upload` has the
same optional `precondition`, called at C4.c with the ETag of the object row the
Complete would replace (None when there is none). It is built by `write_precondition`
from the request headers, exactly as for PutObject, and the rules below are the same.
Complete has no separate "completing" state: the upload's `open` to `completed` change
is the compare-and-set at C4.a, inside the same transaction, so the rollback that
undoes a refused precondition also puts the state back. The upload is left `open` with
every part row and part blob as before the request. Only the assembled blob (a fresh
one, registered at C2) is discarded; nothing else was written. Every other Complete
error is decided before the condition: NoSuchUpload (before assembly, and at C4.a),
InvalidPart (C1, C3 and C4.b), so a request that is both invalid and refused by its
condition gets the invalid answer. `Last-Modified` of a Complete stays the upload's
initiation time, taken from the upload row, not the time of the commit.

There is no early check before the body is read (PutObject) or before the parts are
assembled (Complete). One would only save writing bytes that are bound to be refused,
and it could never decide success. Leaving it out keeps one decision point and the
order of errors the same with and without a condition (a body that fails its checksum
is BadDigest whatever the conditions say). The cost is that a refused PUT writes and
discards its bytes, and a refused Complete assembles a whole new blob, a copy of every
listed part, and discards it. For a large upload that is the full size of the object
in I/O for nothing, and it is repeated by every retry that is refused again. An early
check may be added later as a refusal only (C1 already names the place).

| Header | Current row exists | No current row |
|---|---|---|
| `If-None-Match: *` | 412 PreconditionFailed | proceed |
| `If-None-Match: <etag>` | 412 if the ETag matches (weak comparison), else proceed | proceed |
| `If-Match: *` | proceed | 404 NoSuchKey |
| `If-Match: <etag>` | proceed if the ETag matches (strong comparison), else 412 | 404 NoSuchKey |

Choices, all in `server/api/conditions.py`:

- Matching is the evaluator GET and HEAD use: `*`, comma lists, quoted and unquoted
  values are accepted, repeated header lines are one list, and a blank value is no
  condition. `If-Match` uses strong comparison, so `W/"x"` never matches it.
  `If-None-Match` uses weak comparison (RFC 9110 13.1.2).
- `If-None-Match: <etag>` is evaluated as RFC 9110 defines it, not refused. The S3 API
  reference documents `If-None-Match` on writes with the value `*` only. The pinned
  ceph tests send an entity tag: `test_put_object_ifnonmatch_failed` (marked
  `fails_on_aws`) and `test_put_object_if_match` (not marked).
- When both headers are present both are evaluated, `If-Match` first. With no current
  row the answer is therefore 404 even when `If-None-Match` would pass; with a row, a
  failure of either is 412.
- 409 ConditionalRequestConflict is never emitted. S3 documents it for a conditional
  write that conflicts with another request in flight on the same key, so that a client
  retries. Litmus serializes commits on the write lock and each one sees the row the
  previous one left, so there is no in-flight conflict to report: the later request is
  decided on committed state, and 412 (or 404) is a true answer about it. ceph's
  `test_atomic_dual_conditional_write_1mb` expects 412 for this case.
- The 404 cases follow ceph's `test_put_object_if_match` and
  `test_multipart_put_object_if_match`.
- A refused Complete leaves the upload open, with its parts, so the client can retry
  with another condition. The alternatives (closing the upload, or dropping the
  unlisted parts) would cost the client its uploaded bytes for a failure that says
  nothing about the upload.

Two concurrent `If-None-Match: *` PUTs to a new key produce exactly one creator: the
first commit inserts the row, and the second's check sees it and gets 412. Two
`If-Match` PUTs with the same ETag produce one 200 and one 412, because the first
commit changes the ETag. An `If-Match` PUT against a concurrent DeleteObject is 200
(the PUT committed first, then the delete) or 404 (the delete committed first); a 404
PUT creates nothing.

The same holds for Complete. Two uploads to one key completed concurrently with
`If-None-Match: *` produce one 200 and one 412, and the loser's upload stays open with
its parts. A Complete and a PutObject, both `If-None-Match: *`, on a new key produce
exactly one creator. An `If-Match` Complete whose commit comes after a DeleteObject is
404 NoSuchKey and does not bring the object back; its upload is still open.

`x-amz-write-offset-bytes` (append) stays 501. It would fit this design as a new blob
holding the old bytes plus the appended ones, committed only if the current size still
equals the offset, but nothing asks for it yet.

**DeleteObject (implemented).** `delete_object_meta` takes the same optional
`precondition` as `put_object_meta`, called inside BEGIN IMMEDIATE, after the write lock,
with the ETag of the row the transaction deletes (None when there is none). It is built by
`delete_precondition` in `server/api/conditions.py` from the request headers and reuses the
ETag parsing and strong comparison of `If-Match` on PutObject (`*`, quoted, unquoted,
lists, repeated lines, a blank value is no condition, a weak tag never matches). What it
raises rolls the transaction back, so a refused delete leaves the row, its registry entry
and its blob file as they were; the handler collects garbage only after a commit.

| `If-Match` | Current row exists | No current row |
|---|---|---|
| `*` | delete, 204 | 404 NoSuchKey |
| `<etag>` | delete, 204 if the ETag matches (strong comparison), else 412 | 404 NoSuchKey |

A missing key is 404 NoSuchKey, as for PutObject, and rolls the transaction back; an
unconditional delete of a missing key stays 204 (D2.b). The AWS user guide ("How to
perform conditional deletes") says S3 rejects a conditional delete with Not Found when
the object does not exist when the precondition is evaluated. So D2.b answers 204 only
when the request has no `If-Match`: with one, the precondition runs on "no row" first.
ceph's `test_delete_object_if_match` expects 204 there and fails on purpose. 409 is never
emitted, as above: AWS documents it for a concurrent DELETE or PUT that succeeds first,
and commits here are serialized on the write lock, so the loser is decided on committed
state (412 or 404). A kill before the commit leaves the object, and the same request then
succeeds; a kill after it leaves the object gone and the blob garbage for the sweep, and
the same request retried is 404 (D2, D3).

`x-amz-if-match-last-modified-time` and `x-amz-if-match-size` stay 501 on
purpose (AWS documents them for directory buckets only), and so does `If-None-Match`. They would
be evaluated against the same row at D2.c. A DeleteObjects entry's `LastModifiedTime` and
`Size` (inside the batch transaction at X2) and AbortMultipartUpload's
`x-amz-if-match-initiated-time` (at A2.b) are not implemented and need no further layout
change.

**DeleteObjects (implemented).** An `Object` may carry an `ETag` element, read as an
`If-Match` value (`*`, quoted, unquoted, lists, strong comparison; `delete_objects_precondition`
in `server/api/conditions.py` shares the parsing and the comparison with
`delete_precondition`). The store's `delete_objects_checked` takes a precondition per key
and runs the batch in the one transaction of §6.4. Each key's condition is called after
the write lock, in request order, with the ETag of that key's row at that moment (None
when there is none), and before anything is written for the key.

Unlike the single key preconditions it does not raise: it returns the refusal, or None.
A raise would roll the transaction back, and a refused key must not undo the other keys'
deletes. Because the decision comes before the key's first write, a refused key leaves
nothing to undo, and the transaction commits the keys that passed. The handler turns the
refusal into that key's `Error` entry.

| `ETag` of the entry | Current row exists | No current row |
|---|---|---|
| none, or blank | `Deleted` | `Deleted` (as S3 reports missing keys) |
| `*` | `Deleted` | `Error` NoSuchKey |
| `<etag>` | `Deleted` if the ETag matches (strong comparison), else `Error` PreconditionFailed | `Error` NoSuchKey |

A refused key is left untouched: its row, registry entry and blob are as before, and no
blob goes to garbage. A key listed twice is decided twice, in order, against the row the
first entry left, so `{ETag: e}` twice on one key is `Deleted` then `Error` NoSuchKey.
`LastModifiedTime` and `Size` stay a per key NotImplemented error, decided at X1 before the
`ETag`, so an entry with either is kept whatever its `ETag` says. Quiet mode lists every
`Error` and omits `Deleted`. The batch is 200 whatever the per key results; 409 is never
emitted, for the reason given for DeleteObject.

The kill points are those of §6.4. At `delete-objects-before-commit` nothing is committed, so every
object is still there and the same request then succeeds. At `delete-objects-after-commit`
the keys whose conditions held are gone and their blobs are garbage for the sweep; the
same request retried is NoSuchKey for those keys, and PreconditionFailed for a key that
still fails.

### §11. CopyObject and UploadPartCopy (R13)

Status: CopyObject (sequence in §6.10) and UploadPartCopy (sequence in §6.6A) are
implemented as below. A PUT with `x-amz-copy-source` on any other query is 501. The
checksum a CopyObject stores and returns is decision 33 and §20.

**Decided (decision 8): copy the bytes into a new ID.** CopyObject is PutObject whose
source is another object's blob:

1. Resolve the source row and open its blob, with GET's ENOENT retry (§8). The source
   conditions (`x-amz-copy-source-if-match` and the rest) are evaluated against the row
   that named the opened blob, because those are the bytes being copied. The
   evaluator is the one GET and HEAD use, over a view of the request that shows only
   the four `x-amz-copy-source-if-*` headers under the names of the RFC 9110
   preconditions: the request's own `If-Match` and `If-None-Match` are conditions on
   the destination and must never be read as conditions on the source.
2. REGISTER **[C]**; WRITE_BLOB from the source descriptor **[Ff] [Fd]**.
3. The P4 transaction for the destination key, including destination preconditions
   **[C]**; GC of the destination's old blob.

The source can be overwritten or deleted mid-copy without harm (the descriptor pins
it): the copy is of the version whose row the conditions were decided on. A source
replaced between the row read and the open is copied as it then is, with the conditions
decided again on that row; one deleted in that window is NoSuchKey; four replacements in
a row are 503 SlowDown (decision 14). Lost source bytes (the file gone, the row
unchanged) are 500.

Choices (AWS documents less than this; each is Litmus's own where it says so):

- **Source format.** `bucket/key`, a leading slash optional, percent-decoded once (a
  `+` is a plus), with an optional `?versionId=null` split off before decoding. A
  value that is not bucket and key, has an unknown query, or is not valid UTF-8 once
  decoded is 400 InvalidArgument naming the header (the API reference gives no code for
  a malformed source). `versionId` other than `null` and access point or Outposts ARNs
  are 501.
- **Error order.** Destination bucket, source syntax, directives, the request's headers
  under REPLACE, source bucket, source key, the self-copy refusal, the source
  conditions, then the copy. The 400s come before the conditions, as in §10.
- **Source conditions.** Each failing alone is 412 PreconditionFailed. AWS documents
  only two combinations (If-Match true with If-Unmodified-Since false is 200;
  If-None-Match false with If-Modified-Since true is 412) and ceph expects 412 for
  If-Match and If-None-Match alone. The RFC 9110 order gives both documented
  combinations; the evaluator's "not modified" answer is reported as 412.
- **Destination conditions.** `If-Match` and `If-None-Match` of the request, as for
  PutObject (§10), in the commit.
- **Metadata directive.** COPY (the default): Content-Type, the system headers and the
  user metadata come from the source, and the request's are ignored. REPLACE: they come
  from the request, as for PutObject (Content-Type defaults to `binary/octet-stream`),
  and none are kept from the source. Any other value is 400 InvalidArgument.
- **Tagging.** Litmus stores no tags. `x-amz-tagging-directive: COPY`, or REPLACE with an
  empty `x-amz-tagging`, proceeds; REPLACE with tags is 501, since they would be dropped.
  Under COPY, or no directive, a non-empty `x-amz-tagging` is accepted and not read: the
  tags come from the source (CopyObject page, `x-amz-tagging`). AWS's handling of a
  non-empty value there is not found. PutObject and CreateMultipartUpload are different:
  they refuse a non-empty `x-amz-tagging` in the request guard (decision 21).
- **ETag.** The MD5 of the copied bytes, always a plain MD5. A multipart source's
  `md5-N` ETag becomes the MD5 of the whole object, which is what AWS's "the object copy
  will be a single part object" implies (the value is not stated). A single part source's
  ETag is unchanged.
- **Copying onto itself.** Source and destination are the same bucket and key (with
  `versionId` absent or `null`). Without `x-amz-metadata-directive: REPLACE` it is 400
  InvalidRequest and nothing changes (ceph's `test_object_copy_to_itself`). With REPLACE
  it is the ordinary byte copy above (decision 19): a new blob with
  the same bytes, the new headers, a new Last-Modified, and the old blob garbage. AWS
  accepts a self-copy that changes a storage class, encryption or a website redirect
  too; Litmus refuses those headers (decision 24), so REPLACE is its only change.
- **Not honored, as for PutObject:** the checksum headers of the request are ignored,
  and an ACL or grant header is accepted only as no ACL asks for (decision 25); the
  storage class, SSE, website redirect and Object Lock headers are 501 (decision 24). The SSE-C headers for the source
  (`x-amz-copy-source-server-side-encryption-customer-*`) are not ignored: they are 501,
  here and on UploadPartCopy (§11, below).

*Possible later optimization.* A self-copy with REPLACE could be a metadata-only
transaction that keeps the same `blob_id`, since the bytes do not change: the headers
and Last-Modified updated only if the row still names the blob that was read, and no
blob becoming garbage. It would save copying a large object for a header change. It is
not done: it needs a second commit procedure and its own crash analysis for a case the
byte copy already handles correctly.

**UploadPartCopy** is UploadPart whose bytes come from a byte range of the source blob:
U2 to U6 with WRITE_BLOB reading `[first, last]` from the source descriptor (§6.6A). It
shares the source parsing, the source conditions and the open with CopyObject. A missing
source answers NoSuchKey before anything is registered, so
`test_upload_part_copy_percent_encoded_key` passes for the right reason (it passed while
the request was 501). Choices, each Litmus's own where it says so:

- **Range.** `x-amz-copy-source-range` is optional; without it the whole source is the
  part. The S3 API reference gives one form, `bytes=first-last` with zero-based offsets.
  Anything else (no `bytes=`, no `-`, a non-digit, a sign, a space, an open ended or
  suffix range, a list of ranges, a repeated header) is 400 InvalidArgument naming the
  header, which is what ceph's `test_multipart_copy_improper_range` expects. A reversed
  range (first greater than last) is the same 400 InvalidArgument: *Litmus's choice*, the
  reference gives no error for it.
- **Out of bounds.** A `last` at or beyond the source's size, or any range of an empty
  source, is 416 InvalidRange. The reference gives no code for it; ceph's
  `test_multipart_copy_invalid_range` accepts 400 or 416 with the code InvalidRange.
  Unlike GET, a `last` past the end is not clamped. The size is that of the row whose
  blob was opened, after the source conditions.
- **The 5 MB sentence.** The reference says "You can copy a range only if the source
  object is greater than 5 MB". Litmus does not enforce it: it gives no error, and ceph's
  tests copy ranges of sources of 5 bytes. *Litmus's choice.*
- **Error order.** Destination bucket, part number syntax, upload, part number interval,
  source syntax, range syntax, source bucket, source key, source conditions, range bounds,
  then the copy. The reference documents no order. The first four are UploadPart's. The
  range syntax comes before the source is looked up, as the source syntax does for
  CopyObject; the bounds need the row, so a request that fails both a condition and the
  bounds is 412.
- **Source conditions.** The four `x-amz-copy-source-if-*` headers, evaluated as for
  CopyObject (each failing alone is 412; the two combinations the reference documents
  hold). The request's own `If-Match` and `If-None-Match` are not read as conditions on
  the source, and are not evaluated as conditions on the destination: the reference lists
  conditional writes for PutObject and CompleteMultipartUpload only, and UploadPart
  ignores them too.
- **Part size.** The reference gives 5 MiB to 5 GiB and no minimum for the last part.
  Neither bound is enforced at upload, as for UploadPart; the minimum is enforced at
  Complete (decision 27).
- **ETag and body.** The part's ETag is the MD5 of the copied bytes, quoted, in the
  `CopyPartResult` body next to LastModified (the time of the response). There is no
  `ETag` header and no checksum element, and no `x-amz-copy-source-version-id` (Litmus has
  no versions).
- **Replacing a part.** As UploadPart: a fresh blob, the old part recorded and intact
  until the replacing commit, then garbage; of two commits of one part number the later
  wins. Copying a source onto the part of an upload at the same key is allowed.
- **Not honored, 501 on both CopyObject and UploadPartCopy:** the
  `x-amz-copy-source-server-side-encryption-customer-algorithm`, `-key` and `-key-MD5`
  headers. Litmus stores nothing encrypted, and a client that supplies a key for the
  source asks for something it cannot do. They were ignored on CopyObject before this
  change.
- **Source races.** As §11 above: the descriptor pins the blob, so a source overwritten or
  deleted after the open is copied as it was; one replaced between the row read and the
  open is copied as it then is, with the conditions and the range decided again on that
  row; one deleted in that window is NoSuchKey; four replacements in a row are 503
  SlowDown. An upload aborted or completed before the part commit is NoSuchUpload and the
  new blob is discarded.

**Not chosen: shared IDs with reference counts.** Immutable blobs make sharing
safe in principle: add a `refs` count to `blobs`, change it in the same transaction as
each reference, and make a blob `garbage` only when the count reaches zero. GET
reasoning is unchanged. It saves copying large objects but touches every delete,
overwrite and Complete transaction and the sweep's reference check. The schema leaves
room for it.

### §12. versionId=null, and versioning later

Every bucket stays unversioned, and the `objects` row for a key is its `null` version.
GET, HEAD and DELETE with `versionId=null` behave as without it, a DeleteObjects
`<VersionId>null</VersionId>` deletes the row, and ListObjectVersions lists each row
as version `null`, paged like the other listings (§13) with `key-marker` and
`version-id-marker=null`; any other version ID stays 501.

The design eases versioning: every write already produces its own immutable blob, and
replacing a version is already a single transaction. Versioning would add an
`object_versions` table keyed by (bucket, key, version_id) with a `blob_id`, where an
overwrite in a versioned bucket marks the previous version non-current instead of
making its blob `garbage`, and a delete inserts a delete marker. A suspended bucket
overwrites its `null` version exactly as §6.2 does now. Version IDs should be
generated separately rather than reusing blob IDs, so that a metadata-only change
(§11) can create a version without new bytes. Decided (decision 11): no version
columns now; a later migration from `objects` to `object_versions` is mechanical.

### §13. Listing over the new schema (R23)

Listing reads only rows. The `(bucket, key)` primary key in BINARY collation gives the
byte order S3 uses: the database encoding is UTF-8, so BINARY compares UTF-8 bytes,
which orders keys by code point.

**Prefix as a key range.** A prefix is the range `key >= :prefix AND key < :upper`,
where `:upper` is the successor of the prefix: the least string greater than every
string that starts with it. The successor drops trailing U+10FFFF characters (which
have no next code point) and increments the last remaining character, stepping from
U+D7FF to U+E000 over the surrogates, which UTF-8 cannot encode. A prefix that is
empty or made only of U+10FFFF has no successor, and the range has no upper bound.
This replaces `LIKE prefix || '%'`, whose `_` and `%` are wildcards and which ignores
ASCII case. `EXPLAIN QUERY PLAN` shows `SEARCH objects USING PRIMARY KEY (bucket=? AND
key>? AND key<?)` and no temporary B-tree, so rows come out in index order.

**Pages.** A page is read in one read transaction, as a series of range reads in key
order, each with a `LIMIT` of one row more than the page still needs, so the page knows
whether it is truncated without reading further. A marker or start-after key is a
lower bound `key > :marker`. A ListObjectsV2 continuation token records the restart
position itself: after the last key returned (`key > :key`), or, when the page ended
on a common prefix, at that prefix's successor (`key >= :successor`). The next page
therefore restarts correctly even if the follow-up request changes or drops the
delimiter. The token is URL-safe base64 of a small JSON object, so it carries any key,
and the server accepts only the exact encoding it produces.

**Common prefixes and the restart after one.** With a delimiter, a key that contains
the delimiter after the prefix rolls up into its common prefix: the key up to and
including that first delimiter. All keys under a common prefix `cp` lie in
`[cp, successor(cp))`, so after returning `cp` the next range read starts at
`key >= successor(cp)`, past every key under it, however many there are; restarting at
`key > cp` would return `cp` again. A common prefix is returned only if it sorts after
the marker (the S3 ListObjects reference: "CommonPrefixes is filtered out from results
if it is not lexicographically greater than the key-marker"). A marker equal to or
inside a common prefix therefore starts the page at that prefix's successor, and a
page that ended on `cp` continues correctly from `marker = cp`.

**Mutation checks.** Each of the two listing rules above was broken on its own in
`server/metadata/sqlite.py`, the whole local suite was run, and the file was restored;
after both, the suite was back to 1122 passed, 2 xfailed, with no diff. Tests not named
here passed under the mutation.

1. *The `LIKE` prefix query.* `key_range_query` selected `key LIKE prefix || '%'` with
   the marker and restart bounds, in place of the key range. 24 tests failed:
   - in all three listings (V1, V2, ListObjectVersions), `test_prefix_underscore_is_literal`,
     `test_prefix_percent_is_literal` and `test_prefix_is_case_sensitive` (9), and
     `test_v1_`, `test_v2_` and `test_versions_pages_concatenate_to_unpaged_listing`,
     two seeds each (6);
   - `test_percent_in_prefix_marker_and_start_after` under httptools and h11 (2), which
     covers V1 and V2;
   - in `tests/unit/test_sqlite.py`, four cases of `test_key_range_query_uses_primary_key`,
     `test_list_prefix_is_not_a_wildcard`, `test_list_prefix_is_case_sensitive` and
     `test_list_prefix_matches_startswith_on_random_keys` (7), which test the page read
     shared by all three.
2. *The next page restarts inside the last common prefix.* A page that ended on `cp`
   restarts at `key > cp` instead of `key >= successor(cp)`, and a marker equal to or
   inside a common prefix is a plain lower bound. Both parts are needed to cover all three
   listings: V2 continues from its token, V1 and ListObjectVersions from `marker = cp`.
   28 tests failed: 27 directly, and one that hung until the paging loops in the tests
   had caps (see V1):
   - V1: `test_v1_marker_at_or_inside_common_prefix_skips_it` for `a/`, `a/1` and
     `a/15` (3), `test_v1_pages_concatenate_to_unpaged_listing` (2) and
     `test_v1_encoded_elements`. `test_v1_pages_through_common_prefixes` hung before
     the paging loops in the tests had page caps: it paged until a response was not
     truncated, and each page returned `boo/` again, so it had to be deselected for the
     rest of its file to run. Every hand-written paging loop now stops after the most
     pages a correct listing of its keys can take, and this test fails in under a
     second with "paging did not end within 6 pages for 5 keys at max-keys=1".
   - V2: `test_v2_start_after_at_or_inside_common_prefix_skips_it` for `a/` and `a/1`
     (2), `test_v2_token_carries_restart_after_common_prefix`,
     `test_v2_pages_concatenate_to_unpaged_listing` (2) and `test_v2_encoded_elements`.
     The `a/2/x` case passed: no key sorts after it inside `a/`.
   - ListObjectVersions: `test_versions_key_marker_at_or_inside_common_prefix_skips_it`
     for `b/`, `b/1` and `b/15` (3), `test_versions_delimiter_and_common_prefix_page_end`,
     `test_versions_pages_concatenate_to_unpaged_listing` (2) and
     `test_versions_encoded_elements`.
   - Through boto3 against uvicorn: `test_paginators_with_delimiter` with page sizes 1
     and 3 and `test_default_client_decodes_echoed_parameters` (3). botocore stops a
     paginator that receives the same token twice, so these fail instead of hanging.
   - In `tests/unit/test_sqlite.py`: `test_page_restarts_after_common_prefix`,
     `test_page_marker_equal_to_common_prefix_skips_it`,
     `test_page_marker_inside_common_prefix_skips_it`,
     `test_page_marker_inside_common_prefix_under_prefix` and
     `test_page_common_prefix_of_only_max_code_points` (5).

### §14. Requirement coverage

| Req | Where |
|---|---|
| R1 | §4: no key in any path |
| R2 | §6.2, §6.7: fresh path, old bytes unlinked after the commit |
| R3 | §6.3, §6.4, §6.8: one commit first, unlink after |
| R4 | §4: directories created and fsynced at startup only |
| R5 | §6.7 C4: one transaction for the object row and the upload completion |
| R6 | §6.7 C4.a: Complete writes only its fresh blob |
| R7 | §7 |
| R8 | §6.9, decision 7 |
| R9 | §2 |
| R10 | §6 |
| R11 | the kill tables in §6, §7 |
| R12 | §5 |
| R13 | §11 |
| R14 | §9 |
| R15 | §10 |
| R16 | §4 (`_contained` kept for IDs); test changes in §16 |
| R17 | §15 |
| R18 | §17 |
| R19 | §4 |
| R20 | §4, §6.9 |
| R21 | §5 |
| R22 | §5, §6.5 |
| R23 | §5, §13 |
| R24 | §4 |
| R25 | not in this change. After steps (a) and (b) are implemented, you rewrite `docs/crash-model.md`, `docs/publication-barrier.md` and `docs/compatibility.md` before any feature work (decision 15). The README's crash and publication-barrier sections are part of that rewrite (decision 18) |

### §15. Tier B tests

Each test arms one crash point, drives the request into it, asserts the server died by
SIGKILL, then asserts (a) the raw state while the server is down (rows and files read
directly), (b) the sweep report (in-process `run_consistency_sweep` on the dead
server's directory, as the current xfail tests do), (c) what clients see after a
restart, and (d) that the client's natural retry succeeds. Setup requests run on the
armed server only where they cannot reach the armed point; otherwise the test sets up
on an unarmed server and restarts it armed.

New crash points (added to `CRASH_POINTS`); `put-after-bytes`,
`complete-after-assemble` and `complete-after-commit` keep their names and get the
new meaning described in §6.

| Crash point | Test | Proves |
|---|---|---|
| `put-after-register` | `test_put_killed_after_register` | registered ID with no bytes: not visible; row removed; `InterruptedWrite` (false) |
| `put-mid-write` | `test_put_killed_mid_write` | partial temp removed; not visible |
| `put-after-bytes` (existing) | `test_put_killed_after_bytes_before_metadata` (revised, §16) | complete blob not visible; removed by the sweep and reported with bucket and key |
| `put-after-bytes` | `test_overwrite_killed_after_bytes_keeps_old_object` | **R2**: the old object's exact bytes and ETag survive |
| `put-after-commit` | `test_overwrite_killed_after_commit` | new object visible; old blob removed as `InterruptedCleanup` |
| `put-after-gc-unlink` | `test_overwrite_killed_after_gc_unlink` | `garbage` row without a file is purged silently; report clean |
| `copy-after-register` | `test_copy_killed_after_register` | registered ID with no bytes: destination not created; row removed; `InterruptedWrite` (false); source intact |
| `copy-mid-write` | `test_copy_killed_mid_write`, `test_copy_over_an_existing_key_killed_mid_write_keeps_the_old_object` | the first 64 KiB of the source in a temp file; removed with the row; the old destination survives |
| `copy-after-bytes` | `test_copy_killed_after_bytes_to_a_new_key`, `test_copy_over_an_existing_key_killed_after_bytes_keeps_the_old_object` | **R2**: the complete copy is not visible; the old destination's exact bytes and ETag survive; `InterruptedWrite` (true) |
| `copy-after-commit` | `test_copy_to_a_new_key_killed_after_commit_is_complete`, `test_copy_over_an_existing_key_killed_after_commit`, `test_copy_onto_itself_with_replace_killed_after_commit` | the copy is visible; the replaced blob (a self-copy's source blob too) removed as `InterruptedCleanup`; nothing to clean for a new key |
| `copy-after-gc-unlink` | `test_copy_over_an_existing_key_killed_after_gc_unlink` | `garbage` row without a file is purged silently; report clean |
| `delete-before-commit` | `test_delete_killed_before_commit_keeps_object` | **R3**: object and bytes intact (fails on today's code) |
| `delete-after-commit` | `test_delete_killed_after_commit` | 404; blob removed; no `OrphanedMetadata` |
| `delete-objects-after-commit` | `test_delete_objects_killed_after_commit` | all deleted keys 404, keys with per-key errors intact; every blob removed |
| `delete-objects-before-commit` | `test_delete_objects_with_a_passing_etag_killed_before_commit_keeps_the_objects` | both objects intact; the same request then deletes the key whose ETag holds (§10) |
| `delete-objects-after-commit` (ETag) | `test_delete_objects_with_a_passing_etag_killed_after_commit_removes_the_object` | the passing key gone, the refused key intact; the retry is NoSuchKey and PreconditionFailed |
| `part-after-register` | `test_upload_part_killed_after_register` | part not recorded; upload open |
| `part-mid-write` | `test_upload_part_killed_mid_write` | partial temp removed |
| `part-after-bytes` | `test_reupload_part_killed_after_bytes_keeps_old_part` | re-upload: old part still recorded; Complete with the old ETag gives the old bytes |
| `part-after-commit` | `test_reupload_part_killed_after_commit` | new part recorded; old part blob removed with upload_id and part_number |
| `part-copy-after-register` | `test_part_copy_killed_after_register` | part not recorded; row removed; `InterruptedWrite` (false); source intact |
| `part-copy-mid-write` | `test_part_copy_killed_mid_write` | the first 64 KiB of the range (not of the source) in a temp file; removed with the row |
| `part-copy-after-bytes` | `test_part_copy_killed_after_bytes_to_a_new_part`, `test_part_copy_over_a_part_killed_after_bytes_keeps_the_old_part` | the complete copy is not recorded; the old part stays usable by Complete with its ETag; `InterruptedWrite` (true) |
| `part-copy-after-commit` | `test_part_copy_killed_after_commit_to_a_new_part`, `test_part_copy_over_a_part_killed_after_commit` | the copied part is recorded and Complete with its ETag works; the replaced part blob removed as `InterruptedCleanup` |
| `complete-after-register` | `test_complete_killed_after_register` | upload open, parts intact, retry works |
| `complete-mid-assemble` | `test_complete_killed_mid_assemble` | partial temp removed; upload open |
| `complete-after-assemble` (existing) | `test_complete_killed_after_assemble_before_metadata` (revised) | assembled blob not visible, removed; upload open, parts intact |
| `complete-after-assemble` | `test_complete_over_existing_killed_after_assemble_keeps_old_object` | **R2/R6**: an existing object at the key keeps its bytes |
| `complete-after-commit` (existing) | `test_complete_killed_after_commit_before_part_cleanup` (revised) | object visible; upload closed; part blobs removed by the sweep and reported |
| `complete-after-commit` (existing) | `test_no_open_upload_once_object_committed` (unchanged) | **R5**: never an open upload with a committed object |
| `complete-after-gc-unlink` | `test_complete_killed_after_gc_unlink` | partial cleanup finished by the sweep |
| `abort-before-commit` | `test_abort_killed_before_commit_keeps_upload` | **R3**: parts intact, Complete still works (fails on today's code) |
| `abort-after-commit` | `test_abort_killed_after_commit` | NoSuchUpload; part blobs removed; no `MissingPart` |
| `bucket-delete-after-commit` | `test_delete_bucket_killed_after_commit` | bucket gone, its open uploads aborted (NoSuchUpload), part blobs removed |
| `sweep-after-unlink` | `test_sweep_killed_mid_cleanup` | a sweep killed mid-way is finished by the next start, with the same final state |
| `store-init-after-commit` | `test_first_start_killed_after_store_row` | a first start killed between the `store` row and the marker: restarting with the same blob directory finishes it (marker written, row `ready`); restarting with a different blob directory refuses and deletes nothing |

GC is one procedure; its unlink window is proven through PutObject and Complete, and
DeleteObject, DeleteObjects, Abort, CopyObject and UploadPartCopy reuse it unchanged. The
window between WRITE_BLOB's fsync and its rename leaves the same state class as
`*-mid-write` (a temp file only) and needs no separate point.

### §16. Other tests, and existing tests this design changes

New tests outside Tier B (conformance or Tier A):

- `k` and `k/` are independent objects; `a/b` and `a/b/c` coexist (R1).
- Keys with `.` and `..` segments, and keys of 1024 bytes, round-trip, and no file
  appears outside `blobs/` (R16, R19).
- GET racing overwrite and delete, deterministically: a fault hook between the row read
  and `open` commits an overwrite (or delete) and GCs, and GET must return the new
  version (or 404), never 500 or a mix; an open descriptor keeps reading the old body
  after the overwrite.
- Concurrent same-key PUTs with handlers in real threads (not only `asyncio.gather` on
  one loop), checking one winner, ETag matches body, no leftover `pending` or
  `garbage` rows.
- A refused Complete commit (upload aborted after assembly, via a fault hook) leaves the
  existing object's bytes and row unchanged (R6).
- DeleteBucket with an open upload deletes the bucket and aborts the upload: the
  upload then gets NoSuchUpload, its part rows are gone, and no part file remains
  (decision 7).
- A second server on the same `S3_BLOB_DIR` refuses to start (I6).
- Startup refuses a version-1 database and `S3_DATA_DIR` / `S3_PARTS_DIR`.
- Later, with conditional writes (not in steps (a) and (b)): two concurrent
  `If-None-Match: *` PUTs create exactly one object.

**Store identity tests (I7, §7 step 6).** Each refusal test asserts that the server exits
non-zero before accepting requests, that the log names the mismatch, and that the blob
directory and the database are unchanged: the same files with the same bytes before
and after, except that a `LOCK` file may appear (§7 step 5 runs before the check).
That proves the sweep never ran.

- `test_first_start_writes_matching_store_ids`: a fresh database and a fresh blob
  directory. Afterwards the `store` row is `ready` and `blobs/STORE_ID` holds the same
  ID; a restart with the same pair starts.
- `test_refuses_database_with_fresh_blob_dir`: a `ready` database paired with an
  empty blob directory, and with a missing one (step 4; nothing is created).
- `test_refuses_blob_dir_of_another_store`: a `ready` database paired with a blob
  directory whose marker holds a different ID.
- `test_refuses_fresh_database_with_used_blob_dir`: a new, empty database paired with a
  blob directory that has a marker.
- `test_refuses_missing_marker`: the marker deleted from a `ready` store.
- `test_refuses_unmarked_blob_dir_with_files`: a first start against a blob directory
  holding blob files but no marker (step 6a).
- `test_second_server_sharing_database_with_other_blob_dir_refuses`: two subprocess
  servers on one `S3_DB_PATH` with different `S3_BLOB_DIR`s. The second refuses and
  the first keeps serving.
- `test_initializing_row_is_adopted_only_by_its_blob_dir`: an `initializing` row
  (written directly) is finished by its recorded blob directory and refused by any
  other. The SIGKILL version of this window is the Tier B test
  `test_first_start_killed_after_store_row` (§15).

**Barrier order tests (Tier A).** Under SIGKILL the page cache survives, so no Tier B
test can tell whether an fsync was actually called; a missing **[Ff]** or **[Fd]**
passes every crash test above. These tests record the order of the calls themselves,
in process, and fail if one is missing or out of order. There is one per write path:

| Test | Request | Required order, all on the blob that becomes visible |
|---|---|---|
| `test_put_object_barrier_order` | PutObject over an existing key | WRITE_BLOB step 3 **[Ff]** on the temp file, then step 4 rename `tmp/<id>.tmp` → `blobs/<xx>/<id>`, then step 5 **[Fd]** on `blobs/<xx>`, then the P4 visibility commit |
| `test_upload_part_barrier_order` | UploadPart re-uploading a part number | the same three calls for the new part blob, then the U4 visibility commit |
| `test_complete_barrier_order` | CompleteMultipartUpload over an existing key | ASSEMBLE_BLOB's **[Ff]** on the assembled temp file, its rename, **[Fd]** on the destination fanout directory, then the C4 visibility commit (object row and upload completion) |

How they record. The test wraps `os.fsync`, `os.rename` and `os.replace` with recorders
that log each call and pass it through, and installs a SQLite trace callback
(`Connection.set_trace_callback`, on every connection the store opens) that logs each
`BEGIN IMMEDIATE`, statement and `COMMIT`. Each fsync is identified by
`os.fstat(fd)`: (device, inode) of a regular file, compared with the inode of the blob
that ends up visible, or of a directory, compared with the blob's fanout directory.
That way the test does not depend on how the implementation names its descriptors.
The visibility commit is the `COMMIT` that closes the transaction containing the
`UPDATE blobs SET state = 'live'` for that blob.

Each test fails if any of the four is missing; if they occur in another order; if the
file fsync is on another inode (for example the source or a part instead of the new
blob); if the directory fsync is on another directory (for example `tmp/` instead of
`blobs/<xx>`); or if the visibility commit begins before the directory fsync has
returned. Because the requests overwrite an existing key or part, each test also
checks two neighboring rules: the registration commit comes before the temp file is
opened (I2), and the replaced blob is not unlinked before the visibility commit (I4).

Fsyncs covered: the per-write **[Ff]** and **[Fd]** on the PutObject, UploadPart and
Complete paths, and the **[C]** that publishes each. Not covered: the WAL fsync inside
SQLite's commit (the tests trust `synchronous=FULL` and check only the order of the
`COMMIT`), the startup directory fsyncs and the store marker's fsyncs (§4, §7), and
paths with no file barrier (DeleteObject, DeleteObjects, Abort, DeleteBucket, whose only
durable step is their commit). Directory fsync is skipped on Windows, so these tests
are POSIX-only, like the Tier B suite.

Existing tests that reach into the current layout or storage API and will need edits
once this is implemented. Approved (decision 10) on two conditions: the assertion
lines of the two strict xfail tests stay byte for byte identical, and their markers
come off only after the tests have XPASSed. None is edited by this document.

- `chaos/scenarios/test_crash_points.py`: helpers `_object_dir`, `_part_dir` and
  `_sweep` (backend constructor); the assertions that list `objects/<bucket>` and read
  `objects/<bucket>/<key>` in `test_put_killed_after_bytes_before_metadata` and
  `test_complete_killed_after_assemble_before_metadata`; and, in
  `test_complete_killed_after_commit_before_part_cleanup`, the assertion that part files
  remain after the restart. The restart's sweep now removes them, so the assertion
  moves to before the restart, and the test additionally asserts that they are gone
  after it. The two strict xfail tests need their setup lines (the
  `objects/<bucket>/<key>` and `parts/<upload_id>` existence checks, and `_sweep`)
  pointed at the registry. Their assertion lines stay byte for byte identical, and
  their markers come off only after they XPASS (§17).
- `chaos/scenarios/test_desync.py`, `chaos/scenarios/test_corruption.py`,
  `tests/unit/test_consistency.py`: `storage._object_path(bucket, key)` and
  `storage._part_path(...)` become a helper that resolves the blob path through the
  metadata row. Same assertions.
- `tests/unit/test_consistency.py`, `tests/unit/test_etag.py`,
  `tests/conformance/test_buckets.py`: seed objects through `write_object(bucket, key,
  ...)`, `assemble_parts(bucket, key, upload_id, ...)` and `put_object_meta`, which
  become ID-based. Same assertions.
- `tests/unit/test_sqlite.py`: `ObjectMeta` gains `blob_id`;
  `test_write_rollback_on_exception` inserts into `objects` with the version-1 column
  list. It would still pass, but by failing on the missing `blob_id` instead of on the
  foreign key it means to test, so it should add `blob_id`.
- `tests/unit/test_filesystem_paths.py`: it tests that bucket, key and upload ID strings
  cannot escape their directories. With IDs, the backend takes only IDs; the tests
  become "malformed IDs are refused and write nothing" plus "no client string reaches
  the backend".
- `tests/conformance/test_path_safety.py`: R16 asks to remove the tests that pin the
  400 rejection. Approved instead (decision 10): keep each test's invariant (no file
  outside storage, the sentinel and `meta.db` untouched, no object planted in another
  bucket) and change the expected outcome from 400 to a normal store-and-read of the
  literal key. This lands in step (b).
- `chaos/scenarios/test_enospc.py`: the fault injector targets the method names
  `write_object` and `assemble_parts`. If the backend methods are renamed, the strings
  must follow. Otherwise the fault never fires and the tests fail loudly (not
  vacuously).
- `chaos/fixtures/subprocess.py` and `chaos/fixtures/inprocess.py`: `S3_DATA_DIR` /
  `S3_PARTS_DIR` and the backend constructor change (decision 6).

Unaffected: the HTTP-only suites (`test_objects.py`, `test_multipart.py`,
`test_range.py`, `test_version_id.py`, `test_conditional_*.py` until conditional
support lands, `test_unsupported_requests.py`, `test_connection_reuse.py`,
`test_concurrent.py`, `test_crash_multipart.py`).

### §17. ceph/s3-tests and the strict xfails

The `-k` filter is untouched, so the selected count stays 330. Baseline 99 of 330,
checked against `docs/s3-tests-results.txt` at 8f9c4a9: 99 PASSED, 224 FAILED,
7 SKIPPED, 330 lines. The change from 98 is `test_100_continue_error_retry`, FAILED to
PASSED in 8f9c4a9 (the `Expect: 100-continue` connection fix); it is the only line
of the results file that commit changed.

Expected outcome of the implementation, steps (a) and (b) of decision 15: **no ceph
outcome changes**. Both expectations are to be confirmed by the ceph rerun after each
step:

- (a) the layout: see the next paragraph. DeleteBucket aborting open uploads
  (decision 7) looks the same as today from the ceph harness's side (§6.9).
- (b) lifting the dot-segment rejection: `s3tests/functional/test_s3.py` has no
  literal key with a `.` or `..` segment (searched; keys built at run time were not
  traced).

**Flipped by the layout change alone: none.** The three candidates named in R18 stop
failing on the file-versus-directory collision, but still fail later on
`Delimiter` and common prefixes, which are listing work (category c):

- `test_bucket_list_delimiter_basic` and `test_bucket_listv2_delimiter_basic` create
  `foo/bar` and `foo/bar/xyzzy` (today a 500), then assert `CommonPrefixes`.
- `test_bucket_list_delimiter_not_skip_special` creates `0/` and `0/1000` and so on, then
  asserts delimiter output.

Their outcome stays FAILED; only the reason changes, which `docs/s3-tests-results.txt`
does not record. Tests that must keep passing and depend on what this design
preserves: `test_atomic_read_*` (open descriptors keep old bytes), `test_atomic_write_*`,
`test_atomic_dual_write_*`, `test_atomic_multipart_upload_write`,
`test_multipart_upload_overwrite_existing_object`,
`test_upload_part_copy_percent_encoded_key`.

**Flipped later by conditional-write support (R15), which this design makes safe.**
This is not part of steps (a) and (b); conditional writes come later, in your order
(decision 15). If conditional PutObject and Complete are implemented as in §10, I
expect these 12 to go from FAILED to PASSED. I read each test body; none was run:

- `test_put_object_ifmatch_good`, `test_put_object_ifmatch_overwrite_existed_good`,
  `test_put_object_ifnonmatch_good`, `test_put_object_ifnonmatch_nonexisted_good`: the
  condition holds, and the write must succeed (these passed before the 501 change
  only because the header was ignored).
- `test_put_object_ifmatch_failed`, `test_put_object_ifnonmatch_failed`,
  `test_put_object_ifnonmatch_overwrite_existed_failed`: 412, and the old body stays.
- `test_put_object_ifmatch_nonexisted_failed`: `If-Match: *` on a missing key gives 404
  NoSuchKey, and nothing is created.
- `test_put_object_if_match`: the full matrix of §10 on one key, including after a
  delete.
- `test_atomic_conditional_write_1mb`: an `If-Match: *` overwrite; reads during the
  upload see the old body.
- `test_atomic_dual_conditional_write_1mb`: a second conditional PUT commits while the
  first is still sending its body, so the first must be refused at its commit (412).
  This passes only because the check runs at commit time, not at request start.
- `test_multipart_put_object_if_match`: the same matrix on CompleteMultipartUpload.

Conditional writes are separate work from the layout and must be rerun on ceph when
implemented. Not flipped even then:
`test_put_current_object_if_match`, `test_put_current_object_if_none_match`,
`test_put_object_current_if_match`, `test_multipart_put_current_object_if_match`,
`test_multipart_put_current_object_if_none_match` (they enable bucket versioning,
which returns 501). `test_copy_object_ifmatch_good` / `_failed` (CopyObject) flip with
CopyObject (§11).

**Eased, not flipped:** `test_multipart_upload_resend_part` and
`test_multipart_upload` need Content-Type and user metadata carried from
CreateMultipartUpload to the object (the schema has the columns; the handlers are
category d / j1 work), and `test_multipart_upload` also re-completes a completed upload
(j3). Decision 35 supersedes that: the second Complete is NoSuchUpload and both ceph tests fail on purpose. Of the 6 non-versioned conditional delete tests, `test_delete_object_if_match` is
evaluated (§10) but fails on purpose: it expects 204 for `If-Match` on a missing key and
Litmus answers 404. `test_delete_object_if_match_last_modified_time` and `_size` (501) and
their `test_delete_objects_*` counterparts stay failing on purpose (directory buckets only);
`test_delete_objects_if_match` is evaluated (§10) and passes. The `current` and `version`
variants enable bucket versioning and stay failing.
`test_multipart_resend_first_finishes_last` is not helped: its Complete request lists
part 1 twice, and the stale ETag gets InvalidPart.

**The two strict xfails: both should flip in step (a).** They will XPASS, which turns
the run red. Per decision 10, the markers come off only after that XPASS is seen, and
the assertion lines stay byte for byte identical:

- `test_gap_sweep_reports_orphaned_file_after_put_crash`: after `put-after-bytes` the
  sweep reports `InterruptedWrite` with `bucket == "crash-bucket"` and `key ==
  "obj.bin"`, which is not `OrphanedMetadata`, so the assertion holds as written. This
  works only with the registry, which decision 2 keeps.
- `test_gap_sweep_reports_part_files_of_completed_upload`: after
  `complete-after-commit` the part blobs are `garbage` rows carrying the upload ID, and
  the sweep reports one `InterruptedCleanup` per part with that `upload_id`, which is
  not `MissingPart`.

Both tests' setup lines check `objects/<bucket>/<key>` and `parts/<upload_id>` and
build the old backend. Those lines would call `pytest.fail` (a real failure, not an
xfail) until they are pointed at the new layout. Only those setup lines change (§16).

### §18. Open questions

Places where a requirement was unclear, conflicted with another, or could not be met as
written. Each is marked **Resolved** (with the decision that settled it) or **Open**.

1. **Resolved. Ground rule wording vs the registration commit.** The ground rule lists
   "temp file, fsync, rename, fsync of the parent directory, then the SQLite commit",
   and the design adds a commit *before* the temp file (registration). Your reading:
   the rule means the bytes are durable before the commit that makes them visible. The
   registration commit makes nothing visible, so it is consistent with the rule. The
   visibility commits are P4, U4 and C4, and the barrier order tests (§16) check
   exactly that order.
2. **Resolved (decision 2). R7 and the first strict xfail vs a one-commit write path.**
   The registration commit stays, so the sweep reports orphaned files by bucket and
   key, and the xfail's assertion holds as written.
3. **Resolved (decisions 3 and 15). R7 "whether it repairs" vs the crash-model doc.**
   The sweep deletes temp files and the files of `pending` and `garbage` rows, and
   reports everything else. You rewrite `docs/crash-model.md` to match, after steps (a)
   and (b).
4. **Resolved (decision 7); the S3 fact itself stays unconfirmed. R8: general purpose
   bucket behavior.** DeleteBucket aborts open uploads inside its own transaction
   (§6.9). Whether S3 refuses DeleteBucket on a general purpose bucket with
   in-progress uploads is still not confirmed. The decision does not depend on it:
   the pinned ceph teardown never aborts uploads before deleting buckets (§6.9).
5. **Resolved (decision 9), with a caveat. R14: which concurrent PUT wins.** The last
   commit wins and Last-Modified is stamped at commit time. Still unconfirmed: whether
   that matches the S3 User Guide's "latest timestamp wins" wording (from memory), and
   the fact that a backwards wall-clock step can give the winner an earlier
   Last-Modified.
6. **Resolved (§10). R15: 412 vs 409.** S3 documents 409 ConditionalRequestConflict
   for some conflicting conditional writes; ceph's
   `test_atomic_dual_conditional_write_1mb` expects 412 for what looks like that case.
   PutObject returns 412 and never emits 409: commits serialize on the write lock, so
   a conditional write is always decided on committed state.
7. **Resolved (decision 10). R16 vs the rule on test changes.** The
   `test_path_safety.py` tests are converted, not deleted (§16), in step (b).
8. **Resolved (decision 4). R12 "migration".** No migration; the server refuses to start
   on version-1 data or the old environment variables (§5).
9. **Resolved (decision 17). Upload and request bucket/key mismatch.** UploadPart,
   Complete and Abort look up the upload by ID only. The check that the upload belongs
   to the request's bucket and key (§6.6, §6.7) stays in the design and is implemented
   with the multipart work, not in step (a). Whether it matches S3 is still to be
   confirmed then. Decision 34 implements the check and records what was found.
10. **Open. R23 and keys that are not valid UTF-8.** Keys are stored as SQLite TEXT,
    today and in this design. Whether a percent-encoded key with invalid UTF-8 can
    reach the handlers, and what S3 does with it, is not covered by any requirement.
    Listing order (BINARY collation) assumes valid UTF-8.
11. **Resolved (decision 16). The first-start exception to the store identity rule.**
    Applied literally, refusing every one-sided store would leave a store that can never
    start after a kill between the database write and the marker write at first start.
    §7 accepts exactly one one-sided state: an `initializing` row, adopted only by the
    blob directory recorded with it, and only if the store never held data.
12. **Resolved (decision 18). README sections in R25.** The README's crash and
    publication-barrier sections are part of your doc rewrite after steps (a) and (b).

### §19. Decisions

Recorded on 2026-10-23.

1. **Direction.** Option A, opaque IDs with a blob registry, is the layout to implement.
2. **Registration commit.** Keep the `pending` registry state: one extra commit per
   PutObject, UploadPart and Complete, a precise sweep that reports bucket and key, and
   both strict xfails flipping as written.
3. **Sweep actions.** Delete temp files and the files of `pending` and `garbage` rows;
   report everything else (§7). No quarantine.
4. **Old data.** No migration. The server refuses to start on version-1 data or when
   `S3_DATA_DIR` / `S3_PARTS_DIR` is set (§5).
5. **Fanout.** One level of 256 directories, created and fsynced at startup (§4).
6. **Configuration.** `S3_BLOB_DIR` (default `data/blobs`) replaces `S3_DATA_DIR` and
   `S3_PARTS_DIR`; the server refuses to start if the old variables are set. The chaos
   fixtures change accordingly.
7. **DeleteBucket with open uploads.** Abort the bucket's open uploads inside the
   DeleteBucket transaction (§6.9), with the crash point `bucket-delete-after-commit`.
   Checked first, as you asked: the pinned s3-tests teardown
   (`setup_teardown` → `nuke_prefixed_buckets` → `nuke_bucket` in
   `s3tests/functional/__init__.py` at `5b39871`) lists object versions, deletes them
   with DeleteObjects and calls DeleteBucket. It does **not** abort multipart uploads
   first.
8. **CopyObject.** Copy bytes into a new ID (§11); no shared IDs or reference counts.
9. **Concurrent PUT winner.** The last commit wins; Last-Modified is stamped at commit
   (§9).
10. **Test edits.** The edits listed in §16 are approved, on two conditions: the
    assertion lines of the two strict xfail tests stay byte for byte identical, and
    their `xfail` markers come off only after the tests have XPASSed.
11. **Versioning preparation.** No version columns now (§12).
12. **DeleteObjects.** One transaction per request (§6.4).
13. **Schema details.** User metadata as one JSON column, plus the system header
    columns listed in §5.
14. **GET retry bound.** Three ENOENT retries, then 503 SlowDown on the fourth failed
    open: at most four opens per GET. Three racing commits are served on the fourth
    open; four get 503 (§8).
15. **Order of work.** The implementation covers only:
    - (a) the layout, the blob registry, the sweep, the startup lock and store
      identity, the Tier B tests in §15 and the other tests in §16, including the
      barrier order tests;
    - (b) lifting the dot-segment rejection (R16), with the `test_path_safety.py`
      conversion.

    Each step gets its own ceph rerun and a regenerated `docs/s3-tests-results.txt`.
    After (a) and (b), you rewrite `docs/crash-model.md`, `docs/publication-barrier.md`
    and `docs/compatibility.md` before any feature work starts. Conditional writes
    (§10) and CopyObject / UploadPartCopy (§11) come later, in an order you choose.
    They are not part of this implementation. Done since: conditional writes and
    deletes (§10), CopyObject (§11, §6.10, decision 19) and UploadPartCopy (§11, §6.6A,
    decision 20).

Recorded during step (a):

16. **First-start exception.** Accepted, on one condition: an `initializing` store row
    is finished only by the blob directory recorded with it, and only if the store
    never held data. No table other than `store` and `schema_version` may have a row
    (bucket rows count), and `blobs/` may contain nothing except `LOCK`, empty fanout
    directories, a `tmp/` that is empty or holds only the store ID temp file, and
    `STORE_ID` when it holds this store's own ID (left by a kill between the marker
    write and the ready update). Otherwise the server refuses to start (§7). A first
    start where neither side has an ID also requires every table other than `store`
    and `schema_version` to be empty (§7 step 6a). The refusal tests (§16) allow a `LOCK`
    file to appear; every other file must be unchanged.
17. **Upload bucket/key check.** Kept in the design (§6.6, §6.7), implemented with the
    multipart work, not in step (a). Implemented by decision 34.
18. **README.** The README's crash and publication-barrier sections are part of your
    doc rewrite after steps (a) and (b) (decision 15).

Recorded with CopyObject:

19. **CopyObject.** Implemented as §6.10 and §11 describe. Choices you made: a copy onto
    itself with `x-amz-metadata-directive: REPLACE` is the ordinary byte copy, and the
    metadata-only commit is recorded in §11 as a possible later optimization;
    `x-amz-tagging-directive: REPLACE` with a non-empty `x-amz-tagging` is 501 while
    COPY, and REPLACE with an empty tag set, proceed; each source condition that fails
    on its own is 412 PreconditionFailed.

Recorded with UploadPartCopy:

20. **UploadPartCopy.** Implemented as §6.6A and §11 describe. Choices you made: a reversed
    range is 400 InvalidArgument; "you can copy a range only if the source object is
    greater than 5 MB" is not enforced; the 5 MiB minimum part size is not enforced
    at upload (Complete enforces it, decision 27); the `x-amz-copy-source-server-side-encryption-customer-*`
    headers are 501 on both CopyObject and UploadPartCopy, where CopyObject had ignored
    them. The CLI's copy of an object above its multipart threshold reads the source's tags
    with GetObjectTagging first; decision 22 answers it, and the copy succeeds.

Recorded with the `x-amz-tagging` refusal:

21. **`x-amz-tagging` on PutObject and CreateMultipartUpload.** A non-empty value
    (anything left after stripping whitespace, not parsed) is 501 NotImplemented, decided
    in the request guard before any body byte is read, so nothing is written. An empty or
    blank value is accepted: the PutObject and CreateMultipartUpload pages say nothing
    about it (not found), and the CopyObject page gives `x-amz-tagging` the default "the
    empty value". The check does not apply to a PUT with `x-amz-copy-source`
    (CopyObject, decision 19) or to UploadPart and UploadPartCopy.

Recorded with GetObjectTagging:

22. **GetObjectTagging.** `GET /{bucket}/{key}?tagging` answers 200 with an empty `TagSet`
    (namespace `http://s3.amazonaws.com/doc/2006-03-01/` on `Tagging`, as the API
    reference's sample response). This is an answer from a store that holds no tags, not
    tagging support: no request can set a tag (decision 21, CopyObject §6.10 and
    PutObjectTagging being 501), so the true tag set of every object is empty. The
    handler reads no blob and writes nothing: the bucket is checked (NoSuchBucket), then
    the key's row (NoSuchKey). `versionId` is handled as for GetObject (`null` or 501);
    `x-amz-request-payer` is not evaluated, and no `x-amz-version-id` is sent, as for
    GetObject. `x-amz-expected-bucket-owner` is evaluated by the request guard like
    every object operation (decision 23). Range, conditional headers and
    `response-*` do not apply; `?tagging` with any other parameter (besides `x-id`) is
    501 in the guard. PutObjectTagging, DeleteObjectTagging, the bucket tagging
    operations and `HEAD ?tagging` stay 501. The GetObjectTagging page lists no errors
    and does not say how an empty `TagSet` is written (not found).

23. **Bucket owner condition.** The request guard compares `x-amz-expected-bucket-owner`,
    stripped of surrounding whitespace, with `S3_OWNER_ID` exactly (case sensitive, no
    format check; an empty or blank value is absent) on the 17 operations whose API
    reference pages define it, and `x-amz-source-expected-bucket-owner` on CopyObject and
    UploadPartCopy. A mismatch is 403 AccessDenied, last in the guard: after the bucket
    name check and every 501, before routing, so before the body is read and before any
    lookup (it beats NoSuchBucket, NoSuchKey and NoSuchUpload). The destination header is
    checked before the source header. ListBuckets and CreateBucket ignore both headers
    (S3 User Guide, "Verifying bucket ownership with bucket owner condition"). AWS
    documents the value as a 12 digit account ID; `S3_OWNER_ID` also serves as the
    canonical owner ID, which AWS keeps separate. The check reads nothing and writes
    nothing: no write path, blob or metadata, changed. Details and what AWS leaves
    unsaid: docs/compatibility.md, "Bucket owner condition".

Recorded with the feature header refusals:

24. **Feature headers Litmus does not have.** The request guard answers 501 NotImplemented,
    before any body byte is read, to a non-empty (after stripping whitespace) value of
    the SSE, SSE-KMS and bucket key headers, `x-amz-website-redirect-location` and the six
    `x-amz-object-lock-*` headers on PutObject, CreateMultipartUpload and CopyObject; of
    the three SSE-C headers on those, UploadPart and the destination side of
    UploadPartCopy; of `x-amz-storage-class` unless it is exactly `STANDARD`; and, on
    CreateBucket, of `x-amz-bucket-object-lock-enabled` unless it is `false` (any case)
    and `x-amz-bucket-namespace` unless it is `global`. Litmus stores plaintext, has no
    Object Lock or website hosting and one storage class, so honoring none of them would
    succeed without doing what the client asked. The refusals sit with the other 501s,
    so the owner check (decision 23) stays last. The guard reads headers only and
    nothing else changed: no write path, blob or metadata. The checksum headers are
    not decided here; the ACL, grant and object ownership headers are decision 25. Details and what AWS leaves
    unsaid: docs/compatibility.md, "Feature headers Litmus refuses". Decision 36 adds the SSE-C
    headers on ListParts to the refusal.

Recorded with the ACL header handling:

25. **ACLs are disabled (BucketOwnerEnforced).** Litmus answers the ACL, grant and object
    ownership request headers as a bucket with Object Ownership set to
    BucketOwnerEnforced, the default for a new bucket: the owner has full control and
    there are no other grants. On PutObject, CreateMultipartUpload and CopyObject,
    `x-amz-acl` absent, `private` or `bucket-owner-full-control` is accepted and any other
    value, and any non-blank `x-amz-grant-full-control`, `-read`, `-read-acp` or
    `-write-acp`, is 400 AccessControlListNotSupported. On CreateBucket, any non-blank
    `x-amz-object-ownership` other than `BucketOwnerEnforced` is 501 NotImplemented in the
    request guard, and `x-amz-acl` other than `private` or any non-blank grant header
    (those four and `x-amz-grant-write`) is 400 InvalidBucketAclWithObjectOwnership. A
    value that is empty after stripping whitespace is absent; values are compared
    exactly. The object write check runs in the handler right after the destination
    bucket lookup, before the body is read and, for CopyObject, before the copy source is
    parsed or opened (§6.2, §6.5, §6.10); after the guard's 501s and the 403 owner check
    (decision 23). The CreateBucket check runs after the name is validated and before the
    existence check. Headers are read and compared; nothing is stored, and no write path,
    blob or metadata changed. Authentication is a separate matter, out of scope as before:
    accepting an ACL header says nothing about network access. Details and what AWS
    leaves unsaid: docs/compatibility.md, "ACL and grant headers".

Recorded with the Complete size and checksum headers:

26. **`x-amz-mp-object-size` is verified, the checksum headers are refused.**
    CompleteMultipartUpload reads `x-amz-mp-object-size` (S3 API reference,
    "CompleteMultipartUpload": the expected total object size; a mismatch is `HTTP 400
    InvalidRequest`). A value that is empty after stripping whitespace is absent. The
    value must be a plain non-negative decimal integer, digits only after stripping
    whitespace; anything else is 400 InvalidArgument, which is Litmus's choice, since the
    handling of a malformed value is not found in the documentation. A valid value that
    differs from the total of the stored sizes of exactly the listed parts is 400
    InvalidRequest; a part uploaded and not listed does not count. The order of the errors
    extends §6.7 C1: NoSuchUpload, the size syntax, MalformedXML, InvalidPartOrder,
    InvalidPart, (decision 27 adds EntityTooSmall here,) the size mismatch, then the
    conditions. Every check is decided from the
    part rows before a blob is registered, so a refusal leaves the upload open with all
    its parts, and the write sequence (§6.7 C2 to C5) is unchanged. Separately, a
    non-blank `x-amz-checksum-crc32`, `-crc32c`, `-crc64nvme`, `-sha1`, `-sha256`,
    `-sha512`, `-md5`, `-xxhash64`, `-xxhash3`, `-xxhash128` or `x-amz-checksum-type` on
    Complete is 501 NotImplemented in the request guard, with the other 501 refusals:
    Litmus stores no part checksums and does not record an upload's algorithm or type, so
    it cannot verify them. AWS accepts some of them unvalidated when
    CreateMultipartUpload named no algorithm (S3 User Guide, "Checking object integrity
    for data uploads"), a case Litmus cannot tell apart for that reason. The per part
    `Checksum*` elements in the body stay accepted and unverified: the default AWS CLI
    sends them on every multipart upload, and verifying them needs the stored part
    checksums that the checksum design will add. That design must also settle the
    conflict between "CompletedPart" (400 InvalidPartOrder) and "Checking object
    integrity for data uploads" (500) for non-consecutive part numbers with checksums.
    Details and what AWS leaves unsaid: docs/compatibility.md, "Checksum and size headers
    on CompleteMultipartUpload".

Recorded with the minimum part size:

27. **CompleteMultipartUpload enforces the minimum part size.** Every listed part except
    the last listed one must be stored at `S3_MIN_PART_SIZE` bytes or more, otherwise
    400 EntityTooSmall (S3 API reference, "CompleteMultipartUpload", Special errors:
    "Each part must be at least 5 MB in size, except the last part", HTTP 400; S3 User
    Guide, "Amazon S3 multipart upload limits": 5 MiB to 5 GiB, no minimum for the last
    part; S3 API reference, "Error": EntityTooSmall, 400). The default is 5242880, the
    5 MiB of the User Guide. The setting is a non-negative decimal integer read when the
    server starts, and a value that is not one stops the start; `create_app` takes a
    `min_part_size` that overrides it, which is how tests choose theirs. Lowering it
    diverges from AWS and is meant for tests; 0 turns the check off. The size judged is
    the part row's stored size, and only the listed parts count: a part uploaded and not
    listed is not judged, and the last listed part is exempt whatever its number. An
    upload with one listed part has no minimum. UploadPart and UploadPartCopy do not
    check, as AWS raises the error at Complete ("UploadPart" lists no EntityTooSmall).
    The check sits in §6.7 C1 between InvalidPart and the `x-amz-mp-object-size`
    mismatch, so the order is NoSuchUpload, the size syntax, MalformedXML,
    InvalidPartOrder, InvalidPart, EntityTooSmall, the size mismatch, then the
    conditions. The AWS pages say nothing of the order of EntityTooSmall against the other errors (not
    found); this order is Litmus's choice. The check reads the part rows only, so it is decided before a blob
    is registered, a refusal leaves the upload open with all its parts, and the write
    sequence (§6.7 C2 to C5) is unchanged. Details: docs/compatibility.md, "The Complete
    body".

Recorded with schema version 3:

28. **Schema version 3, migrated in place from version 2.** The schema version becomes
    3. A version 2 database is migrated in place at startup; a version 3 database is
    left alone; version 1 is refused as decision 4 says, and a version above 3 is
    refused as written by a newer server. This revises decision 4 for version 2 only:
    decision 4's text stands, and it still governs version 1. The constructor of the
    metadata store only inspects an existing database (it creates a missing or empty
    one at version 3, its only write), and `migrate()` makes the change. The startup
    order is: refuse the old environment, build the store (the version refusals
    happen here, before the lock), refuse a missing blob directory, take the lock,
    check the store identity, `migrate()`, then create the tree and run the sweep. The
    migration runs after the identity check, so an identity refusal leaves the
    database unmigrated. The new columns are unused by this commit. Details: §20.

29. **PutObject stores and returns its checksum.** The `objects` checksum columns are
    written by PutObject, in the same transaction as the rest of the row, and returned
    by its 200 response as `x-amz-checksum-<algorithm>` and `x-amz-checksum-type:
    FULL_OBJECT`. The algorithm is the one the request claimed, by header or by
    aws-chunked trailer, else CRC64NVME (S3 API reference, PutObject). Choices: a
    trailer counts as a checksum provided with the request, which the reference does not
    say; a request with only `Content-MD5` stores CRC64NVME; the stored value is the
    base64 of the digest Litmus computed, so a claim in a non-canonical base64 form is
    answered canonically; `x-amz-checksum-algorithm` stays ignored on PutObject, since
    the reference does not list it as a PutObject request header. CompleteMultipartUpload
    and CopyObject write NULL in all three columns, replacing a previous object's.
    GetObject and HeadObject do not return the checksum yet. Details: §20.

30. **GetObject and HeadObject return the stored checksum when asked.** With
    `x-amz-checksum-mode: ENABLED`, a whole object 200 carries `x-amz-checksum-<algorithm>`
    and `x-amz-checksum-type`, read from the row (S3 API reference, GetObject and
    HeadObject: the mode must be enabled to retrieve the checksum; the valid value is
    ENABLED). Choices, where the reference says nothing: the value is matched case
    insensitively after stripping, and blank is absent; any other value is 400
    InvalidArgument; the mode is parsed after the bucket check and before the key lookup;
    a 206, a HEAD whose Range GET would answer 206, a 304, a 412 and a 416 carry no
    checksum, because botocore validates the header against the body it receives and the CLI
    downloads large objects as ranged GETs with the mode set; a false `If-Range` gives a
    whole 200 with the checksum; a row with NULL columns returns none; `partNumber` stays
    ignored, so it returns the whole object and its own checksum. Details: §20.

31. **Uploads name a checksum algorithm and type, and parts store a checksum and a time.**
    CreateMultipartUpload stores `x-amz-checksum-algorithm` and `x-amz-checksum-type` with
    the upload and echoes both (S3 API reference, CreateMultipartUpload: both are request
    and response headers). The allowed pairs are the S3 User Guide's, "Checking object
    integrity for data uploads in Amazon S3": CRC64NVME with FULL_OBJECT, CRC32 and CRC32C
    with either, SHA1 and SHA256 with COMPOSITE. An upload is named when it carried a
    non-blank algorithm; the effective algorithm E is the named one, or CRC64NVME. UploadPart
    stores E and the digest of the part's bytes, and the commit time, with the part row;
    UploadPartCopy hashes the copied bytes in E and does the same. UploadPart answers with
    the claim's header only (API_UploadPart: the response headers are "only present if the
    checksum was provided in the request"); CopyPartResult carries `Checksum<E>` only for a
    named upload (API_CopyPartResult: "present if the multipart upload request was created
    with" the algorithm) and the stored time as `LastModified`. Choices, where the reference
    says nothing: the default types (CRC64NVME FULL_OBJECT, the other four COMPOSITE);
    InvalidRequest for a disallowed pair, a type alone, and a claim in another algorithm
    than a named upload's (decided from the header or trailer name before the body is read);
    InvalidArgument for a value that is not an AWS name; echoing nothing, and storing NULL,
    for an unnamed upload; computing and storing E for a part sent without a claim; for an
    unnamed upload a claim in another computed algorithm is verified, CRC64NVME is stored,
    and only the claim is echoed; blank `x-amz-checksum-algorithm` and `-type` on Create are
    absent (claim headers are not changed); the five uncomputed algorithm names are 501 in
    the guard; request checksum headers stay ignored on UploadPartCopy, whose page defines
    none. CompleteMultipartUpload is not changed: it still accepts the per part
    `Checksum*` elements unverified. Details: §20.

Recorded with the Complete checksums:

32. **CompleteMultipartUpload verifies and stores checksums.** It reads the open upload with
    `get_open_upload`, checks the Part `Checksum*` elements and the checksum headers against
    the stored part checksums and the upload's algorithm and type, and commits the object's
    checksum, algorithm and type in the transaction that marks the upload completed. An
    upload is named when Create sent a non-blank algorithm; E is the named algorithm, else
    CRC64NVME. COMPOSITE: the digest, in the upload's algorithm, of the concatenated raw
    part digests in listed order, base64 encoded, plus "-N" for the N listed parts; that
    wire form is what is stored and returned. FULL_OBJECT: the CRC folded over the stored
    part values and sizes with awscrt's combine functions. An unnamed upload's object gets
    CRC64NVME FULL_OBJECT, parallel to PutObject, and the response says so. An upload that
    lists a part stored without a checksum (a part of schema version 2) gets no object
    checksum and no checksum response element. Sources: S3 API reference,
    "CompleteMultipartUpload" (the headers; a type that differs from the one at Create is
    `BadDigest`; the response carries `Checksum<ALG>` after `ETag` and `ChecksumType` last)
    and "CompletedPart" (the ten `Checksum*` names; with a checksum on each part, part
    numbers start at 1 and are consecutive, else 400 `InvalidPartOrder`); S3 User Guide,
    "Checking object integrity for data uploads" (COMPOSITE and FULL_OBJECT; without the
    algorithm at Create, MD5, SHA512 and XXHash on Complete are `InvalidRequest` and the
    other four are accepted and not validated). The "-N" suffix and the composite formula
    are not found in the documentation; they rest on the ceph/s3-tests vectors. Rules, where
    the reference says nothing: for a named upload a Part element in the upload's algorithm
    must equal the stored part value, or 400 BadDigest, and one in another algorithm is 400
    InvalidRequest, decided across all Parts before any value is compared; an unnamed upload
    compares only `ChecksumCRC64NVME`, the checksum it stores, accepts a part without a stored
    checksum, and accepts the other elements unverified. A header in the upload's algorithm
    is compared with the object checksum in wire form (BadDigest on a difference or a
    malformed value); a header in another algorithm, or more than one value header, is
    InvalidRequest; `x-amz-checksum-type` that is neither COMPOSITE nor FULL_OBJECT, in any
    letter case, is InvalidArgument, and a valid one that differs from the upload's is
    BadDigest. An unnamed upload validates `x-amz-checksum-crc64nvme` (it names the checksum
    Litmus stores), accepts crc32, crc32c, sha1 and sha256 unvalidated, refuses MD5, SHA512
    and the XXHash headers with InvalidRequest, and ignores a valid type. Only COMPOSITE
    uploads require the listed part numbers to be exactly 1 to N (400 InvalidPartOrder, as
    "CompletedPart" says, not the user guide's 500); the page does not mention FULL_OBJECT,
    so CRC32 and CRC32C FULL_OBJECT uploads and unnamed uploads still allow gaps. Blank
    headers and elements are absent. This supersedes these sentences of decision 26:
    "Separately, a non-blank `x-amz-checksum-crc32`, ... on Complete is 501 NotImplemented in
    the request guard, with the other 501 refusals: Litmus stores no part checksums and does
    not record an upload's algorithm or type, so it cannot verify them."; "AWS accepts some
    of them unvalidated when CreateMultipartUpload named no algorithm ..., a case Litmus
    cannot tell apart for that reason."; "The per part `Checksum*` elements in the body stay
    accepted and unverified: ..."; and "That design must also settle the conflict between
    "CompletedPart" (400 InvalidPartOrder) and "Checking object integrity for data uploads"
    (500) ...". It also supersedes the last sentence of decision 31, "CompleteMultipartUpload
    is not changed: it still accepts the per part `Checksum*` elements unverified." The
    request guard no longer answers 501 to any Complete checksum header. The write sequence
    (§6.7 C2 to C5) is unchanged, and so is `commit_completed_upload`. Details and the order
    of the errors: §20.

33. **CopyObject stores and returns a checksum.** The copy hashes the bytes the existing
    writer loop copies and commits the digest, the algorithm and the type FULL_OBJECT in the
    same transaction as the rest of the destination row, so an overwrite replaces all three
    and a kill leaves either the old row with its own columns or the new row with all
    three. The algorithm is the non-blank `x-amz-checksum-algorithm` header (any letter
    case, stored in upper case), else the source row's `checksum_algorithm`, else CRC64NVME.
    `CopyObjectResult` returns `ETag`, `LastModified`, `ChecksumType` and `Checksum<ALG>`, in
    the order of the response syntax. A name that is not one of the ten AWS names is 400
    InvalidArgument, decided after the directives and before the REPLACE headers and the
    source lookup; SHA512, MD5, XXHASH64, XXHASH3 and XXHASH128 are 501 in the request
    guard. Sources: S3 API reference, "CopyObject" (`x-amz-checksum-algorithm`: "Unrecognized
    or unsupported values will respond with the HTTP status code `400 Bad Request`"; the
    response syntax); S3 User Guide, "Copying, moving, and renaming objects" ("Amazon S3
    calculates a new checksum value after the object is copied. Amazon S3 doesn't directly
    copy the value of the checksum"); S3 User Guide, "Checking object integrity for data
    uploads in Amazon S3" ("With a copy command, the checksum of the object is a direct
    checksum of the full object"; without a named algorithm "S3 uses the existing
    algorithm", and CRC-64/NVME when the source has none). Choices: the copy recomputes, as
    the user guide says, where the API reference says the source's "checksum value will be
    copied to the new object by default"; a composite source (a value ending in "-N") gets
    a direct full object value in its own algorithm; the type is always FULL_OBJECT; a blank
    header is absent; the five uncomputed names are 501 and an unknown name is 400
    InvalidArgument, for which the reference gives a status and no code; a copy onto
    itself that changes only the algorithm, without `x-amz-metadata-directive: REPLACE`,
    stays 400 InvalidRequest (the pages read say nothing about it: not found), and with
    REPLACE recomputes in the named algorithm; `x-amz-checksum-<algorithm>` and
    `x-amz-sdk-checksum-algorithm` stay ignored, since the reference does not define them
    on CopyObject. This supersedes these sentences: of decision 29, "CompleteMultipartUpload
    and CopyObject write NULL in all three columns, replacing a previous object's." (for
    CopyObject; its Complete half is decision 32's; the rest of decision 29 stands, including
    that `x-amz-checksum-algorithm` stays ignored on PutObject); of §11, "the checksum
    headers of the request are ignored" under "Not honored, as for PutObject" (`x-amz-checksum-algorithm`
    is now read on CopyObject); and of §20, the sentences saying that CopyObject writes NULL
    in the `objects` columns, which are updated in place. The write
    sequence (§6.10) and the crash points are unchanged. Details: §20.

34. **An upload belongs to one bucket and key.** UploadPart, UploadPartCopy,
    CompleteMultipartUpload and AbortMultipartUpload find the upload by its ID, its bucket
    and its key together, compared exactly as stored (SQLite BINARY collation, so a key that
    differs only in case is another key; the key is decoded as an object key is). This
    fulfills decision 17 ("Kept in the design (§6.6, §6.7), implemented with the multipart
    work") and open item 9. The rule is one: the store lookups `get_upload`, `get_open_upload`,
    `upload_exists` and `abort_upload` require the bucket and key, `record_part` scopes by
    the bucket and key its `PartMeta` carries, and `commit_completed_upload` scopes its
    compare-and-set by the bucket and key of the `ObjectMeta`, so the check inside the
    BEGIN IMMEDIATE commit is scoped too. A bucket that does not exist is 404 NoSuchBucket;
    otherwise an upload that is not the upload of this bucket and key is 404 NoSuchUpload,
    the same answer as for an unknown ID. Both are decided before any body byte is read
    and before a blob is registered, so a refused request changes nothing and the upload
    stays usable through its own bucket and key. For UploadPartCopy they concern the
    destination; the source is handled as before. New order: UploadPart, UploadPart's
    partNumber integer check, then NoSuchBucket, then NoSuchUpload, then the part number
    range; Complete, NoSuchBucket immediately before step 2 of §20, which becomes the
    scoped NoSuchUpload; Abort, NoSuchBucket then NoSuchUpload; UploadPartCopy keeps its
    destination NoSuchBucket first. A request through a deleted bucket is therefore
    NoSuchBucket, where decision 7's abort of the bucket's uploads used to show as
    NoSuchUpload. It also fixes Complete through a missing bucket, which was 500
    InternalError (the object insert violated the bucket foreign key). Sources: S3 API
    reference, "UploadPart", "UploadPartCopy", "CompleteMultipartUpload" and
    "AbortMultipartUpload" say nothing about a request whose bucket or key differs from
    the upload's (not found); each documents NoSuchUpload (404), and "Error responses"
    lists NoSuchBucket (404). Choices: NoSuchUpload, not NoSuchBucket or NoSuchKey, for a
    mismatch, since it is the one documented error for an upload that does not match.
    No write sequence changes, so no crash point is added. Details: §6.6, §6.7, §20.
35. **A retried Complete on a completed upload is NoSuchUpload (closes j3).** A
    CompleteMultipartUpload for an upload that an earlier Complete finished is 404
    NoSuchUpload, and the retry changes nothing: the object keeps its bytes and ETag, and no
    upload, part or blob row and no file is added. Sources: S3 API reference, "Error
    responses" (`API_Error`) lists NoSuchUpload (404 Not Found) as "The specified multipart
    upload does not exist. The upload ID might be invalid, or the multipart upload might have
    been aborted or completed."; "CompleteMultipartUpload" lists the same NoSuchUpload (404
    Not Found) among its special errors and documents no replay of the first answer. Neither
    page documents a 200 for a repeated Complete. Litmus follows
    the documented answer and has not observed what S3 itself returns; the earlier claim
    that S3 answers a retry with 200 and the same ETag rested only on two ceph tests
    (`test_multipart_upload` and `test_multipart_upload_small`, each of which sends one
    extra Complete) and is withdrawn. No replay is implemented. j3 is
    closed as conformant: the strict xfail `test_gap_retried_complete_returns_same_etag`
    became the ordinary test `test_retried_complete_after_a_killed_commit_is_no_such_upload`,
    and `tests/conformance/test_complete_retry.py` pins the plain retry. No numbered decision
    recorded the 200 expectation or a rule for removing the j3 marker, so this decision
    supersedes these sentences outside the numbered decisions, each by name: in "Noted along
    the way", item 21, "re-completing a completed upload, which S3 accepts (j3)"; in §5,
    "re-completing a completed upload (j3) need"; in §9, the row "Complete vs Complete, same
    upload", "re-completing, j3, is separate work that the retained rows allow"; and in §17,
    "`test_multipart_upload` also re-completes a completed upload (j3)", whose claim that
    Litmus should accept the second Complete is withdrawn: the two ceph tests fail on
    purpose (compatibility.md, "Two ceph tests send a second Complete"). The retained rows
    stay as §5 describes them. No write sequence changes, so no crash point is added.
    Decision 10's conditions apply only to the two strict xfail tests named in item 7 of
    "Noted along the way", so they do not apply to the j3 test, which never XPASSed and whose
    assertions this decision rewrites.
36. **ListParts is implemented (GET `?uploadId`).** `GET /{bucket}/{key}?uploadId=` lists
    the parts of an open upload, as §21 describes. It reads and changes nothing, so no
    write sequence, crash point or schema change is added. Each choice below names the page
    that settles it, or says "not found" where the pages do not; every "not found" is a
    choice. The pages are S3 API reference "ListParts" (`API_ListParts`), "Part"
    (`API_Part`), "Initiator" (`API_Initiator`), "Owner" (`API_Owner`) and "Error
    responses" (`API_Error`).
    - *Request.* Query `uploadId`, `max-parts` and `part-number-marker`
      (`API_ListParts`, "Request Syntax"). Any other query parameter beside `uploadId` is
      501, as for GetObjectTagging, including `versionId`: ListParts takes none, so
      `versionId=null` is not accepted here either (not found; a choice). `x-id` is ignored,
      as everywhere in the guard, so an SDK's `x-id=ListParts` does not make the request 501
      (not found; a choice). `HEAD ?uploadId` stays 501: the page defines no HEAD.
    - *Headers.* `x-amz-expected-bucket-owner` is evaluated by the guard, so a wrong owner is
      403 AccessDenied (`API_ListParts`: "the request fails with the HTTP status code `403
      Forbidden` (access denied)"). `x-amz-request-payer` is ignored, as on every operation
      (compatibility.md), and no `x-amz-request-charged` is sent. The three SSE-C headers
      are 501 in the guard, as for UploadPart, decided at the same point (decision 24): a
      non-blank value of any of them on a GET with `uploadId` and no query parameter but
      `max-parts` and `part-number-marker`, ahead of the owner check. UploadPart and
      CompleteMultipartUpload differ here (Complete ignores them) and the pages do not say
      which S3 follows (not found; a choice). Litmus stores no upload encrypted with a key,
      so a request that supplies one asks for something it cannot do.
    - *max-parts.* "The `ListParts` request returns a maximum of 1,000 uploaded parts. The
      limit of 1,000 parts is also the default value." (`API_ListParts`). A larger value is
      capped at 1000 and `MaxParts` echoes the capped value; this is `_max_keys`' rule. 0 is
      accepted and gives no parts with `IsTruncated` false and no `NextPartNumberMarker`, as
      `max-keys=0` does; the page says nothing about 0 (not found). A negative value, a
      sign, a space, a fraction, an exponent, a non-ASCII digit or an empty value is 400
      InvalidArgument with `ArgumentName` and `ArgumentValue`, as for `max-keys`; the page
      names no error for them (not found), and `API_Error` lists InvalidArgument (400
      Bad Request). Leading zeros are read as the number.
    - *part-number-marker.* "Specifies the part after which listing should begin. Only
      parts with higher part numbers will be listed." (`API_ListParts`). Absent means 0
      (not found). A value that is not decimal digits is 400 InvalidArgument as above
      (not found). Any decimal integer is accepted: a marker above the highest part, or
      above 10000, lists no parts (not found). `PartNumberMarker` echoes the request's
      value, or 0, written without leading zeros. `max-parts` is read before
      `part-number-marker`, so with both bad the error names `max-parts` (a choice).
    - *Paging.* Parts are those above the marker, in ascending part number, at most
      `MaxParts` of them. `IsTruncated` is true when more remain ("A list can be truncated
      if the number of parts exceeds the limit returned in the MaxParts element"). When
      truncated, `NextPartNumberMarker` is the last returned part number ("this element
      specifies the last part in the list, as well as the value to use for the
      `part-number-marker` request parameter in a subsequent request"). The page does not
      say it is absent when the list is not truncated; Litmus omits it then (a choice).
    - *Elements.* In the order of the response syntax: `Bucket`, `Key`, `UploadId`,
      `PartNumberMarker`, `NextPartNumberMarker`, `MaxParts`, `IsTruncated`, the `Part`
      elements, `Initiator`, `Owner`, `StorageClass`, `ChecksumAlgorithm`, `ChecksumType`.
      Bucket, Key and UploadId are the request's. Within a `Part` the order is that of the
      page's sample response, `PartNumber`, `LastModified`, `ETag`, `Size`; the response
      syntax lists the elements alphabetically and the sample is the order S3 sends (a
      choice). The part's checksum element follows `Size` (not found; a choice).
    - *Part.* `ETag` is quoted as UploadPart returns it. `LastModified` is the stored
      time of the part's last commit, in whole seconds as ListObjects writes it
      (`YYYY-MM-DDTHH:MM:SS.000Z`); `API_Part`: "Date and time at which the part was
      uploaded." It is always written: an open part always has the time (§20 backfills it
      for parts that were open at the migration), so a NULL is not handled. The checksum
      element is `Checksum<algorithm>` in the part's stored algorithm and is omitted when
      the row has no checksum, which only parts uploaded before the version 3 migration
      lack (`API_Part`: "Required: No"). An upload that named no algorithm still shows
      `ChecksumCRC64NVME` on its parts: "This checksum is present if ... the object was
      uploaded without a checksum (and Amazon S3 added the default checksum,
      `CRC64NVME`, to the uploaded object)" (`API_Part`).
    - *Upload level.* `Initiator` and `Owner` both carry `S3_OWNER_ID` as `ID` and
      `S3_OWNER_DISPLAY_NAME` as `DisplayName`, as ListBuckets builds `Owner`: Litmus has
      one identity, and `API_ListParts` says that for an AWS account Initiator "provides the
      same information as the `Owner` element". `StorageClass` is `STANDARD`, the only class
      Litmus stores. `ChecksumAlgorithm` and `ChecksumType` are the upload's, written only
      for an upload that named an algorithm, as CreateMultipartUpload echoes them only
      then; the page does not say what an upload that named none returns (not found; a
      choice). No `x-amz-abort-date` or `x-amz-abort-rule-id` is sent: the page sends them
      only when a lifecycle rule matches, and Litmus has no lifecycle.
    - *Errors and their order.* The page lists no errors (not found). After the guard
      (501 for an unimplemented request or an extra parameter, then 403 AccessDenied for a
      wrong owner), the order is 400 InvalidArgument for a bad `max-parts` or
      `part-number-marker`, 404 NoSuchBucket, 404 NoSuchUpload (a choice). NoSuchUpload
      covers an unknown upload ID, an aborted or completed upload and an upload of another
      bucket or key (decision 34), and `API_Error` describes it: "The upload ID might be
      invalid, or the multipart upload might have been aborted or completed." A blank
      `uploadId` is an unknown ID, as for Complete and Abort, which read `?uploadId=` as an
      upload ID of "" and answer NoSuchUpload after the bucket check.
    - *One read.* The upload row and the page of parts are read in one read transaction
      (§21), so a racing Complete, Abort or UploadPart gives either the whole list of the
      state before it or 404 NoSuchUpload, never a mixture. This is a choice; the pages say
      nothing of concurrency.


### §20. Checksums and schema version 3

Schema version 3 adds the columns that later commits use to store checksums with
objects, uploads and parts. This section covers the migration and the columns. After
decision 33 every column is used. PutObject, CompleteMultipartUpload and CopyObject write the
`objects` columns, GetObject and HeadObject return them, and every read of an object row (`_object`) reads them,
so the store's data API is used only after `migrate()`. CreateMultipartUpload writes the two
`multipart_uploads` columns and UploadPart and UploadPartCopy write the three
`multipart_parts` columns; `_upload` and `get_parts` read them. CompleteMultipartUpload
reads them and verifies against them (decision 32).

**CompleteMultipartUpload (decision 32).** `_complete` reads the open upload with
`get_open_upload` where it read `upload_exists`, parses the checksum headers with
`complete_claims` (`server/api/checksum_headers.py`), and passes the upload, the claims and
the parsed Parts (each with its non-blank `Checksum*` elements) to
`complete_multipart_upload`. The computation and the checks are in
`server/multipart/checksums.py`: `object_checksum` builds the COMPOSITE or FULL_OBJECT value
from the listed parts' stored checksums and sizes, and returns None when any of them is NULL;
`verify_part_elements` and `verify_claims` decide the elements and the headers. The checksum,
algorithm and type go in the `ObjectMeta` that `commit_completed_upload` writes in the same
transaction as the completed state, after `complete-after-assemble` and before
`complete-after-commit`: a kill before the commit leaves the upload open with every part
checksum as it was, a kill after leaves the object row with its checksum beside the completed
upload and the listed part rows with their checksums and a NULL `blob_id`. The error order,
steps 1 to 14 all decided from the part rows and the request before a blob is registered, is:

1. the request guard's 501s (none is a Complete checksum header any more);
2. NoSuchUpload;
3. InvalidArgument for the syntax of `x-amz-mp-object-size`;
4. the shape of the checksum headers: InvalidRequest (several value headers, another
   algorithm on a named upload, MD5, SHA512 or an XXHash on an unnamed one), then
   InvalidArgument (type text);
5. MalformedXML;
6. InvalidPartOrder, the numbers not strictly ascending;
7. InvalidPart;
8. InvalidPartOrder, a COMPOSITE upload whose numbers are not exactly 1 to N;
9. EntityTooSmall;
10. InvalidRequest, a Part element in another algorithm (named uploads), over all Parts;
11. BadDigest, the first Part element in E that differs;
12. InvalidRequest, `x-amz-mp-object-size` does not match;
13. BadDigest, the type header (named uploads);
14. BadDigest, the checksum header value;
15. the assembly: NoSuchUpload if the upload is gone, InvalidPart from `PartsChanged`;
16. the commit's conditions, If-Match and If-None-Match.

Decision 34 puts NoSuchBucket immediately before step 2, scopes step 2 and the re-read in
step 15 to the request's bucket and key, and scopes the commit's compare-and-set the same
way.

A part uploaded again between the checksum read and the commit is InvalidPart (step 15) and
the upload stays open with the new part's own checksum. `commit_completed_upload` is not
changed.

**The columns.** Each is added after the table's version 2 columns. The algorithm
columns accept `CRC32`, `CRC32C`, `CRC64NVME`, `SHA1` and `SHA256` or NULL; the type
columns accept `FULL_OBJECT` and `COMPOSITE` or NULL.

- `multipart_uploads`: `checksum_algorithm`, `checksum_type`.
- `multipart_parts`: `checksum` (text), `checksum_algorithm`, `last_modified` (text).
- `objects`: `checksum` (text), `checksum_algorithm`, `checksum_type`.

One definition of these columns builds both the fresh `CREATE TABLE` statements and the
migration's `ALTER TABLE` statements, so a new database and a migrated one have the same
columns in the same order, the same CHECK constraints and the same indexes. A test
compares them with `PRAGMA table_xinfo`, the index list and the CHECK behavior.

**The migration.** `migrate()` runs, in one `BEGIN IMMEDIATE` transaction: the eight
`ALTER TABLE ... ADD COLUMN` statements, an `UPDATE` that backfills
`multipart_parts.last_modified`, and `UPDATE schema_version SET version = 3`. It returns
at once when the store knows the database is version 3, and re-reads the version inside
the transaction, so a second server that migrated first makes it a no-op. The last thing
inside the transaction is the crash point `migrate-before-commit`; the commit is followed
by `migrate-after-commit`. A failure of any statement, including a full disk, rolls
everything back: the database stays version 2 with no new column, and the next start
tries again. A kill at either crash point leaves version 2 or version 3, never a mix
(docs/crash-model.md).

**The order.** `migrate()` runs after the blob directory lock and the store identity
check (§7, decision 28), and before the tree is created and the sweep runs. Only the
server that holds the lock can migrate, and a database that is refused for its identity
or its blob directory is not changed. The version refusals come earlier still, from the
constructor, before the lock.

**The backfill.** A part of an open upload that still holds its bytes gets the
`created_at` of its blob row as `last_modified`: the registry records when the blob was
registered, which for a part is when its UploadPart began, and no earlier record of when
the part was written exists in version 2. It is the closest value the database has, and
it is exact to the registration, not to the commit. S3's ListParts reports a
`LastModified` per part, which Litmus does not answer yet; a later commit will use this
column for it. A part whose bytes Complete released
(`blob_id` NULL) has no blob row to read, so its `last_modified` stays NULL, as do all
the other new columns on every existing row. Decision 36 and §21 use `last_modified` for ListParts.

**PutObject (decision 29).** `_upsert_object` writes `checksum`, `checksum_algorithm` and
`checksum_type` on insert and in the `ON CONFLICT` update, so every overwrite replaces
them, with NULL when the new object has none. `UploadChecks` (server/checksums.py)
computes the stored digest while the body is written: with the claim's hasher when an
algorithm was claimed, else with a CRC64NVME hasher of its own, and `stored_checksum()`
returns the algorithm and the base64 of the digest only after the verify callback has
succeeded. `_put_object` puts them, with `FULL_OBJECT`, in the `ObjectMeta` that
`put_object_meta` commits, after the crash point `put-after-bytes` and before
`put-after-commit`; a kill before the commit leaves the old row, checksum included. The
response headers are named by `server/api/checksum_headers.py`. CopyObject
sets them from the bytes it copies (decision 33), and so does CompleteMultipartUpload
(decision 32).
`UploadPart` stores the checksum of its upload's effective algorithm with the part (decision
31); the DeleteObjects body check
(`verify_body`) computes only the claimed checksum and stores nothing.

**GetObject and HeadObject (decision 30).** `checksum_mode_enabled`
(`server/api/checksum_headers.py`) reads `x-amz-checksum-mode` once, in `_get_object` and
`_head_object` right after `_response_overrides` and before the key lookup, so the order of
errors is NoSuchBucket, InvalidArgument, NoSuchKey, then the conditions. `_checksum_headers`
turns the row's three columns into the response headers, or into nothing when `checksum` is
NULL. GET adds them only on its whole object 200: when `_resolve_range` returns a range (the
206 branch) it adds none, and when a false `If-Range` made the Range ignored the 200 carries
them. HEAD adds them unless `_resolve_range` returns a range; a 304 (`_not_modified`) and a
412 are built as before. The columns are read with the row, in the same query that gives the
size and the blob, so the checksum and the bytes served are those of one row (§8).

**CreateMultipartUpload (decision 31).** `_initiate` parses the two headers with
`create_checksum` (`server/api/checksum_headers.py`) right after `request_object_headers`
and before the upload ID is made, so a refusal creates no row. The guard
(`_check_create_checksum_algorithm`, `server/api/request_guard.py`) has already answered
501 for SHA512, MD5, XXHASH64, XXHASH3 and XXHASH128. The parser strips and upper-cases both
values; a blank value is absent; it applies the default type and the allowed pairs and
raises InvalidArgument for a name that is not an AWS one and InvalidRequest for a type
alone or a disallowed pair. `create_upload` takes `checksum_algorithm` and `checksum_type`
as keyword only arguments and writes them in the insert of the upload row, NULL in both for
an upload that names none; the response carries the two headers only for a named upload.
`UploadMeta` gains the two fields at its end, and `get_open_upload` returns the upload in one
read, or None unless it is open.

**UploadPart (decision 31).** `_upload_part` reads the open upload once with
`get_open_upload`, in the position `upload_exists` had (after the part number is parsed as an
integer, before its interval is checked), and computes E. `UploadChecks.from_headers`
(`server/checksums.py`) takes `store_as=E` and `named`: for a named upload a claim whose
algorithm, known from the `x-amz-checksum-<algorithm>` header or the `x-amz-trailer` name,
is not E is InvalidRequest, decided after the claim syntax checks, including the Content-MD5
decode, and before the body is read or a blob is registered. The stored algorithm is E; when
the claim is in another algorithm (only an unnamed upload can reach that) both digests are
computed, the claim is verified as before and E is stored. `claimed_checksum()` returns the
claim and its verified value for the response header. `PartMeta` gains `checksum`,
`checksum_algorithm` and `last_modified` at its end, each None by default. `record_part`
writes the three columns on insert and in the `ON CONFLICT` update, and stamps a
`last_modified` of None under the write lock, setting it on the passed object, as
`put_object_meta` does, so its return value is unchanged. The checksum and the time are
committed in the same transaction as the part row, after the crash point `part-after-bytes`
and before `part-after-commit`: a kill before the commit leaves the previous part row with its
own checksum and time, and a kill after leaves the new row with all three.

**UploadPartCopy (decision 31).** `upload_part_copy` reads the open upload with
`get_open_upload` where it read `upload_exists`, hashes each chunk of the existing copy loop
in E, and puts the digest, E and no time in the `PartMeta` that `record_part` commits (the
commit stamps the time). `CopyPartResult` takes `LastModified` from that stored time, in
whole seconds, instead of the time of the response, and carries `Checksum<E>` only when the
upload is named. The source row's own checksum is not used and need not exist. A kill at
`part-copy-after-bytes` leaves the old part row, checksum and time included, and one at
`part-copy-after-commit` the new row with all three.

**CopyObject (decision 33).** `copy_object` parses `x-amz-checksum-algorithm` once with
`copy_algorithm` (`server/api/checksum_headers.py`), after the two directives and before the
REPLACE headers and the source lookup, so a name that is not an AWS algorithm is 400
InvalidArgument even for a missing source; the request guard has already answered 501 to the
five uncomputed names (`_check_copy_object`). Once the source row is open the algorithm is
the named one, else `src.checksum_algorithm`, else CRC64NVME, and the hasher is fed each chunk
of the existing copy loop beside `writer.write`. The digest, the algorithm and `FULL_OBJECT`
go in the `ObjectMeta` that `put_object_meta` commits, after `copy-after-bytes` and before
`copy-after-commit`: a kill at the first leaves no new row, or, for an overwrite, the old row
with its own three columns, and a kill at the second leaves the new row with all three. The
source row's value is never copied, so a composite source gets a direct value, a source with
NULL columns (schema version 2) gets CRC64NVME, and a source overwritten while the copy reads
it gives the digest of the bytes copied, which agrees with the ETag and the body. The result
document is built from the committed `ObjectMeta`, so `LastModified` and the checksum are the
stored ones.

### §21. ListParts

ListParts (decision 36) is a read of one open upload and one page of its parts. It writes
no row, creates no file, registers no blob and passes no crash point, and the schema is
version 3 as §20 leaves it.

**The read.** `list_parts_page(upload_id=, bucket=, key=, part_number_marker=, max_parts=)`
on the metadata store takes all five as keyword arguments with no default. In one
`_snapshot()` read transaction it selects the `multipart_uploads` row with that upload ID,
bucket and key (the decision 34 scope, compared exactly as stored) and `state = 'open'`;
if there is none it returns None. Otherwise it selects the `multipart_parts` rows of the
upload whose `part_number` is greater than the marker, in part number order, with `LIMIT
max_parts + 1`. The extra row is not returned: it only tells that the page is truncated.
With `max_parts` 0 the second select is skipped and the page is empty and not truncated. A
WAL reader sees the committed state at its first read, so the upload row and the parts are
one state: a Complete, Abort or UploadPart that commits between the two selects is not
seen by either. Part rows carry the upload's bucket and key through a join, as
`get_parts` does.

**The handler.** `server/api/list_parts.py`, reached from the GET route when the query has
`uploadId`. The guard has already answered 501 for any query parameter other than
`uploadId`, `max-parts`, `part-number-marker` and `x-id`, and AccessDenied for a wrong
owner. The handler then:

1. reads `max-parts` (default and cap 1000, 0 allowed), then `part-number-marker`
   (absent is 0); a value that is not ASCII decimal digits is 400 InvalidArgument. A value
   of more than 18 digits is far above 1 to 10000: a `max-parts` that long is the cap and a
   marker that long is passed to the store as 10^18, so no integer is converted that
   SQLite or `int()` would refuse; the response echoes the marker's digits;
2. 404 NoSuchBucket if the bucket does not exist;
3. calls `list_parts_page`, and 404 NoSuchUpload if it returns None;
4. writes the document.

**The document.** Element order and rules are those of decision 36. `NextPartNumberMarker`
is written only when `IsTruncated` is true, and is the part number of the last `Part`
written. A `Part` is `PartNumber`, `LastModified`, `ETag`, `Size` and, when both stored
checksum columns are set, `Checksum<algorithm>`. `Initiator` and `Owner` are built from
settings. `ChecksumAlgorithm` and `ChecksumType` are written only when the upload row has
an algorithm.

**Tests.** `tests/conformance/test_list_parts.py` pins every element and its order, the
paging rules, the parameter values, the error order and that a read changes no row or file.
`tests/conformance/test_list_parts_boto3.py` pages with boto3's paginator. The Tier A
races are in `chaos/scenarios/test_races_list_parts.py`: ListParts against Complete,
Abort and an UploadPart that replaces part 1, each run before the read, between its two
selects and after it.
