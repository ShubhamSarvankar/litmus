# Storage layout redesign

Status: accepted, not implemented. The design follows the requirements below; the
decisions are recorded in §19.

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
    copied bytes, or a shared ID with reference counting). Both currently return 501
    via `x-amz-copy-source` in `server/api/request_guard.py`.

14. **(asked) Concurrent same-key PUTs.** What happens under the new design. Today they
    are safe only by accident: handlers do blocking file and SQLite I/O inside async
    functions, so the event loop serializes them. Moving I/O to a thread pool would
    reintroduce a race in the current layout.

15. **(asked) Conditional writes evaluated atomically with the metadata commit.**
    If-Match / If-None-Match on PutObject and CompleteMultipartUpload (and later
    CopyObject) must be checked in the same transaction as the metadata commit, after
    the new bytes are durable at a fresh path. A failed precondition must leave the
    old object's bytes and metadata untouched. They currently return 501
    (`CONDITIONAL_PUT_OBJECT_HEADERS` and `CONDITIONAL_COMPLETE_HEADERS` in
    `server/api/request_guard.py`). Conditional deletes (also 501 today) should follow
    the same model.

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
    (`docs/s3-tests-results.txt`). Known direct candidates:
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
    allow these.

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

Status: accepted, not implemented. Reviewed on 2026-10-23; the decisions are recorded
in §19 and the open questions marked in §18. Requirement numbers are written R1 to R25
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
  the object deletes them.
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

P1. Read-only checks: the bucket exists (else NoSuchBucket). If the request has
    `If-Match` / `If-None-Match`, an early check against the current row may refuse
    before any bytes are written (§10); it is an optimization, not the decision.

P2. `id = REGISTER('object', bucket, key)` **[C]**. `⚡put-after-register`

P3. `WRITE_BLOB(id, body)` **[Ff] [Fd]** (`⚡put-mid-write` inside). `⚡put-after-bytes`

P4. BEGIN IMMEDIATE

    a. old = SELECT blob_id, etag, upload_id FROM objects WHERE bucket = ? AND key = ?
    b. Evaluate If-Match / If-None-Match against old (§10). On failure: ROLLBACK,
       unlink the blob, RELEASE(id), answer 412 PreconditionFailed or 404 NoSuchKey.
    c. INSERT INTO objects (..., blob_id = id, size, etag, last_modified = now,
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
    unsupported conditions, the 1000-key limit) produce per-key errors as today.

X2. BEGIN IMMEDIATE; for each remaining entry, in request order: steps D2.a to D2.f,
    recording a `Deleted` or per-key `Error` entry. COMMIT **[C]** once for the whole
    request. `⚡delete-objects-after-commit`

X3. `GC(<every old blob_id>)` **[C]**.

X4. 200 with the DeleteResult.

One transaction per request instead of one per key: S3 promises no atomicity across
keys, but one transaction means one fsync instead of up to 1000, and a kill leaves
either none or all of the request's deletes committed. Either way the client got no
response and retries. A key listed twice is deleted once and reported `Deleted` twice,
as S3 reports missing keys. Decision 12.

| Kill at | State left behind | Sweep at restart | Clients see after restart |
|---|---|---|---|
| before X2's COMMIT | unchanged | nothing | every object still present |
| `delete-objects-after-commit` | rows gone; their blobs `garbage`, files present | unlink each, delete rows; one `InterruptedCleanup` per file | every deleted key 404; keys that got per-key errors untouched |

#### §6.5 CreateMultipartUpload

One transaction **[C]**: INSERT INTO multipart_uploads (state = 'open', plus
Content-Type, the system headers and user metadata from the request, R22). No files.
A foreign key failure means NoSuchBucket. A kill before the commit leaves nothing;
after it, an open upload with no parts.

#### §6.6 UploadPart, including re-uploading a part number

U1. Read-only checks: the upload is open and belongs to this bucket and key (else
    NoSuchUpload; the bucket and key check is new, see open question 9 in §18); the
    part number is between 1 and 10000.

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

#### §6.7 CompleteMultipartUpload

C1. Read-only: parse and validate the body (an empty body is MalformedXML, separate
    work). The upload is open and belongs to this bucket and key (else NoSuchUpload).
    Read the part rows and validate the requested list: ascending part numbers, every
    part recorded with a matching ETag (else InvalidPart / InvalidPartOrder). Remember
    each requested part's `blob_id` and size. Optional early conditional check (§10).

C2. `id = REGISTER('object', bucket, key, upload_id)` **[C]**. `⚡complete-after-register`

C3. `ASSEMBLE_BLOB(id, <the remembered part blob_ids>)` **[Ff] [Fd]**
    (`⚡complete-mid-assemble` inside). `⚡complete-after-assemble`

    If a part file is missing during assembly: the failure path runs (unlink,
    RELEASE), then the part row is read again. If it now names a different blob, the
    part was re-uploaded during assembly: InvalidPart. If the upload is gone or
    completed: NoSuchUpload. If it still names the same blob, the bytes are lost: 500
    (the sweep reports it as MissingPart).

C4. BEGIN IMMEDIATE

    a. SELECT state FROM multipart_uploads WHERE upload_id = ?; anything but 'open':
       ROLLBACK, unlink, RELEASE(id), NoSuchUpload. The existing object is untouched,
       because nothing outside the fresh blob was written (R6).
    b. Each requested part's current multipart_parts.blob_id must equal the blob that
       was assembled; otherwise a re-upload committed during assembly: ROLLBACK,
       unlink, RELEASE(id), InvalidPart.
    c. old = the current object row; evaluate If-Match / If-None-Match against it
       (§10). On failure: ROLLBACK, unlink, RELEASE(id), 412 or 404. The upload stays
       open.
    d. INSERT INTO objects (..., blob_id = id, size = sum of part sizes, etag =
       composite ETag, last_modified = now, headers and user_metadata copied from the
       upload row, upload_id = this upload) ON CONFLICT (bucket, key) DO UPDATE ...
    e. UPDATE blobs SET state = 'live' WHERE blob_id = id AND state = 'pending'
    f. If old: its blob becomes 'garbage'; if old.upload_id (an earlier completed
       upload): delete that upload's part and upload rows.
    g. Listed parts: their blobs become 'garbage'; UPDATE multipart_parts SET
       blob_id = NULL for them (ETag and size stay, R21).
    h. Unlisted parts: their blobs become 'garbage'; delete their part rows.
    i. UPDATE multipart_uploads SET state = 'completed'
       WHERE upload_id = ? AND state = 'open' (exactly one row, else ROLLBACK).

COMMIT **[C]**. This one transaction carries both the object metadata (d) and the
upload completion (i), as `commit_completed_upload` does today (R5).
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
   | `initializing`, id X, `blob_dir` B | missing or X, and realpath(`S3_BLOB_DIR`) is B | finish the first start (below, from step c) |
   | `initializing` | anything else | refuse |

   First start:

   a. Refuse unless `blobs/` holds nothing but `LOCK`, `tmp/` and empty fanout
      directories. Blob files without a marker belong to some other store.
   b. Generate `store_id = token_hex(16)`; INSERT the `store` row with
      `state = 'initializing'` and `blob_dir = realpath(S3_BLOB_DIR)` **[C]**. If the
      insert fails because a row appeared (a second server initializing the same
      database at the same moment), read the row again and go back to the table.
      `⚡store-init-after-commit`
   c. Write `tmp/STORE_ID.tmp` (removing a leftover first) **[Ff]**, rename it to
      `blobs/STORE_ID`, **[Fd]** `blobs/`.
   d. UPDATE the row to `state = 'ready'` **[C]**.

   The database is written first, so a kill at any point leaves a state the table
   recognizes. A kill after b and before d leaves an `initializing` row and possibly the
   marker, and the next start with the same blob directory finishes c and d. That
   `initializing` row is the one case where only one side holds the ID and the server
   still starts; refusing it would leave a store that can never start after a kill
   during its first start (decision 16). Recording `blob_dir` means that only the blob
   directory chosen at first start can adopt an `initializing` row.
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
     are lost: 500 InternalError (reported by the sweep as `OrphanedMetadata`). After
     three retries in a row (three commits replacing the key during one GET), answer
     503 SlowDown. Decision 14.
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
complete body with its ETag. `last_modified` is assigned inside the commit, so the
winner also has the latest Last-Modified, unless the wall clock steps backwards (open
question 5).

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
| Complete vs Complete, same upload | the second sees `completed` and gets NoSuchUpload (re-completing, j3, is separate work that the retained rows allow) |
| Complete vs Abort | the first commit wins; the other gets NoSuchUpload |
| PUT or CreateMultipartUpload vs DeleteBucket | if the bucket delete commits first, the other's commit fails on the foreign key: NoSuchBucket |

### §10. Conditional writes and deletes (R15)

Scope: not part of the implementation (decision 15). Conditional writes come later, in
an order you set. This section records how they fit, so that steps (a) and (b) leave
them needing no layout change. Until then the 501 responses stay as they are.

The authoritative check always runs inside the commit transaction (P4.b, C4.c), against
the row as it is at that moment, after the new bytes are durable at their fresh path.
A failure rolls back, releases the new blob and leaves the old object's bytes and row
untouched. For Complete the upload stays open, so the client can retry without
re-uploading parts. An optional early check before the body is read (P1, C1) only
saves writing bytes that are bound to be refused; it never decides success.

| Header | Current row exists | No current row |
|---|---|---|
| `If-None-Match: *` | 412 PreconditionFailed | proceed |
| `If-None-Match: <etag>` | 412 if the ETag matches, else proceed | proceed |
| `If-Match: *` | proceed | 404 NoSuchKey |
| `If-Match: <etag>` | proceed if the ETag matches, else 412 | 404 NoSuchKey |

ETags are compared without surrounding quotes. The 404 cases follow ceph's
`test_put_object_if_match` and `test_multipart_put_object_if_match`. Two concurrent
`If-None-Match: *` PUTs to a new key produce exactly one creator: the first commit
inserts the row, and the second's check sees it and gets 412. If a conflicting write
commits between the early check and the commit, the answer is 412, which is what
ceph's `test_atomic_dual_conditional_write_1mb` expects (S3 also documents 409
ConditionalRequestConflict for some conflicts; open question 6).

`x-amz-write-offset-bytes` (append) stays 501. It would fit this design as a new blob
holding the old bytes plus the appended ones, committed only if the current size still
equals the offset, but nothing asks for it yet.

Conditional deletes use the same model: `If-Match`, `x-amz-if-match-last-modified-time`
and `x-amz-if-match-size` are evaluated against the row at D2.c, a DeleteObjects
entry's `ETag`, `LastModifiedTime` and `Size` inside the batch transaction at X2, and
AbortMultipartUpload's `x-amz-if-match-initiated-time` at A2.b. `docs/compatibility.md`
defers real support until after the conditional-header work (category e); this design
does not block it and needs no further layout change for it.

### §11. CopyObject and UploadPartCopy (R13)

Scope: not part of the implementation (decision 15); both keep returning 501 until you
schedule them.

**Decided (decision 8): copy the bytes into a new ID.** CopyObject is PutObject whose source is
another object's blob:

1. Resolve the source row and open its blob, with GET's ENOENT retry (§8). The source
   conditions (`x-amz-copy-source-if-match` and the rest) are evaluated against the
   row that named the opened blob, because those are the bytes being copied.
2. REGISTER **[C]**; WRITE_BLOB from the source descriptor **[Ff] [Fd]**.
3. The P4 transaction for the destination key, including destination preconditions
   **[C]**; GC of the destination's old blob.

The source can be overwritten or deleted mid-copy without harm (the descriptor pins
it). Copying a key onto itself is either 400 InvalidRequest (no metadata change, as
ceph's `test_object_copy_to_itself` expects) or, with
`x-amz-metadata-directive: REPLACE`, a metadata-only transaction that keeps the same
`blob_id`, since the bytes do not change.

UploadPartCopy is UploadPart whose bytes come from a byte range of the source blob:
U2 to U6 with WRITE_BLOB reading `[first, last]` from the source descriptor. A missing
source must answer NoSuchKey before anything is registered, so
`test_upload_part_copy_percent_encoded_key` keeps passing for the right reason.

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
as version `null`, exactly as today; any other version ID stays 501.

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
byte order S3 uses. Prefix listing should replace `LIKE prefix || '%'` (wildcards,
ASCII case folding) with an indexed range: `key >= :prefix ORDER BY key`, reading
pages and stopping at the first key that does not start with the prefix. Markers and
continuation tokens become `key > :marker`. Delimiters, `max-keys` and paging of
`?versions` are listing work (category c, j9), unchanged by the layout except that
the index now supports them.

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
| R25 | not in this change. After steps (a) and (b) are implemented, you rewrite `docs/crash-model.md`, `docs/publication-barrier.md` and `docs/compatibility.md` before any feature work (decision 15). The README's crash and publication-barrier sections are not assigned yet (open question 12) |

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
| `delete-before-commit` | `test_delete_killed_before_commit_keeps_object` | **R3**: object and bytes intact (fails on today's code) |
| `delete-after-commit` | `test_delete_killed_after_commit` | 404; blob removed; no `OrphanedMetadata` |
| `delete-objects-after-commit` | `test_delete_objects_killed_after_commit` | all deleted keys 404, keys with per-key errors intact; every blob removed |
| `part-after-register` | `test_upload_part_killed_after_register` | part not recorded; upload open |
| `part-mid-write` | `test_upload_part_killed_mid_write` | partial temp removed |
| `part-after-bytes` | `test_reupload_part_killed_after_bytes_keeps_old_part` | re-upload: old part still recorded; Complete with the old ETag gives the old bytes |
| `part-after-commit` | `test_reupload_part_killed_after_commit` | new part recorded; old part blob removed with upload_id and part_number |
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
DeleteObject, DeleteObjects and Abort reuse it unchanged. The window between
WRITE_BLOB's fsync and its rename leaves the same state class as `*-mid-write` (a temp
file only) and needs no separate point.

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
directory and the database are unchanged: the same files and the same rows before and
after. That proves the sweep never ran.

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
which returns 501), and `test_copy_object_ifmatch_good` / `_failed` (CopyObject).

**Eased, not flipped:** `test_multipart_upload_resend_part` and
`test_multipart_upload` need Content-Type and user metadata carried from
CreateMultipartUpload to the object (the schema has the columns; the handlers are
category d / j1 work), and `test_multipart_upload` also re-completes a completed upload
(j3). The 6 non-versioned conditional delete tests (`test_delete_object_if_match`,
`test_delete_object_if_match_last_modified_time`, `test_delete_object_if_match_size`
and their `test_delete_objects_*` counterparts) fit §10 but wait for category e; the
`current` and `version` variants enable bucket versioning and stay failing.
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
6. **Open. R15: 412 vs 409.** S3 documents 409 ConditionalRequestConflict for some
   conflicting conditional writes; ceph's `test_atomic_dual_conditional_write_1mb`
   expects 412 for what looks like that case. §10 returns 412. This waits for the
   conditional-write work, which is outside steps (a) and (b).
7. **Resolved (decision 10). R16 vs the rule on test changes.** The
   `test_path_safety.py` tests are converted, not deleted (§16), in step (b).
8. **Resolved (decision 4). R12 "migration".** No migration; the server refuses to start
   on version-1 data or the old environment variables (§5).
9. **Open. Upload and request bucket/key mismatch.** UploadPart, Complete and Abort look
   up the upload by ID only. §6.6 and §6.7 add a check that the upload belongs to the
   request's bucket and key (NoSuchUpload otherwise). That this matches S3 is not
   confirmed, and it is not in the requirements. Confirm or drop before step (a)
   lands.
10. **Open. R23 and keys that are not valid UTF-8.** Keys are stored as SQLite TEXT,
    today and in this design. Whether a percent-encoded key with invalid UTF-8 can
    reach the handlers, and what S3 does with it, is not covered by any requirement.
    Listing order (BINARY collation) assumes valid UTF-8.
11. **Open (decision 16). The first-start exception to the store identity rule.** You
    asked to refuse startup when only one side has the store ID. Applied literally, a
    kill between the database write and the marker write at first start would leave a
    store that can never start again. §7 therefore accepts exactly one one-sided state,
    an `initializing` row adopted only by the blob directory recorded with it, and
    refuses everything else.
12. **Open. README sections in R25.** R25 also lists the README's crash and
    publication-barrier sections. Decision 15 assigns you `docs/crash-model.md`,
    `docs/publication-barrier.md` and `docs/compatibility.md`; the README is not
    assigned to anyone yet.

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
14. **GET retry bound.** Three ENOENT retries, then 503 SlowDown (§8).
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
    They are not part of this implementation.

Still needed from you:

16. **First-start exception (open question 11).** Accept §7's rule that an
    `initializing` store row, adopted only by the blob directory recorded with it, is
    the one one-sided state that may start. The alternative is to refuse it as well;
    an operator would then have to empty both sides after a kill during first start.
17. **Upload bucket/key check (open question 9).** Keep or drop the check that an upload
    belongs to the request's bucket and key.
18. **README (open question 12).** Who rewrites the README's crash and
    publication-barrier sections, and when.
