# Publication Barrier

## The invariant

After returning `200 OK` on a `PUT /{bucket}/{key}` or `CompleteMultipartUpload`, a
subsequent `GET` will return the correct bytes — even if the process is killed
immediately after the `200` is sent.

This is not obvious. It requires a specific ordering of operations and a specific
set of fsync calls. This document formalizes that sequence.

---

## The durable write sequence (single-part PUT)

```
1. Stream body to a temp file in the same directory as the final path
2. Compute MD5 incrementally while streaming (no second read)
3. fsync(temp_file_fd)          — data is durable on disk
4. close(temp_file_fd)
5. rename(temp_path, final_path) — atomic at the POSIX layer
6. fsync(parent_dir_fd)         — directory entry is durable
7. Commit object metadata to SQLite (WAL + synchronous=FULL)
8. Return 200 OK
```

Steps 1–6 write the bytes. Step 7 makes the object *visible*. Step 8 tells the client.

### Why each step matters

**Step 1 — temp file in the same directory:** `rename` is only guaranteed atomic
when source and destination are on the same filesystem. Writing to the same directory
ensures this.

**Step 3 — fsync the file fd:** Without this, the OS may not have flushed the write
buffer to the storage device before the rename. A crash after rename but before flush
would produce a visible object with corrupt or zero bytes.

**Step 5 — rename instead of write-in-place:** If we wrote directly to the final path
and crashed mid-write, the object would be partially written. `rename` is atomic — the
final path either points to the old file or the new complete file, never to a partial one.

**Step 6 — fsync the parent directory:** On Linux, a crash after `rename` but before
the directory entry is flushed can leave the directory pointing to neither the old nor
the new file. `fsync`-ing the directory fd makes the rename durable. (Skipped on
Windows where directory fsync is not supported — see `docs/crash-model.md`.)

**Step 7 — metadata after bytes:** If the process is killed after step 6 but before
step 7, the file exists on disk but has no metadata row. The startup consistency sweep
will detect this as an orphaned file. The object is not visible to clients — a `GET`
returns `404`. This is the correct behavior: the `200` was never sent, so the client
will retry.

If the process is killed after step 7 but before step 8 (the `200`), the object is
visible and correct. The client may not have received the `200` and may retry — a
second `PUT` of the same key is handled correctly (idempotent overwrite).

---

## The durable write sequence (CompleteMultipartUpload)

```
1. Validate all listed parts exist with matching ETags
2. Concatenate part files into a temp file (same durable write sequence as above)
3. fsync(temp_file_fd)
4. close(temp_file_fd)
5. rename(temp_path, final_path)
6. fsync(parent_dir_fd)
7. Commit object metadata to SQLite
8. Mark upload as complete in SQLite  } single transaction
9. Delete part files
10. Return 200 OK
```

Steps 7 and 8 are committed in a single SQLite transaction. If the process is killed
between step 6 and step 7, the assembled file exists on disk but the object is not
visible (no metadata row). The startup sweep detects the incomplete upload.

If killed between steps 9 and 10, the part files may linger on disk. The startup sweep
detects and logs this as `MissingPart` issues. The assembled object is already visible
and correct — the cleanup is cosmetic.

---

## SQLite configuration

Every connection applies:

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;
```

`WAL` mode means writers don't block readers and a crash during a write leaves the
database in a consistent state (the in-progress transaction is simply lost).
`synchronous=FULL` means the WAL file is flushed to disk before each transaction
commits — this is the setting that provides process-kill durability.

---

## What this does NOT protect against

- **OS crash / kernel panic:** The fsync calls ensure data is ordered for the storage
  device's write queue, but a kernel panic can discard in-flight I/O. This would require
  `O_DIRECT` or battery-backed write caches, which are out of scope.
- **Storage hardware failure / bit rot:** Not addressed.
- **Concurrent writers:** Two simultaneous PUTs to the same key will result in one
  winning — the last `rename` wins at the filesystem level, and the last metadata commit
  wins at the SQLite level. The GET will return a consistent object (one of the two
  bodies, never a mix), but which one wins is not deterministic.