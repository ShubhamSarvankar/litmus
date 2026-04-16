# Crash Model

## What failure class is covered

Litmus guarantees storage invariants across **process kills (SIGKILL) only**.

This means: if the server process is killed at any point — including mid-write, mid-transaction, or immediately after sending a `200 OK` — the following invariants hold after restart:

1. Any object for which the client received `200 OK` on a `PUT` is retrievable via `GET` with the correct bytes and ETag.
2. Any multipart upload for which the client received `200 OK` on `CompleteMultipartUpload` is retrievable via `GET` with the correct bytes and composite ETag.
3. No partial or corrupt object is ever visible. A `GET` returns either the full correct object or `404 NoSuchKey` — never a truncated body, never a body that doesn't match the ETag.

## What is NOT covered

- **OS crash / kernel panic**: not covered. fsync guarantees that data reaches the OS page cache and is ordered for durability, but a kernel panic before the drive flushes is outside scope.
- **Power loss / storage hardware failure**: not covered. This would require `O_DIRECT`, battery-backed write caches, or equivalent — none of which are implemented here.
- **Bit rot / silent data corruption by the storage medium**: not covered.
- **Multi-node consistency**: not applicable — this is a single-process server.

These exclusions are intentional and documented. Litmus is a correctness project, not a production storage system.

## What "200 OK" guarantees

### On `PUT /{bucket}/{key}`

After the client receives `200 OK`:

- The object bytes are in a named file on disk (`{data_dir}/objects/{bucket}/{key}`).
- The file was written via the durable write sequence: temp file → `fsync(fd)` → `close(fd)` → `rename(temp, final)` → `fsync(parent_dir_fd)`. The `rename` is atomic at the POSIX layer; `fsync` on the parent directory ensures the directory entry is durable.
- The object metadata row is committed in SQLite with `journal_mode=WAL` and `synchronous=FULL`. WAL mode ensures that a crash during a write does not corrupt the database. `synchronous=FULL` ensures the WAL is flushed to disk before the transaction is considered committed.
- **Bytes are written before metadata is committed** (the publication barrier). If the process is killed after the file is written but before the metadata row is committed, the object is not visible — a subsequent `GET` returns `404`. The startup consistency sweep will detect the orphaned file and log a warning.

### On `CompleteMultipartUpload`

After the client receives `200 OK`:

- All parts have been assembled into the final object file via the same durable write sequence.
- The object metadata row is committed in a single SQLite transaction that also marks the upload as complete.
- If the process is killed during assembly, the transaction rolls back. The object is not visible. Parts remain on disk and in metadata as an incomplete upload; the startup sweep detects and logs this.

## Startup consistency sweep

On every startup, before accepting requests, the server runs a consistency sweep that:

1. Checks every object in metadata has a corresponding file on disk. Reports `OrphanedMetadata` if not.
2. Checks every recorded part in an incomplete upload has a corresponding part file on disk. Reports `MissingPart` if not.

The sweep **logs warnings** for each issue found. It does **not** auto-delete or auto-repair. It does **not** prevent the server from starting. Repair is a manual operator action.

## SQLite configuration

Every connection applies:

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
```

`WAL + synchronous=FULL` is the correct setting for process-kill durability. It is slower than `synchronous=NORMAL` but correct. This is a deliberate tradeoff documented here so it is not quietly changed later.