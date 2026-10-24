# Litmus

A server for a subset of the S3 REST API, written to test storage correctness rather
than to scale. Objects are files on the local filesystem; metadata is in SQLite. A
chaos suite kills the server at named steps of every write path and checks what is
left on disk, in the database and visible to clients after a restart.

[![CI](https://github.com/shubhamsarvankar/litmus/actions/workflows/ci.yml/badge.svg)](https://github.com/shubhamsarvankar/litmus/actions/workflows/ci.yml)
[![Chaos](https://github.com/shubhamsarvankar/litmus/actions/workflows/chaos.yml/badge.svg)](https://github.com/shubhamsarvankar/litmus/actions/workflows/chaos.yml)
![Python](https://img.shields.io/badge/python-3.12-blue)

---

## What is tested

- **Crash behavior.** Tier B tests start a real server, make it kill itself with
  SIGKILL at a named step of PutObject, DeleteObject, DeleteObjects, UploadPart,
  CompleteMultipartUpload, AbortMultipartUpload, DeleteBucket, the startup sweep or
  the first start, and check the state after a restart. Tier A tests inject faults in
  process (raised OSErrors, corrupted or missing files, deterministic races).
- **Write ordering.** Barrier order tests check that PutObject, UploadPart and
  CompleteMultipartUpload fsync the new file, rename it into place and fsync its
  directory before the commit that makes it visible.
- **Protocol conformance.** Conformance tests cover the implemented operations.
  Against ceph/s3-tests, 142 of 330 selected tests pass; every failure is explained in
  [`docs/compatibility.md`](docs/compatibility.md).

---

## API

The app registers 12 routes: 11 for S3 requests and `/health`. FastAPI's default
documentation routes are turned off, so a bucket named `docs`, `redoc` or
`openapi.json` behaves like any other bucket. The S3 routes dispatch on query
parameters as follows.

| Method | Path | Operation |
|---|---|---|
| GET | `/` | ListBuckets, with the configured `Owner` |
| PUT | `/{bucket}` | CreateBucket |
| HEAD | `/{bucket}` | HeadBucket |
| DELETE | `/{bucket}` | DeleteBucket. Refused while the bucket has objects; open multipart uploads are aborted. |
| GET | `/{bucket}` | ListObjects (V1). With `list-type=2`: ListObjectsV2. With `versions`: ListObjectVersions. Keys are listed in UTF-8 byte order; `prefix` is an exact, case sensitive prefix. V1 and V2 also apply `delimiter` and `max-keys`; V1 pages with `marker` and `NextMarker` and shows the configured `Owner` in each `Contents`; V2 pages with `continuation-token` or `start-after`, reports `KeyCount`, and shows `Owner` with `fetch-owner=true`. |
| POST | `/{bucket}?delete` | DeleteObjects |
| PUT | `/{bucket}/{key}` | PutObject. With `partNumber` and `uploadId`: UploadPart. |
| GET | `/{bucket}/{key}` | GetObject, with `Range`, `If-Match` and `versionId=null` |
| HEAD | `/{bucket}/{key}` | HeadObject |
| DELETE | `/{bucket}/{key}` | DeleteObject. With `uploadId`: AbortMultipartUpload. |
| POST | `/{bucket}/{key}?uploads` | CreateMultipartUpload |
| POST | `/{bucket}/{key}?uploadId` | CompleteMultipartUpload |
| GET | `/health` | Health check |

The table lists POST `/{bucket}/{key}` twice because one route serves both multipart
operations. Requests for S3 operations that are not implemented (ACLs, versioning
configuration, CopyObject, ListParts, ListMultipartUploads, POST Object and others)
get 501 NotImplemented before reaching a handler, and change nothing. Keys are stored
as the exact string the client sent.

---

## Storage

Each object version and each uploaded part is an immutable file `blobs/<xx>/<id>`
under a random ID; no bucket name or key is part of a path. SQLite maps keys to blob
IDs and records every blob file in a registry. Details:
[`docs/publication-barrier.md`](docs/publication-barrier.md).

## Crash model

Litmus is tested against process kills (SIGKILL) on POSIX. After a kill and restart:
an object that got 200 on PutObject or CompleteMultipartUpload is readable with the
right bytes and ETag; a write killed before its commit leaves the previous object, or
no object, visible; and a GET returns one whole version or an error, never a mix.
OS crash, power loss and hardware failure are out of scope; the fsyncs only matter
for those, and no test checks their effect. Details, including what the startup
sweep deletes and what it only reports: [`docs/crash-model.md`](docs/crash-model.md).

## Publication barrier

New bytes are durable before the commit that makes them visible. Every write goes to
a fresh file: temp file, fsync, rename, fsync of its directory, then one SQLite
commit. Replaced and deleted files are unlinked only after their commit. Before any
byte is written, the new blob ID is committed as pending, so the startup sweep can
find the leftovers of a write that died. Details:
[`docs/publication-barrier.md`](docs/publication-barrier.md).

---

## Running the server

```bash
uv sync
uv run uvicorn server.app:app --host 127.0.0.1 --port 8000
```

Configuration is by environment variable:

| Variable | Default | Meaning |
|---|---|---|
| `S3_BLOB_DIR` | `data/blobs` | Directory of the blob files |
| `S3_DB_PATH` | `data/litmus.db` | SQLite database |
| `S3_OWNER_ID` | `litmus` | ID in every `Owner` element; Litmus has one account and no authentication |
| `S3_OWNER_DISPLAY_NAME` | `litmus` | DisplayName in every `Owner` element |

The server refuses to start:

- if `S3_DATA_DIR` or `S3_PARTS_DIR` is set (both belong to the earlier storage
  layout);
- on a database with schema version 1 (earlier data is not migrated: use a new data
  directory);
- if the database and the blob directory do not belong together (each holds the same
  store ID, written at first start);
- if another server already uses the same blob directory.

Example requests:

```bash
curl -X PUT http://127.0.0.1:8000/my-bucket
curl -X PUT --data-binary @README.md http://127.0.0.1:8000/my-bucket/README.md
curl http://127.0.0.1:8000/my-bucket/README.md
```

Requests are not authenticated.

---

## Running the tests

```bash
uv run pytest                       # tests/ and chaos/scenarios/
uv run pytest tests/                # unit and conformance tests
uv run pytest chaos/scenarios/      # Tier A and Tier B chaos tests
```

The Tier B tests (`test_crash_points.py`, `test_crash_multipart.py`,
`test_store_identity.py`, `test_raw_paths.py`) start server processes and need
SIGKILL; they are skipped on Windows.

ceph/s3-tests (Linux or WSL2): start a server against an empty data directory, then
run `bash scripts/run_ceph_s3tests.sh`. See
[`docs/compatibility.md`](docs/compatibility.md).

---

## Repository layout

```
server/
  app.py               app factory and startup hook
  startup.py           startup checks: old variables, lock, store ID
  config.py            settings from environment variables
  consistency.py       startup sweep
  crash_points.py      named crash points for Tier B tests
  connection.py        closes the connection after an early error to an Expect: 100-continue request
  api/                 bucket, object and multipart routes; request_guard.py answers 501
  storage/blobs.py     blob files: IDs, temp files, fsync, rename, lock, store marker
  storage/lifecycle.py removing new blobs after a failed write, and replaced blobs after a commit
  metadata/sqlite.py   schema, blob registry and transactions
  multipart/           CompleteMultipartUpload and the composite ETag
  errors/s3errors.py   S3 error responses
  request_ids.py       x-amz-request-id and x-amz-id-2 on every response
chaos/
  framework/, faults/, fixtures/   fault injection and server process fixtures
  scenarios/           Tier A and Tier B tests
tests/
  conformance/         protocol tests
  unit/                unit tests
scripts/
  run_ceph_s3tests.sh  runs ceph/s3-tests against a running server
docs/
  crash-model.md, publication-barrier.md, compatibility.md
  design/storage-layout.md   design of the storage layout
```

---

## Not implemented

Authentication, ACLs and policies, versioning, CopyObject, listing parameters other
than `prefix`, user metadata, and conditional writes and deletes. Litmus runs as one
process on one machine. See [`docs/compatibility.md`](docs/compatibility.md).
