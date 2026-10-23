# S3 Compatibility

## How to read this document

Litmus is a correctness-focused implementation, not a feature-complete one.
This document states precisely what passes, what fails, and **why** each failure
is out of scope. Unexplained pass rates are not useful — this is the honest version.

Results are based on running `ceph/s3-tests` (`s3tests/functional/test_s3.py`)
against a local Litmus instance.

**Running the suite:** ceph/s3-tests requires Linux (it uses POSIX tooling and its own
virtualenv). On Windows, run it via WSL2 or let CI handle it — the GitHub Actions chaos
workflow runs the script on `ubuntu-latest`. To run manually on Linux:

```bash
# Terminal 1 — start the server
uv run uvicorn server.app:app --host 127.0.0.1 --port 8000

# Terminal 2 — run the suite
bash scripts/run_ceph_s3tests.sh
```

---

## Implemented API surface

| Method | Path | Status |
|--------|------|--------|
| PUT | `/{bucket}` | ✅ Implemented |
| DELETE | `/{bucket}` | ✅ Implemented |
| GET | `/` | ✅ Implemented (list all buckets) |
| PUT | `/{bucket}/{key}` | ✅ Implemented |
| GET | `/{bucket}/{key}` | ✅ Implemented (with Range, If-Match) |
| HEAD | `/{bucket}/{key}` | ✅ Implemented |
| DELETE | `/{bucket}/{key}` | ✅ Implemented |
| POST | `/{bucket}/{key}?uploads` | ✅ Implemented (initiate multipart) |
| PUT | `/{bucket}/{key}?partNumber&uploadId` | ✅ Implemented (upload part) |
| POST | `/{bucket}/{key}?uploadId` | ✅ Implemented (complete multipart) |
| DELETE | `/{bucket}/{key}?uploadId` | ✅ Implemented (abort multipart) |

---

## ceph/s3-tests results by category

> **Current: 103 of 330 selected tests pass.** 220 failed, 7 skipped, 0 errors;
> 508 deselected by the `-k` filter in `scripts/run_ceph_s3tests.sh`. The drop from 116
> is expected: see "Unsupported requests now return 501" below.
>
> - ceph/s3-tests pinned at `5b3987116eca0b4b8fa57d6b10f3db57bab196b6` (2026-10-12).
>   The script checks out exactly this commit; it never pulls.
> - s3-tests virtualenv: Python 3.12.3, packages pinned by `scripts/s3tests-constraints.txt`
>   (boto3/botocore 1.43.118, pytest 9.1.1).
> - Litmus run on Ubuntu 24.04 (WSL2, ext4), Python 3.12.15 via `uv sync --python 3.12`.
> - Per-test outcomes: [`docs/s3-tests-results.txt`](s3-tests-results.txt), regenerated
>   by every run of the script.
>
> **Why the numbers changed from 115 of 325.** The earlier figure was measured against
> whatever s3-tests `master` was at the time, because the script used to `git pull`.
> It reproduces exactly (325 selected, 504 deselected, 115 passed, 205 failed, 5 skipped)
> at s3-tests `fb8b73092bb1dd8db829f1205a9e52e73bf9a232` (2026-04-10). Between that commit
> and the pin, upstream added 9 tests to `test_s3.py`. The `-k` filter deselects 6 of them
> and selects 3: `test_multipart_upload_complete_without_create` (passes), and
> `test_list_objects_restore_status` / `test_list_object_versions_restore_status`
> (skipped: no `[s3 cloud]` config). The outcome of every one of the 325 tests that exist
> at both commits is unchanged. Litmus itself did not change. That gave 116 of 328.
>
> **Two tests restored to the denominator (328 → 330).** The filter used to deselect
> `test_copy_object_ifmatch_failed` and `test_multipart_upload_empty` by exact name.
> Nothing recorded why, both test in-scope behaviour, and both fail for real reasons,
> so they are now selected:
>
> - `test_copy_object_ifmatch_failed`: CopyObject with a non-matching
>   `x-amz-copy-source-if-match` must return 412 PreconditionFailed. Litmus does not
>   implement CopyObject (at the time it treated the request as a zero-byte PUT and
>   returned 200; it now returns 501).
> - `test_multipart_upload_empty`: CompleteMultipartUpload with no request body must
>   return 400 MalformedXML. Litmus fails to parse the empty body and returns 500.
>
> Neither hangs or crashes the server. No other outcome changed. That gave 116 of 330.
>
> **Unsupported requests now return 501 (116 → 103 of 330).** Requests for S3 operations
> Litmus does not implement used to fall through to a plain handler and could destroy
> data (`PUT ?acl` emptied the object, `DELETE ?cors` deleted the bucket, CopyObject
> overwrote the destination with zero bytes). They now return 501 NotImplemented and
> change nothing. 14 tests that passed only because of the old behavior now fail, and
> 1 now passes:
>
> - 12 `test_post_object_*` tests expect 400 for a malformed browser-form POST; they
>   passed because every bucket POST other than `?delete` returned a bare 400. POST
>   Object is not implemented, so they now get 501.
> - `test_bucket_create_special_key_names` calls `put_object_acl`, which used to be a
>   zero-byte PutObject returning 200; it now gets 501.
> - `test_object_copy_zero_size` passed because CopyObject was a zero-byte PUT and its
>   source happened to be empty; CopyObject now gets 501.
> - `test_upload_part_copy_percent_encoded_key` now passes: it expects UploadPartCopy
>   to fail and the target to be unchanged, and gets 501. It passes for the wrong
>   reason (the expected error is a missing source key), so a real UploadPartCopy
>   implementation must keep it passing by returning that error.

| Test category | Result | Notes |
|---|---|---|
| Bucket create / delete / list | **PASS** | Core bucket lifecycle |
| Object PUT / GET / DELETE / HEAD | **PASS** | Core object lifecycle |
| Multipart upload (initiate/upload/complete/abort) | **PARTIAL** | Lifecycle passes; ListMultipartUploads and GetPart not implemented |
| Range GET | **PARTIAL** | `bytes=start-end`, open-end, suffix pass; empty-object range edge case fails |
| `If-Match` precondition | **PASS** | Implemented |
| `If-None-Match` / `If-Modified-Since` / `If-Unmodified-Since` | **FAIL** | Not implemented |
| ETag correctness | **PASS** | Single-part MD5, multipart composite formula |
| ListObjects V1 / V2 (basic) | **PASS** | Basic listing works |
| ListObjects delimiter / maxkeys / marker / pagination | **FAIL** | Not implemented — returns full unfiltered list |
| Object copy (`PUT` with `x-amz-copy-source`) | **FAIL** | Not implemented |
| Object metadata (`x-amz-meta-*`) | **FAIL** | Not stored or returned |
| `x-amz-request-id` response header | **FAIL** | Not returned — some tests require it |
| Bucket naming validation | **FAIL** | No name validation implemented |
| Anonymous / unauthenticated access | **FAIL** | No auth — all requests accepted |
| POST object (browser form upload) | **FAIL** | Not implemented |
| 100-continue / aws-chunked encoding | **FAIL** | Not implemented |
| ACLs | **SKIP** | Auth / access control out of scope |
| Bucket / object policies | **SKIP** | Auth out of scope |
| Object versioning | **SKIP** | Out of scope |
| Server-side encryption | **SKIP** | Out of scope |
| Bucket website / lifecycle / CORS / replication | **SKIP** | Out of scope |
| Object tagging / notifications / object lock | **SKIP** | Out of scope |
| Presigned URLs (SigV4) | **SKIP** | No auth / SigV4 implemented |
| Ownership controls / public access block | **FAIL** | Not implemented |
| Path-style vs. host-style addressing | **PARTIAL** | Path-style only; host-style requires DNS |

---

## Known gaps and rationale

**No authentication.** Litmus accepts all requests without verifying credentials.
This is intentional — the project demonstrates storage correctness, not security.
Adding SigV4 would require significant machinery (canonical request signing, credential
scoping, clock skew handling) that obscures the storage correctness focus.

**No ListObjectsV2.** AWS CLI and boto3 default to `ListObjectsV2` (`GET /{bucket}?list-type=2`).
Litmus implements ListObjects V1 only. This means `aws s3 ls` will fail against a live
server unless `--no-sign-request` and explicit path-style are used with a boto3 client
configured for V1. Adding V2 is straightforward and is a known gap.

**No object copy.** `PUT /{bucket}/{key}` with `x-amz-copy-source` (CopyObject and
UploadPartCopy) is not implemented and returns 501 NotImplemented without changing
anything. (It used to be treated as a zero-byte PUT that overwrote the destination.)

**Keys with "." or ".." path segments are rejected (temporary deviation from S3).**
S3 keys are opaque strings, so `a/../b`, `../x` and `.` are valid keys on S3. Litmus
currently stores each object at a path derived from its key, where such segments would
resolve outside the bucket's directory or onto another key's file. Until the storage
layout stops deriving paths from keys (`docs/design/storage-layout.md`), any request
whose key has a `.` or `..` segment gets 400 InvalidArgument, a DeleteObjects entry for
such a key gets a per-key InvalidArgument error, and bucket names `.` and `..` get 400
InvalidBucketName. Keys that merely contain dots (`a.b`, `..hidden`, `x/.../y`) work.

**No versioning; every bucket is unversioned.** Bucket versioning configuration
requests return 501 NotImplemented. S3 gives every object in an unversioned bucket the
version ID `null`, and Litmus follows that:

- `GET /{bucket}?versions` (ListObjectVersions) lists every object as its single `null`
  version, marked latest. Its parameters have the same gaps as ListObjects: `max-keys`,
  `key-marker`, `version-id-marker`, `delimiter` and `encoding-type` are ignored, it never
  truncates, and the `Prefix` element is always empty. These are fixed together with the
  ListObjects parameters.
- `versionId=null` names the current object: GET, HEAD and DELETE treat it as if it were
  absent. In a DeleteObjects body, `<VersionId>null</VersionId>` deletes the object and
  the `Deleted` entry echoes the version ID.
- Any other version ID gets 501 NotImplemented on GET, HEAD and DELETE, and changes
  nothing; in a DeleteObjects body it gets a per-key `NotImplemented` error and the object
  is kept. The S3 API reference does not say which error S3 returns for a non-null
  version ID on an unversioned bucket, so Litmus does not guess.
- `versionId` on PUT or POST returns 501.

**No ListParts / ListMultipartUploads.** These are `GET` requests with query parameters
(`?partNumber` / `?uploads`) that are structurally similar to the implemented routes
but return XML listings. Not implemented; would be straightforward to add.

**No chunked transfer encoding.** `aws s3 cp` with large files uses
`Transfer-Encoding: chunked` with the `aws-chunked` extension. Litmus reads the
full request body via FastAPI's `await request.body()`, which handles standard chunked
encoding via HTTP/1.1 but not the aws-chunked trailer format.

**Conditional headers.** `If-Match` is implemented. `If-None-Match`, `If-Modified-Since`,
and `If-Unmodified-Since` are not — they are accepted without error but have no effect.

---

## What "PASS" means here

A passing ceph/s3-tests test means: a real boto3 client sent a real HTTP request to
Litmus and received a response that boto3 accepted as correct. This is protocol-level
validation, not documentation-level validation. The same standard ceph uses to validate
Ceph RGW, MinIO, and other production S3-compatible servers.

## What this is not

Litmus is not a production storage system. It is not multi-node, does not implement
ACLs or auth, and makes no claims about OS-crash or power-loss durability. See
`docs/crash-model.md` for the explicit durability contract.