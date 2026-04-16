# S3 Compatibility

## How to read this document

Litmus is a correctness-focused implementation, not a feature-complete one.
This document states precisely what passes, what fails, and **why** each failure
is out of scope. Unexplained pass rates are not useful — this is the honest version.

Results are based on running `ceph/s3-tests` (`s3tests_boto3/functional/test_s3.py`)
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

> Results from running against Litmus on Ubuntu 24.04 (WSL2), Python 3.12.3.
> Run `scripts/run_ceph_s3tests.sh` to reproduce. 325 tests selected (504 deselected
> as out-of-scope via `-k` filter). **115 passed, 205 failed, 5 skipped, 0 errors.**

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

**No object copy.** `PUT /{bucket}/{key}` with `x-amz-copy-source` is not implemented.
The handler ignores the header and treats it as a zero-byte PUT.

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