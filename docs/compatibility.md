# S3 compatibility

Litmus implements a subset of the S3 REST API. This document records how it does
against ceph/s3-tests and why the failing tests fail.

## How the numbers are produced

- **Suite.** `s3tests/functional/test_s3.py` from ceph/s3-tests, pinned at commit
  `5b3987116eca0b4b8fa57d6b10f3db57bab196b6`. `scripts/run_ceph_s3tests.sh` checks out
  exactly that commit and never pulls.
- **Selection.** The script passes a fixed `-k` expression that deselects every test
  whose name contains one of: acl, policy, versioning, website, lifecycle, encryption,
  sse, kms, cors, replication, tagging, notification, analytics, inventory,
  accelerate, requestpayment, bucket_logging, public_access, object_lock, torrent,
  auth, presign, checksum (with capitalized variants where the script lists them).
  The expression is frozen. It selects 330 tests and deselects 508. Selection is by
  name only, so some selected tests still use ACLs, versioning or public access
  blocks; they fail on those calls.
- **Two restored tests.** `test_copy_object_ifmatch_failed` and
  `test_multipart_upload_empty` used to be deselected by exact name. Since commit
  `9b7cd65` they are selected; both fail (see the table).
- **Environment.** The s3-tests virtualenv uses Python 3.12.3 with packages pinned by
  `scripts/s3tests-constraints.txt` (boto3 and botocore 1.43.118, pytest 9.1.1).
  Litmus ran on Ubuntu 24.04 under WSL2 (ext4) with Python 3.12.15, against a fresh
  database and blob directory.
- **Per-test outcomes** are in [`docs/s3-tests-results.txt`](s3-tests-results.txt),
  which every run of the script regenerates. CI does not run the suite.

To run it on Linux, start a server against an empty data directory, then run the
script:

```bash
# Terminal 1
S3_OWNER_ID=testuser S3_OWNER_DISPLAY_NAME="Test User" \
    uv run uvicorn server.app:app --host 127.0.0.1 --port 8000

# Terminal 2
bash scripts/run_ceph_s3tests.sh
```

The script refuses to run if `S3_TEST_CRASH_POINT` is set, or if the server's owner
(the `Owner` element of ListBuckets) is not the `[s3 main]` user it writes into
`s3tests.conf`: ID `testuser`, display name `Test User`.

**Owner.** Litmus has no authentication and one account. Every `Owner` element it
returns carries one fixed identity from `S3_OWNER_ID` and `S3_OWNER_DISPLAY_NAME`
(default `litmus` for both), never anything taken from the request. Because there is
no auth, a test that compares an owner with the configured main user can pass only
because the two were configured to match, not because Litmus identified the caller.
As of the current run, no selected test makes that comparison and passes:
`test_bucket_listv2_fetchowner_notempty` checks only that `Owner` is present;
`test_bucket_list_return_data` and `test_list_multipart_upload_owner` compare owner
values but fail first on GetObjectAcl and ListMultipartUploads, which are 501; the
tests that compare the main user's ID with an ACL owner are deselected by `acl`.
Setting the owner changed no outcome (the run with it matches the run before it).

## Result

**142 passed, 181 failed, 7 skipped, of 330 selected tests.** The 7 skipped tests
(`test_bucket_get_location`, three restore tests, `test_read_through`,
`test_list_objects_restore_status` and `test_list_object_versions_restore_status`)
are skipped by s3-tests itself, not by the `-k` expression. Since the last step in the
table below, the ListObjects (V1) parameters moved 21 tests from failed to passed, and
the ListObjectsV2 parameters 21 more; the ListObjectVersions parameters changed no
outcome (every selected versions test is skipped), though every test's teardown now
pages through ListObjectVersions; nothing else changed.

### How the numbers moved from 115 of 325

The first published figure was 115 passed of 325 selected, 504 deselected. Each step
below is backed by `docs/s3-tests-results.txt` as committed at the named commit, except
the first, which rests on the reproduction recorded in commit `3e74a07`.

| Step | Selected | Deselected | Passed | Cause |
|---|---|---|---|---|
| Start | 325 | 504 | 115 | Measured against s3-tests `master` of the day, because the script used to `git pull`; reproduces at s3-tests `fb8b730`. |
| `3e74a07` pin | 328 | 510 | 116 | s3-tests pinned at `5b39871`. Upstream added 9 tests in between; the filter deselects 6 and selects 3: `test_multipart_upload_complete_without_create` (passes) and the two `restore_status` tests (skipped). The 325 tests present at both commits keep their outcome. |
| `9b7cd65` | 330 | 508 | 116 | `test_copy_object_ifmatch_failed` and `test_multipart_upload_empty` were deselected by exact name; now selected, both fail. |
| `86ce90a` | 330 | 508 | 103 | Unimplemented operations answer 501. 14 passes lost (12 `test_post_object_*`, `test_bucket_create_special_key_names`, `test_object_copy_zero_size`), 1 gained (`test_upload_part_copy_percent_encoded_key`). |
| `79f506d` | 330 | 508 | 98 | Conditional PutObject and Complete answer 501. 5 passes lost: `test_put_object_ifmatch_good`, `test_put_object_ifmatch_overwrite_existed_good`, `test_put_object_ifnonmatch_good`, `test_put_object_ifnonmatch_nonexisted_good`, `test_atomic_conditional_write_1mb`. |
| `8f9c4a9` | 330 | 508 | 99 | `Connection: close` on an early response to `Expect: 100-continue`. `test_100_continue_error_retry` passes. |
| Storage layout (`f94ee21` to `4dea326`) | 330 | 508 | 99 | No outcome changed; see below. |
| `b8e2625` | 330 | 508 | 100 | `x-amz-request-id` on every response. `test_object_requestid_matches_header_on_error` passes. |

So the s3-tests pin accounts for 3 more selected tests, 6 more deselected tests and
1 more pass. Selecting the two named tests accounts for the other 2 selected tests and
2 fewer deselected ones, with no change in passes. Litmus behavior changes account for a
net loss of 16 passes (13 lost to 501s for unimplemented operations, 5 to 501s for
conditional writes, 2 gained by the connection and request ID fixes). None of the change
is attributable to the storage layout work: the results file is the same from `8f9c4a9`
to `9767483`, and the next recorded run, at `b8e2625`, differs from the `8f9c4a9` run in
only the request ID test. No run was recorded at the end of the storage layout commits
alone, so the record shows that their net effect is zero, not that no test changed and
changed back in between.

### Incidental passes

A pass is listed here when the test would also pass against a server that does not
implement the behavior it exercises: because Litmus ignores a header or parameter and
the expected answer equals ignoring it, because the request under test never reaches
Litmus, because the expected answer is a 501 or another failure, or because nothing
is authenticated or owned. By that rule, **27 of the 142 passes are incidental**
(classified before the delimiter was implemented; the listing entries are reviewed
separately):

- Listing, the delimiter is ignored and the expected result is the same without it
  (8): `test_bucket_list_prefix_delimiter_delimiter_not_exist`,
  `test_bucket_list_prefix_delimiter_prefix_delimiter_not_exist`,
  `test_bucket_list_prefix_delimiter_prefix_not_exist`,
  `test_bucket_list_delimiter_prefix_ends_with_delimiter`, and the four
  `test_bucket_listv2_` tests with the same names.
- Conditional GET, the header is ignored and the expected answer is the plain object
  (3): `test_get_object_ifmodifiedsince_good` (a date in the past),
  `test_get_object_ifnonematch_failed` (an ETag that does not match),
  `test_get_object_ifunmodifiedsince_failed` (a date in the future).
- Copy (3): `test_object_copy_key_not_found` and `test_object_copy_bucket_not_found`
  get their 404 from the client's HeadObject on the missing source, and no copy
  request reaches Litmus; `test_upload_part_copy_percent_encoded_key` expects
  UploadPartCopy to fail, and it gets 501.
- Bucket naming, names are not validated and the test only checks that a valid name
  is accepted (9): `test_bucket_create_naming_dns_long`,
  `test_bucket_create_naming_good_contains_hyphen`,
  `test_bucket_create_naming_good_contains_period`,
  `test_bucket_create_naming_good_long_60` to `_63` (4 tests),
  `test_bucket_create_naming_good_starts_alpha`,
  `test_bucket_create_naming_good_starts_digit`.
- Ownership and authentication (3): `test_bucket_create_exists_nonowner` expects 409
  BucketAlreadyExists for a second user, which Litmus answers to every user;
  `test_object_anon_put_write_access` and `test_object_raw_get` set ACLs through the
  `x-amz-acl` header, which Litmus ignores, and their anonymous requests succeed
  because nothing is authenticated.
- User metadata (1): `test_object_metadata_replaced_on_put` expects no metadata after
  a second PUT without any, and Litmus never stores metadata.

Borderline, not counted above (11):

- An empty or absent listing parameter, which S3 also treats as a no-op, so the
  expected result equals ignoring it (8): `test_bucket_list_delimiter_empty`,
  `test_bucket_list_delimiter_none`, `test_bucket_list_maxkeys_none` (Litmus always
  reports `MaxKeys` 1000, which is S3's default), their three `test_bucket_listv2_`
  counterparts, `test_bucket_listv2_fetchowner_defaultempty` and
  `test_bucket_listv2_fetchowner_empty` (S3 omits the owner unless `FetchOwner` is
  true, and Litmus never returns it).
- The ACLs are ignored and the requests are anonymous, but the asserted 404 error
  codes are implemented (3): `test_object_raw_get_bucket_gone`,
  `test_object_raw_get_object_gone`, `test_object_delete_key_bucket_gone`.

## Results by category

Counts are grouped by test name. "Why the rest fail" gives the cause seen in the test
output; "501" means Litmus answers 501 NotImplemented for that request.

| Category | Selected | Passed | Failed | Skipped | What passes; why the rest fail |
|---|---|---|---|---|---|
| Bucket operations | 17 | 9 | 7 | 1 | Create, delete, head, list buckets, deleting a non-empty bucket refused, missing-bucket errors. Fail: re-creating a bucket you own expects success or BucketAlreadyOwnedByYou, and Litmus answers 409 BucketAlreadyExists (2 tests). That answer is deliberate: telling the owner apart needs authentication, which is out of scope; RGW-specific usage and statistics headers (3); ListBuckets ignores MaxBuckets and ContinuationToken (1); one test calls PutObjectAcl, which is 501 (1). |
| Bucket naming | 18 | 9 | 9 | 0 | Valid names are accepted. Fail: Litmus does not validate bucket names apart from rejecting `.` and `..`, so invalid names are accepted. |
| Object operations | 28 | 25 | 3 | 0 | PutObject, GetObject, HeadObject, DeleteObject, ETags, overwrites, zero-byte objects, standard chunked transfer encoding, the atomic read and write tests, and the request ID in an error body matching the `x-amz-request-id` header. Fail: `Content-Encoding: aws-chunked` is not handled; `response-*` query parameters are ignored; a key that is not valid UTF-8 gets 404 instead of 400. |
| User metadata and stored headers | 6 | 0 | 6 | 0 | Fail: `x-amz-meta-*`, `Cache-Control` and `Expires` are not stored or returned. |
| Conditional GET | 8 | 5 | 3 | 0 | `If-Match` is evaluated. Fail: `If-None-Match`, `If-Modified-Since` and `If-Unmodified-Since` are ignored; the tests that pass with them are the ones whose expected answer is the same as ignoring them. |
| Range GET | 6 | 4 | 2 | 0 | Single, open-ended and suffix ranges. Fail: an unsatisfiable range gets 416 without the `InvalidRange` error body. |
| ListObjects (V1) | 41 | 38 | 3 | 0 | `prefix`, `delimiter` and common prefixes, `max-keys` (default, cap, 0, InvalidArgument), `marker`, `NextMarker`, truncation. Fail: `encoding-type` is ignored (1); `allow-unordered`, an RGW extension, is not implemented (1); one test calls GetObjectAcl, which is 501 (1). |
| ListObjectsV2 | 39 | 37 | 2 | 0 | `prefix`, `delimiter` and common prefixes, `max-keys`, `continuation-token`, `start-after`, `KeyCount`, `fetch-owner`, truncation. Fail: `encoding-type` is ignored (1); `allow-unordered`, an RGW extension, is not implemented (1). |
| DeleteObjects | 4 | 2 | 2 | 0 | Bulk delete. Fail: the 1000-key limit is not enforced. |
| Multipart upload | 14 | 8 | 6 | 0 | Create, upload part, complete, abort, wrong ETag and missing part errors, overwriting an existing object. Fail: re-completing a completed upload gets NoSuchUpload, where S3 answers 200 with the same ETag (2); Content-Type given to CreateMultipartUpload is not applied (1); the minimum part size is not enforced (1); a Complete that lists a part number twice gets InvalidPart (1); a Complete with an empty body gets 500 instead of 400 MalformedXML (1). |
| GetObject `partNumber` | 3 | 0 | 3 | 0 | Fail: `partNumber` is ignored and the whole object is returned. |
| ListMultipartUploads | 2 | 0 | 2 | 0 | Fail: 501 (one test fails earlier on PutBucketAcl, also 501). |
| GetObjectAttributes | 5 | 0 | 5 | 0 | Fail: 501 (one test fails earlier on PutBucketVersioning, also 501). |
| `Expect: 100-continue` | 2 | 1 | 1 | 0 | An early error response closes the connection. Fail: the other test expects 403 from a bucket protected by an ACL, and Litmus has no ACLs. |
| Conditional PutObject and Complete | 17 | 0 | 17 | 0 | Fail: `If-Match` and `If-None-Match` on PutObject and CompleteMultipartUpload are 501; several of these tests also enable versioning, which is 501. |
| Conditional delete | 18 | 0 | 18 | 0 | Fail: conditional headers on DeleteObject and conditional entries in DeleteObjects are 501; 12 of these tests enable versioning first, which is 501. |
| CopyObject and UploadPartCopy | 27 | 3 | 24 | 0 | Fail: 501, either for CopyObject and UploadPartCopy themselves or for an ACL or versioning call the test makes first. Of the 3 that pass, 2 expect 404 for a missing source and get it from the client's HeadObject before any copy request is sent; the third expects UploadPartCopy to fail and gets 501. |
| Versioning, restore and read-through | 11 | 0 | 5 | 6 | Fail: PutBucketVersioning is 501, and DeleteObject sends no `x-amz-delete-marker` header. Skipped by s3-tests: 6. |
| Anonymous and ACL-based access | 19 | 1 | 18 | 0 | The one pass, `test_object_anon_put_write_access`, is incidental: it sets the bucket ACL through the `x-amz-acl` header, which Litmus ignores, and its anonymous PUT succeeds because nothing is authenticated. Fail: requests are not authenticated, so anonymous requests are accepted, and ACL operations are 501. |
| POST Object (browser form upload) | 27 | 0 | 27 | 0 | Fail: 501. |
| Presigned URLs (`X-Amz-Expires`) | 5 | 0 | 5 | 0 | Fail: signatures and expiry are not checked, so expired presigned requests are served (3). The other 2 first send `OPTIONS` to the presigned URL and expect 400; `OPTIONS` is S3's CORS preflight, which Litmus does not implement, so it answers 501 NotImplemented (2). |
| Ownership controls and public access block | 13 | 0 | 13 | 0 | Fail: 501. |
| **Total** | **330** | **142** | **181** | **7** | |

## Known gaps

**No authentication.** Litmus does not check credentials or signatures, and no
request is rejected for missing or invalid credentials.

**Unimplemented operations answer 501.** Requests for S3 operations Litmus does not
implement get 501 NotImplemented and change nothing. That includes ACLs, policies,
versioning configuration, tagging, CORS (including `OPTIONS`, the CORS preflight),
lifecycle, POST Object, CopyObject (any request with `x-amz-copy-source`),
ListParts, ListMultipartUploads and GetObjectAttributes.

**Other methods and paths.** Every error is an S3 XML error. A method that no route
accepts for a path gets 405 MethodNotAllowed, with an `Allow` header listing the
methods that path supports. A path that no route matches, such as one with an empty
bucket name (`//x`), gets 400 InvalidURI. An unexpected server error gets 500
InternalError. One case is outside Litmus: with uvicorn's default HTTP parser
(httptools), a method name the parser does not know (for example `BREW`) is rejected
with a plain-text 400 before the request reaches the application. With
`--http h11`, such a request reaches Litmus and gets 405 MethodNotAllowed.

**Listing parameters.** In all three listings the prefix is an exact, case sensitive
prefix of the key (`_` and `%` are ordinary characters), read as an indexed key range,
and keys are listed in UTF-8 byte order. ListObjects (V1) applies `delimiter`,
`max-keys` and `marker` as S3 does: common prefixes count toward `max-keys`, a common
prefix is returned only if it sorts after the marker, and `NextMarker` appears only
when a delimiter was given and the page is truncated. ListObjectsV2 applies the same
`delimiter` and `max-keys` rules, with `start-after` in place of `marker`, and pages
with `continuation-token`: an opaque token that records where the next page starts,
including the restart after a common prefix, and round trips any key. A malformed token
is InvalidArgument. `KeyCount` counts keys and common prefixes, and `Owner` appears
only with `fetch-owner=true`. ListObjectVersions applies `prefix`, `delimiter` and
`max-keys` the same way and pages with `key-marker` and `version-id-marker`: a page
starts after the key-marker (with V1's rule for a key-marker equal to or inside a
common prefix), and a truncated page returns `NextKeyMarker`, the last key or common
prefix returned, and `NextVersionIdMarker`, the last version returned. Every bucket is
unversioned, so each key appears once, as version `null`, latest, with the configured
`Owner`; `KeyMarker` and `VersionIdMarker` are always echoed. All three listings ignore
`encoding-type`.

**Listing choices AWS does not document.** Where the AWS API reference is silent,
Litmus does the following; each choice is covered by a conformance test.

- `MaxKeys` in the response echoes the value applied, so a request for more than 1000
  shows 1000. (ceph/s3-tests checks only the default, 1000.)
- `max-keys` is parsed as ASCII digits only. An empty value, a sign (`-1`, `+1`),
  whitespace, a decimal point, hex or non-ASCII digits are InvalidArgument; any
  number of digits is accepted and capped at 1000.
- With both `continuation-token` and `start-after`, the token decides where the page
  starts and `start-after` is only echoed. The ListObjectsV2 reference says that
  StartAfter is echoed when sent and that the token continues the listing, but not
  which wins. `test_bucket_listv2_both_continuationtoken_startafter` passes under
  either rule, because its token comes from a request with the same `start-after`.
- An empty `continuation-token` is no token (and is echoed empty), as
  `test_bucket_listv2_continuationtoken_empty` expects.
- In ListObjectVersions, `NextKeyMarker` is the last key returned, as in the
  reference's own `max-keys=3` example and as boto3's paginator and ceph's teardown
  expect; the reference's prose ("the first key not returned") disagrees with that
  example.
- A ListObjectVersions page that ends on a common prefix has `NextKeyMarker` (the
  prefix) and no `NextVersionIdMarker`, since it returned no version after it. boto3's
  paginator omits a missing marker from the next request.
- `version-id-marker` without `key-marker` is InvalidArgument. A `version-id-marker`
  other than `null` names a version that cannot exist in an unversioned bucket, and
  gets 501, as other version IDs do. An empty one is no marker.
- `fetch-owner` is true when its value is `true` in any letter case, and false
  otherwise.

**Characters XML 1.0 does not allow.** XML 1.0 allows tab, LF, CR, U+0020 to U+D7FF,
U+E000 to U+FFFD and U+10000 to U+10FFFF. Litmus accepts keys with the other
characters, U+0000 to U+0008, U+000B, U+000C, U+000E to U+001F, U+FFFE and U+FFFF, and
writes request values into XML responses as they are, so any response that echoes such
a value is not well-formed XML. Checked against a running server with U+0000, U+0001,
U+0008, U+000B, U+000C, U+001F, U+FFFE and U+FFFF in a key or parameter, these
responses do not parse:

- ListObjects, ListObjectsV2 and ListObjectVersions with such a key in the bucket
  (`Key`), and ListObjects with such a `prefix`, `marker` or `delimiter` (echoed);
- InitiateMultipartUpload and CompleteMultipartUpload results (`Key`);
- NoSuchKey errors from GetObject (`Key`), NoSuchBucket errors (`BucketName`), and
  the InvalidArgument error for `max-keys` (`ArgumentValue`).

A DeleteObjects request cannot carry such a key: its body is XML, and with the
character written as a character reference (`&#x1;`) Litmus answers 500, because the
body fails to parse. Two related cases with allowed characters: a key with CR is
stored, but XML parsers turn a CR in the response into LF, so listings show the key
with LF instead, and a DeleteObjects request with a literal CR deletes the LF form
(answering Deleted) and leaves the key with CR in place. A key with LF cannot be
stored at all: no route matches the path, and PutObject gets 400 InvalidURI.

**Keys are stored literally.** A key is the exact string the client sent, including
keys with `.` or `..` segments. Clients that resolve dot segments before sending
(httpx does) must percent-encode them. Bucket names `.` and `..` are rejected with
InvalidBucketName.

**Version IDs.** Every bucket is unversioned. `GET /{bucket}?versions` lists each
object once, as version `null`, and `version-id-marker` accepts only `null`. GET, HEAD and DELETE accept `versionId=null` as the current
object, and a DeleteObjects entry with `<VersionId>null</VersionId>` deletes the
object. Any other version ID gets 501 (or a per-key NotImplemented error in
DeleteObjects), and `versionId` on PUT or POST gets 501.

**Chunk-framed bodies are refused.** PutObject and UploadPart with a chunk-framed
(aws-chunked) body get 501 NotImplemented before any byte is written or any blob is
registered, so no object, part or pending blob is left behind. Litmus identifies such a
body the way S3 documents it: `x-amz-content-sha256` starting with `STREAMING-` (for
example `STREAMING-UNSIGNED-PAYLOAD-TRAILER`), or an `x-amz-decoded-content-length`
header. `Content-Encoding: aws-chunked` alone is not a marker, since S3 does not
require it: such a request is stored as an ordinary body. botocore and boto3 1.36.0+
and AWS CLI v2 2.23.0+ send chunk-framed bodies with a trailing checksum for
PutObject and UploadPart by default when the endpoint URL is `https://`, including a
custom endpoint (and so behind a TLS-terminating proxy); against a plain `http://`
endpoint they send an ordinary body with an `x-amz-checksum-*` header. Before this
check, Litmus answered 200 and stored the chunk sizes and trailer as object bytes.
Clients can avoid framing by using an `http://` endpoint, or by setting
`request_checksum_calculation = when_required` (PutObject and UploadPart do not
require a checksum, so none is sent unless the caller sets `ChecksumAlgorithm`).

**Checksums are not verified.** `x-amz-checksum-*` headers (and
`x-amz-sdk-checksum-algorithm`) on PutObject and UploadPart are accepted but never
checked, so a request whose body does not match its checksum still gets 200. No
checksum is stored, and GetObject and HeadObject return no `x-amz-checksum-*` headers.
`Content-MD5` is not checked either.

**Conditional writes are 501.** PutObject with `If-Match`, `If-None-Match` or
`x-amz-write-offset-bytes`, and CompleteMultipartUpload with `If-Match` or
`If-None-Match`, get 501 NotImplemented. The existing object is untouched and the
upload stays open.

**Conditional deletes are 501.** DeleteObject and AbortMultipartUpload with any
precondition header get 501 and change nothing. A DeleteObjects entry with `ETag`,
`LastModifiedTime` or `Size` gets a per-key NotImplemented error and is kept; the
other entries are still deleted.

**Re-completing an upload.** After a Complete that committed, a retried Complete for
the same upload gets NoSuchUpload. S3 answers it with 200 and the same ETag. The
retry changes nothing in Litmus. A strict xfail test records the S3 behavior.

**Empty Complete body.** CompleteMultipartUpload with an empty body gets 500, because
the body is not parsed as XML. S3 answers 400 MalformedXML.

**Multipart details.** Content-Type and user metadata given to CreateMultipartUpload
are not applied to the object. The minimum part size is not enforced. `partNumber`
on GET is ignored.

**Conditional GET.** Only `If-Match` is evaluated on GET. `If-None-Match`,
`If-Modified-Since` and `If-Unmodified-Since` are ignored.
