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

**161 passed, 162 failed, 7 skipped, of 330 selected tests.** The 7 skipped tests
(`test_bucket_get_location`, three restore tests, `test_read_through`,
`test_list_objects_restore_status` and `test_list_object_versions_restore_status`)
are skipped by s3-tests itself, not by the `-k` expression. Since the last step in the
table below, bucket name validation moved 7 tests from failed to passed, the
ListObjects (V1) parameters moved 21 tests from failed to passed, and
the ListObjectsV2 parameters 21 more; the ListObjectVersions parameters changed no
outcome (every selected versions test is skipped), though every test's teardown now
pages through ListObjectVersions; `encoding-type=url` moved 2 more
(`test_bucket_list_encoding_basic` and `test_bucket_listv2_encoding_basic`), and the
DeleteObjects limit of 1000 Objects moved 2 more (`test_multi_object_delete_key_limit`
and `test_multi_objectv2_delete_key_limit`), and the `InvalidRange` error body moved 2
more (`test_ranged_request_invalid_range` and `test_ranged_request_empty_object`), and
storing the system headers moved 3 more (`test_object_write_cache_control`,
`test_object_write_expires` and `test_object_content_encoding_aws_chunked`), and storing
user metadata moved 3 more (`test_object_set_get_metadata_none_to_good`, `_none_to_empty`
and `_overwrite_to_empty`). Nothing else changed, although botocore sends `encoding-type=url` on every listing it builds,
so every selected test that lists a bucket, and every teardown, now reads encoded keys
and decodes them.

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
| `74b5902` | 330 | 508 | 121 | `delimiter`, `max-keys` and `marker` in ListObjects (V1). 21 passes gained. |
| `4cee3b0` | 330 | 508 | 142 | ListObjectsV2 paging with `continuation-token` and `start-after`. 21 passes gained. |
| `2011375` | 330 | 508 | 144 | `encoding-type=url` in all three listings. 2 passes gained. |
| Bucket name validation | 330 | 508 | 151 | CreateBucket enforces the AWS naming rules. 7 passes gained: `test_bucket_create_naming_bad_ip`, `_bad_short_one`, `_bad_short_two`, `_bad_starts_nonalpha`, `_dns_dash_at_end`, `_dns_dot_dot`, `_dns_underscore`. Recorded at the commit that adds the validation. |
| Trailing slash on a bucket path | 330 | 508 | 151 | `/{bucket}/` is the bucket, for every method. No outcome changed, and none was expected to: the results file is identical to the previous one, and the server's access log for the run (7523 requests) holds no request to `/{bucket}/`. The only paths ending in a slash besides `/` were `PUT /{bucket}/asdf/` (2) and `PUT /{bucket}/0/` (1), which are keys. Recorded at the commit that adds the route. |
| DeleteObjects MalformedXML | 330 | 508 | 151 | A DeleteObjects body that is not well-formed XML gets 400 MalformedXML instead of 500. No outcome changed, and none was expected to: the results file is identical to the previous one, and the one selected test that expects MalformedXML, `test_multipart_upload_empty`, is CompleteMultipartUpload, which this change does not touch. All 195 DeleteObjects requests in the run were answered 200. Recorded at the commit that adds the error. |
| DeleteObjects document validation | 330 | 508 | 153 | The Delete document is validated whole before anything is deleted: root, 1 to 1000 Objects, one Key each, Quiet. More than 1000 Objects is 400 MalformedXML. 2 passes gained: `test_multi_object_delete_key_limit` and `test_multi_objectv2_delete_key_limit`, as forecast; no other outcome changed. Recorded at the commit that adds the validation. |
| InvalidRange error body | 330 | 508 | 155 | An unsatisfiable Range on GET is raised as the `InvalidRange` S3 error (416) instead of a bare 416, so it carries `Code`, `Message`, `RequestId` and `HostId`. 2 passes gained: `test_ranged_request_invalid_range` and `test_ranged_request_empty_object`, as forecast before the change; no other outcome changed. Recorded at the commit that adds the error. |
| RFC 9110 Range handling | 330 | 508 | 155 | A range whose last byte is past the object is clamped and served as 206; a Range that does not parse, a unit other than `bytes`, and a set of ranges are ignored (200, whole object). No outcome changed, and none was expected to: the results file is identical to the previous one. Every Range in a selected GET test either ends inside the object or expects `bytes=40-50` to be 416, and the copy-range tests fail on 501. Recorded at the commit that adds the handling. |
| System headers on PutObject | 330 | 508 | 158 | PutObject stores `Cache-Control`, `Content-Disposition`, `Content-Encoding` (without `aws-chunked`), `Content-Language` and `Expires`, and GET and HEAD return them. 3 passes gained: `test_object_write_cache_control`, `test_object_write_expires` and `test_object_content_encoding_aws_chunked`, as forecast; the results file differs from the previous one in those three lines only. Recorded at the commit that adds the storage. |
| User metadata on PutObject | 330 | 508 | 161 | PutObject stores `x-amz-meta-*` and GET and HEAD return it, with RFC 2047 decoding on store and encoding on return and a 2048 byte limit (400 MetadataTooLarge). 3 passes gained: `test_object_set_get_metadata_none_to_good`, `_none_to_empty` and `_overwrite_to_empty`, as forecast; the results file differs from the previous one in those three lines only. `test_object_set_get_unicode_metadata` stays failing, as forecast: it fails on the RFC 2047 value Litmus returns, which botocore hands back undecoded. `test_object_metadata_replaced_on_put`, which passed because Litmus stored nothing, now passes because an overwrite replaces the metadata: against a copy of the server whose overwrite keeps the old metadata it fails, so it leaves the incidental list (27 to 26). Recorded at the commit that adds the storage. |

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
is authenticated or owned. By that rule, **26 of the 161 passes are incidental**.

The listing entries were reclassified after `delimiter`, paging and `encoding-type`
were implemented, by running the listing tests (`-k test_bucket_list`, 84 tests, 76
passing) against copies of the server with listing parameters ignored: `delimiter`
alone, `prefix` alone, `max-keys` alone, `fetch-owner` forced off or forced on, and
`prefix`, `delimiter`, `max-keys`, `marker`, `start-after`, `continuation-token` and
`fetch-owner` all together. In every copy, "ignored" means the same thing: the
parameter has no effect on which keys, common prefixes, truncation and next marker or
token come back (and, for `max-keys`, an invalid value is not rejected), while the
server still echoes every request value exactly as the real handler does. Echoing a
value back does not show that the parameter works. (One check stays in the all-ignored
copy: a malformed `continuation-token` is still rejected; no listing test sends one.)

The rule for a listing pass: it is incidental or borderline when the test is about a
listing parameter (it sends one, sends it empty or false, or leaves it out on purpose
to check the default) and it passes with all of them ignored. It is borderline when
the parameter is empty, false or absent, which S3 also treats as a no-op, and
incidental otherwise. A test about no parameter is not classified by this rule: it is
genuine. `encoding-type` is not counted as a parameter here, since botocore sends it
on every listing. Of the 76 passes, 26 survive the all-ignored copy: 8 are incidental
(below), 15 are borderline (below), and 3 only list an empty bucket
(`test_bucket_list_distinct`, `test_bucket_list_empty`, `test_bucket_list_long_name`)
and are genuine. The other 50 need an implemented parameter.

The three `prefix_delimiter_*_not_exist` tests (`prefix_delimiter_prefix_not_exist`,
`prefix_delimiter_delimiter_not_exist` and
`prefix_delimiter_prefix_delimiter_not_exist`, each as `test_bucket_list_` and
`test_bucket_listv2_`) are genuine for prefix filtering: they fail when `prefix` is
ignored. Their delimiter never occurs in the keys, so they still pass when the
delimiter is ignored, and they do not exercise delimiter support.

- Listing, a non-empty value of the parameter under test changes nothing in the
  answer, so the test passes with every parameter ignored (8):
  `test_bucket_list_delimiter_not_exist` and `test_bucket_list_delimiter_unreadable`
  (a delimiter that occurs in no key), `test_bucket_list_delimiter_prefix_ends_with_delimiter`
  (the only key is the prefix itself, so no delimiter follows the prefix),
  `test_bucket_list_marker_unreadable` and `test_bucket_listv2_startafter_unreadable`
  (a marker below every key), and the V2 forms of the three delimiter tests. The
  ends-with-delimiter pair still fails on a server that applies the delimiter but
  ignores the prefix, which rolls the key up into the common prefix `asdf/`.
- Conditional GET, the header is ignored and the expected answer is the plain object
  (3): `test_get_object_ifmodifiedsince_good` (a date in the past),
  `test_get_object_ifnonematch_failed` (an ETag that does not match),
  `test_get_object_ifunmodifiedsince_failed` (a date in the future).
- Copy (3): `test_object_copy_key_not_found` and `test_object_copy_bucket_not_found`
  get their 404 from the client's HeadObject on the missing source, and no copy
  request reaches Litmus; `test_upload_part_copy_percent_encoded_key` expects
  UploadPartCopy to fail, and it gets 501.
- Bucket naming, the test only checks that a valid name is accepted (9). Still
  incidental after name validation: all 9 also pass against a copy of the server with
  the check removed (run for this change), so they do not show that validation works;
  the 7 tests that do show it fail on that copy, and so do the 2 that stay failing.
  The 9: `test_bucket_create_naming_dns_long`,
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

Borderline, not counted above (18):

- An empty or absent listing parameter, which S3 also treats as a no-op, so the
  expected result equals ignoring it, and the test passes with every parameter ignored
  (15): `test_bucket_list_delimiter_empty`, `test_bucket_list_delimiter_none`,
  `test_bucket_list_prefix_empty`, `test_bucket_list_prefix_none`,
  `test_bucket_list_marker_empty`, `test_bucket_list_marker_none`,
  `test_bucket_list_maxkeys_none` (the default `MaxKeys` is 1000), their
  `test_bucket_listv2_` counterparts (the four delimiter and prefix tests and
  `test_bucket_listv2_maxkeys_none`), `test_bucket_listv2_continuationtoken_empty`,
  and `test_bucket_listv2_fetchowner_defaultempty` and
  `test_bucket_listv2_fetchowner_empty` (S3 omits the owner unless `FetchOwner` is
  true; the first sends no `fetch-owner`, the second `false`). The prefix, marker and
  continuation-token entries are new to this list: they were never ignored, so they
  were not flagged before, but they meet the same definition. Without the prefix and
  marker entries the count is 12.
- The ACLs are ignored and the requests are anonymous, but the asserted 404 error
  codes are implemented (3): `test_object_raw_get_bucket_gone`,
  `test_object_raw_get_object_gone`, `test_object_delete_key_bucket_gone`.

## Results by category

Counts are grouped by test name. "Why the rest fail" gives the cause seen in the test
output; "501" means Litmus answers 501 NotImplemented for that request.

| Category | Selected | Passed | Failed | Skipped | What passes; why the rest fail |
|---|---|---|---|---|---|
| Bucket operations | 17 | 9 | 7 | 1 | Create, delete, head, list buckets, deleting a non-empty bucket refused, missing-bucket errors. Fail: re-creating a bucket you own expects success or BucketAlreadyOwnedByYou, and Litmus answers 409 BucketAlreadyExists (2 tests). That answer is deliberate: telling the owner apart needs authentication, which is out of scope; RGW-specific usage and statistics headers (3); ListBuckets ignores MaxBuckets and ContinuationToken (1); one test calls PutObjectAcl, which is 501 (1). |
| Bucket naming | 18 | 16 | 2 | 0 | Valid names are accepted; names that break the AWS rules (length, characters, edges, adjacent periods, IP format, reserved prefixes and suffixes) get 400 InvalidBucketName. Fail: `test_bucket_create_naming_dns_dot_dash` (`foo.-bar`) and `test_bucket_create_naming_dns_dash_dot` (`foo-.bar`) expect 400, and AWS's current rules allow both names, so Litmus accepts them (see Known gaps). |
| Object operations | 28 | 26 | 2 | 0 | PutObject, GetObject, HeadObject, DeleteObject, ETags, overwrites, zero-byte objects, standard chunked transfer encoding, `Content-Encoding` with its `aws-chunked` token removed, the atomic read and write tests, and the request ID in an error body matching the `x-amz-request-id` header. Fail: `response-*` query parameters are ignored; a key that is not valid UTF-8 gets 404 instead of 400. |
| User metadata and stored headers | 6 | 5 | 1 | 0 | `Cache-Control`, `Expires` and `x-amz-meta-*` are stored and returned (see "System headers" and "User metadata" under Known gaps). Fail: `test_object_set_get_unicode_metadata` expects a non-ASCII value back as the raw string, and Litmus returns it RFC 2047 encoded, as the AWS documentation describes (see "User metadata"). |
| Conditional GET | 8 | 5 | 3 | 0 | `If-Match` is evaluated. Fail: `If-None-Match`, `If-Modified-Since` and `If-Unmodified-Since` are ignored; the tests that pass with them are the ones whose expected answer is the same as ignoring them. |
| Range GET | 6 | 6 | 0 | 0 | Single, open-ended and suffix ranges, a last byte past the end clamped; an unsatisfiable range, on an object with bytes or on an empty one, gets 416 `InvalidRange` with the standard error body. |
| ListObjects (V1) | 41 | 39 | 2 | 0 | `prefix`, `delimiter` and common prefixes, `max-keys` (default, cap, 0, InvalidArgument), `marker`, `NextMarker`, truncation, `encoding-type=url`. Fail: `allow-unordered`, an RGW extension, is not implemented (1); one test calls GetObjectAcl, which is 501 (1). |
| ListObjectsV2 | 39 | 38 | 1 | 0 | `prefix`, `delimiter` and common prefixes, `max-keys`, `continuation-token`, `start-after`, `KeyCount`, `fetch-owner`, truncation, `encoding-type=url`. Fail: `allow-unordered`, an RGW extension, is not implemented (1). |
| DeleteObjects | 4 | 4 | 0 | 0 | Bulk delete, and the limit of 1000 Objects (400 MalformedXML for more). |
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
| **Total** | **330** | **161** | **162** | **7** | |

## AWS CLI

`scripts/aws_cli_smoke.sh` runs an unmodified AWS CLI v2 against a scratch Litmus
server and checks the results. It was last run with aws-cli 2.38.5 (Linux, installed
outside `/mnt/`); the script refuses versions older than 2.23. It sets no checksum,
transfer client or s3 config overrides, uses temporary dummy credentials, and passes
`--endpoint-url http://127.0.0.1:PORT`.

Verified, with content checks and not only exit codes:

- `s3 mb`, `s3 ls` (buckets, a prefix, and `--recursive`) with keys containing a space,
  a `+`, a non ASCII character and nested `dir/` prefixes, names printed exactly
- `s3 cp` up and down of a 1 KiB file and a 40 MiB file (multipart, above the 8 MiB
  threshold), sha256 compared
- `s3api put-object`, `s3api get-object`, `s3api head-object`, and `s3api get-object
  --range` on the 40 MiB object, bytes compared
- `s3 sync` of 20 files up, a second sync that transfers nothing, and a sync down into
  an empty directory, compared with `diff -r`
- `s3 rm` of one key, `s3 rm --recursive`, `s3 rb` of a non-empty bucket failing with
  BucketNotEmpty, and `s3 rb --force`

Limits of this check:

- Only `http://` was tested. Litmus does not serve TLS itself, and what the CLI sends
  over `https://` was not captured.
- Credentials are not checked: any access key and secret are accepted.
- `s3 cp s3://b/x s3://b/y` (and anything else that needs CopyObject) fails with 501
  NotImplemented and creates no destination key.
- `s3 sync` compares size and modification time. LastModified has one second
  resolution, so files modified in the same second they were uploaded can be uploaded
  again by the next sync; the script backdates its source files for this reason.

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
bucket name (`//x`), gets 400 InvalidURI. `/{bucket}/`, a trailing slash and no key,
is the bucket, not an object with an empty key: every method on it behaves as on
`/{bucket}`, query parameters included. A path-style request names its bucket by the
first slash-delimited component and its key by the rest of the Request-URI (S3 User
Guide, "Virtual hosting of general purpose buckets",
<https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.md>, "HTTP `Host`
header bucket specification"); S3's bucket-level requests, such as `GET /?list-type=2`,
send no key. The AWS pages do not state outright that `/{bucket}/` equals `/{bucket}`,
so that equivalence is read from those two. `/{bucket}//` has the key `/`, and
`/{bucket}/dir/` the key `dir/`; both are objects. An unexpected server error gets 500
InternalError. One case is outside Litmus: with uvicorn's default HTTP parser
(httptools), a method name the parser does not know (for example `BREW`) is rejected
with a plain-text 400 before the request reaches the application. With
`--http h11`, such a request reaches Litmus and gets 405 MethodNotAllowed.

**Range.** GET serves one `bytes=` range, per RFC 9110 (sections 14.1.1, 14.1.2 and
14.2). The unit is matched without regard to case. A range with no byte in the object is
416 `InvalidRange`, with the standard error body (`Code`, `Message`, `RequestId`,
`HostId`) and `Content-Range: bytes */{size}`: an int-range whose first byte is at or
past the length (so every int-range on an empty object), and a suffix of length 0. A
last byte at or past the end is clamped to the last byte (`bytes=5-999` on 10 bytes is
206 `bytes 5-9/10`), and a suffix longer than the object returns all of it. HEAD with a
range GET refuses is 416 too, without a body. Documented: the GetObject reference says
"Amazon S3 doesn't support retrieving multiple ranges of data per `GET` request"; the
Error reference lists `InvalidRange`, "The requested range cannot be satisfied.", 416;
the HeadObject reference says "If the Range is not satisfiable, S3 returns a `416 -
Requested Range Not Satisfiable` error" and that an error to HEAD carries no exact code
(HEAD has no body).

Undocumented choices, none compared with S3. The AWS pages say nothing on any of them.

- A Range GET ignores is answered 200 with the whole object and no `Content-Range`.
  GET ignores a range when: the unit is not `bytes` or the header has no `=` (RFC 9110
  14.2: "An origin server MUST ignore a Range header field that contains a range unit it
  does not understand"); the specifier is not valid, meaning it does not match
  `first-pos "-" [last-pos]` or `"-" suffix-length` in ASCII digits (so `bytes=abc`,
  `bytes=`, `bytes=1-2-3`, `bytes=+1-5` and `bytes= 0-9`), or has a last byte below its
  first (`bytes=50-10`), or has a numeral of more than 4000 digits (RFC 9110 14.2: a
  server "MAY ignore or reject" an invalid ranges-specifier; ignoring was chosen over a
  416 because 416 says a valid range cannot be satisfied, and RFC 9110 14.2 asks for it
  only when "the received Range field-value contains a valid ranges-specifier"; the
  digit cap keeps a hostile numeral from reaching integer conversion, per the MUST in
  14.1.2 to prevent parsing errors on large numerals; any numeral shorter than that is
  exact, so the clamp holds for values past 64 bits).
- More than one range (`bytes=0-9,20-29`) is ignored the same way. The basis is RFC
  9110 14.2, "A server that supports range requests MAY ignore the Range header field"
  and "MAY ignore or reject" a set of many small ranges, with the GetObject statement
  that S3 does not support multiple ranges. Ignoring was chosen over an S3 error because
  the error list has no code for it, and 200 with the whole object is a correct answer
  that no client has to special-case. Multi-range used to be 400 with a plain-text body.
- A suffix range with a non-zero length on an empty object is satisfiable under RFC
  9110 14.1.2 ("the only satisfiable form of range-spec in a GET request is a
  suffix-range with a non-zero suffix-length"), but no 206 can describe zero bytes
  (`Content-Range` would need a last byte below its first). RFC 9110 14.2 lets a server
  ignore Range for a zero-length representation, so it is answered 200 with an empty
  body. Every other range on an empty object is 416.
- The 416 carries `Content-Range: bytes */{size}` (RFC 9110 14.4: a server "SHOULD
  send" it); the AWS pages do not mention it. Its `InvalidRange` body holds no
  `ActualObjectSize` or `RangeRequested`.
- `bytes=-0` is unsatisfiable (RFC 9110 14.1.2: a suffix needs a non-zero length).

Known gaps. HEAD does not narrow `Content-Length` for a range it could satisfy (S3
does), and it ignores the ranges GET ignores; it is 416 only where GET is. RFC 9110
14.2 defines Range for GET only.

**Bucket names.** CreateBucket enforces the rules in "General purpose bucket naming
rules" (<https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html>):
3 to 63 characters; only lowercase letters, digits, `.` and `-`; begin and end with a
letter or digit; no `..`; not shaped like an IP address (four dot-separated runs of
digits, whatever their value); no prefix `xn--`, `sthree-` or `amzn-s3-demo-`; no suffix
`-s3alias`, `--ol-s3`, `.mrap`, `--x-s3`, `--table-s3` or `-an`. An invalid name is 400
InvalidBucketName and creates nothing, and the check runs before the existence check.
Not enforced: the Transfer Acceleration rule (no periods), because Litmus has no
acceleration; and global uniqueness, because Litmus has one account. `-an` is refused
because AWS allows it only in an account regional namespace, which needs the
`x-amz-bucket-namespace` header; Litmus does not implement namespaces. Only CreateBucket
validates. Every other operation looks the name up as it is: a bucket in the store is
served whatever its name (AWS keeps pre-2018 us-east-1 buckets with uppercase letters and
underscores working, which is the case for a store written before this check), and a name
not in the store is 404 NoSuchBucket, the answer for any missing bucket. The exceptions
are `.` and `..`, which are 400 InvalidBucketName on every request (see
`docs/design/storage-layout.md`, decision 16). Two selected ceph tests contradict the AWS
rules and fail on purpose: `test_bucket_create_naming_dns_dot_dash` (`foo.-bar`) and
`test_bucket_create_naming_dns_dash_dot` (`foo-.bar`) expect 400, but the current AWS
page has no rule against a period next to a hyphen, so Litmus accepts both.

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
`Owner`; `KeyMarker` and `VersionIdMarker` are always echoed.

**`encoding-type=url`.** All three listings accept `encoding-type=url`, echo
`<EncodingType>url</EncodingType>`, and write the key-valued elements url-encoded: every
byte of the UTF-8 form is `%XX` except the unreserved characters (`A-Z a-z 0-9 - _ . ~`)
and `/`, so a space is `%20`, `+` is `%2B` and `test_file(3).png` is
`test_file%283%29.png` (the reference's example). This is the encoding botocore inverts:
it sends `encoding-type=url` on every ListObjects, ListObjectsV2 and ListObjectVersions
request it builds, and, when `EncodingType` is echoed, decodes with `unquote_plus`,
which turns a bare `+` into a space; so `+` must be `%2B`. The elements encoded per
operation are the ones botocore decodes (`botocore/handlers.py`, `decode_list_object`,
`decode_list_object_v2`, `decode_list_object_versions`), which match the lists in the
ListObjectsV2 and ListObjectVersions references except where noted below:

- ListObjects (V1): `Marker`, `NextMarker`, `Delimiter`, `Contents/Key`,
  `CommonPrefixes/Prefix`. Not `Prefix`. **Choice:** the V1 `Prefix` is echoed raw
  under `encoding-type=url`, for three reasons: botocore does not decode it (it does
  for ListObjects V2 and ListObjectVersions), so an encoded V1 `Prefix` would reach
  every boto3 caller percent-encoded; RGW does not encode it; and the current
  ListObjects reference lists no encoded elements at all for `EncodingType`. (The older
  wording botocore quotes in its source listed "Delimiter, Marker, Prefix, NextMarker,
  Key".) A V1 `prefix` with a character XML 1.0 forbids therefore still makes the
  response ill-formed, with or without `encoding-type`.
- ListObjectsV2: `Prefix`, `Delimiter`, `StartAfter`, `Contents/Key`,
  `CommonPrefixes/Prefix` (the reference: "Delimiter, Prefix, Key, and StartAfter").
  `ContinuationToken` and `NextContinuationToken` are opaque and never encoded; the
  older reference wording listed `ContinuationToken`, botocore does not decode it.
- ListObjectVersions: `KeyMarker`, `NextKeyMarker`, `Prefix`, `Delimiter`,
  `Version/Key`, `CommonPrefixes/Prefix` (the reference: "KeyMarker, NextKeyMarker,
  Prefix, Key, and Delimiter"). `VersionIdMarker` and `NextVersionIdMarker` are not
  encoded.

In all three, `CommonPrefixes/Prefix` is encoded because botocore decodes it and
`test_bucket_list_encoding_basic` expects it; the references name only "Prefix". Without
`encoding-type` nothing is encoded and no `EncodingType` element is returned.

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
- `encoding-type` accepts exactly `url`, the one value the references list ("Valid
  Values: url"). Any other value, including `URL`, is 400 InvalidArgument with
  `ArgumentName` `encoding-type` and `ArgumentValue` the value sent; the message,
  "Invalid Encoding Method specified in Request", is taken from MinIO's error table
  (`ErrInvalidEncodingMethod`), not from S3: the AWS references document only the
  valid value, and Litmus has not compared the message with S3's response. An empty `encoding-type` is treated as absent, like an empty
  delimiter. (RGW ignores any value but `url`, case-insensitively, and encodes
  nothing.)

**Characters XML 1.0 does not allow.** XML 1.0 allows tab, LF, CR, U+0020 to U+D7FF,
U+E000 to U+FFFD and U+10000 to U+10FFFF. Litmus accepts keys with the other
characters, U+0000 to U+0008, U+000B, U+000C, U+000E to U+001F, U+FFFE and U+FFFF, and
writes request values into XML responses as they are. Without `encoding-type`, a
listing that holds such a key, or echoes such a `prefix`, `marker`, `start-after`,
`key-marker` or `delimiter`, is written raw and answered 200, and the body is not
well-formed XML. `encoding-type=url` is the remedy: the key-valued elements become
`%XX` strings and the response parses. AWS documents the same hazard (ListObjectsV2
reference): "A `200 OK` response can contain valid or invalid XML. Make sure to design
your application to parse the contents of the response and handle it appropriately."
Checked against a running server with U+0000, U+0001, U+0008, U+000B, U+000C, U+001F,
U+FFFE and U+FFFF in a key or parameter, these responses do not parse whatever the
client sends:

- The ListObjects (V1) `Prefix`, which is never encoded (see above), so a V1 listing
  with such a `prefix` is ill-formed even with `encoding-type=url`;
- InitiateMultipartUpload and CompleteMultipartUpload results (`Key`);
- NoSuchKey errors from GetObject (`Key`), NoSuchBucket errors (`BucketName`), and
  the InvalidArgument error for `max-keys` (`ArgumentValue`).

A DeleteObjects request cannot carry such a key: its body is XML, and a reference to
one of those characters (`&#x1;`, `&#0;`, `&#xFFFE;`), like one to a surrogate or to a
code point above U+10FFFF, is not well-formed (XML 1.0, section 4.1, "Legal
Character": "Characters referred to using character references MUST match the
production for Char"). It gets 400 MalformedXML, as does any body that is not
well-formed XML or not UTF-8 (an empty body, markup that does not nest, an undefined
entity, entity expansion beyond the parser's limits), and nothing in the request is
deleted. Such a key is removed with DeleteObject (`DELETE /{bucket}/{key}`, percent-encoded),
not with DeleteObjects. References to allowed
characters work and decode to those characters: `&#9;`, `&#10;`, `&#13;`,
`&#x1F600;`, `&#xE000;`, `&#x10FFFF;` and the five predefined entities.

A key with CR is stored. XML parsers turn a CR written literally into LF (XML 1.0,
section 2.11), so there are two effects. In a listing, the key is shown with LF
instead. In DeleteObjects, a literal CR in a `<Key>` names the LF form: the request
deletes the key with LF if there is one, answers Deleted either way, and leaves the
key with CR in place. A reference does name it: `&#13;` is a CR that survives parsing
(AWS's DeleteObjects reference uses it for this, in "DeleteObjects containing a
carriage return"), so a key with CR can be deleted that way, and a key with LF next to
it is left alone. Clients that write the key into the body unescaped, botocore among
them, cannot delete a key with CR. Litmus also writes the key into its own response
unescaped, so the `Deleted` entry for a CR key carries a literal CR, which a client's
parser reads as LF. A key with LF cannot be stored at all: no route matches the path,
and PutObject gets 400 InvalidURI.

**DeleteObjects document.** The body is checked whole, before anything is deleted, and
a refusal anywhere in it, even after valid entries, deletes nothing. The root must be
`Delete`, in the S3 namespace or in none; there must be 1 to 1000 `Object` elements,
each with exactly one `Key`; `Quiet`, if present, appears once and holds `true` or
`false`. Everything else a well-formed document can hold is read as follows.

- Basis. The S3 API reference for DeleteObjects says "The request can contain a list of
  up to 1,000 keys", marks `Delete` and `Object` Required and `Quiet` optional with
  "When you add this element, you must set its value to `true`", and ObjectIdentifier
  marks `Key` Required with "Minimum length of 1".
- Quiet true lists only the keys that failed (a per-key error, such as a version ID or
  condition Litmus does not implement); a request with no failures answers an empty
  `DeleteResult`. The reference describes quiet mode in those words: "the response
  includes only keys where the delete operation encountered an error".
- `VersionId` is read as `versionId` is elsewhere: `null` names the current object and
  is echoed back in `Deleted`; any other value, including an empty one, is a per-key
  NotImplemented error and the key is kept, while the other entries are still deleted.
  `ETag`, `LastModifiedTime` and `Size` are the same per-key NotImplemented. These
  per-key errors are not document errors and do not stop the other entries.
- Duplicate keys: each entry is answered, the key is deleted once. Elements Litmus does
  not know are ignored, and so are repeated `VersionId`, `ETag`, `LastModifiedTime`
  and `Size` elements past the first. The reference documents none of this.
- Elements are read in the root's namespace, so an `Object` that opts out of the root's
  namespace is not an Object and a `Key` that does is not a Key.
- A `Key` or `VersionId` or `Quiet` that contains an element is refused, since its text
  before the element would name a different key than the client sent.
- An empty `Key` is accepted, against "Minimum length of 1", on purpose: it is the only
  way to remove an object stored under the empty key by an older Litmus (see "Objects
  under the empty key").

Undocumented choices. The S3 error list (API reference, Error, Code) has no code for a
Delete document that breaks the rules above, and none for more than 1000 Objects: no
TooManyKeys code appears in it. Litmus answers 400 MalformedXML for all of them, the
code whose description is XML "that doesn't conform to the published XSD". The same
holds for each of these, which the reference does not settle: a wrong root element or
a root in another namespace, no `Object`, an `Object` with no `Key` or two, a second
`Quiet`, and a `Quiet` that is not exactly `true` or `false` (so `1`, `0`, `TRUE`, an
empty one: the reference types it Boolean without saying whether XML Schema's `1` and
`0` or other spellings count; surrounding white space is allowed). An empty body is
MalformedXML too, though the error list gives MissingRequestBodyError ("when the user
sends an empty XML document as a request") for an empty document; the empty document is
read as a body that does not parse. None of these choices has been compared with S3's
responses. The status for more than 1000 Objects is 400, which is what
`test_multi_object_delete_key_limit` expects; the code is the choice.

Checksums: DeleteObjects verifies `Content-MD5` and one `x-amz-checksum-<algorithm>`
over the raw request body when the request carries them, and requires neither; see
"Checksums: verified on PutObject, UploadPart and DeleteObjects" below. An internal DTD entity
(`<!ENTITY e "x">`) in the body is expanded, within the parser's limits.

**Keys are stored literally.** A key is the exact string the client sent, including
keys with `.` or `..` segments. Clients that resolve dot segments before sending
(httpx does) must percent-encode them. Bucket names `.` and `..` are rejected with
InvalidBucketName.

**Version IDs.** Every bucket is unversioned. `GET /{bucket}?versions` lists each
object once, as version `null`, and `version-id-marker` accepts only `null`. GET, HEAD and DELETE accept `versionId=null` as the current
object, and a DeleteObjects entry with `<VersionId>null</VersionId>` deletes the
object. Any other version ID gets 501 (or a per-key NotImplemented error in
DeleteObjects), and `versionId` on PUT or POST gets 501.

**Chunk-framed (aws-chunked) bodies: one framing is decoded.** botocore and boto3
1.36.0+ and AWS CLI v2 2.23.0+ send PutObject and UploadPart bodies as aws-chunked with
a trailing checksum by default. Litmus identifies such a body the way S3 documents it:
`x-amz-content-sha256` starting with `STREAMING-`, or an `x-amz-decoded-content-length`
header. `Content-Encoding: aws-chunked` alone is not a marker, since S3 does not require
it: such a request is stored as an ordinary body. Its `aws-chunked` token is dropped
from the stored `Content-Encoding` (see "System headers" below).

- Supported, on PutObject and UploadPart only: `x-amz-content-sha256:
  STREAMING-UNSIGNED-PAYLOAD-TRAILER` (compared exactly) with `x-amz-trailer:
  x-amz-checksum-crc64nvme` (one entry, any letter case), `x-amz-decoded-content-length`
  and `Content-Length`. This is what the AWS CLI v2 sends with its default settings over
  `http://`. The body is a lower case hex size line, the data, CRLF, any number of
  chunks, `0`, CRLF, one trailer line `x-amz-checksum-crc64nvme:<base64 of the 8 byte
  big endian CRC64NVME>`, CRLF, and a final CRLF. The decoded bytes are what is stored:
  the ETag is their MD5, and the framing, the trailer and the checksum are not kept.
  The trailer, and any `Content-MD5` or `x-amz-sdk-checksum-algorithm`, is verified
  before the blob is made durable (see "Checksums" below).
- Still 501 NotImplemented, before the body is read and before anything is registered or
  written: the signed variants `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` and
  `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` (and any other `STREAMING-` value);
  `x-amz-decoded-content-length` with any other `x-amz-content-sha256`; the supported
  streaming value on any request other than PutObject and UploadPart (a framed
  CreateBucket, which would otherwise ignore the body, or a framed DeleteObjects,
  CreateMultipartUpload or CompleteMultipartUpload, which would otherwise be read as plain
  XML), and an `x-amz-content-sha256` naming a `STREAMING-` payload or an
  `x-amz-decoded-content-length` on any such request, whatever its method; the supported
  streaming value without `x-amz-trailer`; and an `x-amz-trailer` that lists more than
  one entry or names anything but `x-amz-checksum-crc64nvme`, so CRC32, CRC32C, SHA-1
  and SHA-256 trailers, which botocore sends over `https://` by default, are refused.
  The AWS CLI v2 sends the supported framing over `http://` with its default settings
  (see "AWS CLI" above). A client can also avoid framing by setting
  `request_checksum_calculation = when_required`.
- Strictness: the size line is 1 to 32 hex digits (either case) and CRLF, with no sign,
  whitespace or chunk extension; every line ends in CRLF and a bare LF is an error;
  nothing may follow the final CRLF; the framed body must be exactly `Content-Length`
  bytes; the trailer is exactly one line whose name matches `x-amz-trailer` in any
  letter case; its value must be strict base64 of 8 bytes. Reading stops as soon as the
  decoded bytes exceed `x-amz-decoded-content-length` (at most 5 GiB).
- Errors AWS documents (S3 API reference, "Error responses",
  https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html): a checksum that
  does not match the body is 400 BadDigest; a malformed checksum value is 400
  InvalidDigest; a missing `Content-Length` is 411 MissingContentLength.
- Errors AWS does not document for this case, so these are Litmus's own choices:

  - Framing errors (bad or missing size, bare LF, chunk extension, bytes after the final
    CRLF, a body that ends inside the framing, a missing, repeated, overlong or
    colon-less trailer line, framed size different from `Content-Length`): 400
    InvalidRequest, with a `Reason` element naming the fault.
  - A trailer line whose name is not the one in `x-amz-trailer`: 400 InvalidRequest.
  - The decoded length differing from `x-amz-decoded-content-length`, short or long: 400
    IncompleteBody, the code for fewer bytes than were announced.
  - A missing, non-numeric, signed, space-padded or non-ASCII
    `x-amz-decoded-content-length`, or one above 5 GiB: 400 InvalidRequest.
- Nothing is left behind by a refused or interrupted framed upload: no object, part or
  registry row, an empty `tmp/`, and a clean consistency sweep. A re-upload of a part
  that is refused leaves the earlier part and its ETag as they were.
- Memory: a framed body is decoded as it arrives. Each received buffer is written to
  the blob at once, and the decoder keeps at most a size line or the trailer line, so
  memory does not grow with the object. Plain bodies are not streamed: they are still
  read whole into memory before they are written, as before.

**Checksums: verified on PutObject, UploadPart and DeleteObjects, not stored.** A PutObject
or UploadPart body is checked against the checksums its request claims, before the blob is
made durable, so a refused request leaves no object, part or file. Checked:

- `Content-MD5`: base64 of the 16 byte MD5 of the body. It may accompany one checksum,
  and both are verified.
- One of `x-amz-checksum-crc32`, `-crc32c`, `-crc64nvme`, `-sha1`, `-sha256`: base64 of
  the digest (4, 4, 8, 20 and 32 bytes), checked against the body.
- The CRC64NVME trailer of a framed body, as before. On a framed body `Content-MD5` is
  checked against the decoded bytes, not the framed ones.
- `x-amz-sdk-checksum-algorithm`, which must name the algorithm of the checksum header
  (or of the trailer) in the request, in any letter case.

Errors AWS documents (S3 API reference, PutObject): a value that does not match the body,
or an `x-amz-sdk-checksum-algorithm` that differs from the checksum header, is 400
BadDigest. The reference says an SDK header with no matching checksum header or trailer
fails with "400 Bad Request" and names no code. AWS's error list describes InvalidDigest
as a Content-MD5 or checksum value that is not valid, and BadDigest as one that did not
match, which is how Litmus uses them (the page's table could not be fetched when this was
written; the descriptions are as given by the project owner). The rest are Litmus's own
choices, not documented by AWS:

- A checksum or `Content-MD5` value that is not strict base64 of the right size
  (including an empty one) is 400 InvalidDigest, found before the body is read.
- `x-amz-sdk-checksum-algorithm` with no checksum header or trailer to match (also when
  only `Content-MD5` is present), or sent twice: 400 InvalidRequest.
- More than one `x-amz-checksum-<algorithm>` value header (also the same one twice), or
  a value header together with a trailer: 400 InvalidRequest. Because a framed body
  always has its trailer, a framed request with any `x-amz-checksum-<algorithm>` header is
  refused; `Content-MD5` is the one header claim a framed request can make.
- An `x-amz-checksum-<name>` value header naming any other algorithm (`md5`, `sha512`,
  `xxhash64`, `xxhash3`, `xxhash128`, or a name AWS does not list) is 501 NotImplemented,
  before the body is read. `x-amz-checksum-mode`, `-algorithm` and `-type` carry no
  value and are not covered by this rule.
- A repeated `Content-MD5` header reads as one comma separated value, so it is 400
  InvalidDigest.

DeleteObjects takes the same claims over the XML body, with the same rules and errors,
in this order: NoSuchBucket, the checksum, then the document (MalformedXML), and a
refusal anywhere deletes nothing. A claim that is not valid is refused before the body
is read, after the bucket check; an `x-amz-checksum-<name>` of another algorithm is 501 before the bucket is looked up.
One choice differs from the S3 API reference, which says "The Content-MD5 request
header is required for all Multi-Object Delete requests" for general purpose buckets:
Litmus requires no checksum at all. botocore 1.36+ sends `x-amz-checksum-crc32` and
`x-amz-sdk-checksum-algorithm: CRC32` instead of `Content-MD5`, and real S3 accepts
that; a client that sends nothing is accepted too. Not requiring one is Litmus's own
choice, and so is the error for each refusal above that AWS does not document.

Not done:

- Nothing is stored and nothing is returned: PutObject and UploadPart answer with `ETag`
  only, and GetObject and HeadObject return no `x-amz-checksum-*` header whatever
  `x-amz-checksum-mode` says. S3 stores the checksum and, without one from the client,
  adds CRC64NVME.
- Multipart checksums are not implemented: `x-amz-checksum-algorithm` and
  `x-amz-checksum-type` on CreateMultipartUpload, the `Checksum*` elements of the
  CompleteMultipartUpload body, `x-amz-checksum-*` and `x-amz-mp-object-size` on
  Complete are all ignored, and composite and full object checksums are not computed.
  The 501 for other algorithms applies to PutObject, UploadPart and DeleteObjects only.
- The trailers `x-amz-checksum-crc32`, `-crc32c`, `-sha1` and `-sha256`, which botocore
  sends over `https://`, are still 501 (see above); only CRC64NVME is decoded.
- `x-amz-content-sha256` holding a hex digest is not compared with the body.
- The algorithms S3's documentation now lists beyond these five (MD5, SHA512 and the
  XXHash family) are not computed.

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

**Objects under the empty key.** Before `/{bucket}/` became the bucket, `PUT /{bucket}/`
stored an object under the key `""`. No S3 client sends such a key (the API model gives the
`Key` parameter a minimum length of 1, which botocore enforces), and no request can now
name one at a path. A store written by that version may
still hold such an object. It is not orphaned: ListObjects lists it with an empty
`<Key>`, DeleteBucket refuses with 409 BucketNotEmpty while it is there, and
DeleteObjects removes it, given a raw body such as
`<Delete><Object><Key></Key></Object></Delete>` (`POST /{bucket}?delete`).
botocore refuses that body for the same reason, so it takes a hand-written request. GET, HEAD and DELETE on `/{bucket}/` no longer reach it. Nothing creates a new
one: no other path in Litmus stores an empty key.

**Empty Complete body.** CompleteMultipartUpload with an empty body gets 500, because
the body is not parsed as XML. S3 answers 400 MalformedXML. So does any other body
that is not well-formed XML or not UTF-8: Complete has its own parser, which still
lets the error escape, though DeleteObjects now answers 400 for the same bodies.

**User metadata.** PutObject stores the `x-amz-meta-*` headers in the object row's
`user_metadata` column, and GET and HEAD return them, with the prefix, on 200 and on 206.
They commit in the same upsert as the object row (`chaos/scenarios/test_crash_user_metadata.py`).
An overwrite replaces the whole set; nothing is merged with the old object's. The S3
documentation says that names are stored in lower case, that metadata cannot be changed
after the upload except by a copy, that the user-defined metadata of a PUT request is
limited to 2 KB, "measured by taking the sum of the number of bytes in the UTF-8
encoding of each key and value", that values with non-ASCII characters are RFC 2047
decoded before they are stored and RFC 2047 encoded when they are returned, and that
headers of the same name are combined into a comma-delimited list when they are
retrieved (S3 User Guide, "Working with object metadata", "User-defined metadata"). The
rest is Litmus's choice, listed here:

- Size. Litmus counts the UTF-8 bytes of each name without the `x-amz-meta-` prefix plus
  the UTF-8 bytes of its value, as stored: after duplicates are joined and encoded words
  decoded. The documentation does not say whether the prefix counts. Exactly 2048 is
  accepted and 2049 is refused with 400 `MetadataTooLarge`, before the body is read and
  before any blob is registered, so nothing is written and the old object is untouched.
  The bucket is checked first, so a missing bucket is still 404. The 8 KB limit on the
  whole PUT header is not enforced separately, and the 2 KB limit on system-defined
  metadata is not enforced either. The error code is S3's for "metadata headers exceed
  the maximum allowed metadata size".
- A name sent more than once has its values joined with `, `, as S3 does on retrieval;
  the documentation does not say how a PUT's duplicates are combined.
- An empty value is kept as an empty value, and returned as an empty header.
- A header that is only the prefix, `x-amz-meta-`, has no name and is ignored.
- `x-amz-missing-meta` is never returned: everything Litmus stores can be returned.
- RFC 2047 on store. An encoded word `=?charset?encoding?text?=` is decoded when the
  charset is `UTF-8`, `US-ASCII` or `ISO-8859-1` (compared without regard to case) and
  the encoding is `B` (base64) or `Q` (quoted-printable, with `_` for a space). White
  space between two adjacent encoded words is dropped (RFC 2047 6.2). An encoded word
  that cannot be decoded (another charset, a payload that is not valid base64 or
  quoted-printable, bytes that are not valid in the charset) is kept as text. The
  documentation names no charset or encoding; its one example is UTF-8 with `B`.
- Raw non-ASCII bytes that are not an encoded word are read as UTF-8 when they are valid
  UTF-8, and as ISO-8859-1 otherwise. The documentation is silent. Latin-1 bytes that
  happen to form valid UTF-8 are therefore read as UTF-8.
- RFC 2047 on return. A value of printable ASCII (space to `~`) is returned as it is.
  Any other value, including one with a control character, is returned as one UTF-8
  `B` encoded word, never split at the RFC's 75 character limit. A value decoded from
  `=?UTF-8?Q?a=0D=0Ab?=` therefore cannot split a response header.
- `test_object_set_get_unicode_metadata` fails because Litmus follows the AWS
  documentation over ceph. The test sends the string `Hello World\xe9` (raw ISO-8859-1
  byte `E9`) and expects the same string back. Litmus returns
  `=?UTF-8?B?SGVsbG8gV29ybGTDqQ==?=`, as the documentation describes, and botocore passes
  a metadata header value on undecoded (`_parse_header_map` in `botocore/parsers.py`).
  Upstream marks the test `fails_on_rgw`.
- CreateMultipartUpload and UploadPart ignore `x-amz-meta-*`; the completed object has
  none.

**System headers.** PutObject stores `Cache-Control`, `Content-Disposition`,
`Content-Encoding`, `Content-Language` and `Expires` with the object, and GET and HEAD
return them, on 200 and on 206, beside `Content-Type`. An overwrite replaces all of them:
a header the new PUT lacks is cleared. They commit in the same transaction as the object
row, so a kill leaves the old object with its old headers or the new object with its new
ones (`chaos/scenarios/test_crash_system_headers.py`). The S3 documentation says what each
header is (PutObject, "Request Parameters"; S3 User Guide, "System-defined object
metadata") and that GET and HEAD return them; the rest is Litmus's choice, listed here:

- `Content-Encoding` loses every `aws-chunked` token, compared without regard to case,
  and keeps the others in order, joined with `, `. If none is left, nothing is stored and
  no `Content-Encoding` is returned. S3's documentation says only that `aws-chunked` may
  appear in the header of a chunked upload (S3 User Guide, "Checking object integrity
  for data uploads", "Trailing checksum headers"); that it is not kept comes from
  `test_object_content_encoding_aws_chunked`, which expects `gzip, aws-chunked` and
  `aws-chunked, gzip` to read back as `gzip` and `aws-chunked` as no header.
- A header sent more than once is read as one comma separated list (RFC 9110 5.3), and
  returned joined with `, `.
- A blank value is stored as no value, and nothing is returned.
- `Expires` is stored and returned exactly as sent, whether or not it parses as a date.
  The S3 documentation is silent on an `Expires` that is not a date. The only basis is
  botocore (1.43.118, the pinned version): `handle_expires_header` in `botocore/handlers.py`
  keeps an unparseable `Expires` response header as `ExpiresString` instead of failing,
  which suggests S3 can return one.
- Header bytes pass through as latin-1, as the server reads and writes headers; no
  RFC 2047 decoding or encoding is applied to these headers.
- The 2 KB limit the documentation gives for system-defined metadata in a PUT header
  is not enforced.
- CreateMultipartUpload does not apply any of them to the completed object yet.

**Multipart details.** Content-Type and user metadata given to CreateMultipartUpload
are not applied to the object. The minimum part size is not enforced. `partNumber`
on GET is ignored.

**Conditional GET.** Only `If-Match` is evaluated on GET. `If-None-Match`,
`If-Modified-Since` and `If-Unmodified-Since` are ignored.
