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
  `9b7cd65` they are selected; the first passes since CopyObject and the second since the
  Complete body validation (see the table).
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

**195 passed, 128 failed, 7 skipped, of 330 selected tests.** The 7 skipped tests
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
and `_overwrite_to_empty`), and the `response-*` overrides moved 1 more
(`test_object_raw_response_headers`), and the headers and metadata of CreateMultipartUpload
moved 1 more (`test_multipart_upload_resend_part`), and conditional GET and HEAD moved 3 more
(`test_get_object_ifnonematch_good`, `test_get_object_ifmodifiedsince_failed` and
`test_get_object_ifunmodifiedsince_good`), and `If-Match` and `If-None-Match` on PutObject moved 11 more (the four `test_put_object_ifmatch_*`, the four `test_put_object_ifnonmatch_*`, `test_put_object_if_match` and the two `test_atomic_*conditional_write_1mb`), and the Complete body validation moved 1 more (`test_multipart_upload_empty`), and CopyObject moved 13 more (the nine `test_object_copy_*` tests without ACLs, versioning or a second account, and the four `test_copy_object_if*`). `If-Match` on DeleteObject moved no test (see the table), and the per key `ETag` of DeleteObjects moved 1 more (`test_delete_objects_if_match`), and UploadPartCopy moved 6 more (`test_multipart_copy_small`, `_without_range`, `_special_names`, `_multiple_sizes`, `_invalid_range` and `_improper_range`). Disabling ACLs moved 5 from passed to failed, on purpose (`test_object_raw_get`, `test_object_raw_get_bucket_gone`, `test_object_raw_get_object_gone`, `test_object_delete_key_bucket_gone` and `test_object_anon_put_write_access`: they create a bucket or put an object with a public ACL, which AWS refuses on a bucket with the default Object Ownership setting). The minimum part size moved 1 more, from failed to passed (`test_multipart_upload_size_too_small`). Nothing else changed, although botocore sends `encoding-type=url` on every listing it builds,
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
| `response-*` overrides on GET and HEAD | 330 | 508 | 162 | `response-cache-control`, `-content-disposition`, `-content-encoding`, `-content-language`, `-content-type` and `-expires` set the response header on GET (200 and 206) and HEAD, and are never stored. 1 pass gained: `test_object_raw_response_headers`, as forecast: it asserts the five headers the overrides set, its setup (`ACL` on CreateBucket and PutObject, `private` for both, which is still accepted since the ACL headers are decided: see "ACL and grant headers") already passed, and it is the only selected test that sends a `response-*` parameter. No other outcome changed; the results file differs from the previous one in that line only. Recorded at the commit that adds the overrides. |
| Multipart upload headers | 330 | 508 | 163 | CreateMultipartUpload stores `Content-Type`, the system headers and `x-amz-meta-*` with the upload and Complete gives them to the object, which is dated by the upload's initiation time. 1 pass gained: `test_multipart_upload_resend_part`, as forecast: it completes each upload once, checks the object's Content-Type and metadata, and fails today only on `binary/octet-stream == text/bla`. `test_multipart_upload` stays failing as forecast: it fails at its second Complete (`NoSuchUpload`) before it reaches its Content-Type assertion. No selected test reads Last-Modified of a multipart object. The results file differs from the previous one in that one line only. Recorded at the commit that adds the storage. |
| Conditional GET and HEAD | 330 | 508 | 166 | `If-Match`, `If-None-Match`, `If-Modified-Since` and `If-Unmodified-Since` are evaluated on GET and HEAD in RFC 9110 13.2.2 order; a 304 carries the ETag and the stored Cache-Control and Expires. 3 passes gained: `test_get_object_ifnonematch_good` (304), `test_get_object_ifmodifiedsince_failed` (304) and `test_get_object_ifunmodifiedsince_good` (412), as forecast; the results file differs from the previous one in those three lines only. The three passing conditional GET tests on the incidental list stay there: they pass on a copy of the server from before this change as well (26 stays 26). Recorded at the commit that adds the evaluation. |
| `If-Range` on GET | 330 | 508 | 166 | A `Range` is applied when `If-Range` holds (strong entity tag, or a date equal to `Last-Modified` once the object is a second old) and ignored otherwise (200, whole object). No outcome changed, and none was expected to: no selected test sends `If-Range`; the results file is identical to the previous one. Recorded at the commit that adds the evaluation. |
| Conditional PutObject | 330 | 508 | 177 | `If-Match` and `If-None-Match` on PutObject are evaluated inside the commit transaction against the row it replaces (412 PreconditionFailed, or 404 NoSuchKey for `If-Match` with no object); `Last-Modified` is now assigned inside that commit. 11 passes gained, exactly the 11 forecast: `test_put_object_ifmatch_good`, `_failed`, `_overwrite_existed_good` and `_nonexisted_failed`; `test_put_object_ifnonmatch_good`, `_failed`, `_nonexisted_good` and `_overwrite_existed_failed`; `test_atomic_conditional_write_1mb`; `test_atomic_dual_conditional_write_1mb`; and `test_put_object_if_match`. The results file differs from the previous one in those 11 lines only. None is incidental (26 stays 26): each failed with 501 before. Still failing, as forecast: the six that need versioning or CompleteMultipartUpload (`test_put_current_object_if_match`, `test_put_current_object_if_none_match`, `test_put_object_current_if_match`, and the three `test_multipart_put_*if_*match`). Recorded at the commit that adds the evaluation. |
| Conditional CompleteMultipartUpload | 330 | 508 | 178 | `If-Match` and `If-None-Match` on CompleteMultipartUpload are evaluated inside the Complete commit transaction against the row it replaces, with the same rules as PutObject (412 PreconditionFailed, or 404 NoSuchKey for `If-Match` with no object); a refused Complete leaves the upload open with all its parts. 1 pass gained, as forecast: `test_multipart_put_object_if_match`. The results file differs from the previous one in that one line only. It is not incidental (26 stays 26): it failed with 501 before. Still failing, as forecast: `test_multipart_put_current_object_if_match` and `test_multipart_put_current_object_if_none_match`, which enable versioning first (501) and read `VersionId`. No other selected test sends `If-Match` or `If-None-Match` on Complete. Recorded at the commit that adds the evaluation. |
| Complete body validation | 330 | 508 | 179 | CompleteMultipartUpload no longer trusts its body. The part list was sorted and assembled as written, so a part number listed twice with the right ETag was assembled twice (size and composite ETag counted it twice) and parts out of order were silently sorted: a silent correctness bug, which this document had wrongly described as InvalidPart. Now an empty, non-UTF-8, malformed or wrong-document body, a Part without PartNumber or ETag and a non-integer PartNumber are 400 MalformedXML (were 500 or accepted); a list that is not strictly ascending, repeats included, is 400 InvalidPartOrder; a PartNumber outside 1 to 10000 is 400 InvalidPart; all decided before a blob is registered. Choices: MalformedXML for an empty body (MissingRequestBodyError is the alternative), the error order NoSuchUpload, MalformedXML, InvalidPartOrder, InvalidPart, then the conditions, InvalidPart for an out of range number, and EntityTooSmall not enforced. 1 pass gained, as forecast: `test_multipart_upload_empty`. The results file differs from the previous one in that one line only. It is not incidental (26 stays 26): it failed with 500 before. Still failing, as forecast: `test_multipart_upload_size_too_small` (EntityTooSmall, not implemented) and `test_multipart_resend_first_finishes_last` (lists part 1 twice, was InvalidPart, now InvalidPartOrder). Recorded at the commit that adds the validation. |
| Conditional DeleteObject | 330 | 508 | 179 | `If-Match` on DeleteObject is evaluated inside the delete's commit transaction against the ETag of the row it deletes: 412 PreconditionFailed when it does not match (strong comparison; `*`, quoted, unquoted and lists as for PutObject; a weak tag never matches), and the object, its row and its blob stay as they were. On a key that does not exist it is 404 NoSuchKey, `*` included, and nothing changes; an unconditional delete of a missing key stays 204 ([AWS, "How to perform conditional deletes"](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.md)). `x-amz-if-match-last-modified-time` and `x-amz-if-match-size` stay 501, and so does `If-None-Match`. 409 is never emitted. No pass gained, as forecast: the one test that exercises the feature, `test_delete_object_if_match`, expects 204 on a missing key and fails on purpose (see "Conditional DeleteObject" under Known gaps). The results file is identical to the previous one. None of the 18 conditional delete tests passes: 12 enable versioning (501), `test_delete_object_if_match_last_modified_time` and `_size` send the directory bucket headers (501, deliberate), and the three non-versioned `test_delete_objects_*` need per key conditions (a later task). Recorded at the commit that adds the evaluation. |
| Conditional DeleteObjects | 330 | 508 | 180 | The `ETag` element of a DeleteObjects `Object` is evaluated per key, inside the one transaction that deletes the batch, after the write lock and against that key's current row (`*`, quoted, unquoted and lists, strong comparison, as for DeleteObject). A condition that holds puts the key in `Deleted`; one that fails puts it in `Error` with Code PreconditionFailed and leaves the key untouched; one on a key that does not exist is an `Error` with Code NoSuchKey ([AWS, "How to perform conditional deletes"](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.md)). A refused key does not roll back the others. An empty `ETag` is no condition. `LastModifiedTime` and `Size` stay a per key NotImplemented, deliberately. 1 pass gained, as forecast: `test_delete_objects_if_match`. The results file differs from the previous one in that one line only. It is not incidental (26 stays 26): it fails against the server as of 9dcfba5, where its first request gets NotImplemented, and the new tests fail with this change reverted. Still failing, as forecast: the other 8 `test_delete_objects_*` tests, on purpose (see "Conditional DeleteObjects" under Known gaps). Recorded at the commit that adds the evaluation. |
| CopyObject | 330 | 508 | 193 | A PUT with `x-amz-copy-source` and no query copies the source's bytes into a new blob and commits the destination like PutObject (see "CopyObject" under Known gaps). UploadPartCopy, a `versionId` other than `null`, an ARN source and a REPLACE of the tag set with tags stay 501. 13 passes gained, as forecast, test by test: `test_object_copy_zero_size`, `_16m`, `_same_bucket`, `_diff_bucket`, `_verify_contenttype`, `_to_itself`, `_to_itself_with_metadata`, `_retaining_metadata`, `_replacing_metadata`, and `test_copy_object_ifmatch_good`, `_ifmatch_failed`, `_ifnonematch_good`, `_ifnonematch_failed`. The results file differs from the previous one in those 13 lines only (26 lines, 13 out and 13 in), and the denominator stays 330. All 13 are genuine (26 incidental and 18 borderline stay, so 149 genuine): each failed before with 501, and the three that expect an error pass only because the check exists: against a server with the source conditions and the self-copy refusal switched off, `test_object_copy_to_itself`, `test_copy_object_ifmatch_failed` and `test_copy_object_ifnonematch_good` fail. Still failing, as forecast: the 7 `test_multipart_copy_*` tests (UploadPartCopy), the 2 versioned copy tests (versioning) and the 2 `test_object_copy_not_owned_*` tests (a second user, and PutObjectAcl). `test_upload_part_copy_percent_encoded_key`, `test_object_copy_bucket_not_found` and `test_object_copy_key_not_found` still pass, incidentally. Recorded at the commit that adds CopyObject. |
| UploadPartCopy | 330 | 508 | 199 | A PUT with `x-amz-copy-source`, `partNumber` and `uploadId` copies a range of the source's bytes into a new part blob and records it like UploadPart (see "UploadPartCopy" under Known gaps). A `versionId` other than `null`, an ARN source and the `x-amz-copy-source-server-side-encryption-customer-*` headers (on this and on CopyObject) are 501. 6 passes gained, as forecast, test by test: `test_multipart_copy_small`, `_without_range`, `_special_names`, `_multiple_sizes`, `_invalid_range` and `_improper_range`. The results file differs from the previous one in those 6 lines only (12 lines, 6 out and 6 in), and the denominator stays 330. All 6 are genuine (26 incidental and 18 borderline stay, so 155 genuine): each failed before with 501, and the two that expect an error (`_invalid_range`, `_improper_range`) pass only because the range is parsed and checked against the source's size. Still failing, as forecast: `test_multipart_copy_versioned` (it enables versioning first, 501). `test_upload_part_copy_percent_encoded_key` still passes, now with 404 NoSuchKey (its copy source names `anyfilename%25.txt`, which decodes once to the key `anyfilename%.txt`, and there is none) instead of 501, and stays in the incidental list because a server that answers 501 passes it too. The CLI's copy of an object above its multipart threshold still fails, at GetObjectTagging (501), before it sends an UploadPartCopy. Recorded at the commit that adds UploadPartCopy. |
| Refuse `x-amz-tagging` | 330 | 508 | 199 | PutObject and CreateMultipartUpload answer 501 to a non-empty `x-amz-tagging` instead of dropping it (see "`x-amz-tagging` on PutObject and CreateMultipartUpload" under Known gaps). No outcome changed, and none was expected to: no selected test sends the header, because the filter deselects every test marked `tagging` and the lifecycle and policy tests that set tags. The results file is identical to the previous one. Recorded at the commit that adds the refusal. |
| GetObjectTagging, always empty | 330 | 508 | 199 | `GET /{bucket}/{key}?tagging` answers 200 with an empty TagSet (see "GetObjectTagging" under Known gaps). It is an answer from a store that holds no tags, not tagging support: PUT and DELETE `?tagging` and every bucket tagging request stay 501. No outcome changed, and none was expected to: no selected test calls `get_object_tagging`, `put_object_tagging` or `delete_object_tagging` or sends `?tagging` (checked by parsing every function in `test_s3.py` for those calls; the 13 selected tests that reach `_multipart_upload`, whose `tagging` parameter they never pass, are the only other matches), because the filter deselects the tests whose names say tagging. The results file is identical to the previous one. The AWS CLI's s3 to s3 copy of a 40 MiB object now succeeds (see "AWS CLI"). |
| Bucket owner condition | 330 | 508 | 199 | `x-amz-expected-bucket-owner` and `x-amz-source-expected-bucket-owner` are compared with `S3_OWNER_ID` in the request guard; a mismatch is 403 AccessDenied (see "Bucket owner condition" under Known gaps). No outcome changed: the only selected test that sends the header, `test_expected_bucket_owner`, fails first at `put_bucket_acl`, which is 501. |
| Refuse unsupported feature headers | 330 | 508 | 199 | Server-side encryption, SSE-C, website redirect, Object Lock and storage class headers on PutObject, CreateMultipartUpload, CopyObject, UploadPart and UploadPartCopy, and `x-amz-bucket-object-lock-enabled` and `x-amz-bucket-namespace` on CreateBucket, answer 501 instead of being dropped (see "Feature headers Litmus refuses" under Known gaps). No outcome changed, and none was expected to: no selected test sends any of them, and the results file is identical to the previous one. The 11 selected tests whose source mentions a storage class are the five GetObjectAttributes tests, which ask for `StorageClass` in `ObjectAttributes` (not the header; the operation is 501), and the six skipped cloud tests, which skip before sending anything. |
| ACLs disabled | 330 | 508 | 194 | `x-amz-acl`, the `x-amz-grant-*` headers and `x-amz-object-ownership` are no longer accepted and dropped: Litmus behaves as a bucket with Object Ownership set to BucketOwnerEnforced (see "ACL and grant headers" under Known gaps). 5 passes lost, on purpose, exactly as forecast: `test_object_raw_get`, `test_object_raw_get_bucket_gone`, `test_object_raw_get_object_gone`, `test_object_delete_key_bucket_gone` and `test_object_anon_put_write_access`, because CreateBucket with `public-read` or `public-read-write` is 400 InvalidBucketAclWithObjectOwnership. The results file differs from the previous one in those 5 lines only. `test_object_raw_response_headers` stays passed (`private` is accepted). The other ACL tests stay failed, some at an earlier step: `test_create_bucket_bucket_owner_preferred` and `test_create_bucket_object_writer` now fail at CreateBucket (501), where they failed at GetBucketOwnershipControls (501) before. Of the 5, `test_object_raw_get` and `test_object_anon_put_write_access` were incidental and the other three borderline, so the figures are 24 incidental, 15 borderline and 155 genuine (genuine unchanged). Recorded at the commit that adds the handling. |
| Complete size and checksum headers | 330 | 508 | 194 | CompleteMultipartUpload verifies `x-amz-mp-object-size` (400 InvalidRequest on a mismatch with the total of the listed parts, 400 InvalidArgument on a malformed value) and refuses the `x-amz-checksum-*` headers and `x-amz-checksum-type` with 501 (see "Checksum and size headers on CompleteMultipartUpload" under Known gaps). No outcome changed, and none was expected to: no selected test sends any of these on Complete. The tests that send a checksum header there (`test_multipart_checksum_sha256`, `test_multipart_reupload_checksum_and_etag` and the five `test_multipart_use_cksum_helper_*` tests) carry the `checksum` marker, which the `-k` filter deselects, no ceph test sends `x-amz-mp-object-size` (`MpuObjectSize`) or `ChecksumType` on Complete, and boto3 sends neither that header nor a checksum header on Complete by default. The results file is identical to the previous one. Recorded at the commit that adds the checks. |
| Checksum trailers | 330 | 508 | 194 | The aws-chunked trailers `x-amz-checksum-crc32`, `-crc32c`, `-sha1` and `-sha256` are decoded and verified like the CRC64NVME one, on PutObject and UploadPart. No outcome changed, and none was expected to: no selected test sends an aws-chunked body (boto3 over `http://` sends a checksum header), and the results file is identical to the previous one. Recorded at the commit that adds the trailers. |
| Minimum part size | 330 | 508 | 195 | CompleteMultipartUpload answers 400 EntityTooSmall when a listed part other than the last listed one is stored smaller than `S3_MIN_PART_SIZE` (5 MiB by default; see "The minimum part size" under Known gaps). 1 pass gained, exactly as forecast: `test_multipart_upload_size_too_small`, which uploads ten parts of 10 KiB and expects 400 EntityTooSmall. The results file differs from the previous one in that one line only. No other selected test changed outcome, and none was expected to: every selected test that completes a multipart upload uses parts of 5 MiB or more except the last, or has a single part, or fails before the check (the 1 byte and 8 byte parts of `test_multipart_upload_small`, `test_multipart_resend_first_finishes_last` and the conditional tests are single part uploads). Recorded at the commit that adds the check. |
| Schema version 3 | 330 | 508 | 195 | The schema becomes version 3 and a version 2 database is migrated in place at startup, after the blob directory lock and the store ID check (docs/design/storage-layout.md decision 28 and §20). The new columns are not read or written yet. No outcome changed, and none was expected to: the results file is identical to the previous one (195 passed, 128 failed, 7 skipped, of 330 selected, 508 deselected). |
| PutObject stores its checksum | 330 | 508 | 195 | PutObject stores the checksum of the object (the claimed algorithm, else CRC64NVME) in the object row and returns `x-amz-checksum-<algorithm>` and `x-amz-checksum-type: FULL_OBJECT` (see "Checksums"). CompleteMultipartUpload and CopyObject leave the columns NULL. No outcome changed, and none was expected to: GetObject and HeadObject return no checksum yet, and the selected tests that read a checksum back or send `x-amz-checksum-algorithm` carry the `checksum` marker or need it on Get. The results file is identical to the previous one (24 incidental, 15 borderline and 156 genuine stay). Recorded at the commit that adds the storage. |
| Checksum mode on GET and HEAD | 330 | 508 | 195 | GetObject and HeadObject return the stored checksum (`x-amz-checksum-<algorithm>` and `x-amz-checksum-type`) on a whole object 200 when the request sends `x-amz-checksum-mode: ENABLED` (see "Checksums"). No outcome changed, as forecast: the selected tests that send the mode or read a checksum back carry the `checksum` marker, and botocore's own validation of a whole object GET passes, since the stored value is the digest of the stored bytes. The results file is identical to the previous one (`git diff -U0` is empty; 24 incidental, 15 borderline and 156 genuine stay). The count of passing tests that received a checksum header was not measured: the server log does not record response headers. Recorded at the commit that adds the mode. |
| Multipart checksums on Create, UploadPart and UploadPartCopy | 330 | 508 | 195 | CreateMultipartUpload stores and echoes `x-amz-checksum-algorithm` and `x-amz-checksum-type`; UploadPart stores the checksum of the upload's effective algorithm with the part and echoes the claim's header; UploadPartCopy stores the checksum and time and returns `Checksum<algorithm>` for a named upload and the stored `LastModified` (decision 31; see "Checksums"). No outcome changed, as forecast: the 20 passing multipart tests create unnamed uploads and boto3 sends CRC32 on each UploadPart, so each is a claim in another algorithm on an unnamed upload, which is verified and answered with 200. The results file is identical to the previous one (`git diff -U0` is empty; 24 incidental, 15 borderline and 156 genuine stay). Recorded at the commit that adds the multipart checksums. |
| Complete checksums | 330 | 508 | 195 | CompleteMultipartUpload verifies the Part `Checksum*` elements and the `x-amz-checksum-*` and `x-amz-checksum-type` headers against the stored part checksums and the upload's algorithm and type, stores the object's checksum (COMPOSITE with a "-N" suffix, FULL_OBJECT, or CRC64NVME FULL_OBJECT for an upload that named no algorithm) and returns `Checksum<ALG>` and `ChecksumType` (see "Checksum and size headers on CompleteMultipartUpload" under Known gaps). The 501 for the Complete checksum headers is gone. No outcome changed, and none was expected to: no selected test sends a Part checksum element or a Complete checksum header, all of them create unnamed uploads (so gaps stay allowed), and the 7 passing tests that read a multipart object whole (`test_multipart_copy_multiple_sizes`, `_small`, `_special_names`, `_without_range`, `test_multipart_upload_contents`, `test_multipart_upload_overwrite_existing_object` and `test_multipart_upload_resend_part`) now have botocore validate the combined CRC64NVME, and still pass. The results file is identical to the previous one (24 incidental, 15 borderline and 156 genuine stay). Recorded at the commit that adds the verification. |
| CopyObject checksums | 330 | 508 | 195 | CopyObject hashes the bytes it copies and stores the checksum, algorithm and type FULL_OBJECT with the destination row, in the algorithm `x-amz-checksum-algorithm` names, else the source's, else CRC64NVME, and returns `ChecksumType` and `Checksum<algorithm>` in `CopyObjectResult` (see "Checksums", decision 33). SHA512, MD5 and the XXHash names are 501; an unknown name is 400 InvalidArgument. No outcome changed, as forecast: the 10 selected tests that copy with CopyObject or `client.copy` and read the copy back (`test_object_copy_zero_size`, `_16m`, `_same_bucket`, `_diff_bucket`, `_verify_contenttype`, `_to_itself_with_metadata`, `_retaining_metadata`, `_replacing_metadata`, `test_copy_object_ifmatch_good` and `_ifnonematch_failed`) now have a whole object GET that botocore validates against the copy's stored checksum, and still pass; the tests that expect an error, the UploadPartCopy tests and the versioned and second user tests are unchanged. The results file is identical to the previous one (24 incidental, 15 borderline and 156 genuine stay). Recorded at the commit that adds the checksum to CopyObject. |
| Upload bucket and key | 330 | 508 | 195 | UploadPart, UploadPartCopy, CompleteMultipartUpload and AbortMultipartUpload check that the upload belongs to the request's bucket and key (decision 34; see "An upload belongs to one bucket and key" under Known gaps): a missing bucket is 404 NoSuchBucket, another bucket or key is 404 NoSuchUpload. Complete through a missing bucket, which was 500 InternalError, is now NoSuchBucket (fixed). No outcome changed, as forecast: the failing multipart tests fail for other reasons (the re-complete NoSuchUpload, InvalidPartOrder for a repeated part, `PartsCount`, 501 for versioning and ListMultipartUploads), and the passing ones use the upload's own bucket and key. The results file is identical to the previous one (`git diff -U0` is empty; 24 incidental, 15 borderline and 156 genuine stay). Recorded at the commit that adds the check. |
| Retried Complete decided (decision 35) | 330 | 508 | 195 | A Complete on a completed upload is 404 NoSuchUpload by decision 35, no longer a gap: j3 is closed as conformant and its strict xfail is an ordinary test. `test_multipart_upload` and `test_multipart_upload_small`, which send an extra Complete, move from that gap to the ceph tests that fail on purpose (checked once in a scratch copy with the extra Complete removed: both pass, not counted). No outcome changed, as forecast: no production code changed, so the results file is not regenerated and the figures stay 195 passed, 128 failed, 7 skipped. Recorded at the commit that makes the decision. |
| ListParts (decision 36) | 330 | 508 | 195 | `GET /{bucket}/{key}?uploadId=` lists the parts of an open upload (see "ListParts" under Known gaps), so it is no longer 501; it refuses the three SSE-C headers with 501, as UploadPart does. No outcome changed, as forecast: the only selected tests that reach `list_parts` are the six object ownership tests that call the helper `get_multipart_acl_owner`, and each fails earlier on an operation that is still 501 or refused (details under "ListParts"). The results file is identical to the previous one (`git diff -U0` is empty; the denominator stays 330, and 24 incidental, 15 borderline and 156 genuine stay). The category table is unchanged. Recorded at the commit that adds the operation. |
| Decimal numbers decided (decision 37) | 330 | 508 | 195 | Every decimal number in a request is read by `parse_decimal` (see "Decimal numbers in a request"): a value over 4300 digits in `max-keys`, `x-amz-copy-source-range` or `x-amz-decoded-content-length` is no longer a 500, and an oversized value is answered as an out of range one. No outcome changed, as forecast: the selected tests that send an unusual value to these sites are `test_bucket_list_maxkeys_invalid` (`blah`, still 400 InvalidArgument), `test_multipart_copy_invalid_range` (`bytes=0-21` on 5 bytes, still 416 InvalidRange) and `test_multipart_copy_improper_range` (six malformed ranges, still 400 InvalidArgument); none sends a long numeral, a sign or padding. The Range tests are unaffected, as Range keeps its rule. 195 passed, 128 failed, 7 skipped of 330, unchanged. |

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
is authenticated or owned. By that rule, **24 of the 195 passes are incidental**.
Of the 195 passes, 24 are incidental and 15 are borderline, which leaves 156 genuine.
The pass gained with the minimum part size, `test_multipart_upload_size_too_small`, is
genuine: against a server that does not enforce the minimum the Complete succeeds and the
test fails.
Disabling ACLs took 2 incidental and 3 borderline passes off the lists (see "ACL and
grant headers" under Known gaps): 26 and 18 before.

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
- Conditional GET, the header changes nothing and the expected answer is the plain
  object (3): `test_get_object_ifmodifiedsince_good` (a date in the past),
  `test_get_object_ifnonematch_failed` (an ETag that does not match),
  `test_get_object_ifunmodifiedsince_failed` (a date in the future). They stay on the
  list now that the three headers are evaluated: they pass whether the header is
  ignored or evaluated (run for this change against a copy of the server from before
  it: 5 of the 8 conditional GET tests pass there, these 3 among them).
- Copy (3): `test_object_copy_key_not_found` and `test_object_copy_bucket_not_found`
  get their 404 from the client's HeadObject on the missing source, and no copy
  request reaches Litmus; `test_upload_part_copy_percent_encoded_key` expects
  UploadPartCopy to fail, and it gets 404 NoSuchKey (it passed with 501 before
  UploadPartCopy was implemented). It still passes against a server that answers 501,
  so it stays on this list, but only a server that decodes the copy source once, and no
  more, gives it the 404: the key it names, `anyfilename%.txt`, is not the stored
  `anyfilename%25.txt`.
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
- Ownership and authentication (1): `test_bucket_create_exists_nonowner` expects 409
  BucketAlreadyExists for a second user, which Litmus answers to every user.
  `test_object_anon_put_write_access` and `test_object_raw_get` were on this list while
  the `x-amz-acl` header was ignored; they now fail, because Litmus refuses the public
  ACL they set.

Borderline, not counted above (15):

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
- Three tests were listed here while the ACLs were ignored (the requests are anonymous,
  but the asserted 404 error codes are implemented): `test_object_raw_get_bucket_gone`,
  `test_object_raw_get_object_gone` and `test_object_delete_key_bucket_gone`. They now
  fail with the public ACL refused, so none is borderline any more.

## Results by category

Counts are grouped by test name. "Why the rest fail" gives the cause seen in the test
output; "501" means Litmus answers 501 NotImplemented for that request.

| Category | Selected | Passed | Failed | Skipped | What passes; why the rest fail |
|---|---|---|---|---|---|
| Bucket operations | 17 | 9 | 7 | 1 | Create, delete, head, list buckets, deleting a non-empty bucket refused, missing-bucket errors. Fail: re-creating a bucket you own expects success or BucketAlreadyOwnedByYou, and Litmus answers 409 BucketAlreadyExists (2 tests). That answer is deliberate: telling the owner apart needs authentication, which is out of scope; RGW-specific usage and statistics headers (3); ListBuckets ignores MaxBuckets and ContinuationToken (1); one test calls PutObjectAcl, which is 501 (1). |
| Bucket naming | 18 | 16 | 2 | 0 | Valid names are accepted; names that break the AWS rules (length, characters, edges, adjacent periods, IP format, reserved prefixes and suffixes) get 400 InvalidBucketName. Fail: `test_bucket_create_naming_dns_dot_dash` (`foo.-bar`) and `test_bucket_create_naming_dns_dash_dot` (`foo-.bar`) expect 400, and AWS's current rules allow both names, so Litmus accepts them (see Known gaps). |
| Object operations | 28 | 23 | 5 | 0 | PutObject, GetObject, HeadObject, DeleteObject, ETags, overwrites, zero-byte objects, standard chunked transfer encoding, `Content-Encoding` with its `aws-chunked` token removed, the `response-*` header overrides on GET, the atomic read and write tests, and the request ID in an error body matching the `x-amz-request-id` header. Fail: a key that is not valid UTF-8 gets 404 instead of 400; `test_object_raw_get`, `test_object_raw_get_bucket_gone`, `test_object_raw_get_object_gone` and `test_object_delete_key_bucket_gone` create a bucket with `public-read` through `x-amz-acl`, which Litmus refuses on purpose (400 InvalidBucketAclWithObjectOwnership: see "ACL and grant headers" under Known gaps); they also need anonymous access, which is out of scope. `test_object_raw_response_headers` still passes, because it sends `private`. |
| User metadata and stored headers | 6 | 5 | 1 | 0 | `Cache-Control`, `Expires` and `x-amz-meta-*` are stored and returned (see "System headers" and "User metadata" under Known gaps). Fail: `test_object_set_get_unicode_metadata` expects a non-ASCII value back as the raw string, and Litmus returns it RFC 2047 encoded, as the AWS documentation describes (see "User metadata"). |
| Conditional GET | 8 | 8 | 0 | 0 | `If-Match`, `If-None-Match`, `If-Modified-Since` and `If-Unmodified-Since` are evaluated on GET and HEAD. 3 of the 8 passes are incidental (see above): they pass with the headers ignored. |
| Range GET | 6 | 6 | 0 | 0 | Single, open-ended and suffix ranges, a last byte past the end clamped; an unsatisfiable range, on an object with bytes or on an empty one, gets 416 `InvalidRange` with the standard error body. |
| ListObjects (V1) | 41 | 39 | 2 | 0 | `prefix`, `delimiter` and common prefixes, `max-keys` (default, cap, 0, InvalidArgument), `marker`, `NextMarker`, truncation, `encoding-type=url`. Fail: `allow-unordered`, an RGW extension, is not implemented (1); one test calls GetObjectAcl, which is 501 (1). |
| ListObjectsV2 | 39 | 38 | 1 | 0 | `prefix`, `delimiter` and common prefixes, `max-keys`, `continuation-token`, `start-after`, `KeyCount`, `fetch-owner`, truncation, `encoding-type=url`. Fail: `allow-unordered`, an RGW extension, is not implemented (1). |
| DeleteObjects | 4 | 4 | 0 | 0 | Bulk delete, and the limit of 1000 Objects (400 MalformedXML for more). The per key `ETag` tests are under Conditional delete. |
| Multipart upload | 14 | 11 | 3 | 0 | Create, upload part, complete, abort, wrong ETag and missing part errors, an empty Complete body (400 MalformedXML), overwriting an existing object, and the Content-Type, system headers and user metadata given to CreateMultipartUpload reaching the object (see "Multipart upload headers" under Known gaps). Fail: re-completing a completed upload gets NoSuchUpload, where S3 answers 200 with the same ETag (2); a Complete that lists a part number twice (`test_multipart_resend_first_finishes_last`, with two different ETags for part 1) is refused, now with InvalidPartOrder, where the test expects success (1). See "The Complete body" under Known gaps. The minimum part size is enforced (see "The minimum part size"), so `test_multipart_upload_size_too_small` passes. These 2 fail on purpose: decision 35 and "Two ceph tests send a second Complete" under Known gaps. |
| GetObject `partNumber` | 3 | 0 | 3 | 0 | Fail: `partNumber` is ignored and the whole object is returned. |
| ListMultipartUploads | 2 | 0 | 2 | 0 | Fail: 501 (one test fails earlier on PutBucketAcl, also 501). |
| GetObjectAttributes | 5 | 0 | 5 | 0 | Fail: 501 (one test fails earlier on PutBucketVersioning, also 501). |
| `Expect: 100-continue` | 2 | 1 | 1 | 0 | An early error response closes the connection. Fail: the other test expects 403 from a bucket protected by an ACL, and Litmus has no ACLs. |
| Conditional PutObject and Complete | 17 | 12 | 5 | 0 | `If-Match` and `If-None-Match` on PutObject are decided in the commit: `*`, entity tags quoted and unquoted, 412 and 404 (see "Conditional PutObject" under Known gaps), including the two atomic tests that overwrite while another PUT is in flight. `If-Match` and `If-None-Match` on CompleteMultipartUpload are decided in its commit the same way (`test_multipart_put_object_if_match`). Fail: the 5 others enable versioning first, which is 501. |
| Conditional delete | 18 | 1 | 17 | 0 | `If-Match` on DeleteObject is decided in the commit (see "Conditional DeleteObject" under Known gaps), but the one non-versioned test for it, `test_delete_object_if_match`, expects 204 on a missing key, which Litmus refuses on purpose (404 NoSuchKey, as AWS). The per key `ETag` of DeleteObjects is decided in the batch's commit (see "Conditional DeleteObjects" under Known gaps), and `test_delete_objects_if_match` passes. Fail: `x-amz-if-match-last-modified-time` and `x-amz-if-match-size` on DeleteObject and `LastModifiedTime` and `Size` in DeleteObjects are refused on purpose (501, or a per key NotImplemented); 12 of these tests enable versioning first, which is 501. |
| CopyObject and UploadPartCopy | 27 | 22 | 5 | 0 | CopyObject: copies within and across buckets, of 0 bytes and 16 MiB, with COPY and REPLACE of the metadata and Content-Type, onto itself (400 InvalidRequest, or 200 with REPLACE), and with source conditions (`x-amz-copy-source-if-match` and `-if-none-match`). UploadPartCopy: whole and ranged part copies of 1 byte to 10 MiB, completed into objects whose bytes equal the source's (`test_multipart_copy_small`, `_without_range`, `_special_names`, `_multiple_sizes`), a range past the end of the source (416 `InvalidRange`) and malformed ranges (400 `InvalidArgument`). Fail: `test_multipart_copy_versioned` enables versioning first (501); `test_object_copy_not_owned_bucket` expects 403 for a second user, and Litmus has no authentication; `test_object_copy_not_owned_object_bucket` calls PutObjectAcl (501); the 2 versioned copy tests enable versioning first (501). Of the 22 that pass, 3 are incidental: 2 expect 404 for a missing source and get it from the client's HeadObject before any copy request is sent; the third, `test_upload_part_copy_percent_encoded_key`, expects UploadPartCopy to fail and gets 404 NoSuchKey, which a server answering 501 would also satisfy. |
| Versioning, restore and read-through | 11 | 0 | 5 | 6 | Fail: PutBucketVersioning is 501, and DeleteObject sends no `x-amz-delete-marker` header. Skipped by s3-tests: 6. |
| Anonymous and ACL-based access | 19 | 0 | 19 | 0 | Fail: requests are not authenticated, so anonymous requests are accepted, and ACL operations are 501. `test_object_anon_put_write_access`, the one pass before, now fails on purpose: it creates a bucket with `public-read` through `x-amz-acl`, which Litmus refuses (400 InvalidBucketAclWithObjectOwnership), because AWS refuses a public ACL on a bucket with the default Object Ownership setting, and it also needs anonymous access, which is out of scope. |
| POST Object (browser form upload) | 27 | 0 | 27 | 0 | Fail: 501. |
| Presigned URLs (`X-Amz-Expires`) | 5 | 0 | 5 | 0 | Fail: signatures and expiry are not checked, so expired presigned requests are served (3). The other 2 first send `OPTIONS` to the presigned URL and expect 400; `OPTIONS` is S3's CORS preflight, which Litmus does not implement, so it answers 501 NotImplemented (2). |
| Ownership controls and public access block | 13 | 0 | 13 | 0 | Fail: 501. |
| **Total** | **330** | **195** | **128** | **7** |

## AWS CLI

`scripts/aws_cli_smoke.sh` runs an unmodified AWS CLI v2 against a scratch Litmus
server and checks the results. It was last run with aws-cli 2.38.5 (Linux, installed
outside `/mnt/`); the script refuses versions older than 2.23. It sets no checksum,
transfer client or s3 config overrides, uses temporary dummy credentials, and passes
`--endpoint-url http://127.0.0.1:PORT`.

The script has 11 steps: `mb` and `ls`, `cp` round trip, `s3api` put and get, key names,
`sync`, `s3api` head and range, one CopyObject, a copy in parts, `cp
--checksum-algorithm`, `get-object` and `head-object` with `--checksum-mode ENABLED`, and
`rm` and `rb`. All 11 passed.

Verified, with content checks and not only exit codes:

- `s3 mb`, `s3 ls` (buckets, a prefix, and `--recursive`) with keys containing a space,
  a `+`, a non ASCII character and nested `dir/` prefixes, names printed exactly
- `s3 cp` up and down of a 1 KiB file and a 40 MiB file (multipart, above the 8 MiB
  threshold), sha256 compared
- `s3api put-object`, `s3api get-object`, `s3api head-object`, and `s3api get-object
  --range` on the 40 MiB object, bytes compared
- `s3 sync` of 20 files up, a second sync that transfers nothing, and a sync down into
  an empty directory, compared with `diff -r`
- `s3 cp s3://b/small.bin s3://b/copied.bin` (one CopyObject, under the 8 MiB
  threshold): bytes compared after a download, the copy's ETag equal to the source's,
  the copy listed. The same copy of the 40 MiB object (step 9) succeeds: above the
  multipart threshold the CLI reads the source's tags first (`HEAD`, then `GET
  ?tagging`, answered with an empty TagSet), then sends CreateMultipartUpload, five
  UploadPartCopy requests and CompleteMultipartUpload. The destination's bytes equal the
  source's (sha256 and `cmp`), and its Content-Type, user metadata, size and ETag equal
  the source's. Until GetObjectTagging was answered, this copy failed with NotImplemented
  at that call and the CLI never sent an UploadPartCopy. boto3's managed copy of a 20 MiB
  object is also tested in `tests/conformance/test_upload_part_copy_boto3.py`.
- `s3 cp --checksum-algorithm CRC32`, `CRC32C`, `SHA1` and `SHA256`, up and down, of a
  1 KiB file (one PutObject) and a 20 MiB file (three UploadParts of 8, 8 and 4 MiB),
  bytes compared. The CLI sends each checksum as an aws-chunked trailer, which Litmus
  decodes and verifies.
- Downloads: the CLI sets `x-amz-checksum-mode` on GET, so a whole object download of an
  object written by PutObject or CopyObject is validated against its stored checksum, an
  object written by CompleteMultipartUpload has one too (a COMPOSITE value is not the
  checksum of the body, and botocore does not compare it with the body), and a ranged
  download carries none.
- `s3 rm` of one key, `s3 rm --recursive`, `s3 rb` of a non-empty bucket failing with
  BucketNotEmpty, and `s3 rb --force`

Limits of this check:

- Only `http://` was tested. Litmus does not serve TLS itself, and what the CLI sends
  over `https://` was not captured.
- Credentials are not checked: any access key and secret are accepted.
- `s3 sync` compares size and modification time. LastModified has one second
  resolution, so files modified in the same second they were uploaded can be uploaded
  again by the next sync; the script backdates its source files for this reason.

## Known gaps

**No authentication.** Litmus does not check credentials or signatures, and no
request is rejected for missing or invalid credentials.

**Unimplemented operations answer 501.** Requests for S3 operations Litmus does not
implement get 501 NotImplemented and change nothing. That includes ACLs, policies,
versioning configuration, tagging (except GetObjectTagging, below), CORS (including
`OPTIONS`, the CORS preflight),
lifecycle, POST Object, ListMultipartUploads and GetObjectAttributes. A request
with `x-amz-copy-source` that is not a plain PUT of a key (any other method, or a
query other than `x-id`) is 501 too.

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
  `LastModifiedTime` and `Size` are the same per-key NotImplemented, whatever the entry's
  `ETag` says; `ETag` is evaluated (see "Conditional DeleteObjects" under Known gaps). These
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
  STREAMING-UNSIGNED-PAYLOAD-TRAILER` (compared exactly) with `x-amz-trailer` naming
  one of the five trailers S3 lists (S3 User Guide, "Checking object integrity for data
  uploads", trailing checksum headers): `x-amz-checksum-crc32`, `-crc32c`,
  `-crc64nvme`, `-sha1` or `-sha256` (one entry, any letter case),
  `x-amz-decoded-content-length` and `Content-Length`. This is what the AWS CLI v2 sends
  over `http://`, with its default settings (CRC64NVME) and with `--checksum-algorithm`
  CRC32, CRC32C, SHA1 or SHA256 (captured with aws-cli 2.38.5; all four are framed like
  CRC64NVME, one chunk per part). The body is a lower case hex size line, the data, CRLF,
  any number of chunks, `0`, CRLF, one trailer line `x-amz-checksum-<algorithm>:<base64
  of the digest>`, CRLF, and a final CRLF, where the digest is the big endian CRC for
  CRC32, CRC32C and CRC64NVME (4, 4 and 8 bytes) and the SHA digest for SHA1 and SHA256
  (20 and 32 bytes). The decoded bytes are what is stored:
  the ETag is their MD5, and the framing and the trailer are not kept (PutObject keeps the checksum: see "Checksums" below).
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
  one entry or names anything but those five (so `x-amz-checksum-sha512`, `-md5` and
  the XXHash trailers are refused, though S3 lists those algorithms for headers).
  The AWS CLI v2 sends the supported framing over `http://` with its default settings
  (see "AWS CLI" above). A client can also avoid framing by setting
  `request_checksum_calculation = when_required`.
- Strictness: the size line is 1 to 32 hex digits (either case) and CRLF, with no sign,
  whitespace or chunk extension; every line ends in CRLF and a bare LF is an error;
  nothing may follow the final CRLF; the framed body must be exactly `Content-Length`
  bytes; the trailer is exactly one line whose name matches `x-amz-trailer` in any
  letter case; its value must be strict base64 of exactly the digest size of that
  algorithm. Reading stops as soon as the
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

**Checksums: verified on PutObject, UploadPart and DeleteObjects; stored by PutObject, UploadPart, UploadPartCopy and CompleteMultipartUpload, returned by PutObject, UploadPart, CompleteMultipartUpload and GetObject and HeadObject.** A PutObject
or UploadPart body is checked against the checksums its request claims, before the blob is
made durable, so a refused request leaves no object, part or file. Checked:

- `Content-MD5`: base64 of the 16 byte MD5 of the body. It may accompany one checksum,
  and both are verified.
- One of `x-amz-checksum-crc32`, `-crc32c`, `-crc64nvme`, `-sha1`, `-sha256`: base64 of
  the digest (4, 4, 8, 20 and 32 bytes), checked against the body.
- The trailer of a framed body (CRC32, CRC32C, CRC64NVME, SHA1 or SHA256), computed over
  the decoded bytes with the same calculators as the header form. On a framed body
  `Content-MD5` is checked against the decoded bytes, not the framed ones.
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

What PutObject stores and returns (schema version 3, `objects.checksum`,
`checksum_algorithm`, `checksum_type`; docs/design/storage-layout.md decision 29 and §20).
The 200 response carries `x-amz-checksum-<algorithm>`, in lower case, and
`x-amz-checksum-type: FULL_OBJECT`, and the same three values are committed in the
transaction that writes the object row, so a crash leaves both or neither. A refused
request (4xx, 412) carries neither. S3 API reference, PutObject: the `x-amz-checksum-crc64nvme`
response header "is present if the object was uploaded with the `CRC64NVME` checksum
algorithm, or if it was uploaded without a checksum (and Amazon S3 added the default
checksum, `CRC64NVME`, to the uploaded object)"; "For `PutObject` uploads, the checksum type
is always `FULL_OBJECT`"; and the response syntax lists `x-amz-checksum-crc32`, `-crc32c`,
`-crc64nvme`, `-sha1`, `-sha256` and `x-amz-checksum-type`. Choices, where the reference
says nothing:

- *The default.* A request with no checksum stores and returns CRC64NVME, computed over the
  bytes (the reference above). A request with only `Content-MD5` counts as having none.
- *A trailer is a checksum provided with the request.* The reference does not say how an
  aws-chunked trailer is treated; Litmus stores the trailer's algorithm, not CRC64NVME.
- *Canonical encoding.* The value stored and returned is the base64 of the digest Litmus
  computed, not the request's string. A claim whose base64 differs from the canonical one only
  in the unused trailing bits (which Litmus accepts as a valid claim of that digest) is
  answered with the canonical string. The reference says nothing about this.
- *`x-amz-checksum-algorithm` is ignored on PutObject.* It is not a PutObject request header
  in the reference (the request syntax has `x-amz-sdk-checksum-algorithm` and the
  `x-amz-checksum-<algorithm>` value headers only), so any value, valid or not, is neither
  checked nor an error; the algorithm stored follows the value header or the trailer.
- *An empty body* is checksummed like any other.
- *Overwrites replace the checksum*, with NULL when the new object has none.

UploadPart and UploadPartCopy store a checksum with the part (decision 31, below), and
CompleteMultipartUpload and CopyObject store the object's (decisions 32 and 33, below).
CopyObject returns its checksum in the CopyObjectResult body, not in headers. DeleteObjects
stores nothing.

**GetObject and HeadObject (decision 30).** A request with `x-amz-checksum-mode: ENABLED`
gets the stored checksum back on a whole object 200, as `x-amz-checksum-<algorithm>` in
lower case and `x-amz-checksum-type`. S3 API reference, GetObject and HeadObject: "To
retrieve the checksum, this mode must be enabled"; the valid value is `ENABLED`; the
response headers are `x-amz-checksum-crc32`, `-crc32c`, `-crc64nvme`, `-sha1`, `-sha256`
and `x-amz-checksum-type`; and, for GetObject, the checksum headers are "only present if
the checksum was uploaded with the object". Without the mode no checksum header is sent,
as before. The mode is parsed after the bucket check and before the key lookup, so the
order of errors on GET and HEAD is NoSuchBucket, InvalidArgument (a `response-*` override,
then the mode), NoSuchKey, then the conditions (412, 304). The reference says nothing about
the following, so each is Litmus's own choice:

- *Case.* The value is compared case insensitively after stripping, so `enabled` (which the
  CLI sends on GET) and `Enabled` are accepted. A blank or whitespace value counts as absent.
- *Other values.* Any other value is 400 InvalidArgument, which names the header and the
  value. The reference lists no error for it. A repeated header arrives joined with ", ", so
  `ENABLED, ENABLED` is refused.
- *A ranged response carries no checksum.* The reference does not say what a Range request
  returns. A 206 on GET, and on HEAD a Range that GET would answer with a 206, carry none:
  botocore compares the header with the body it receives, and the CLI downloads large
  objects as ranged GETs with the mode set, so a whole object checksum on a part of the
  bytes would fail every such download. A false `If-Range` makes the Range ignored and the
  response a whole 200, which carries the checksum. An unsatisfiable range stays 416 and
  carries none.
- *A 304 and a 412 carry none.* The 304 response is unchanged.
- *A row without a checksum returns none*, with or without the mode. These are the objects
  written before schema version 3 (a CopyObject of one writes a checksum for the copy), and a
  multipart object whose listed parts include one written before it. An object that a PutObject replaced has the new
  object's checksum.
- *`partNumber` is still ignored* (see "Multipart details" and the GetObject `partNumber`
  row above). A GET or HEAD with `?partNumber=N` and the mode returns the whole object and
  its whole object checksum, which match each other; it is not the checksum of a part.

The value is read from the row, not computed, so it is the checksum of the bytes as
PutObject stored them. botocore (1.36 and later) sets the mode on every GetObject by
default and validates a whole object body against the header, so boto3 now raises
`FlexibleChecksumError` when the stored checksum and the bytes differ.

**CreateMultipartUpload, UploadPart and UploadPartCopy (decision 31).** An upload is
*named* when CreateMultipartUpload carried a non-blank `x-amz-checksum-algorithm`. The
effective algorithm E is the named one, or CRC64NVME when the upload named none. A *claim*
is a checksum the UploadPart request sent, by `x-amz-checksum-<algorithm>` header or by
aws-chunked trailer. The upload row stores the algorithm and type
(`multipart_uploads.checksum_algorithm`, `checksum_type`), NULL in both for an unnamed
upload; each part row stores E, the base64 of its digest of the part's bytes and the time
of its commit (`multipart_parts.checksum`, `checksum_algorithm`, `last_modified`), in the
transaction that records the part, so a crash leaves the old part with its own checksum
and time or the new one with all three (docs/design/storage-layout.md decision 31 and §20).
Sources, from the S3 API reference and the S3 User Guide page "Checking object integrity for
data uploads in Amazon S3":

- *CreateMultipartUpload.* The request headers are `x-amz-checksum-algorithm` (`CRC32 |
  CRC32C | SHA1 | SHA256 | CRC64NVME | SHA512 | MD5 | XXHASH64 | XXHASH3 | XXHASH128`)
  and `x-amz-checksum-type` (`COMPOSITE | FULL_OBJECT`), and the response carries both
  headers. The user guide's table gives the allowed pairs for a multipart upload:
  CRC64NVME with FULL_OBJECT; CRC32 and CRC32C with either; SHA1 and SHA256 with
  COMPOSITE (the other five algorithms are composite only). A named upload answers with
  both headers in canonical upper case; the type it echoes is the type it stored.
- *UploadPart.* The reference for `x-amz-sdk-checksum-algorithm` says "This checksum
  algorithm must be the same for all parts and it match the checksum value supplied in the
  `CreateMultipartUpload` request", and each checksum response header "will only be present
  if the checksum was provided in the request". The response therefore carries the claim's
  header with its verified value, and only that.
- *UploadPartCopy.* API_CopyPartResult: each `Checksum<algorithm>` element is "present if
  the multipart upload request was created with" that algorithm, so CopyPartResult carries
  `Checksum<E>` for a named upload and no checksum element for an unnamed one.

What each request does, where the cases are Litmus's:

- A named upload, a claim in E: verified, stored, echoed. 200.
- A named upload, a claim in another algorithm: 400 InvalidRequest, decided from the
  header or trailer name after the claim syntax checks (including the Content-MD5
  decode) and before the body is read; nothing is registered or written.
- A named upload, no claim: E is computed and stored; no checksum header is returned.
- An unnamed upload, a CRC64NVME claim: verified, stored, echoed.
- An unnamed upload, a claim in another computed algorithm (boto3 sends CRC32 by default):
  the claim is verified, CRC64NVME is computed over the same bytes and stored, and only
  the claim's header is echoed.
- An unnamed upload, no claim: CRC64NVME is stored; no checksum header is returned.
- UploadPartCopy hashes the copied bytes in E in the copy loop and stores the value, the
  algorithm and the time; the source's own checksum is never used, so a source with none or
  with another algorithm is copied like any other. `LastModified` in CopyPartResult is the
  stored part time (UTC, whole seconds), no longer the time of the response.

Choices, where the reference says nothing (each is Litmus's own; "not found" in the
reference for every one):

- *The default types.* With only an algorithm, CRC64NVME gets FULL_OBJECT and CRC32,
  CRC32C, SHA1 and SHA256 get COMPOSITE. The reference names no default.
- *Refusals.* A disallowed algorithm and type pair, and a type with no algorithm, are 400
  InvalidRequest; a value that is not an AWS algorithm or type name is 400 InvalidArgument,
  naming the header. The reference documents none of these answers. A repeated header
  arrives joined with ", " and so is not a name. Every refusal creates no upload.
- *Matching.* Algorithm and type are compared case insensitively after stripping, and
  stored and returned in canonical upper case.
- *Blank counts as absent, for these two headers only.* A blank or whitespace
  `x-amz-checksum-algorithm` or `x-amz-checksum-type` on CreateMultipartUpload is absent,
  in the request guard and in the parser, as a blank `x-amz-checksum-mode` is absent on GET
  and HEAD. The claim headers keep their behavior: a blank `x-amz-checksum-<algorithm>`
  value on UploadPart stays 400 InvalidDigest.
- *The five uncomputed names are 501.* A non-blank `x-amz-checksum-algorithm` naming SHA512,
  MD5, XXHASH64, XXHASH3 or XXHASH128, in any letter case, is 501 NotImplemented in the
  request guard, beside the other 501 header refusals, before the bucket is looked up.
- *Nothing is echoed for an unnamed upload.* The reference does not say what S3 returns
  for an upload created without an algorithm, so Litmus echoes neither header, stores NULL,
  and treats the upload as CRC64NVME for its parts.
- *A checksum is computed for a part sent without one.* The reference says the response
  headers are present only when a checksum was provided, but not whether S3 stores one
  otherwise; Litmus stores E, as PutObject stores CRC64NVME for a request with none.
- *A claim in another algorithm on a named upload is 400 InvalidRequest.* The reference
  says only that the algorithm "must be the same for all parts"; the error is a choice. The
  same claim on an unnamed upload is accepted, because there is no named algorithm to
  differ from, and it is not stored.
- *UploadPartCopy ignores request checksum headers.* `x-amz-checksum-<algorithm>` and
  `x-amz-sdk-checksum-algorithm` stay ignored on UploadPartCopy because API_UploadPartCopy
  defines no request checksum headers (not found on that page); the five uncomputed
  algorithm names stay 501 in the request guard, as on every PUT with `partNumber` and
  `uploadId`.
- *The part time is the commit time*, taken under the write lock, for UploadPart and
  UploadPartCopy alike; a re-upload replaces it. A part whose bytes Complete released
  keeps no time that is read.

**CompleteMultipartUpload (decision 32).** It verifies the Part `Checksum*` elements and the
checksum headers against the stored part checksums and the upload's algorithm and type, and
stores the object's checksum, algorithm and type with the completed state, in one
transaction. The rules, the choices and what the documentation leaves unsaid are under
"Checksum and size headers on CompleteMultipartUpload" below.

**CopyObject (decision 33).** CopyObject hashes the bytes it copies, in the writer loop that
writes the new blob, and stores the digest, the algorithm and the type FULL_OBJECT with the
destination row, in the same commit as the rest of the row (`objects.checksum`,
`checksum_algorithm`, `checksum_type`). An overwrite replaces all three. The destination's
algorithm is, in this order: the non-blank `x-amz-checksum-algorithm` header, compared case
insensitively and stored in upper case; else the source row's algorithm; else CRC64NVME (a
source written before schema version 3 has none). `CopyObjectResult` carries `ETag`,
`LastModified`, `ChecksumType` and `Checksum<ALG>`, in the order API_CopyObject gives, and
GET and HEAD return the value with the mode as for any object. A name that is not one of the
ten AWS names is 400 InvalidArgument naming the header, decided with the other header syntax
errors (after the directives, before the REPLACE headers and the source lookup, so an
unknown name beats NoSuchKey); SHA512, MD5, XXHASH64, XXHASH3 and XXHASH128 are 501 in the
request guard, beside the other 501 header refusals. Sources: S3 API reference, CopyObject,
`x-amz-checksum-algorithm` ("You can optionally specify a different checksum algorithm to
use with the `x-amz-checksum-algorithm` header. Unrecognized or unsupported values will
respond with the HTTP status code `400 Bad Request`") and the response syntax; S3 User
Guide, "Copying, moving, and renaming objects" ("Whether you choose to use the same
algorithm or a new one, Amazon S3 calculates a new checksum value after the object is
copied. Amazon S3 doesn't directly copy the value of the checksum. All copied objects
without checksums and specified destination checksum algorithms automatically gain a
`CRC-64NVME` checksum algorithm"); S3 User Guide, "Checking object integrity for data
uploads in Amazon S3" ("If you don't specify an algorithm, S3 uses the existing algorithm.
If the source object doesn't have a specified checksum algorithm or checksum value, Amazon
S3 uses the CRC-64/NVME algorithm to calculate the checksum value for the destination
object", and "With a copy command, the checksum of the object is a direct checksum of the
full object"). Choices:

- *Recomputing, not copying the value.* API_CopyObject says "When you copy an object, if the
  source object has a checksum, that checksum value will be copied to the new object by
  default". The user guide says S3 "doesn't directly copy the value of the checksum" and
  calculates a new one. Litmus follows the user guide: it always hashes the copied bytes. The
  two agree whenever the source's value is the full object checksum of the same bytes, which
  is every PutObject object, and differ only for a composite source.
- *A composite source gets a direct full object value.* A source whose value ends in "-N"
  is copied with its algorithm and a checksum of the whole copied bytes, with no suffix
  ("the checksum of the object is a direct checksum of the full object"; "the checksum value
  changes even though the data doesn't").
- *The type is always FULL_OBJECT*, whatever the source's. The pages say the copy's value is
  of the full object and name no type for it.
- *Blank counts as absent.* A blank or whitespace `x-amz-checksum-algorithm` on CopyObject is
  absent, in the request guard and in the parser, as on CreateMultipartUpload. The reference
  says nothing about an empty value.
- *The five uncomputed names are 501.* Litmus cannot compute them, and a copy that silently
  used another algorithm would answer a request it did not honor. The reference says
  unsupported values are 400; the 501 is Litmus's, the same as on CreateMultipartUpload.
- *400 InvalidArgument for an unknown name.* The reference gives the status and no code.
- *A copy onto itself that changes only the algorithm* is 400 InvalidRequest, as any copy
  onto itself without `x-amz-metadata-directive: REPLACE`, and with REPLACE it is the
  ordinary byte copy, recomputed in the named algorithm. S3's behavior for a self copy that
  changes only the checksum algorithm is not found in API_CopyObject or in the user guide pages
  "Copying, moving, and renaming objects" and "Checking object integrity for data uploads
  in Amazon S3" (the error list page was not read for it); the user guide says a copy onto itself
  is how metadata is changed, and names no checksum use of it. This is Litmus's choice.
- *Request checksum value headers are ignored.* API_CopyObject defines neither
  `x-amz-checksum-<algorithm>` nor `x-amz-sdk-checksum-algorithm` as a request header (not
  found on that page), so, as on UploadPartCopy, a CRC32, CRC32C, CRC64NVME, SHA1 or SHA256
  value header and an `x-amz-sdk-checksum-algorithm` of any value are neither verified nor
  an error, and do not choose the algorithm; measured, a wrong value is a 200 and the copy
  is stored in the source's algorithm. The five uncomputed value headers (`x-amz-checksum-sha512`,
  `-md5` and the XXHash three) stay 501 in the request guard, as on every PUT without a
  subresource.
- *LastModified.* The commit time, as before.

Not done:

- An object written before schema version 3 has no checksum (its columns are NULL), and S3
  stores one and adds CRC64NVME when the client sends none. A multipart object whose listed
  parts include one written before schema version 3 has none either. A CopyObject of either
  computes a checksum for its copy (decision 33).
- The 501 for other algorithms applies to PutObject, UploadPart, UploadPartCopy and
  DeleteObjects, and to `x-amz-checksum-algorithm` on CreateMultipartUpload and CopyObject. On Complete,
  MD5, SHA512 and the XXHash headers are 400 InvalidRequest, as the user guide says for an
  upload that did not name them.
- `x-amz-content-sha256` holding a hex digest is not compared with the body.
- The algorithms S3's documentation now lists beyond these five (MD5, SHA512 and the
  XXHash family) are not computed.

**Conditional writes: PutObject and Complete are evaluated, append is 501.** PutObject
with `x-amz-write-offset-bytes` gets 501 NotImplemented; the existing object is
untouched. PutObject and CompleteMultipartUpload with `If-Match` or `If-None-Match` are
evaluated (below).

**Conditional PutObject.** `If-Match` and `If-None-Match` are decided inside the commit
transaction (docs/design/storage-layout.md §10), against the row the commit replaces,
after the new bytes are durable. A refused request rolls the commit back and discards
its bytes; the object, its Last-Modified, the blob registry and `tmp/` are as before.
`If-Match: *` or an ETag with no current object is 404 NoSuchKey, as ceph's tests expect.
A failed condition is 412 PreconditionFailed. Concurrent requests are decided in commit
order: of 8 `If-None-Match: *` PUTs to a new key exactly one wins, and of two `If-Match`
PUTs with the same ETag one gets 200 and one 412.

Choices where the S3 documentation is silent or differs:

- `If-None-Match` with an entity tag is evaluated as RFC 9110 13.1.2 defines it (weak
  comparison: 412 if the object has that ETag, else the write proceeds), not refused.
  The S3 API reference documents `If-None-Match` on PutObject with the value `*` only.
  The ceph tests that send an entity tag are `test_put_object_ifnonmatch_failed`, which
  is marked `fails_on_aws`, and `test_put_object_if_match`, which is not.

  Which passes depend on this. Two passing tests send an entity tag in `If-None-Match`
  and are marked `fails_on_aws`: `test_put_object_ifnonmatch_good` (a tag that does not
  match, expects 200) and `test_put_object_ifnonmatch_failed` (the current ETag
  unquoted, expects 412). `test_put_object_if_match` depends on it too (the current ETag
  gives 412, a bad tag gives 200) and is not marked `fails_on_aws`, so the suite's own
  markers disagree. `test_put_object_ifnonmatch_nonexisted_good` and
  `test_put_object_ifnonmatch_overwrite_existed_failed` send `*`, which AWS documents
  for PutObject, so their `fails_on_aws` markers predate that support. All of these
  count as genuine: each fails against the server as of dfe4d20, and the new tests fail
  with the a0f0bde server change reverted.
- ETags are matched as for GET: `*`, comma lists, quoted and unquoted values; `If-Match`
  uses strong comparison, so a weak tag never matches it.
- With both headers, both are evaluated, `If-Match` first: with no object that is 404
  even when `If-None-Match` would pass; with an object, either failing is 412.
- 409 ConditionalRequestConflict is never emitted. S3 documents it for a conditional
  write that conflicts with another request in flight on the same key. Commits here
  serialize on the database write lock and the later one is decided on the committed
  result of the earlier, so the answer is a plain 412 (or 404), which is also what
  ceph's `test_atomic_dual_conditional_write_1mb` expects. A client that retries on 409
  will not see one.
- There is no early check before the body is read: a refused PUT writes and discards
  its bytes, and a body that fails its checksum is BadDigest whatever the conditions.
- `Last-Modified` of a PUT is now taken inside the commit, under the write lock, so
  concurrent PUTs to a key leave the latest Last-Modified on the one that commits last
  (unless the wall clock steps back). CompleteMultipartUpload still dates the object
  with the upload's initiation time.

**Conditional CompleteMultipartUpload.** The same two headers, the same function
(`write_precondition`) and the same rules as PutObject, decided inside Complete's commit
transaction against the row it replaces (docs/design/storage-layout.md §10, §6.7 C4.c).
Every other Complete error comes first: NoSuchUpload and InvalidPart are answered
whatever the headers say, and only a request that passes them can be 412 or 404.
`Last-Modified` stays the upload's initiation time.

- A refused Complete leaves the upload open, with all its parts, exactly as before the
  request, so a retry with a condition that holds succeeds without re-uploading. There
  is no completing state: the change to `completed` is made in the same transaction as
  the check, and the rollback undoes it. The assembled blob is discarded like a refused
  PutObject's.
- There is no early check, so a refused Complete assembles a whole new blob (a copy of
  every listed part) and throws it away. For a large upload that is the object's full
  size in I/O, repeated by every refused retry. An early refusal can be added later.
- Concurrent requests are decided in commit order: of two uploads to one key completed
  with `If-None-Match: *` one gets 200 and one 412 (the loser's upload stays open); a
  Complete and a PutObject with `If-None-Match: *` on a new key have one winner; an
  `If-Match` Complete that commits after a DeleteObject is 404 and brings nothing back.
- `x-amz-if-match-initiated-time` is not a Complete header and is ignored there, as it
  was. AWS defines it only on AbortMultipartUpload, where it is still 501.
- A retried Complete after a kill that follows the commit is NoSuchUpload, as without a
  condition (the gap above, unchanged). That gap was j3, which decision 35 closes, so this
  answer is conformant.
- EntityTooSmall did not exist at this step (it is enforced now, see "The minimum part
  size" below). InvalidPartOrder
  and MalformedXML for a Complete body were added after this step (see the same section).
- `test_multipart_put_object_if_match` is not marked `fails_on_aws` and sends `*`, `badetag`
  and the current ETag, so it depends on the entity-tag rule above; it counts as genuine,
  because it fails against the server as of 45abd9b and the new tests fail with this
  change reverted. No new pass is `fails_on_aws`.

**Conditional DeleteObject.** `If-Match` on DeleteObject is decided inside the delete's
commit transaction, after the write lock, against the ETag of the row that commit deletes
(docs/design/storage-layout.md §10). The matching is the one PutObject uses: `*`, quoted
and unquoted tags, comma lists, repeated lines as one list, a blank value as no condition,
strong comparison, so `W/"x"` never matches. A value that does not match is 412
PreconditionFailed, and the object, its row and its blob are untouched. Choices:

- **A missing key is 404 NoSuchKey**, whatever the value, `*` included, and nothing
  changes. The AWS user guide page ["How to perform conditional deletes"](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.md)
  says that if the object does not exist when either precondition is evaluated, S3
  rejects the request with a Not Found error. An unconditional delete of a missing key
  stays 204. A blank `If-Match` is no condition, so it is 204. The bucket is still
  checked first, so a missing bucket is NoSuchBucket. A client that retries a delete
  whose first attempt committed (a kill after the commit) gets 404 on the retry.
- **`test_delete_object_if_match` fails on purpose.** The ceph test expects 204 for
  `If-Match: *` and `If-Match: badetag` on a missing key (`-ENOENT doesn't raise error in
  delete op`, Ceph RGW's behavior); AWS returns Not Found, and so does Litmus. The test is
  marked `fails_on_aws` ("only supported for directory buckets"), but the marker predates
  AWS supporting `If-Match` on DeleteObject and DeleteObjects in general purpose buckets
  (September 2025), so it no longer says the header is unsupported there. The other 17
  conditional delete tests fail for the reasons in the table.
- **`x-amz-if-match-last-modified-time` and `x-amz-if-match-size` stay 501**, alone or
  with `If-Match`, and nothing is deleted. AWS's conditional delete support on general
  purpose buckets covers `If-Match` only; these two headers are still documented for
  directory buckets only, and Litmus has no directory buckets, so the 501 is a deliberate
  choice, not a gap.
- **`If-None-Match` on DeleteObject is unchanged: 501** (as are `If-Modified-Since` and
  `If-Unmodified-Since`, which the reference does not list for DeleteObject either), so no
  precondition on a DELETE is silently dropped.
- 409 ConditionalRequestConflict is never emitted. AWS documents it for a concurrent
  DELETE or PUT that succeeds first. Litmus serializes commits on the write lock and each
  one is decided on the row the previous one left, so there is no in-flight conflict to
  report: a delete that loses finds a different ETag (412) or no key (404).
- AbortMultipartUpload still refuses every conditional header, `If-Match` included: it is
  501 and the upload stays open.
- **DeleteObjects:** see the next paragraph.

**Conditional DeleteObjects.** The `ETag` element of an `Object` in a DeleteObjects body is
decided per key, inside the one transaction that deletes the batch, after the write lock
and against that key's current row (docs/design/storage-layout.md §10). The value is read
as an `If-Match` value is for DeleteObject: `*`, quoted and unquoted tags, comma lists,
strong comparison, so a weak tag never matches. A condition that holds puts the key in
`Deleted`. One that fails puts it in `Error` with Code PreconditionFailed and the key is
untouched: its row, ETag, Last-Modified and blob are as before, and no blob goes to
garbage. A refused key does not roll back the others: the condition is decided before
anything is written for the key, so the transaction has nothing to undo and commits the
keys that passed. The batch is 200 whatever the per key results, Quiet mode lists every
`Error` and omits `Deleted`, and the 1000 key limit, MalformedXML handling and the empty
Key rule are as before. 409 is never emitted, for the reason given for DeleteObject.
Choices:

- **A key that does not exist, with a condition, is an `Error` with Code NoSuchKey**,
  `*` included, and the rest of the batch continues. The AWS user guide page ["How to
  perform conditional deletes"](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.md)
  says that if the object does not exist when the precondition is evaluated, S3 rejects
  the request with a Not Found error, which for a batch is that key's `Error`. The page
  does not give the per key Code; NoSuchKey is the code DeleteObject answers. A key with
  no `ETag` element that does not exist stays `Deleted`, as before.
- **An empty or blank `ETag` is no condition**, the same as a blank `If-Match`: the key is
  deleted (or, if missing, reported `Deleted`). The reference does not say what an empty
  `ETag` means; this follows DeleteObject.
- **`LastModifiedTime` and `Size` stay a per key NotImplemented error, deliberately.**
  AWS documents them for directory buckets only, and Litmus has none. An entry with either
  is kept whatever its `ETag` says, so no condition is silently dropped.
- An `ETag` element with a child element has no value to compare. It is a per key
  NotImplemented, as every ETag was before this change, not a MalformedXML.
- Entries are decided in request order. A key listed twice with a correct `ETag` is
  `Deleted` and then `Error` NoSuchKey, since the second entry sees the row the first
  removed.
- `VersionId` `null` with an `ETag` is evaluated and echoed in `Deleted` or `Error`; any
  other `VersionId` is still a per key NotImplemented.

Eight ceph tests fail on purpose. `test_delete_objects_if_match` passes, but only its
first two requests test the feature: its third sends a bad ETag for a deleted key and
asserts the status is 200, which it is (the entry is an `Error` with Code NoSuchKey; the
comment calls the Ceph RGW behavior "-ENOENT doesn't raise error in delete op"). The 8:

- `test_delete_objects_if_match_last_modified_time` and `test_delete_objects_if_match_size`
  expect PreconditionFailed for a wrong `LastModifiedTime` or `Size`, and Litmus answers
  NotImplemented on purpose (directory buckets only).
- `test_delete_objects_current_if_match`, `test_delete_objects_version_if_match`, and the
  `_last_modified_time` and `_size` variants of both (6 tests) enable bucket versioning
  first, which is 501, and read `DeleteMarker` and `VersionId`. The `current_` and
  `version_` variants of `LastModifiedTime` and `Size` fail for both reasons.

**CopyObject.** A PUT with `x-amz-copy-source` and no query parameter copies the source's
bytes into a new blob and commits the destination like a PutObject
(docs/design/storage-layout.md §6.10 and §11). The sources are the AWS API reference
for CopyObject and the user guide pages "Copying, moving, and renaming objects" and
"How to prevent object overwrites with conditional writes". The error list and the
older REST CopyObject page could not be read, so the points marked *Litmus's choice*
rest on the ceph tests or on nothing documented.

- *The source.* `bucket/key`, URL-encoded, a leading slash optional (the reference's
  pattern is `\/?.+\/.+`). Percent escapes are decoded once, a `+` stays a plus, and a
  `?versionId=` is split off before decoding, so a key with a `?` must send `%3F`.
  `?versionId=null` names the current object; any other version ID, and an access point
  or Outposts ARN, is 501 NotImplemented. A value that is not a bucket and a key, has
  another query, repeats the header or does not decode as UTF-8 is 400 InvalidArgument
  naming `x-amz-copy-source` (*Litmus's choice*: the reference gives no code for a
  malformed source). A missing source bucket is 404 NoSuchBucket and a missing source key
  is 404 NoSuchKey (the codes of every other operation; the reference lists neither).
- *Error order* (*Litmus's choice*): the destination bucket, the source syntax, the
  directives, the `x-amz-checksum-algorithm` name (400 InvalidArgument), the request's
  headers under REPLACE, the source bucket, the source key, the refusal of a copy onto
  itself, the source conditions, then the copy and the commit. The five uncomputed
  algorithm names are 501 in the request guard, before all of these.
- *Source conditions.* `x-amz-copy-source-if-match`, `-if-none-match`,
  `-if-modified-since` and `-if-unmodified-since` are decided on the source row whose blob
  is opened, with the evaluator and the RFC 9110 order of conditional GET. The reference
  documents two combinations, and both hold: If-Match true with If-Unmodified-Since false
  copies (200), and If-None-Match false with If-Modified-Since true is 412. *Litmus's
  choice:* each condition that fails on its own is 412 PreconditionFailed. AWS documents
  no status for a single condition, and ceph expects 412 for If-Match and for
  If-None-Match (`test_copy_object_ifmatch_failed`, `test_copy_object_ifnonematch_good`);
  the date conditions follow. Values are read as for GET: `*`, quoted and unquoted tags
  and lists work, a weak tag never matches `-if-match`, a date that is not valid is
  ignored. A source replaced while the request reads its row is copied as it then is,
  with the conditions decided again on that row; the request's own `If-Match` and
  `If-None-Match` are never read as source conditions.
- *Destination conditions.* `If-Match` and `If-None-Match` of the request are decided in
  the commit, as for PutObject (the reference and the conditional writes page support
  both for CopyObject): 412, or 404 NoSuchKey for `If-Match` with no object. 409 is never
  emitted.
- *Metadata directive.* COPY, the default, gives the destination the source's
  Content-Type, `Cache-Control`, `Content-Disposition`, `Content-Encoding`,
  `Content-Language`, `Expires` and `x-amz-meta-*`; the request's are ignored. REPLACE
  takes them all from the request and keeps nothing of the source, so a header not sent is
  dropped (the user guide: all user-configurable metadata must be named in the request).
  A REPLACE without `Content-Type` gets `binary/octet-stream`, what PutObject stores when
  it is absent (the code's default; AWS's own default is not documented in the pages read).
  A value other than COPY or REPLACE, in either directive, is 400 InvalidArgument.
- *Tags.* Litmus stores none. `x-amz-tagging-directive: COPY`, or REPLACE with an empty or
  absent `x-amz-tagging`, copies. REPLACE with a non-empty `x-amz-tagging` is 501
  NotImplemented and changes nothing, since the tags would be dropped.
- *The ETag.* The MD5 of the copied bytes, in the `CopyObjectResult` body (with
  `LastModified`) and on later GET and HEAD. For a source uploaded in parts it is a plain
  MD5, not `md5-N`: the reference says the copy "will be a single part object", without
  stating the ETag, so this is inferred. For a single part source it equals the source's.
  `Last-Modified` is the commit's time.
- *Copying onto itself.* Without `x-amz-metadata-directive: REPLACE`, 400 InvalidRequest
  and nothing changes (ceph's `test_object_copy_to_itself`; the code is not in the pages
  read). With REPLACE it is the ordinary byte copy: a new blob with the same bytes, the
  request's headers, a new Last-Modified, and the old blob garbage. AWS also accepts a
  self-copy that changes a storage class, encryption or a website redirect; Litmus
  refuses those headers (501, see "Feature headers Litmus refuses"), so REPLACE is its only
  change. A metadata-only commit that keeps the
  blob is a possible later optimization.
- *What is checked.* Before the copy is made durable its size is compared with the
  source row's and, for a source not assembled from parts, its MD5 with the row's ETag;
  a mismatch is 500 and nothing is stored. A multipart source is checked by size only.
  A source whose file is missing with its row unchanged, or has another size, is 500.
- *Not honored, as for PutObject:* the ACL headers and the checksum value headers
  (`x-amz-checksum-<algorithm>`, `x-amz-sdk-checksum-algorithm`) of the request are
  ignored; `x-amz-checksum-algorithm` is read (see "Checksums", decision 33) (the expected bucket owner headers are evaluated, see "Bucket owner condition").
  The storage class, SSE, SSE-C (destination), website redirect and Object Lock headers are
  501 (see "Feature headers Litmus refuses"). The
  `x-amz-copy-source-server-side-encryption-customer-algorithm`, `-key` and `-key-MD5`
  headers are 501 NotImplemented, here and on UploadPartCopy: Litmus stores nothing
  encrypted, and a key supplied for the source asks for something it cannot do (until
  UploadPartCopy was added they were ignored on CopyObject). `x-amz-write-offset-bytes` is still 501.

**UploadPartCopy.** A PUT with `x-amz-copy-source`, `partNumber` and `uploadId` copies
a byte range of the source into a new part blob and records it as UploadPart records a
part (docs/design/storage-layout.md §6.6A and §11). The source is the AWS API reference
for UploadPartCopy and UploadPart and the user guide pages "Amazon S3 multipart upload
limits" and "Uploading and copying objects using multipart upload". The reference gives
no error for a bad range, no order of errors and no ETag rule, so the points marked
*Litmus's choice* rest on the ceph tests or on nothing documented.

- *The source and the conditions* are CopyObject's: the same `x-amz-copy-source` format
  (an ARN or a `versionId` other than `null` is 501), the same four
  `x-amz-copy-source-if-*` headers decided on the source row whose blob is opened, with
  the same 412 for each failing alone (*Litmus's choice*) and the two documented
  combinations. The request's own `If-Match` and `If-None-Match` are not read as
  conditions on the source. They are not conditions on the destination either: the
  reference lists conditional writes for PutObject and CompleteMultipartUpload only, and
  UploadPart ignores them as well.
- *The range.* `x-amz-copy-source-range` is optional; without it the whole source is the
  part. The reference's form is `bytes=first-last`, zero-based offsets. Anything else
  (no `bytes=`, a missing end, a non-digit, a sign, a space, an open ended or suffix
  range, several ranges, a repeated header) is 400 InvalidArgument naming the header,
  as ceph's `test_multipart_copy_improper_range` expects. *Litmus's choice:* a reversed
  range (`bytes=5-2`) is the same 400 InvalidArgument; the reference gives no error for
  it.
- *Out of bounds.* A `last` at or beyond the size of the source, and any range of an empty
  source, is 416 InvalidRange. *Litmus's choice:* the reference gives no status or code;
  ceph's `test_multipart_copy_invalid_range` accepts 400 or 416 with the code
  InvalidRange. Unlike GET, a range is not clamped. The size is that of the source row
  that was opened, so the check comes after the source conditions.
- *The 5 MB sentence.* The reference says: "You can copy a range only if the source
  object is greater than 5 MB." *Litmus's choice:* it is not enforced. The sentence names
  no error, and ceph's `test_multipart_copy_invalid_range` and
  `test_multipart_copy_improper_range` use a source of 5 bytes and expect a range error,
  not a refusal of the range itself.
- *Error order* (*Litmus's choice*; the reference documents none): the destination
  bucket, the part number if it is not an integer (InvalidPart), the upload (NoSuchUpload),
  the part number interval (InvalidPart), the source syntax (400 InvalidArgument), the
  range syntax (400 InvalidArgument), the source bucket (NoSuchBucket), the source key
  (NoSuchKey), the source conditions (412), the range against the size (416), then the
  copy and the commit. The part number checks are UploadPart's, in its order.
  Decision 37 supersedes "the part number if it is not an integer (InvalidPart)" here: the
  number is read as ASCII digits with an optional leading minus, and anything else is
  InvalidPart in the same place (see "Decimal numbers in a request").
- *Part size.* The user guide gives parts of 5 MiB to 5 GiB and no minimum for the last
  part. Neither bound is checked when a part is uploaded, as for UploadPart, so a copied
  part of 1 byte is accepted whatever its number; Complete refuses it with EntityTooSmall
  unless it is the last listed part (see "The minimum part size").
- *The response.* 200 with a `CopyPartResult` body holding the ETag and LastModified. The
  ETag is the MD5 of the copied bytes, quoted, plain even when the source was uploaded in
  parts; the reference says only "Entity tag of the object" and does not give the rule.
  `LastModified` is the part's stored time, the time of its commit in whole seconds (the
  reference says "Date and time at which the object was uploaded"). For an upload that
  named a checksum algorithm the body also carries `Checksum<algorithm>` of the copied
  bytes, and for an unnamed upload no checksum element (decision 31 above). There is no
  `ETag` header (the reference's example has none) and no `x-amz-copy-source-version-id`.
- *Replacing a part.* A copy over a part number writes a new blob; the part recorded
  before stays recorded and intact until the replacing commit, as for UploadPart (the
  reference: a part uploaded again with the same number overwrites the previous one). A
  copy and an upload of one part number race like two uploads: the later commit wins. A
  source may be the key of the upload being filled.
- *What is checked.* Before the part is made durable its length is compared with the
  range's and, when the range is the whole of a source not assembled from parts, its MD5
  with the row's ETag; a mismatch is 500 and nothing is recorded. A range of a source
  has no stored digest, so only its length is checked, and a multipart source is checked
  by length only. A source whose file is missing with its row unchanged, or has another
  size, is 500.
- *Races.* A source overwritten or deleted after the open is copied as it was (the open
  descriptor pins the blob). One replaced between the row read and the open is copied as
  it then is, with the conditions and the range decided again on that row. An upload
  aborted or completed before the part commit is 404 NoSuchUpload and the new blob is
  discarded.
- *Not honored:* `x-amz-copy-source-server-side-encryption-customer-*` is 501 (above), and
  so are the SSE-C headers for the destination (see "Feature headers Litmus refuses"); the
  request payer header is ignored, as for UploadPart. The expected owner headers are evaluated (see "Bucket owner condition").
- *The AWS CLI.* An `aws s3 cp` between two keys above the multipart threshold copies in
  parts, and before the first part it calls GetObjectTagging on the source, which Litmus
  answers with an empty TagSet (see "GetObjectTagging" below). The copy succeeds.
  `aws s3api upload-part-copy`, boto3's `client.copy` and ceph's tests send UploadPartCopy
  directly.

**`x-amz-tagging` on PutObject and CreateMultipartUpload.** A non-empty value is 501
NotImplemented, decided in the request guard before any body byte is read, so no blob,
temp file, metadata row or upload is created and an existing object is unchanged. Litmus
stores no tags, and the S3 API reference gives the header a tag-set ("The tag-set must be
encoded as URL Query parameters", PutObject and CreateMultipartUpload, "Request
Syntax"), so storing the object without them would drop what the client asked for. The
value is not parsed: anything left after stripping whitespace is refused. An empty or
blank value is accepted, since it asks for no tags. The pages for PutObject and
CreateMultipartUpload say nothing about an empty value (not found); the CopyObject page's
"The default value is the empty value" for `x-amz-tagging` is the closest guidance. The
header on UploadPart and UploadPartCopy, which take none, is not read. A PUT with
`x-amz-copy-source` is CopyObject, which decides the header with its directive (see
"Tags" under CopyObject): under COPY, or no directive, the tags come from the source
("If you choose `COPY` for the `x-amz-tagging-directive`, you don't need to set the
`x-amz-tagging` header, because the tag-set will be copied from the source object
directly", CopyObject, `x-amz-tagging`), so the header is accepted and not read. What AWS
does with a non-empty value there is not found. `x-amz-acl` is decided separately (see "ACL and grant headers").

**GetObjectTagging.** `GET /{bucket}/{key}?tagging` answers 200 with an empty tag set:

    <?xml version="1.0" encoding="UTF-8"?>
    <Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet/></Tagging>

This is not tagging support. Litmus stores no tags, and no request can give an object
any: PutObject and CreateMultipartUpload refuse a non-empty `x-amz-tagging` (above),
CopyObject refuses a REPLACE with tags and takes the source's tags, which are empty, under
COPY, and PutObjectTagging is 501. So the tag set of every object is empty, and the empty
answer is always true. It exists because the AWS CLI reads the source's tags with this
call before an s3 to s3 copy in parts, and the copy failed at it. What stays 501:
PutObjectTagging (`PUT ?tagging`), DeleteObjectTagging (`DELETE ?tagging`), the bucket
tagging operations (`GET`, `PUT` and `DELETE /{bucket}?tagging`), `HEAD ?tagging`, and
`?tagging` with any query parameter besides `versionId` (`x-id` is ignored), which
includes `partNumber`, `uploadId` and `response-*`. The tagging subresource decides the
operation, so none of GetObject's handling applies: `Range`, the conditional headers and
`response-*` are not read.

The GetObjectTagging API reference page defines the request as `GET /{Key+}?tagging&
versionId={VersionId}` with `x-amz-expected-bucket-owner` and `x-amz-request-payer`, and
a 200 response with an optional `x-amz-version-id` header and a `Tagging` root holding a
`TagSet` of `Tag` elements; its sample response writes the namespace
`http://s3.amazonaws.com/doc/2006-03-01/` on `Tagging`. The page lists no errors, says
nothing about an object with no tags or how an empty `TagSet` is written, and lists no
conditional headers (all not found), so the empty element above is Litmus's reading of a
`TagSet` array with no members. The bucket is checked first (404 NoSuchBucket), then the
key (404 NoSuchKey). `versionId` is handled as for GetObject: `null` names the current
object and any other value is 501. As for GetObject, `x-amz-request-payer` is not
evaluated, and no `x-amz-version-id` is sent. `x-amz-expected-bucket-owner` is evaluated,
as for every object operation (see "Bucket owner condition").

**Bucket owner condition.** `x-amz-expected-bucket-owner` is evaluated on the 18
operations whose API reference pages define it: HeadBucket, DeleteBucket, ListObjects,
ListObjectsV2, ListObjectVersions, PutObject, GetObject, HeadObject, DeleteObject,
DeleteObjects, GetObjectTagging, CreateMultipartUpload, UploadPart, UploadPartCopy,
ListParts, CompleteMultipartUpload, AbortMultipartUpload and CopyObject. On CopyObject and
UploadPartCopy it names the destination bucket's owner, and
`x-amz-source-expected-bucket-owner` names the source bucket's owner; those are the only
two operations that read the source header. A value that differs from `S3_OWNER_ID` is
403 AccessDenied, "Access Denied", in the standard error body. The check is in the request
guard, the last step, so it follows the bucket name check and every 501 refusal, and it
runs before the handler and before the body is read. Documented: S3 User Guide,
"Verifying bucket ownership with bucket owner condition" ("If the parameter doesn't match
the bucket owner's account ID, the request fails with an `Access Denied` error message";
for CopyObject the first parameter is the destination bucket's and a second is the
source bucket's; "Bucket owner condition isn't available for CreateBucket, ListBuckets
... Amazon S3 ignores any bucket owner condition parameters included with requests to
these operations"), and each operation's API reference page ("the request fails with the
HTTP status code `403 Forbidden` (access denied)", PutObject, `x-amz-expected-bucket-owner`).
The S3 API reference page "Error" (`API_Error`, its `Code` element, the list of error
codes) gives AccessDenied the description "Access Denied" and the HTTP status 403 Forbidden;
Litmus sends that code, message and status.

The choices that the pages leave open:

- *Comparison.* The value, with surrounding whitespace stripped, is compared with
  `S3_OWNER_ID` exactly: case sensitive. The User Guide page says the value must be an
  AWS account ID (12 digits); Litmus has one identity, `S3_OWNER_ID`, which also serves as
  the canonical owner ID in `Owner` elements, and AWS keeps those two apart. A client that
  sends the configured owner ID matches, whatever it looks like.
- *No format validation.* A value that is not 12 digits is a mismatch like any other. What
  AWS does with a malformed value is not found.
- *Order.* A mismatch is 403 before NoSuchBucket, NoSuchKey and NoSuchUpload, and loses
  to every 501 and to an invalid bucket name. The pages do not say which of the checks AWS
  runs first (not found).
- *Empty is absent.* An empty or whitespace only value is treated as no header. AWS's
  handling of an empty value is not found.
- *Destination before source.* When both headers are present on a copy and both
  mismatch, the destination header is checked first. The response is the same either way.
- *Source header elsewhere.* `x-amz-source-expected-bucket-owner` on any operation other
  than CopyObject and UploadPartCopy is ignored: only those two pages define it.
- *ListBuckets and CreateBucket* ignore both headers, as the User Guide page says. So
  does `/health`.
- *Selection.* The guard picks the operations by method, whether there is a key, and
  the query: CreateBucket is `PUT /{bucket}` or `PUT /{bucket}/` with no query (`x-id` is
  ignored throughout the guard), and every other request that reaches the end of the guard
  and is one of the 18 is checked. A method no route accepts is not checked, so it still
  gets 405.

`test_expected_bucket_owner` still fails: it calls put_bucket_acl first, which is 501, so
it never reaches a request that carries the header.

**Feature headers Litmus refuses.** Litmus stores plaintext, has no Object Lock, no
website hosting and one storage class, so a request for any of those used to succeed
without doing what it asked. These headers are now 501 NotImplemented ("A header you
provided implies functionality that is not implemented", HTTP 501; S3 API reference, page
"Error", `Code` NotImplemented), decided in the request guard before any body byte is
read, so no blob, temp file, metadata row, upload, part or bucket is created and an
existing object is unchanged. The response names the header in a `Header` element. The
refusals sit with the guard's other 501s, so the owner check stays the last step: a
refused header with a wrong `x-amz-expected-bucket-owner` is 501, not 403. A value that is
empty after stripping whitespace counts as absent (the pages say nothing about an empty
value, not found). Nothing here adds a write path.

- PutObject, CreateMultipartUpload and CopyObject (the destination), any non-empty value
  of `x-amz-server-side-encryption`, `x-amz-server-side-encryption-aws-kms-key-id`,
  `x-amz-server-side-encryption-context`, `x-amz-server-side-encryption-bucket-key-enabled`,
  `x-amz-website-redirect-location` and the six Object Lock headers `x-amz-object-lock-mode`,
  `-retain-until-date`, `-legal-hold`, `-event-hold`, `-event-hold-duration-days` and
  `-event-hold-duration-years`. The three pages define the same headers (S3 API
  reference, PutObject, CreateMultipartUpload and CopyObject, "Request Syntax").
- PutObject, CreateMultipartUpload, UploadPart, and the destination side of CopyObject and
  UploadPartCopy, any non-empty value of `x-amz-server-side-encryption-customer-algorithm`,
  `-customer-key` and `-customer-key-MD5` (S3 API reference, UploadPart and UploadPartCopy,
  which define them for the destination). The source side,
  `x-amz-copy-source-server-side-encryption-customer-*`, was already 501. ListParts refuses
  the same three headers (S3 API reference, ListParts, which defines them) at the same point
  in the guard, on a GET with `uploadId` and no query parameter but `max-parts` and
  `part-number-marker`: it is 501 with a `Header` element naming the header, ahead of the
  owner check, and a blank value is absent (decision 36).
- PutObject, CreateMultipartUpload and CopyObject, `x-amz-storage-class` unless the value
  is exactly `STANDARD`. The pages list `STANDARD | REDUCED_REDUNDANCY | STANDARD_IA |
  ONEZONE_IA | INTELLIGENT_TIERING | GLACIER | DEEP_ARCHIVE | OUTPOSTS | GLACIER_IR | SNOW |
  EXPRESS_ONEZONE | FSX_OPENZFS | FSX_ONTAP | AWS_BACKUP_WARM | AWS_BACKUP_LOW_COST_WARM`
  and say "By default, Amazon S3 uses the STANDARD Storage Class to store newly created
  objects", so naming STANDARD asks for what Litmus does. Every other value is refused,
  including a lowercase `standard`.
- CreateBucket, `x-amz-bucket-object-lock-enabled` unless the value is `false`, compared
  case insensitively, and `x-amz-bucket-namespace` unless the value is `global`, compared
  exactly. The CreateBucket page gives the namespace the valid values `account-regional |
  global`, and gives `x-amz-bucket-object-lock-enabled` no valid values at all, so what S3
  does with a value other than `true` and `false` is not found; Litmus refuses it.

The choices that the pages leave open: a header repeated in one request is refused if any
of its non-blank values is refused. The value is compared after stripping surrounding
whitespace, so ` STANDARD ` is accepted. The case of `x-amz-bucket-namespace` is exact
because the page lists its values as an enumeration. UploadPart and UploadPartCopy ignore
the encryption, redirect, storage class and Object Lock headers: their pages do not
define them (encryption is set when the upload is created). The pages also define the
SSE-C headers on GetObject, HeadObject, CompleteMultipartUpload and others, and
`x-amz-server-side-encryption` on CreateSession; those operations are unchanged (see the
list below). The checksum headers are not decided here. `x-amz-object-ownership` on
CreateBucket is refused here unless it is `BucketOwnerEnforced` (see "ACL and grant
headers"); the ACL and grant headers are answered by the handlers, not the guard.

**ACL and grant headers.** ACLs are disabled. Litmus answers the ACL, grant and Object
Ownership request headers as a bucket whose Object Ownership setting is
BucketOwnerEnforced does: the owner has full control and there are no other grants. Until
this change `x-amz-acl`, the `x-amz-grant-*` headers and `x-amz-object-ownership` were
accepted and dropped, so a client that asked for a public bucket got a private one
without being told. Nothing here adds a write path: the headers are read and compared,
and nothing is stored.

- *The model.* S3 User Guide, "Controlling ownership of objects and disabling ACLs for
  your bucket": "By default, Object Ownership is set to the Bucket owner enforced setting
  and all ACLs are disabled", "the bucket owner automatically owns and has full control
  over every object in the bucket", and "your bucket accepts only `PUT` requests that do
  not specify an ACL or `PUT` requests with bucket owner full control ACLs ... `PUT`
  requests that contain other ACLs (for example, custom grants to certain AWS accounts)
  fail and return a `400` error with the error code `AccessControlListNotSupported`".
  Litmus follows that page, the API reference CreateBucket page ("By default,
  `ObjectOwnership` is set to `BucketOwnerEnforced` and ACLs are disabled") and the AWS CLI
  `create-bucket` and `put-bucket-ownership-controls` reference pages, which repeat it.
  One page disagrees: the User Guide page "Setting Object Ownership when you create a
  bucket" says, under "Using the AWS CLI", "If you don't set Object Ownership when you
  create a bucket by using the AWS CLI, the default setting will be `ObjectWriter` (ACLs
  enabled)", although the same page says earlier that "By default, S3 Object Ownership is
  set to the Bucket owner enforced setting, and ACLs are disabled for new buckets". Litmus
  follows the main Object Ownership page and the API reference over that CLI sentence: a
  request without `x-amz-object-ownership` gets BucketOwnerEnforced. Which one a real
  `aws s3api create-bucket` gets is not found; the sentence matters little to Litmus,
  which receives the header or not.
- *Authentication is a separate matter.* Litmus has none, that is out of scope and
  documented, and accepting an ACL header says nothing about network access. Anyone who
  can reach the port has the owner's full control, whatever ACL they send.
- *Object writes.* PutObject, CreateMultipartUpload and CopyObject: `x-amz-acl` absent,
  `private` or `bucket-owner-full-control` is accepted. Any other value, the other canned
  ACLs (`public-read`, `public-read-write`, `authenticated-read`, `aws-exec-read`,
  `bucket-owner-read`) and values that are not canned ACLs included, is 400
  AccessControlListNotSupported, and so is any non-blank `x-amz-grant-full-control`,
  `x-amz-grant-read`, `x-amz-grant-read-acp` or `x-amz-grant-write-acp` (the four grant
  headers the three pages define; S3 API reference, PutObject, CreateMultipartUpload and
  CopyObject, "Request Syntax"). `x-amz-object-ownership` is not defined on these
  operations and stays ignored.
- *`private` is accepted.* The PutObject and CopyObject pages say a bucket with the
  setting "only accept[s] PUT requests that don't specify an ACL or PUT requests that
  specify bucket owner full control ACLs", and the User Guide Troubleshooting page says
  "you must either specify bucket owner full control ACLs or not specify an ACL". A
  literal reading refuses `private`. Whether AWS does is not found. Litmus accepts it:
  with ACLs disabled the owner has full control and nobody else has any access, which is
  all `private` grants.
- *Exact, case sensitive comparison.* `Private` and `PRIVATE` are refused, as is any value
  not in the list on the PutObject page (`private | public-read | public-read-write |
  authenticated-read | aws-exec-read | bucket-owner-read | bucket-owner-full-control`).
  The pages say nothing about case (not found); the list is an enumeration, as for
  `x-amz-bucket-namespace`. A value that is empty after stripping whitespace counts as
  absent, as for the feature headers. A header repeated in one request is refused if any
  of its non-blank values is.
- *Every grant header is refused, owner only grants included.* A grant is an ACL, and the
  pages say a bucket with disabled ACLs fails requests "that contain other ACLs (for
  example, custom grants to certain AWS accounts)" without defining "other" for a grant
  that names only the owner. `x-amz-grant-full-control: id="<owner>"` would be
  equivalent to bucket owner full control, and the page that allows "an equivalent form
  of this ACL expressed in the XML format" (PutObject) says nothing of the grant headers.
  Litmus does not parse grants, so it refuses them all rather than guess; whether AWS
  accepts an owner only grant header is not found.
- *CreateMultipartUpload by inference.* Its API reference page lists the same ACL and
  grant headers but does not mention disabled ACLs or AccessControlListNotSupported
  (not found). The User Guide says it of "`PUT` requests", and the upload's object is
  written by the completing request; Litmus applies the PutObject rule when the upload
  is created, so an upload that could never be accepted is not started. UploadPart,
  UploadPartCopy and CompleteMultipartUpload do not define the headers and ignore them.
- *Order.* The check is in the handler, right after the destination bucket lookup: a
  missing bucket stays 404 NoSuchBucket. It is before any request body byte is read, and
  for CopyObject before the copy source is parsed or opened, so a refused ACL with a
  missing, malformed or conditional source is 400 AccessControlListNotSupported, not
  NoSuchKey, InvalidArgument or 412. It comes after the guard's 501 refusals and the 403
  owner check, so a refused ACL beside a refused feature header is 501 and beside a wrong
  `x-amz-expected-bucket-owner` is 403. The pages give no order for AWS (not found); Litmus
  looks up the bucket first, as every other handler does.
- *CreateBucket.* `x-amz-object-ownership` other than `BucketOwnerEnforced` (the page
  lists `BucketOwnerPreferred | ObjectWriter | BucketOwnerEnforced`), including a junk
  value, is 501 NotImplemented in the guard with the other 501 refusals: Litmus has no
  bucket with ACLs enabled, so the setting cannot be honored, and the value is compared
  exactly. `x-amz-acl` other than `private` (the page lists `private | public-read |
  public-read-write | authenticated-read`) and any non-blank `x-amz-grant-full-control`,
  `-read`, `-read-acp`, `-write` or `-write-acp` is 400 InvalidBucketAclWithObjectOwnership.
  The CreateBucket page says "To set an ACL on a bucket as part of a `CreateBucket`
  request, you must explicitly set S3 Object Ownership for the bucket to a different value
  than the default, `BucketOwnerEnforced`", and "If you try to create a bucket with a
  public ACL, the request will fail"; the Troubleshooting page gives the code for a
  `CreateBucket` that "sets Bucket owner enforced and specifies a bucket ACL that provides
  access to an external AWS account". `private` is accepted because the page says that
  with `private`, or no ACL, only `s3:CreateBucket` is required. The check is in the
  handler after the bucket name is validated and before the existence check: an invalid
  name with a refused ACL is InvalidBucketName, and an existing bucket with a refused ACL
  is 400 InvalidBucketAclWithObjectOwnership, not 409. The pages give no order (not
  found). With `ObjectWriter` and `public-read` together the guard's 501 comes first.
- *Messages.* Both codes appear only on the User Guide Troubleshooting page (Object
  Ownership error responses), not in the S3 API error code list (page "Error"). The
  messages are the page's: "The bucket does not allow ACLs" (AccessControlListNotSupported,
  from its `put-object` example) and "Bucket cannot have ACLs set with ObjectOwnership's
  BucketOwnerEnforced setting" (InvalidBucketAclWithObjectOwnership, from its
  `put-bucket-ownership-controls` example, which is a different operation from the one
  Litmus raises it for). Both are 400. The page does not say the messages are the same
  for other operations (not found).

The five selected ceph/s3-tests that create a bucket or put an object with a public ACL
now fail on purpose: `test_object_raw_get`, `test_object_raw_get_bucket_gone`,
`test_object_raw_get_object_gone`, `test_object_delete_key_bucket_gone` and
`test_object_anon_put_write_access`. AWS refuses a public ACL on a bucket with the
default Object Ownership setting, and they also need anonymous access, which is out of
scope. They passed before only because the ACL was dropped. `test_object_raw_response_headers`
still passes: `private` is accepted. The other ACL tests still fail, some at an earlier
step than before.

**Request headers Litmus still ignores.** A header Litmus does not read has no effect, and
the request succeeds as if it were absent. Where this file already says so, the entry
points there instead of repeating it.

Ignored, where a client can observe the difference:

- `x-amz-object-ownership` on PutObject, CreateMultipartUpload and CopyObject, and the
  ACL and grant headers on UploadPart, UploadPartCopy and CompleteMultipartUpload, which
  do not define them (see "ACL and grant headers" for the operations that do).
- Every `x-amz-checksum-<algorithm>` and `x-amz-sdk-checksum-algorithm` on CopyObject (see
  decision 33 under "Checksums") and on UploadPartCopy (see decision 31 under "Checksums"). `x-amz-checksum-algorithm` and `x-amz-checksum-type` on
  CreateMultipartUpload are no longer ignored. The checksum headers, the Part `Checksum*`
  elements and `x-amz-mp-object-size` on CompleteMultipartUpload are no longer ignored (see
  "Checksum and size headers on CompleteMultipartUpload"); an unnamed upload accepts some of
  them unvalidated, as listed there.
- The SSE-C headers on GetObject, HeadObject and CompleteMultipartUpload, and `x-amz-mfa`
  on DeleteObject and DeleteObjects, where the effect is unclear.
- A satisfiable `Range` on HeadObject, which does not narrow `Content-Length`.

Ignored with no observable effect: `x-amz-request-payer`, `x-amz-bypass-governance-retention`,
`x-amz-object-annotation-directive` and `x-amz-optional-object-attributes`. ListParts ignores
`x-amz-request-payer` too and sends no `x-amz-request-charged`.

**ListParts.** `GET /{bucket}/{key}?uploadId=` lists the parts of an open upload, one page at
a time (docs/design/storage-layout.md decision 36 and §21). It reads and changes nothing.
Documented: S3 API reference, "ListParts" (`API_ListParts`), "Part" (`API_Part`),
"Initiator" (`API_Initiator`), "Owner" (`API_Owner`) and "Error responses" (`API_Error`).
Every choice below is one the pages leave open ("not found"), unless a page is named.

- *Query.* `uploadId`, `max-parts` and `part-number-marker`. Any other query parameter
  beside `uploadId`, `versionId=null` included, is 501 (not found); `x-id` is ignored.
  `HEAD ?uploadId` is 501.
- *Headers.* `x-amz-expected-bucket-owner` is evaluated (403 AccessDenied on a mismatch,
  `API_ListParts`). `x-amz-request-payer` is ignored, as everywhere. The three SSE-C headers are
  501, as for UploadPart (see "Feature headers Litmus refuses"); CompleteMultipartUpload
  ignores them, and which S3 follows is not found. Litmus stores no upload encrypted with a
  key, so a request that supplies one asks for something it cannot do, and ignoring the
  headers would answer as if it had.
- *max-parts.* Default and cap 1000 (`API_ListParts`: "The limit of 1,000 parts is also the
  default value"); a larger value is capped and `MaxParts` echoes the capped value; leading
  zeros are read as the number. 0 gives no parts, `IsTruncated` false and no
  `NextPartNumberMarker`, as `max-keys=0` does. A negative value, a sign, a space, a fraction,
  an exponent, a non-ASCII digit or an empty value is 400 InvalidArgument with `ArgumentName`
  and `ArgumentValue`, as for `max-keys`.
- *part-number-marker.* Only parts with higher numbers are listed (`API_ListParts`). Absent
  is 0 (not found). A value that is not decimal digits is 400 InvalidArgument (not found).
  Any other integer is accepted, and a marker at or above the highest part, or above 10000,
  lists no parts. `PartNumberMarker` echoes it without leading zeros. `max-parts` is read
  first, so with both bad the error names `max-parts`.
- *Paging.* `IsTruncated` is true when more parts remain. `NextPartNumberMarker` is the
  last returned part number (`API_ListParts`), written only when `IsTruncated` is true (the
  page does not say it is absent otherwise).
- *Elements.* In the order of the response syntax: Bucket, Key, UploadId, PartNumberMarker,
  NextPartNumberMarker, MaxParts, IsTruncated, Part, Initiator, Owner, StorageClass,
  ChecksumAlgorithm, ChecksumType. Each Part is PartNumber, LastModified, ETag, Size, then
  its checksum element: the order is the page's sample response, since the syntax lists them
  alphabetically, and the checksum's place is not found. `ETag` is quoted as UploadPart
  returns it. `LastModified` is the part's last commit in whole seconds, as ListObjects writes
  it. The checksum element is `Checksum<algorithm>` in the part's stored algorithm and is
  omitted for a part with no stored checksum, which only parts uploaded before the version 3
  migration lack (`API_Part`: "Required: No"). An unnamed upload's parts show
  `ChecksumCRC64NVME` (`API_Part`: present "if the object was uploaded without a checksum").
- *Upload level.* Initiator and Owner both carry `S3_OWNER_ID` and `S3_OWNER_DISPLAY_NAME`.
  `StorageClass` is `STANDARD`. `ChecksumAlgorithm` and `ChecksumType` are written only for an
  upload that named an algorithm; what an unnamed upload returns is not found. No
  `x-amz-abort-date` or `x-amz-abort-rule-id` is sent, because Litmus has no lifecycle.
- *Errors.* The page lists none (not found). The order is the guard (501, then AccessDenied),
  400 InvalidArgument, 404 NoSuchBucket, 404 NoSuchUpload. NoSuchUpload covers an unknown,
  completed or aborted upload (`API_Error`) and an upload of another bucket or key (decision
  34); a blank `uploadId` is an unknown ID, as for Complete and Abort.
- *One read.* The upload and its page of parts are read in one transaction, so a racing
  Complete, Abort or UploadPart gives the whole list of the earlier state or 404 NoSuchUpload.

*ceph/s3-tests.* The only selected tests that reach `list_parts` are six object ownership
tests, through the helper `get_multipart_acl_owner`; all six still fail, and none could
pass: each fails earlier on an operation that is refused, and the helper's first call,
`get_object_acl`, is 501 as well. `test_create_bucket_bucket_owner_enforced` fails at
`get_bucket_ownership` (GetBucketOwnershipControls is 501); `test_create_bucket_bucket_owner_preferred`
and `test_create_bucket_object_writer` fail at CreateBucket with an `ObjectOwnership` other than
`BucketOwnerEnforced` (501); `test_put_bucket_ownership_bucket_owner_enforced` fails at CreateBucket
with `ACL=public-read` (400 InvalidBucketAclWithObjectOwnership); and
`test_put_bucket_ownership_bucket_owner_preferred` and `test_put_bucket_ownership_object_writer`
fail at PutBucketOwnershipControls (501).

**An upload belongs to one bucket and key.** UploadPart, UploadPartCopy,
CompleteMultipartUpload and AbortMultipartUpload find the upload by its ID, bucket and
key together (docs/design/storage-layout.md decision 34). A bucket that does not exist is
404 NoSuchBucket ("Error responses" lists NoSuchBucket, 404, "The specified bucket does not
exist."). Otherwise a request whose bucket or key is not the upload's, a key that differs
only in case included, is 404 NoSuchUpload, the answer for an unknown ID. *Litmus's
choice:* "UploadPart", "UploadPartCopy", "CompleteMultipartUpload" and
"AbortMultipartUpload" say nothing about a request whose bucket or key differs from the
upload's (not found); each documents NoSuchUpload (404) as its error for an upload that
does not exist, so a mismatch gets that. Both checks come before any body byte is read and
before a blob is written, so a refused request changes nothing and the upload stays usable
through its own bucket and key. For UploadPartCopy they concern the destination. The
order: UploadPart, the part number if it is not an integer (InvalidPart), NoSuchBucket,
NoSuchUpload, the part number interval; Complete, NoSuchBucket, then the numbered order
below from NoSuchUpload; Abort, NoSuchBucket, NoSuchUpload. Under a wrong owner header it
is 403, as for every operation that takes the header. A request through a bucket that was
deleted is NoSuchBucket, not NoSuchUpload; the deleted bucket's uploads are still gone
(decision 7). Complete through a missing bucket used to be 500 InternalError (the object
row violated the bucket foreign key) and is now NoSuchBucket.
Decision 37 supersedes "the part number if it is not an integer (InvalidPart)" in that
order: see "Decimal numbers in a request".

**Two ceph tests send a second Complete (decision 35).** `test_multipart_upload` and
`test_multipart_upload_small` complete an upload and then send the same Complete again
(the comment in both reads "check extra client.complete_multipart_upload"), and expect it
to succeed. Litmus answers that second Complete with 404 NoSuchUpload, so both fail on
purpose. S3 API reference, "Error responses" lists NoSuchUpload (404 Not Found) as "The
specified multipart upload does not exist. The upload ID might be invalid, or the multipart
upload might have been aborted or completed."; "CompleteMultipartUpload" lists the same
error and documents no replay of the first answer. Litmus follows the documented answer and
has not observed what S3 itself returns. The earlier claim that S3 answers a retry with 200
and the same ETag rested only on these two tests and is withdrawn; no replay is
implemented, and the retry changes nothing. Checked once, not counted: in a scratch copy
of the s3-tests checkout with the extra Complete removed from each test, both pass against
Litmus (2 passed), so nothing else in them fails. The gap j3 is closed as conformant.

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

**The Complete body.** CompleteMultipartUpload does not take its body at its word.
Until this change it did, and that was a silent correctness bug, not only a missing
error: the part list was sorted and then assembled as written, so a part number listed
twice with the right ETag was accepted and its bytes were assembled twice (the object's
size counted it twice and the composite ETag hashed it twice), and parts listed out of
order were silently put in order. The earlier version of this document said a repeated
part number gets InvalidPart; it did not. An empty, non-XML or non-UTF-8 body was a 500.
Now, after NoSuchUpload for an unknown upload (checked first, as before), and after the
`x-amz-mp-object-size` syntax check (see "Checksum and size headers on
CompleteMultipartUpload"):

- MalformedXML (400): the body is empty, not UTF-8, not well-formed XML, its root is not
  `CompleteMultipartUpload` (with or without the S3 namespace), it has no `Part`, or a
  `Part` has no `PartNumber`, no `ETag` (blank counts as none) or a `PartNumber` that is
  not an integer.
- InvalidPartOrder (400): the part numbers are not strictly ascending. A repeat is refused
  by the same rule, and the list is never sorted. Gaps are allowed (1 then 3 is fine), except
  on a COMPOSITE upload, which needs the numbers to be exactly 1 to N (see "Checksum and
  size headers on CompleteMultipartUpload").
- InvalidPart (400): a `PartNumber` outside 1 to 10000, or a part that was never uploaded
  or whose ETag differs.
- EntityTooSmall (400): a listed part other than the last listed one is stored smaller
  than `S3_MIN_PART_SIZE` bytes (5 MiB by default); see the minimum part size below.
- InvalidRequest (400): `x-amz-mp-object-size` differs from the total size of the listed
  parts.
- Then the `If-Match` and `If-None-Match` conditions (412, or 404). The checksum checks
  come between these steps; the whole order is under "Checksum and size headers on
  CompleteMultipartUpload".

All of it is decided before a blob is registered, so a refused Complete writes nothing:
no object row, no pending or garbage blob, an empty `tmp/`, and the upload still open
with all its parts, so a retry with a correct body succeeds.

These are Litmus's choices where the S3 documentation does not fix the answer:

- An empty body is MalformedXML. The error list also has MissingRequestBodyError ("You
  sent an empty XML document as a request", 400), which is the alternative; Litmus keeps
  MalformedXML because the one selected ceph test that sends no body
  (`test_multipart_upload_empty`) expects it, and because DeleteObjects answers an empty
  body the same way.
- The order of the errors above is Litmus's. The S3 documentation lists the errors but not
  which comes first when a request has several.
- A `PartNumber` outside 1 to 10000 is InvalidPart, not MalformedXML or InvalidArgument:
  the number is an integer, and no such part exists. A number of any length is accepted
  as text and is out of range.
  Decision 37 supersedes "A number of any length is accepted as text and is out of range":
  leading zeros are stripped before the length is judged, so 4300 zeros and a 1 is part 1
  (see "Decimal numbers in a request").
- The order of the errors, with EntityTooSmall after InvalidPart and before the
  `x-amz-mp-object-size` mismatch, is Litmus's. Neither "CompleteMultipartUpload" nor
  "Error" says where EntityTooSmall comes among the other errors: not found.

**The minimum part size.** CompleteMultipartUpload answers 400 EntityTooSmall when a
listed part other than the last listed one is stored smaller than `S3_MIN_PART_SIZE`
bytes. The default is 5242880 (5 MiB). S3 API reference, "CompleteMultipartUpload",
Special errors: EntityTooSmall, "Each part must be at least 5 MB in size, except the last
part", 400 Bad Request. S3 User Guide, "Amazon S3 multipart upload limits": part size
5 MiB to 5 GiB, "no minimum size limit on the last part". S3 API reference, "Error":
EntityTooSmall, "Your proposed upload is smaller than the minimum allowed object size",
400. Choices and what the pages leave unsaid:

- The setting is a non-negative decimal integer, read when the server starts like the
  others. A value that is not one (`-1`, `1.5`, `abc`, empty) stops the start with an
  error naming `S3_MIN_PART_SIZE`. `0` turns the check off. Lowering it diverges from AWS
  and is meant for tests; the test sessions of this repository set it to 0.
- The size judged is the stored size of the part, and only the parts listed in the
  request count: a part uploaded and not listed is not judged. The last listed part is
  exempt, whatever its number or size (a zero byte one included), and an upload with one
  listed part has no minimum. "CompleteMultipartUpload" says "except the last part"; that
  this means the last listed part, not the highest uploaded one, is Litmus's reading.
- UploadPart and UploadPartCopy do not check. "UploadPart" lists only NoSuchUpload among
  its special errors, and "CompleteMultipartUpload" is the page that lists
  EntityTooSmall.
- The error response carries Code and Message only. Elements such as the offending part
  number and sizes are not in the pages read: not found.
- The check reads the part rows, so it is decided before a blob is registered. A refusal
  leaves the upload open with all its parts, and the write sequence is unchanged. Design
  decision 27 in docs/design/storage-layout.md records it.

`test_multipart_resend_first_finishes_last` fails on purpose. It lists part 1 twice, first
with a stale ETag and then with the current one, and expects the Complete to succeed.
Litmus refuses any repeated part number with InvalidPartOrder, because S3 requires the list
in ascending order and accepting a repeat is how a part got assembled twice. The test
would pass only by tolerating repeats, which is the behavior this change removes.

**Checksum and size headers on CompleteMultipartUpload (decision 32).** Complete verifies
`x-amz-mp-object-size`, the checksum headers, `x-amz-checksum-type` and the Part `Checksum*`
elements, and stores and returns the object's checksum. An upload is *named* when
CreateMultipartUpload sent a non-blank `x-amz-checksum-algorithm`; E is the named algorithm,
else CRC64NVME.

`x-amz-mp-object-size`:

- It is verified. S3 API reference, "CompleteMultipartUpload": it is "the expected total
  object size of the multipart upload request", and a mismatch with the actual size "results
  in an `HTTP 400 InvalidRequest` error". The size compared is the total of the stored sizes
  of exactly the listed parts, so a part that was uploaded and not listed does not count.
  The documentation says nothing else about the header.
- A malformed value is 400 InvalidArgument. The value must be a plain non-negative decimal
  integer, digits only after stripping surrounding whitespace: `-1`, `1.5`, `0x10`, `+5`,
  `1e3`, `1_000`, letters, and digits with a space inside are refused, and leading zeros are
  accepted. AWS's handling of a malformed value is not found in the S3 API reference,
  "CompleteMultipartUpload" or the "Error" page; InvalidArgument is Litmus's choice. A value
  with more than 18 significant digits is a mismatch, not an error.
- A header, element or value that is empty after stripping whitespace counts as absent.

The object checksum:

- COMPOSITE (CRC32 and CRC32C by choice of the client, SHA1 and SHA256 always): the digest,
  in the upload's algorithm, of the concatenated raw part digests in listed order, base64
  encoded, plus "-N" for the N listed parts. FULL_OBJECT (CRC64NVME always, CRC32 and CRC32C
  by choice): the CRC of the whole listed bytes, combined from the stored part checksums and
  sizes with awscrt, never recomputed from the bytes. Only the listed parts take part.
- The response carries `Checksum<ALG>` after `ETag` and `ChecksumType` last, and GetObject
  and HeadObject return the stored value and type with `x-amz-checksum-mode: ENABLED`.
- The "-N" suffix and the composite formula were not found in the AWS documentation (S3 User
  Guide, "Checking object integrity for data uploads", says only that a composite checksum
  is "calculated based on the individual checksums of each part"). They rest on the
  ceph/s3-tests vectors: `test_multipart_checksum_sha256` (SHA256, one part, `-1`),
  `test_multipart_reupload_checksum_and_etag` and `test_multipart_use_cksum_helper_sha256`
  (SHA256, three parts, `-3`), `test_multipart_use_cksum_helper_sha1` (SHA1, `-3`), and the
  FULL_OBJECT vectors of `test_multipart_use_cksum_helper_crc32`, `_crc32c` and `_crc64nvme`.
  The CRC32 and CRC32C composite formula has no AWS vector: it is the same construction with
  the CRC as the digest, and is checked only against calculators in this repository's tests.
- An unnamed upload's object gets CRC64NVME FULL_OBJECT, and the response says so, parallel to
  PutObject. The documentation does not say what S3 stores for an upload that named nothing;
  this is a choice.
- An upload that lists a part stored without a checksum (a part of schema version 2) gets no
  object checksum and no checksum element in the response.

The Part elements (S3 API reference, "CompletedPart": ten `Checksum*` names, each "present
if the multipart upload request was created with" that algorithm; what S3 does when one
differs is not found):

- A named upload: an element in the upload's algorithm must equal the stored part value, or
  400 BadDigest; a malformed one is BadDigest too. An element in any other algorithm is 400
  InvalidRequest, decided over all Parts before any value is compared. No element is
  accepted.
- An unnamed upload: `ChecksumCRC64NVME` is compared with the stored value, because it names
  the checksum Litmus stores; equal is accepted, unequal or malformed is BadDigest, and a
  part with no stored checksum is accepted unverified. Elements in other algorithms are
  accepted unverified.
- The default AWS CLI sends a `ChecksumCRC64NVME` per part on every multipart upload, so an
  unnamed upload completes with them, as they are the stored values.

The headers (S3 API reference, "CompleteMultipartUpload": the headers are data integrity
checks and a type that differs from the one given at Create is `BadDigest`; S3 User Guide,
"Checking object integrity for data uploads": for CRC32, CRC32C, SHA1 and SHA256 without the
algorithm at Create, a header "is currently accepted but not validated or stored", and for
MD5, SHA512 and the XXHash family a value without the algorithm at Create is `InvalidRequest`):

- A named upload: the header in the upload's algorithm is compared with the object checksum
  in wire form, "-N" included for COMPOSITE; equal is accepted, unequal or malformed is
  400 BadDigest. A header in another algorithm, or more than one value header, is 400
  InvalidRequest. `x-amz-checksum-type` that is not COMPOSITE or FULL_OBJECT, in any letter
  case, is 400 InvalidArgument; a valid type that differs from the upload's is BadDigest;
  an equal one is accepted.
- An unnamed upload: `x-amz-checksum-crc64nvme` is validated against the object checksum
  (mismatch is BadDigest; accepted when the object has none), because it names the checksum
  Litmus stores. `x-amz-checksum-crc32`, `-crc32c`, `-sha1` and `-sha256` are accepted and
  not validated. `-md5`, `-sha512`, `-xxhash64`, `-xxhash3` and `-xxhash128` are 400
  InvalidRequest, as is more than one value header. A type that is not a valid name is
  InvalidArgument; a valid one is accepted and ignored.
- The request guard no longer answers 501 to any of these headers.

Part numbers. S3 API reference, "CompletedPart": when a checksum (`x-amz-checksum-crc32`,
`-crc32c`, `-sha1` or `-sha256`) is applied to each part, the part numbers must start at 1 and
be consecutive, else 400 `InvalidPartOrder`. The page does not mention FULL_OBJECT. Litmus
applies the rule to COMPOSITE uploads only, so a COMPOSITE upload whose listed numbers are not
exactly 1 to N is 400 InvalidPartOrder, and CRC32 and CRC32C FULL_OBJECT uploads, CRC64NVME
uploads and unnamed uploads allow gaps. S3 User Guide, "Checking object integrity for data
uploads", says non-consecutive numbers with part checksums are an `HTTP 500 Internal Server`
error. The two pages conflict; Litmus follows "CompletedPart" and does not follow the user
guide's 500.

The order of the errors is Litmus's; the documentation lists the errors and not which comes
first. Steps 1 to 14 are decided before a blob is registered, so a refusal leaves the upload
open with all its parts, as a failed condition does:

1. the request guard's 501s (no checksum header is one any more);
2. NoSuchUpload;
3. InvalidArgument, the syntax of `x-amz-mp-object-size`;
4. the shape of the checksum headers: InvalidRequest, then InvalidArgument for the type text;
5. MalformedXML;
6. InvalidPartOrder, the numbers not strictly ascending;
7. InvalidPart;
8. InvalidPartOrder, a COMPOSITE upload whose numbers are not 1 to N;
9. EntityTooSmall (see "The minimum part size");
10. InvalidRequest, a Part element in another algorithm (named uploads);
11. BadDigest, the first Part element in E that differs;
12. InvalidRequest, the size mismatch;
13. BadDigest, the type header (named uploads);
14. BadDigest, the checksum header value;
15. the assembly: NoSuchUpload if the upload is gone, InvalidPart if a part changed;
16. the commit's conditions, If-Match and If-None-Match.

Choices in this change, in one place:

- `x-amz-checksum-crc64nvme` on an unnamed upload is validated, because it names the checksum
  Litmus stores.
- An unnamed upload's object gets CRC64NVME FULL_OBJECT, and the response says so, parallel to
  PutObject.
- A COMPOSITE gap is 400 InvalidPartOrder per "CompletedPart", not the user guide's 500.
- The CRC32 and CRC32C composite formula has no AWS vector.
- An upload that lists a legacy part gets no object checksum.
- The "-N" suffix and the composite formula rest on ceph/s3-tests vectors, not on AWS pages.

**A framed upload with `Transfer-Encoding: chunked` and no `Content-Length` is 411.**
botocore builds an aws-chunked upload for an `https://` endpoint with `Transfer-Encoding:
chunked`, removes `Content-Length` and sets `x-amz-decoded-content-length`
(`botocore/httpchecksum.py`, `_apply_request_trailer_checksum`, botocore 1.42.89). Litmus
requires `Content-Length` for a framed body (the framed size is checked against it), so it
answers 411 MissingContentLength before the body is read. The S3 API reference says a
chunked upload must do one of two things: give the total length in `Content-Length`, or
send `Transfer-Encoding` with a value other than `identity` and omit `Content-Length` (S3
API reference, "Signature Calculations for the Authorization Header: Transferring
Payload in Multiple Chunks (Chunked Upload) (AWS Signature Version 4)"). So real S3
accepts this request and Litmus does not; this is a gap, not a decision, and it is not
tested for the checksum trailers beyond the refusal itself. The pages "Signature
calculations for trailing headers (chunked uploads) (AWS Signature Version 4)" and
"Checking object integrity for data uploads" do not mention `Transfer-Encoding` (not
found); both show `x-amz-decoded-content-length` as required. No client reaches this
today: Litmus has no TLS, and botocore over `http://` sends a checksum header and a plain
body instead of a trailer. The AWS CLI sends `Content-Length` with its trailers over
`http://` (captured with aws-cli 2.38.5), which Litmus decodes.

**Response header overrides.** GetObject and HeadObject take the query parameters
`response-cache-control`, `response-content-disposition`, `response-content-encoding`,
`response-content-language`, `response-content-type` and `response-expires`. Each sets the
header of that name in the response to that request: it replaces the stored value, or
adds the header when the object has none (S3 API reference, GetObject, "Overriding
response header values through the request", and the URI request parameters of GetObject
and HeadObject, "Sets the ... header of the response"). An override is never stored: a
later request without it gets the stored headers. The rest is Litmus's choice, listed
here:

- Status. GetObject says the overridden values "are included only in a successful
  response, that is, when the HTTP status code 200 OK is returned". Litmus also applies
  them to a 206, which goes beyond that sentence. An error response (404, 412, 416)
  carries none.
- Requests. GetObject says the parameters need a request signed with an Authorization
  header or a presigned URL, and "cannot be used with an unsigned (anonymous) request".
  Litmus has no authentication, so it applies them on every request.
- HeadObject documents the same six parameters, and says a HEAD "has the same options as
  a GET" and returns the GET response without the body, so HEAD applies them too. A Range
  on HEAD is ignored as before.
- An empty value is no override: the stored header, if any, is returned. The
  documentation is silent; this matches a blank header being stored as no value.
- A value with a control character other than a tab (CR, LF, NUL, the other C0
  characters or DEL, once percent-decoded) is refused with 400 InvalidArgument, whose
  `ArgumentName` is the parameter and which does not repeat the value. RFC 9110 5.5 says a
  field value must not contain CR, LF or NUL; the documentation does not say what S3 does.
  The check comes after the bucket check and before the key is looked up, on GET and HEAD
  alike, so a bad value is 400 even for a key that does not exist.
- A non-ASCII value is percent-decoded as UTF-8 and sent as its UTF-8 bytes, so
  `response-content-disposition=inline%3B%20f%3Dcaf%C3%A9` puts those bytes in the header.
  Percent-encoded bytes that are not valid UTF-8 are read as U+FFFD, as the query parser
  decodes them. RFC 2047 is not applied: the documentation uses it for user metadata only.
  The documentation is silent about non-ASCII overrides.
- A parameter sent more than once reads as its last value, as every other query
  parameter does here; if the last one is empty, there is no override.
- `response-expires` is sent as given, whether or not it is a date, like a stored
  `Expires`.
- Any other `response-*` parameter (`response-content-md5`, `response-foo`), a parameter
  name in another letter case, and a name with a trailing space are ignored, as any
  unknown query parameter is. The documentation lists the six only.

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
  metadata is not enforced either. `MetadataTooLarge` is the code S3's error list gives
  for metadata headers that exceed the maximum allowed size (S3 API reference, "Error
  responses", <https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html>).
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
- CreateMultipartUpload stores `x-amz-meta-*` with the upload and Complete gives it to the
  object; UploadPart and Complete ignore it (see "Multipart upload headers").

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
- CreateMultipartUpload stores them with the upload and Complete gives them to the
  object (see "Multipart upload headers"); UploadPart and Complete ignore them.

**Multipart upload headers.** CreateMultipartUpload takes the headers PutObject takes for
an object's properties: `Content-Type`, `Cache-Control`, `Content-Disposition`,
`Content-Encoding`, `Content-Language`, `Expires` and `x-amz-meta-*` (S3 API reference,
CreateMultipartUpload, "Request Syntax"). Litmus parses them with the PutObject code, so
every rule under "System headers" and "User metadata" applies unchanged: the `aws-chunked`
token is dropped from `Content-Encoding`, repeated headers are joined with `, `, a blank
system header is no value, RFC 2047 is decoded and encoded for metadata, and metadata over
2048 bytes is refused with 400 `MetadataTooLarge`. That refusal comes after the bucket
check and before the upload is created, so it creates no upload and no row. The headers
are stored in the upload's row, in the transaction that creates the upload.
CompleteMultipartUpload copies them onto the object's row in the transaction that
completes the upload, so a kill leaves the old object with its old headers and the upload
open, or the new object with the upload's headers and the upload completed
(`chaos/scenarios/test_crash_multipart_headers.py`). Completing over an existing object
replaces its headers and metadata entirely, as PutObject does: a header the upload did not
have is cleared. The documentation does not say in so many words that the headers of
CreateMultipartUpload become the object's; it is the one request in the multipart API
that documents them, and the tests in ceph/s3-tests expect it.

- UploadPart and CompleteMultipartUpload document none of these headers: UploadPart's
  request syntax lists `Content-Length`, `Content-MD5`, the checksum headers and the SSE-C
  headers, and Complete's lists the checksum headers, `x-amz-mp-object-size`, `If-Match`,
  `If-None-Match` and the SSE-C headers. Litmus ignores them there (a client that sends
  `Content-Type` or `x-amz-meta-*` on a part or on Complete changes nothing), and the
  object keeps what CreateMultipartUpload was given.
- Last-Modified. S3 User Guide, "System-defined object metadata", gives it as "the object
  creation date or the last modified date, whichever is the latest. For multipart uploads,
  the object creation date is the date of initiation of the multipart upload." A
  completed object's Last-Modified is therefore the time its upload was created, not the
  time Complete ran. GET, HEAD, ListObjects, ListObjectsV2 and ListObjectVersions all read
  the one stored value, so they agree (a listing rounds it to the second, as for every
  object). Completing over an object gives the new upload's initiation time, even when
  that is earlier than the replaced object's.
- An upload created before Litmus stored these headers has the default columns: completing
  it gives an object with `binary/octet-stream` and no other header or metadata, dated by
  its initiation time. No migration is needed.

**Decimal numbers in a request (decision 37).** Every decimal number taken from a request
is read by one parser, `parse_decimal` in `server/numerals.py`: one or more ASCII digits
0 to 9 and nothing else (no sign, whitespace, underscore or other Unicode digit); leading
zeros are stripped first; a value of more than 18 digits after that is 10^18, which is above
every size, count and part number Litmus compares a request value with; it never raises.
Python 3.12's `int()` refuses more than 4300 digits, which made `max-keys`,
`x-amz-copy-source-range` and `x-amz-decoded-content-length` answer 500 for such a value.
The S3 documentation names no answer for an oversized or malformed number (not found, for
each line below), so each answer is Litmus's choice: an oversized value gets the answer of an
out of range value of its field.

- *`max-keys`.* An oversized value is capped to 1000 and answers 200, as 1001 does.
- *`max-parts` and `part-number-marker` (ListParts).* Unchanged: an oversized `max-parts` is
  the cap, an oversized marker lists no parts, and the response echoes the marker's digits
  without leading zeros.
- *`x-amz-mp-object-size`.* Unchanged: an oversized value is a mismatch, 400 InvalidRequest.
- *Complete `PartNumber`.* The syntax is unchanged (an optional `-` and ASCII digits, else
  400 MalformedXML, so `+1` is still MalformedXML). Leading zeros are stripped before the
  length is judged, so 4300 zeros and a 1 is part 1. A number of more than 18 significant
  digits is InvalidPart, and its `PartNumber` element carries the digits as sent, without
  leading zeros and with a leading `-` kept. The order check compares the true signed
  values, not the saturated ones: two distinct oversized numbers get the answers 10001 and
  10002 get in either order, and equal values get the answer a repeated part gets.
- *`partNumber` of UploadPart and UploadPartCopy.* ASCII digits give their value; a value
  outside 1 to 10000, an oversized one included, is 400 InvalidPart with a `PartNumber`
  element, as 10001 is, in the same place in the order of the checks (for an oversized
  value the element carries the digits sent, without leading zeros and with a leading `-`
  kept). `-` and ASCII digits is
  the answer `-1` has (InvalidPart, `PartNumber` `-1`). Anything else, a plus sign,
  whitespace, an underscore, a non-ASCII digit, a fraction or an empty value, is the answer
  `abc` has: 400 InvalidPart with no `PartNumber`. `+1`, ` 1`, `1_0` and an Arabic-Indic
  digit used to be accepted, because Python's `int()` reads them. The documented range is
  1 to 10,000 (S3 API reference, "UploadPart" and "UploadPartCopy"). The place of the
  non-numeric check is unchanged: before NoSuchBucket on UploadPart, after it on
  UploadPartCopy (making them agree is a separate fix).
- *`x-amz-copy-source-range`.* Numerals are read by value. A first or last byte past the
  source, an oversized one included, is the 416 InvalidRange of `bytes=0-100` on a smaller
  source. A first byte above the last is the 400 InvalidArgument of `bytes=5-2`; two numerals
  that are both oversized are ordered by their digits. Leading zeros are read by value.
- *`x-amz-decoded-content-length`.* An oversized value is the 400 InvalidRequest of 5 GiB
  plus 1. Leading zeros are read by value.
- *`Content-Length` of an aws-chunked request.* Read by `parse_decimal`; every header the
  old code accepted gets the same answer, and one with long leading zeros is its value. The
  HTTP server comes first: under httptools such a header reaches Litmus; under h11 the parser
  refuses it before Litmus sees it (its own plain text 400, "Invalid HTTP request received.").
- *`Range` on GET and HEAD.* Unchanged, and not read with `parse_decimal`: a numeral of more
  than 4000 digits still makes the specifier invalid and the header is ignored (see "Range"
  above), which already keeps it away from `int()` by design.

**Multipart details.** `partNumber` on GET is ignored (with `x-amz-checksum-mode` it returns the whole object and its whole object checksum).

**Conditional GET and HEAD.** `If-Match`, `If-None-Match`, `If-Modified-Since` and
`If-Unmodified-Since` are evaluated on GET and HEAD, against the one row the response is
built from, after the bucket and key are known to exist and before the blob is opened
(so a missing bucket or key is 404 whatever the conditions, RFC 9110 13.2.1). They run in
the order of RFC 9110 13.2.2: `If-Match` (false: 412), else `If-Unmodified-Since` (false:
412), then `If-None-Match` (false: 304), else `If-Modified-Since` (false: 304). Both
combinations the S3 GetObject reference documents follow from it. A conditional request
is answered before `Range` is looked at, so 412 and 304 beat 416 and 206. `If-Range`
is evaluated on GET after them (below).

Choices where the S3 documentation is silent or the RFC leaves room:

- A 304 carries `ETag` and, when stored, `Cache-Control` and `Expires`, with no body.
  `Last-Modified` is left out: RFC 9110 15.4.5 says a sender should not generate
  representation metadata other than the fields it lists, and the ETag is present. S3's
  own 304 may differ; the documentation does not say. Content-Type, user metadata and
  `response-*` overrides are not sent on a 304.
- Entity tags are read as RFC 9110 8.8.3 writes them, and a tag sent without its quotes
  is read as that opaque tag (ceph's write tests send both forms). `If-Match` uses strong
  comparison (8.8.3.2), so a weak tag never matches; `If-None-Match` uses weak
  comparison. `*` matches any existing object and is a wildcard only on its own, not
  inside a list. A list is comma separated; a comma inside a quoted tag is part of it;
  repeated header lines are one list. A blank header is no condition.
- A date is an HTTP-date in IMF-fixdate, RFC 850 or asctime form (RFC 9110 5.6.7) with
  the zone GMT. A header that is blank, repeated, a list of dates or otherwise not a
  valid date is ignored (13.1.3, 13.1.4), not a 400. Dates compare at the second that
  `Last-Modified` shows: the stored microseconds are dropped.
- `If-Unmodified-Since` is ignored when `If-Match` is present, and `If-Modified-Since`
  when `If-None-Match` is present, even when the present header passes.
- HEAD evaluates the same conditions. Before this change HEAD ignored all of them, `If-Match`
  included: a HEAD with a non-matching `If-Match` got 200. That gap is closed. A 412 on
  HEAD has the status and headers of the GET answer and, as for any HEAD, no body.

**If-Range.** On a GET that carries a `Range`, `If-Range` is step 5 of RFC 9110 13.2.2,
after the four conditions above. When it holds, the `Range` applies (206, or 416 when no
byte satisfies it). When it does not, the `Range` is ignored and the whole object is
sent with 200 (13.1.5: "a recipient of an If-Range header field MUST ignore the Range
header field if the If-Range condition evaluates to false"). It is evaluated on the row
GET ends up serving, so a GET that raced an overwrite compares it with the new row: a
validator naming the old version gets the whole new object, never a slice of it. The
`response-*` overrides apply to the 206 and to the 200 alike.

- An entity tag holds when it is not weak and equals the ETag character by character
  (strong comparison, 8.8.3.2). A tag sent without its quotes is read as that opaque tag,
  as for the other conditions.
- A date holds when it equals `Last-Modified` exactly, at the second the header carries,
  and `Last-Modified` is a strong validator. 13.1.5: the condition is false if the date
  is not a strong validator. 8.8.2.2 calls a modification date weak unless it can be
  deduced to be strong; Litmus treats it as strong once the object is at least one second
  old, since any later change then falls in another second. An object written less than a
  second ago never matches a date.
- The RFC does not say what a value is that is neither a valid entity tag nor a valid
  HTTP-date. Litmus treats it as not holding and sends the whole object, the safe
  answer when the validator cannot be checked, rather than ignoring the header and
  serving a partial response. That covers a blank value, a list, `*`, an unterminated
  quote, a date in any other zone than GMT, and a header sent twice.
- Without a `Range` the header is ignored (13.1.5: a server MUST ignore it). A `Range`
  that is itself ignored (a set of ranges, a unit other than bytes, a malformed
  specifier) leaves the answer 200 whether `If-Range` holds or not.
- HEAD ignores `If-Range`: step 5 is for GET, and 14.2 says range handling is defined for
  GET alone. HEAD still answers 416 for an unsatisfiable `Range` whatever `If-Range`
  says, as it did before.
