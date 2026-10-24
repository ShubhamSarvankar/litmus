# Publication barrier

## The rule

New bytes are durable before the commit that makes them visible. A client is told
200 only after that commit.

## How objects are stored

Each object version and each uploaded part is one file, `blobs/<xx>/<id>`, where
`<id>` is 32 random hex characters and `<xx>` is its first two. No bucket name, key,
upload ID or part number is ever part of a path, so every key is stored as the exact
string the client sent, including keys such as `k` and `k/`, or keys with `.` and
`..` segments. A blob file is written once and never modified; it is only read and,
at the end, unlinked.

The SQLite database maps each key to the ID of its blob. A registry table records
every blob from before its file is created until after the file is unlinked:
`pending` while it is being written, `live` once an object or part refers to it, and
`garbage` once nothing does.

## The registration commit

Before any byte is written, the new blob ID is committed to the registry as
`pending`. This commit makes nothing visible. It records which file belongs to which
write, so that the startup sweep can remove the leftovers of a write that died, and
report the bucket and key it was for.

## PutObject, new key or overwrite

1. Commit the new blob ID as `pending`.
2. Write the body to `blobs/tmp/<id>.tmp`, computing its MD5, and fsync the file.
3. Rename it to `blobs/<xx>/<id>` and fsync `blobs/<xx>`.
4. In one transaction: point the key's row at the new blob, mark the new blob `live`,
   and mark the blob it replaces `garbage`.
5. Unlink the replaced blob and remove its registry row.
6. Answer 200 with the ETag.

The old object's file is never touched before step 4, because the new bytes always go
to a fresh file. A kill before step 4 leaves the previous object (or no object)
visible, and the sweep removes the new blob. A kill after step 4 leaves the new
object visible, and the sweep removes the old file if step 5 did not finish.

## DeleteObject and DeleteObjects

One transaction deletes the object row and marks its blob `garbage`; the file is
unlinked after that commit. A row never points at a missing file: a kill before the
commit leaves the object and its bytes in place, and a kill after it leaves the file
for the sweep. DeleteObjects deletes every listed key in one transaction.

## UploadPart, including re-uploading a part number

UploadPart writes a new blob exactly as PutObject does, then commits it as the
part's blob, provided the upload is still open; otherwise it answers NoSuchUpload.
Re-uploading a part number writes another new blob. The previous part stays recorded
and intact until the commit that replaces it.

## CompleteMultipartUpload

1. Check that every listed part is recorded with a matching ETag.
2. Commit a new blob ID as `pending`.
3. Concatenate the part blobs into it, with the same fsync, rename and directory
   fsync as PutObject.
4. In one transaction:
   - mark the upload completed, which is refused (NoSuchUpload) if the upload is no
     longer open;
   - check that each listed part still names the blob that was assembled, which is
     refused (InvalidPart) if a part was re-uploaded during assembly;
   - point the key's row at the assembled blob and mark it `live`;
   - mark the part blobs, and any object blob it replaces, `garbage`.
5. Unlink the blobs marked `garbage`.
6. Answer 200 with the composite ETag.

Complete writes nothing outside its own new blob before its commit, so a refused or
failed Complete leaves an existing object at the key untouched. The rows of the
listed parts keep their ETag and size after completion.

## AbortMultipartUpload and DeleteBucket

AbortMultipartUpload deletes the upload and its part rows and marks the part blobs
`garbage` in one transaction, then unlinks them. DeleteBucket refuses a bucket that
still has objects; otherwise it aborts the bucket's open uploads in the same
transaction that deletes the bucket.

## GET

1. Read the key's row: its blob ID, size and ETag.
2. Open that blob.
3. If the blob was already unlinked, a later commit replaced or deleted the key: read
   the row again and go back to step 2 with the new blob, or answer 404 if the key is
   gone. Three such retries are allowed; the fourth failed open answers 503 SlowDown,
   so one GET opens at most four times.
4. Send the size and ETag from the row that named the opened blob, then stream from
   the open file.

An open file keeps its bytes after it is unlinked, so a GET that has opened a blob
sends that whole version even if an overwrite and its cleanup finish while it is
streaming. A range GET follows the same steps.

## Concurrent writers

Two PUTs to the same key each write their own blob. Their commits run one at a time
under SQLite's write lock, and each commit reads the current row under that lock. The
last commit wins, both clients receive 200, and the blob of the earlier commit is
removed. The test for this runs the two requests on separate threads and holds the
first commit until the second has started its own.
