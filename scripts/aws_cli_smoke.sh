#!/usr/bin/env bash
# aws_cli_smoke.sh
#
# Runs an unmodified AWS CLI v2 against a scratch Litmus server over http and checks
# the results of mb, ls, cp (including s3 to s3), sync, s3api head-object / get-object,
# rm and rb. The CLI runs with its default client behavior: no checksum or s3 config
# overrides are set. Litmus has no authentication, so the credentials are dummies in a
# temp credentials file; ~/.aws is never read.
#
# Requires: AWS CLI v2 >= 2.23 on a Linux PATH, uv (or python3 with the project
# dependencies), curl, python3.
#
# Usage, from anywhere:
#   bash scripts/aws_cli_smoke.sh
#
# Prints PASS or FAIL per step and exits nonzero on the first failure.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MIN_VERSION="2.23"

# --- Resolve the CLI. In WSL the Windows PATH is appended, so a stale shim under /mnt
# can come first.
AWS_BIN="$(command -v aws || true)"
if [ -z "$AWS_BIN" ]; then
    echo "ERROR: aws not found on PATH"
    exit 1
fi
case "$AWS_BIN" in
    /mnt/*)
        echo "ERROR: aws resolves to $AWS_BIN, a Windows install under /mnt/. Put a Linux AWS CLI v2 first on PATH."
        exit 1
        ;;
esac
AWS_VERSION_LINE="$("$AWS_BIN" --version 2>&1)" || {
    echo "ERROR: $AWS_BIN --version failed: $AWS_VERSION_LINE"
    exit 1
}
AWS_VERSION="$(sed -nE 's#^aws-cli/([0-9]+\.[0-9]+\.[0-9]+).*#\1#p' <<< "$AWS_VERSION_LINE")"
if [ -z "$AWS_VERSION" ]; then
    echo "ERROR: cannot parse the CLI version from: $AWS_VERSION_LINE"
    exit 1
fi
if [ "$(printf '%s\n%s\n' "$MIN_VERSION" "$AWS_VERSION" | sort -V | head -n1)" != "$MIN_VERSION" ]; then
    echo "ERROR: aws-cli $AWS_VERSION is older than $MIN_VERSION"
    exit 1
fi
echo "aws: $AWS_BIN"
echo "version: $AWS_VERSION_LINE"

# --- Scratch store, free port, server
WORK="$(mktemp -d)"
SERVER_PID=""
cleanup() {
    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$WORK"
}
trap cleanup EXIT

PORT="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
ENDPOINT="http://127.0.0.1:$PORT"
SERVER_LOG="$WORK/server.log"

if command -v uv > /dev/null; then
    UVICORN=(uv run uvicorn)
else
    UVICORN=(python3 -m uvicorn)
fi
(
    cd "$REPO_ROOT"
    S3_BLOB_DIR="$WORK/store/blobs" S3_DB_PATH="$WORK/store/litmus.db" \
        S3_OWNER_ID=testuser S3_OWNER_DISPLAY_NAME="Test User" \
        exec "${UVICORN[@]}" server.app:app --host 127.0.0.1 --port "$PORT" > "$SERVER_LOG" 2>&1
) &
SERVER_PID=$!

for _ in $(seq 1 100); do
    if curl -sf "$ENDPOINT/health" > /dev/null; then
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "ERROR: server exited during startup:"
        cat "$SERVER_LOG"
        exit 1
    fi
    sleep 0.1
done
if ! curl -sf "$ENDPOINT/health" > /dev/null; then
    echo "ERROR: server not healthy at $ENDPOINT/health"
    cat "$SERVER_LOG"
    exit 1
fi
echo "server: $ENDPOINT (scratch store in $WORK/store)"

# --- Isolated CLI configuration
export AWS_CONFIG_FILE="$WORK/aws_config"
export AWS_SHARED_CREDENTIALS_FILE="$WORK/aws_credentials"
export LC_ALL=C.UTF-8
unset AWS_PROFILE AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
printf '[default]\nregion = us-east-1\n' > "$AWS_CONFIG_FILE"
printf '[default]\naws_access_key_id = AKIDDUMMY\naws_secret_access_key = dummysecret\n' \
    > "$AWS_SHARED_CREDENTIALS_FILE"

aws() { "$AWS_BIN" --endpoint-url "$ENDPOINT" "$@"; }

die() { echo "    $*" >&2; exit 1; }

# ls output is "DATE TIME SIZE NAME" for objects and "PRE NAME" for prefixes; strip the
# leading columns and keep the name exactly, spaces included.
ls_names() {
    sed -E 's/^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:]{8} +[0-9]+ //; s/^ *PRE //'
}

# Bucket listing lines have no size column: "DATE TIME NAME".
bucket_names() {
    sed -E 's/^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:]{8} //'
}

# The CLI separates its "Completed ..." progress text from the result lines with a bare
# carriage return when output is not a terminal; count result lines after splitting on it.
count_lines() { tr '\r' '\n' < "$WORK/out" | grep -c "^$1:" || true; }

sha() { sha256sum "$1" | cut -d' ' -f1; }

DATA="$WORK/data"
mkdir -p "$DATA"
head -c 1024 /dev/urandom > "$DATA/small.bin"
head -c $((40 * 1024 * 1024)) /dev/urandom > "$DATA/large.bin"

run_step() {
    local name="$1" rc
    set +e
    ( set -euo pipefail; "$name" )
    rc=$?
    set -e
    if [ "$rc" -ne 0 ]; then
        echo "FAIL $name"
        echo "--- server log (tail) ---"
        tail -n 20 "$SERVER_LOG"
        exit 1
    fi
    echo "PASS $name"
}

BUCKET="smoke-main"
KBUCKET="smoke-keys"

step_mb_ls() {
    aws s3 mb "s3://$BUCKET" > "$WORK/out" 2>&1 || { cat "$WORK/out"; die "mb failed"; }
    grep -q "make_bucket: $BUCKET" "$WORK/out" || die "mb output: $(cat "$WORK/out")"
    aws s3 ls | bucket_names | grep -qx "$BUCKET" || die "ls does not show $BUCKET"
}

step_cp_roundtrip() {
    aws s3 cp "$DATA/small.bin" "s3://$BUCKET/small.bin" > /dev/null || die "upload small failed"
    aws s3 cp "$DATA/large.bin" "s3://$BUCKET/large.bin" > /dev/null || die "upload large failed"
    mkdir -p "$WORK/down"
    aws s3 cp "s3://$BUCKET/small.bin" "$WORK/down/small.bin" > /dev/null || die "download small failed"
    aws s3 cp "s3://$BUCKET/large.bin" "$WORK/down/large.bin" > /dev/null || die "download large failed"
    [ "$(sha "$DATA/small.bin")" = "$(sha "$WORK/down/small.bin")" ] || die "small sha256 differs"
    [ "$(sha "$DATA/large.bin")" = "$(sha "$WORK/down/large.bin")" ] || die "large sha256 differs"
}

step_s3api_put_get() {
    head -c 300 /dev/urandom > "$WORK/put.bin"
    aws s3api put-object --bucket "$BUCKET" --key put.bin --body "$WORK/put.bin" > "$WORK/out" 2>&1 \
        || { cat "$WORK/out"; die "put-object failed"; }
    aws s3api get-object --bucket "$BUCKET" --key put.bin "$WORK/put.down" > /dev/null || die "get-object failed"
    [ "$(sha "$WORK/put.bin")" = "$(sha "$WORK/put.down")" ] || die "put-object/get-object sha256 differs"
}

step_key_names() {
    aws s3 mb "s3://$KBUCKET" > /dev/null || die "mb $KBUCKET failed"
    local keys=("a b.txt" "plus+sign.txt" "café.txt" "dir/one.txt" "dir/sub/two.txt")
    local k
    for k in "${keys[@]}"; do
        mkdir -p "$WORK/keysrc/$(dirname "$k")"
        printf 'content of %s\n' "$k" > "$WORK/keysrc/$k"
        aws s3 cp "$WORK/keysrc/$k" "s3://$KBUCKET/$k" > /dev/null || die "upload of '$k' failed"
    done

    local got want
    got="$(aws s3 ls "s3://$KBUCKET/" | ls_names)"
    want=$'PRE dir/\na b.txt\ncafé.txt\nplus+sign.txt'
    want="$(sed 's/^PRE //' <<< "$want")"
    [ "$got" = "$want" ] || die "root listing:"$'\n'"$got"$'\n'"expected:"$'\n'"$want"

    got="$(aws s3 ls "s3://$KBUCKET/dir/" | ls_names)"
    want=$'sub/\none.txt'
    [ "$got" = "$want" ] || die "prefix listing:"$'\n'"$got"$'\n'"expected:"$'\n'"$want"

    got="$(aws s3 ls "s3://$KBUCKET/" --recursive | ls_names)"
    want=$'a b.txt\ncafé.txt\ndir/one.txt\ndir/sub/two.txt\nplus+sign.txt'
    [ "$got" = "$want" ] || die "recursive listing:"$'\n'"$got"$'\n'"expected:"$'\n'"$want"

    # The names must also round trip as content.
    for k in "${keys[@]}"; do
        aws s3 cp "s3://$KBUCKET/$k" "$WORK/keydown" > /dev/null || die "download of '$k' failed"
        cmp -s "$WORK/keydown" "$WORK/keysrc/$k" || die "content of '$k' differs"
    done
}

step_sync() {
    local i
    mkdir -p "$WORK/syncsrc/nested/deeper"
    for i in $(seq 1 20); do
        case $((i % 3)) in
            0) f="$WORK/syncsrc/file$i.txt" ;;
            1) f="$WORK/syncsrc/nested/file$i.txt" ;;
            2) f="$WORK/syncsrc/nested/deeper/file$i.txt" ;;
        esac
        head -c $((i * 137)) /dev/urandom > "$f"
    done
    # sync skips a key whose size matches and whose local mtime is not later than the
    # listed LastModified, which has one second resolution. Files created in the same
    # second as the upload would look newer, so backdate them first.
    find "$WORK/syncsrc" -type f -exec touch -d '2 minutes ago' {} +
    aws s3 sync "$WORK/syncsrc" "s3://$BUCKET/sync/" > "$WORK/out" 2>&1 || { cat "$WORK/out"; die "sync up failed"; }
    [ "$(count_lines upload)" -eq 20 ] || die "first sync uploaded $(count_lines upload) files, expected 20"

    aws s3 sync "$WORK/syncsrc" "s3://$BUCKET/sync/" > "$WORK/out" 2>&1 || { cat "$WORK/out"; die "second sync failed"; }
    [ ! -s "$WORK/out" ] || die "second sync transferred something: $(cat "$WORK/out")"

    mkdir -p "$WORK/syncdown"
    aws s3 sync "s3://$BUCKET/sync/" "$WORK/syncdown" > "$WORK/out" 2>&1 || { cat "$WORK/out"; die "sync down failed"; }
    [ "$(count_lines download)" -eq 20 ] || die "sync down fetched $(count_lines download) files, expected 20"
    diff -r "$WORK/syncsrc" "$WORK/syncdown" || die "downloaded tree differs"
}

step_s3api_head_range() {
    local size etag
    size="$(aws s3api head-object --bucket "$BUCKET" --key large.bin --query ContentLength --output text)"
    [ "$size" = "$((40 * 1024 * 1024))" ] || die "ContentLength $size"
    etag="$(aws s3api head-object --bucket "$BUCKET" --key large.bin --query ETag --output text)"
    [[ "$etag" =~ ^\"[0-9a-f]{32}-[0-9]+\"$ ]] || die "multipart ETag has unexpected shape: $etag"

    local start=10485750 len=64 range
    range="$(aws s3api get-object --bucket "$BUCKET" --key large.bin \
        --range "bytes=$start-$((start + len - 1))" "$WORK/range.bin" \
        --query ContentRange --output text)"
    [ "$range" = "bytes $start-$((start + len - 1))/$((40 * 1024 * 1024))" ] || die "ContentRange: $range"
    [ "$(stat -c %s "$WORK/range.bin")" -eq "$len" ] || die "range body is $(stat -c %s "$WORK/range.bin") bytes"
    dd if="$DATA/large.bin" bs=1 skip="$start" count="$len" of="$WORK/range.want" 2> /dev/null
    cmp -s "$WORK/range.want" "$WORK/range.bin" || die "range bytes differ from the source"
}

step_copy_object() {
    # An object under the CLI's multipart threshold is copied with one CopyObject.
    aws s3 cp "s3://$BUCKET/small.bin" "s3://$BUCKET/copied.bin" > "$WORK/out" 2>&1 \
        || { cat "$WORK/out"; die "s3 to s3 cp failed"; }
    mkdir -p "$WORK/copydown"
    aws s3 cp "s3://$BUCKET/copied.bin" "$WORK/copydown/copied.bin" > /dev/null || die "download of the copy failed"
    cmp -s "$DATA/small.bin" "$WORK/copydown/copied.bin" || die "the copy's bytes differ from the source's"

    local src_etag dst_etag
    src_etag="$(aws s3api head-object --bucket "$BUCKET" --key small.bin --query ETag --output text)"
    dst_etag="$(aws s3api head-object --bucket "$BUCKET" --key copied.bin --query ETag --output text)"
    [[ "$dst_etag" =~ ^\"[0-9a-f]{32}\"$ ]] || die "copy ETag has unexpected shape: $dst_etag"
    [ "$dst_etag" = "$src_etag" ] || die "copy ETag $dst_etag, source ETag $src_etag"
    aws s3 ls "s3://$BUCKET/" | ls_names | grep -qx "copied.bin" || die "copied.bin is not listed"
    aws s3api head-object --bucket "$BUCKET" --key small.bin > /dev/null || die "the source is gone"
}

step_copy_large_fails_on_get_object_tagging() {
    # Above the threshold the CLI copies in parts, and before it sends anything it reads
    # the source's tags with GetObjectTagging, which Litmus does not implement. The copy
    # fails there, so no UploadPartCopy is ever sent (the server implements it; the
    # boto3 managed copy in tests/conformance/test_upload_part_copy_boto3.py sends one).
    aws s3 cp "s3://$BUCKET/large.bin" "s3://$BUCKET/copied-large.bin" > "$WORK/out" 2>&1 \
        && die "s3 to s3 cp of the 40 MiB object succeeded: $(cat "$WORK/out")"
    grep -q "NotImplemented" "$WORK/out" || die "no NotImplemented in: $(cat "$WORK/out")"
    grep -q "GetObjectTagging" "$WORK/out" || die "NotImplemented does not name GetObjectTagging in: $(cat "$WORK/out")"
    if aws s3api head-object --bucket "$BUCKET" --key copied-large.bin > /dev/null 2>&1; then
        die "copied-large.bin exists after the failed copy"
    fi
    aws s3 ls "s3://$BUCKET/" | ls_names | grep -qx "copied-large.bin" && die "copied-large.bin is listed"
    aws s3api head-object --bucket "$BUCKET" --key large.bin > /dev/null || die "the source is gone"
    return 0
}

step_rm_rb() {
    aws s3 rm "s3://$BUCKET/small.bin" > /dev/null || die "rm failed"
    if aws s3api head-object --bucket "$BUCKET" --key small.bin > /dev/null 2>&1; then
        die "small.bin still exists after rm"
    fi
    aws s3api head-object --bucket "$BUCKET" --key large.bin > /dev/null || die "rm removed the wrong key"

    aws s3 rm "s3://$BUCKET/sync/" --recursive > "$WORK/out" 2>&1 || { cat "$WORK/out"; die "rm --recursive failed"; }
    [ "$(count_lines delete)" -eq 20 ] || die "rm --recursive deleted $(count_lines delete) keys, expected 20"
    [ -z "$(aws s3 ls "s3://$BUCKET/sync/" --recursive)" ] || die "sync/ not empty after rm --recursive"

    aws s3 rb "s3://$BUCKET" > "$WORK/out" 2>&1 && die "rb of a non-empty bucket succeeded"
    grep -q "BucketNotEmpty" "$WORK/out" || die "no BucketNotEmpty in: $(cat "$WORK/out")"
    aws s3 ls | bucket_names | grep -qx "$BUCKET" || die "bucket gone after a failed rb"

    aws s3 rb "s3://$BUCKET" --force > /dev/null || die "rb --force failed"
    aws s3 ls | bucket_names | grep -qx "$BUCKET" && die "$BUCKET still listed after rb --force"
    aws s3 rb "s3://$KBUCKET" --force > /dev/null || die "rb --force $KBUCKET failed"
    return 0
}

run_step step_mb_ls
run_step step_cp_roundtrip
run_step step_s3api_put_get
run_step step_key_names
run_step step_sync
run_step step_s3api_head_range
run_step step_copy_object
run_step step_copy_large_fails_on_get_object_tagging
run_step step_rm_rb

echo "All steps passed (aws-cli $AWS_VERSION)."
