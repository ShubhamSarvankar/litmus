#!/usr/bin/env bash
# run_ceph_s3tests.sh
#
# Clones ceph/s3-tests and runs the suite against a local litmus instance.
# Requires: Linux/macOS, Python 3.8+ (ceph/s3-tests pins its own venv), git, pip.
# litmus must already be running on localhost:8000.
#
# Usage:
#   # Start litmus first:
#   uv run uvicorn server.app:app --host 127.0.0.1 --port 8000 &
#
#   # Then run this script from the repo root:
#   bash scripts/run_ceph_s3tests.sh
#
# Results are written to docs/compatibility.md (summary) and
# s3tests-results.txt (full pytest output).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
S3TESTS_DIR="$REPO_ROOT/.s3tests"
RESULTS_FILE="$REPO_ROOT/s3tests-results.txt"
S3_HOST="localhost"
S3_PORT="8000"
BUCKET_PREFIX="litmus-test-"

# --- Clone or update ceph/s3-tests ---
if [ ! -d "$S3TESTS_DIR" ]; then
    echo "Cloning ceph/s3-tests..."
    git clone https://github.com/ceph/s3-tests.git "$S3TESTS_DIR"
else
    echo "Updating ceph/s3-tests..."
    cd "$S3TESTS_DIR" && git pull --ff-only && cd "$REPO_ROOT"
fi

# --- Write config file ---
CONFIG_FILE="$S3TESTS_DIR/s3tests.conf"
cat > "$CONFIG_FILE" << EOF
[DEFAULT]
host = $S3_HOST
port = $S3_PORT
is_secure = False

[fixtures]
bucket prefix = $BUCKET_PREFIX

[s3 main]
access_key = testkey
secret_key = testsecret
display_name = Test User
user_id = testuser
email = test@example.com
region = us-east-1

[s3 alt]
access_key = altkey
secret_key = altsecret
display_name = Alt User
user_id = altuser
email = alt@example.com
region = us-east-1

[s3 tenant]
access_key = tenantkey
secret_key = tenantsecret
display_name = Tenant User
user_id = tenantuser
email = tenant@example.com
tenant = testtenant

[iam]
access_key = iamkey
secret_key = iamsecret
display_name = IAM User
user_id = iamuser
email = iam@example.com

[iam root]
access_key = iamrootkey
secret_key = iamrootsecret
user_id = iamrootuser
email = iamroot@example.com

[iam alt root]
access_key = iamaltrootkey
secret_key = iamaltrootsecret
user_id = iamaltrootuser
email = iamaltroot@example.com
EOF

echo "Config written to $CONFIG_FILE"

# --- Bootstrap the s3-tests virtualenv ---
cd "$S3TESTS_DIR"
if [ ! -d ".tox/venv" ] && [ ! -d "venv" ]; then
    echo "Bootstrapping s3-tests virtualenv..."
    if command -v python3.11 &>/dev/null; then
        python3.11 -m venv venv
    elif command -v python3.10 &>/dev/null; then
        python3.10 -m venv venv
    else
        python3 -m venv venv
    fi
    venv/bin/pip install -q --upgrade pip
    venv/bin/pip install -q -r requirements.txt
fi

PYTHON="$S3TESTS_DIR/venv/bin/python"
PYTEST="$S3TESTS_DIR/venv/bin/pytest"

# --- Verify litmus is reachable ---
echo "Checking litmus is running on $S3_HOST:$S3_PORT..."
if ! curl -sf "http://$S3_HOST:$S3_PORT/health" > /dev/null; then
    echo "ERROR: litmus not reachable at http://$S3_HOST:$S3_PORT/health"
    echo "Start it first: uv run uvicorn server.app:app --host 127.0.0.1 --port 8000"
    exit 1
fi
echo "Server is up."

# --- Run the suite ---
# We run only the s3tests/functional/test_s3.py subset and skip known-incompatible
# categories (auth, ACLs, versioning, encryption, website, lifecycle, etc.)
#
# -x is intentionally NOT set — we want to see all results, not stop at first failure.

echo "Running ceph/s3-tests..."
cd "$S3TESTS_DIR"
S3TEST_CONF="$CONFIG_FILE" "$PYTEST" \
    s3tests/functional/test_s3.py \
    -v \
    --tb=short \
    -k "not (acl or ACL or policy or Policy or versioning or Versioning \
             or website or Website or lifecycle or Lifecycle \
             or encryption or Encryption or sse or SSE or kms or KMS \
             or cors or CORS or replication or Replication \
             or tagging or Tagging or notification or Notification \
             or analytics or Analytics or inventory or Inventory \
             or accelerate or Accelerate or requestpayment \
             or bucket_logging or public_access or object_lock \
             or torrent or copy_object_ifmatch_failed \
             or multipart_upload_empty or auth or Auth \
             or presign or checksum)" \
    2>&1 | tee "$RESULTS_FILE" || true

# --- Parse results ---
PASSED=$(grep -c "PASSED" "$RESULTS_FILE" || true)
FAILED=$(grep -c "FAILED" "$RESULTS_FILE" || true)
ERROR=$(grep -c "ERROR" "$RESULTS_FILE" || true)
SKIPPED=$(grep -c "SKIPPED" "$RESULTS_FILE" || true)

echo ""
echo "=============================="
echo "ceph/s3-tests results summary"
echo "=============================="
echo "PASSED:  $PASSED"
echo "FAILED:  $FAILED"
echo "ERROR:   $ERROR"
echo "SKIPPED: $SKIPPED"
echo ""
echo "Full output saved to: $RESULTS_FILE"
echo "Update docs/compatibility.md with these results."

cd "$REPO_ROOT"