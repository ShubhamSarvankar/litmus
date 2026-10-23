#!/usr/bin/env bash
# run_ceph_s3tests.sh
#
# Checks out ceph/s3-tests at a pinned commit and runs the suite against a local
# litmus instance. The s3-tests virtualenv is installed with the exact versions in
# scripts/s3tests-constraints.txt.
# Requires: Linux/macOS, python3 with venv, git, pip.
# litmus must already be running on localhost:8000.
#
# Usage:
#   # Start litmus first:
#   uv run uvicorn server.app:app --host 127.0.0.1 --port 8000 &
#
#   # Then run this script from the repo root:
#   bash scripts/run_ceph_s3tests.sh
#
# Results are written to s3tests-results.txt (full pytest output) and
# docs/s3-tests-results.txt (one "OUTCOME test_name" line per selected test,
# sorted by name). The summary printed at the end must be copied into
# docs/compatibility.md by hand.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
S3TESTS_DIR="$REPO_ROOT/.s3tests"
RESULTS_FILE="$REPO_ROOT/s3tests-results.txt"
JUNIT_FILE="$S3TESTS_DIR/junit.xml"
PER_TEST_FILE="$REPO_ROOT/docs/s3-tests-results.txt"
CONSTRAINTS_FILE="$SCRIPT_DIR/s3tests-constraints.txt"
S3TESTS_REPO="https://github.com/ceph/s3-tests.git"
# Pinned so results only change when litmus changes. Bumping this is a deliberate
# baseline change: rerun, regenerate docs/s3-tests-results.txt, update the docs.
S3TESTS_COMMIT="5b3987116eca0b4b8fa57d6b10f3db57bab196b6"
S3_HOST="localhost"
S3_PORT="8000"
BUCKET_PREFIX="litmus-test-"

# --- Check out ceph/s3-tests at the pinned commit ---
if [ ! -d "$S3TESTS_DIR/.git" ]; then
    echo "Initialising ceph/s3-tests checkout..."
    git init -q "$S3TESTS_DIR"
    git -C "$S3TESTS_DIR" remote add origin "$S3TESTS_REPO"
fi
if ! git -C "$S3TESTS_DIR" cat-file -e "$S3TESTS_COMMIT^{commit}" 2>/dev/null; then
    echo "Fetching ceph/s3-tests $S3TESTS_COMMIT..."
    git -C "$S3TESTS_DIR" fetch -q origin "$S3TESTS_COMMIT"
fi
git -C "$S3TESTS_DIR" checkout -q --detach "$S3TESTS_COMMIT"
ACTUAL_COMMIT="$(git -C "$S3TESTS_DIR" rev-parse HEAD)"
if [ "$ACTUAL_COMMIT" != "$S3TESTS_COMMIT" ]; then
    echo "ERROR: ceph/s3-tests is at $ACTUAL_COMMIT, expected $S3TESTS_COMMIT"
    exit 1
fi
echo "ceph/s3-tests at $ACTUAL_COMMIT"

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
# The interpreter is pinned like the packages: the constraints were captured on
# Python 3.12, and a different minor version can change test behaviour.
S3TESTS_PYTHON_VERSION="3.12"
cd "$S3TESTS_DIR"
if ! command -v "python$S3TESTS_PYTHON_VERSION" &>/dev/null; then
    echo "ERROR: python$S3TESTS_PYTHON_VERSION not found on PATH."
    echo "The s3-tests virtualenv must use Python $S3TESTS_PYTHON_VERSION; install it and rerun."
    exit 1
fi
if [ -d "venv" ] && [ "$(venv/bin/python -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null)" != "$S3TESTS_PYTHON_VERSION" ]; then
    echo "Existing s3-tests virtualenv is not Python $S3TESTS_PYTHON_VERSION; rebuilding it..."
    rm -rf venv
fi
if [ ! -d "venv" ]; then
    echo "Bootstrapping s3-tests virtualenv with python$S3TESTS_PYTHON_VERSION..."
    "python$S3TESTS_PYTHON_VERSION" -m venv venv
fi
# Runs every time so an existing venv converges on the pinned versions.
venv/bin/pip install -q --upgrade pip -c "$CONSTRAINTS_FILE"
venv/bin/pip install -q -r requirements.txt -c "$CONSTRAINTS_FILE"

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
# A junit file left over from an earlier run must never be parsed as this run's results.
rm -f "$JUNIT_FILE"
S3TEST_CONF="$CONFIG_FILE" "$PYTEST" \
    s3tests/functional/test_s3.py \
    -v \
    --tb=short \
    --junitxml="$JUNIT_FILE" \
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

# --- Per-test outcomes (one "OUTCOME test_name" line per selected test, sorted) ---
# Parse into a temp file in the same directory and rename it over the committed
# file only once the parse has succeeded, so a failed run leaves it untouched.
PER_TEST_TMP="$(mktemp "$(dirname "$PER_TEST_FILE")/.s3-tests-results.XXXXXX")"
trap 'rm -f "$PER_TEST_TMP"' EXIT
"$PYTHON" - "$JUNIT_FILE" > "$PER_TEST_TMP" << 'PYEOF'
import sys
import xml.etree.ElementTree as ET

outcomes = []
for case in ET.parse(sys.argv[1]).iter("testcase"):
    tags = {child.tag for child in case}
    if "failure" in tags:
        outcome = "FAILED"
    elif "error" in tags:
        outcome = "ERROR"
    elif "skipped" in tags:
        outcome = "SKIPPED"
    else:
        outcome = "PASSED"
    outcomes.append((case.get("name"), outcome))
if not outcomes:
    sys.exit(f"no test cases found in {sys.argv[1]}")
for name, outcome in sorted(outcomes):
    print(f"{outcome} {name}")
PYEOF
chmod 644 "$PER_TEST_TMP"
mv -f "$PER_TEST_TMP" "$PER_TEST_FILE"

# --- Parse results ---
# Count from the per-test file: grepping the pytest log double-counts failures
# because each one appears both in the -v line and in the short test summary.
SELECTED=$(grep -c "" "$PER_TEST_FILE" || true)
PASSED=$(grep -c "^PASSED " "$PER_TEST_FILE" || true)
FAILED=$(grep -c "^FAILED " "$PER_TEST_FILE" || true)
ERROR=$(grep -c "^ERROR " "$PER_TEST_FILE" || true)
SKIPPED=$(grep -c "^SKIPPED " "$PER_TEST_FILE" || true)
DESELECTED=$(grep -oE "[0-9]+ deselected" "$RESULTS_FILE" | tail -1 | cut -d' ' -f1 || true)

echo ""
echo "=============================="
echo "ceph/s3-tests results summary"
echo "=============================="
echo "s3-tests commit: $S3TESTS_COMMIT"
echo "SELECTED:   $SELECTED"
echo "DESELECTED: ${DESELECTED:-0}"
echo "PASSED:  $PASSED"
echo "FAILED:  $FAILED"
echo "ERROR:   $ERROR"
echo "SKIPPED: $SKIPPED"
echo ""
echo "Full output saved to: $RESULTS_FILE"
echo "Per-test outcomes saved to: $PER_TEST_FILE"
echo "Update docs/compatibility.md with these results."

cd "$REPO_ROOT"