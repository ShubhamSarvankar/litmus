# Litmus

An S3-compatible object storage server built for **correctness**, not scale.
AWS CLI and boto3 work against it unmodified. A chaos suite proves storage
invariants hold across process kills and injected faults.

[![CI](https://github.com/shubhamsarvankar/litmus/actions/workflows/ci.yml/badge.svg)](https://github.com/shubhamsarvankar/litmus/actions/workflows/ci.yml)
[![Chaos](https://github.com/shubhamsarvankar/litmus/actions/workflows/chaos.yml/badge.svg)](https://github.com/shubhamsarvankar/litmus/actions/workflows/chaos.yml)
![Python](https://img.shields.io/badge/python-3.12-blue)

---

## What this demonstrates

- **S3 REST API at the protocol level** — real AWS CLI and boto3 work without
  modification, including multipart uploads, range GETs, and conditional headers.
- **Correctness defined operationally, not by documentation** — a passing test means
  a real S3 client got a real valid response. 115/325 ceph/s3-tests categories pass;
  every failure is documented with an explicit reason.
- **`CompleteMultipartUpload` as a transactional commit** — assembly, metadata commit,
  and cleanup are sequenced so a process kill at any point leaves either a complete
  visible object or no object at all.
- **Crash invariants tested explicitly** — the chaos suite proves storage guarantees
  hold across SIGKILL and injected OSErrors, not just happy-path scenarios.
- **Two-tier fault injection** — Tier A injects faults at the Python layer (fast,
  in-process); Tier B kills the real process mid-operation and verifies state after
  restart.

---

## Architecture

```
litmus/
├── server/
│   ├── api/
│   │   ├── buckets.py       # PUT/DELETE/GET / bucket routes + ListObjects + DeleteObjects
│   │   ├── objects.py       # PUT/GET/DELETE/HEAD object routes
│   │   └── multipart.py     # CreateMPU, UploadPart, Complete, Abort routes
│   ├── storage/
│   │   ├── base.py          # StorageBackend Protocol
│   │   └── filesystem.py    # FilesystemBackend — durable write sequence
│   ├── metadata/
│   │   ├── base.py          # MetadataStore Protocol
│   │   └── sqlite.py        # SQLiteMetadataStore — WAL + FULL synchronous
│   ├── multipart/
│   │   └── state_machine.py # CompleteMultipartUpload transactional commit
│   ├── errors/
│   │   └── s3errors.py      # S3Error hierarchy + XML serializer
│   ├── consistency.py       # Startup consistency sweep
│   ├── config.py            # Settings (env-var driven)
│   └── app.py               # FastAPI app factory + DI wiring
├── chaos/
│   ├── framework/           # FaultInjector, Trigger/Fault protocols
│   ├── faults/              # RaiseOSError, CorruptBytes, HangForever, PartialWrite
│   ├── fixtures/            # Tier A (in-process) + Tier B (subprocess) fixtures
│   └── scenarios/           # test_enospc, test_corruption, test_desync, test_concurrent
│                              test_crash_multipart (Tier B, POSIX only)
├── tests/
│   ├── conformance/         # Bucket, object, multipart, range GET conformance
│   └── unit/                # ETag computation, SQLite transactions, error XML
├── scripts/
│   └── run_ceph_s3tests.sh  # Runs ceph/s3-tests against a live server
└── docs/
    ├── crash-model.md       # Explicit durability contract
    ├── publication-barrier.md # The durable write sequence, formalized
    └── compatibility.md     # ceph/s3-tests results, known gaps
```

---

## API surface

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | List all buckets |
| PUT | `/{bucket}` | Create bucket |
| DELETE | `/{bucket}` | Delete bucket |
| HEAD | `/{bucket}` | Bucket exists check |
| GET | `/{bucket}` | List objects (V1 and V2) |
| POST | `/{bucket}?delete` | Bulk delete objects (DeleteObjects) |
| PUT | `/{bucket}/{key}` | Put object |
| GET | `/{bucket}/{key}` | Get object (with Range, If-Match) |
| HEAD | `/{bucket}/{key}` | Object metadata |
| DELETE | `/{bucket}/{key}` | Delete object |
| POST | `/{bucket}/{key}?uploads` | Initiate multipart upload |
| PUT | `/{bucket}/{key}?partNumber&uploadId` | Upload part |
| POST | `/{bucket}/{key}?uploadId` | Complete multipart upload |
| DELETE | `/{bucket}/{key}?uploadId` | Abort multipart upload |
| GET | `/health` | Health check |

---

## Crash model

Litmus guarantees storage invariants across **process kills (SIGKILL) only**.

After returning `200 OK` on a PUT or CompleteMultipartUpload:
- The object bytes are durable on disk via the temp→fsync→rename→fsync-dir sequence.
- The object metadata is committed in SQLite with `WAL + synchronous=FULL`.
- A subsequent GET will return the correct bytes even if the process is killed
  immediately after the `200` is sent.

OS crash, power loss, and hardware failure are explicitly out of scope.

See [`docs/crash-model.md`](docs/crash-model.md) for the full contract.

---

## Publication barrier

Bytes are written to disk **before** metadata is committed. If the process is killed
between those two steps, the object is not visible — a GET returns `404`. The startup
consistency sweep detects and logs the orphaned file.

The durable write sequence: `temp file → fsync → rename → fsync(parent dir) → SQLite commit`

See [`docs/publication-barrier.md`](docs/publication-barrier.md) for the formal
step-by-step analysis including why each step is necessary.

---

## Getting started

```bash
# Clone and install
git clone https://github.com/YOUR_USERNAME/litmus
cd litmus
uv sync

# Start the server
uv run uvicorn server.app:app --host 127.0.0.1 --port 8000

# Verify with AWS CLI (in another terminal)
aws s3 mb s3://test-bucket --endpoint-url http://localhost:8000 \
  --no-sign-request
aws s3 cp README.md s3://test-bucket/ --endpoint-url http://localhost:8000 \
  --no-sign-request
aws s3 ls s3://test-bucket --endpoint-url http://localhost:8000 \
  --no-sign-request
```

---

## Running tests

**Unit + conformance tests (all platforms):**
```bash
uv run pytest tests/ chaos/scenarios/ -v
```

**Tier B crash tests (Linux / macOS / WSL2 only — requires SIGKILL):**
```bash
uv run pytest chaos/scenarios/test_crash_multipart.py -v
```
These tests are automatically skipped on Windows with a clear message.

**ceph/s3-tests (Linux / WSL2 only):**
```bash
# Start server first in another terminal
uv run uvicorn server.app:app --host 127.0.0.1 --port 8000

# Run the suite
bash scripts/run_ceph_s3tests.sh
```

---

## Compatibility

**115 / 325 ceph/s3-tests pass (35%)** — the same test suite used to validate
production S3-compatible servers including Ceph RGW and MinIO.

Every failure is documented with an explicit reason. The passing categories cover the
core S3 protocol: bucket and object lifecycle, multipart upload, range GET, ETag
correctness, and conditional headers.

See [`docs/compatibility.md`](docs/compatibility.md) for the full breakdown.

---

## Stack

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Language | Python 3.12 | Target roles value Python automation |
| HTTP framework | FastAPI | TestClient enables Tier A fault injection |
| Metadata store | SQLite (stdlib) | WAL + FULL synchronous — correct for process-kill |
| Object storage | Filesystem | Transparent, auditable, no external dependencies |
| Test runner | pytest | Industry standard; asyncio mode for concurrent tests |
| Package manager | uv | Fast, reproducible, native dependency groups |
| Linter | ruff | Fast, opinionated, zero config drift |

---

## What this is not

- **Not production-ready.** No authentication, no multi-node support, no ACLs.
- **Not a MinIO replacement.** MinIO targets production deployments; this targets
  correctness verification.
- **Not OS-crash durable.** The crash model covers SIGKILL only. Power-loss durability
  would require `O_DIRECT` and battery-backed write caches.
- **Not feature-complete.** Object copy, versioning, lifecycle, encryption, and many
  other S3 features are not implemented. See `docs/compatibility.md`.

The point is to demonstrate that correctness can be defined operationally — as passing
tests against real clients — rather than as documentation claims.