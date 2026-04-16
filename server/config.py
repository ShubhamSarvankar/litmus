import os
from dataclasses import dataclass, field
from pathlib import Path


def _env_path(key: str, default: str) -> Path:
    return Path(os.environ.get(key, default))


def _env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, default))


@dataclass
class Settings:
    data_dir: Path = field(default_factory=lambda: _env_path("S3_DATA_DIR", "data/objects"))
    parts_dir: Path = field(default_factory=lambda: _env_path("S3_PARTS_DIR", "data/parts"))
    db_path: Path = field(default_factory=lambda: _env_path("S3_DB_PATH", "data/litmus.db"))
    port: int = field(default_factory=lambda: _env_int("S3_PORT", 8000))
    host: str = "0.0.0.0"
    # crash_model: only process-kill (SIGKILL) durability is guaranteed
    crash_model: str = "process-kill"
    # TEST_WRITE_DELAY_MS: used by Tier B chaos tests to slow writes for reliable SIGKILL timing
    test_write_delay_ms: int = field(default_factory=lambda: _env_int("S3_TEST_WRITE_DELAY_MS", 0))


settings = Settings()
