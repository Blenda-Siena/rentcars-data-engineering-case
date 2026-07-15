from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path


@contextmanager
def pipeline_lock(path: Path):
    """Prevent overlapping local/Airflow runs from rewriting the same partitions."""
    import fcntl
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


class CheckpointStore:
    """Atomic local checkpoint store; production maps to DynamoDB/Airflow metadata."""

    def __init__(self, path: Path):
        self.path = path
        self.values = json.loads(path.read_text()) if path.exists() else {}

    def get(self, source: str) -> str | None:
        return self.values.get(source)

    def window_start(self, source: str, watermark_days: int) -> datetime | None:
        value = self.get(source)
        return datetime.fromisoformat(value) - timedelta(days=watermark_days) if value else None

    def set(self, source: str, value: str | None) -> None:
        if value:
            self.values[source] = value

    def commit(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(json.dumps(self.values, indent=2, sort_keys=True) + "\n")
        os.replace(temporary, self.path)
