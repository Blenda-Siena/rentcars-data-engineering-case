from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from pipeline.compact import compact, small_partitions


def run_demo(root: Path, files: int = 25, rows_per_file: int = 100) -> dict:
    """Create a fragmented partition, compact it and prove row preservation."""
    if root.exists():
        shutil.rmtree(root)
    partition = root / "events" / "ingest_date=2025-03-31"
    partition.mkdir(parents=True)
    for index in range(files):
        start = index * rows_per_file
        table = pa.table({"event_id": [f"demo-{row}" for row in range(start, start + rows_per_file)],
                          "value": list(range(start, start + rows_per_file))})
        pq.write_table(table, partition / f"micro-batch-{index:03}.parquet")
    rows_before = ds.dataset(partition, format="parquet").count_rows()
    detection = small_partitions(root / "events", threshold_mb=128)
    files_before = len(list(partition.glob("*.parquet")))
    compact(partition)
    files_after = len(list(partition.glob("*.parquet")))
    rows_after = ds.dataset(partition, format="parquet").count_rows()
    result = {
        "detected": bool(detection and detection[0]["needs_compaction"]),
        "files_before": files_before,
        "files_after": files_after,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "rows_preserved": rows_before == rows_after,
        "reduction_percent": round((1 - files_after / files_before) * 100, 2),
    }
    print(json.dumps(result, indent=2))
    if not result["detected"] or not result["rows_preserved"] or files_after >= files_before:
        raise SystemExit(1)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("data/demo_small_files"))
    parser.add_argument("--files", type=int, default=25)
    parser.add_argument("--rows-per-file", type=int, default=100)
    args = parser.parse_args()
    run_demo(args.root, args.files, args.rows_per_file)

