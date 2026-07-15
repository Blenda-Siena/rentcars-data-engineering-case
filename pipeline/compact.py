from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import pyarrow.dataset as ds
import pyarrow.parquet as pq


def small_partitions(root: Path, threshold_mb: int = 128) -> list[dict]:
    result = []
    for partition in sorted(p for p in root.rglob("*") if p.is_dir() and "=" in p.name):
        files = list(partition.glob("*.parquet"))
        if not files:
            continue
        total = sum(f.stat().st_size for f in files)
        average = total / len(files)
        if average < threshold_mb * 1024 * 1024:
            result.append({"partition": str(partition), "file_count": len(files),
                           "average_bytes": int(average), "needs_compaction": len(files) > 1})
    return result


def compact(partition: Path, target_mb: int = 128) -> None:
    table = ds.dataset(partition, format="parquet").to_table()
    temporary = partition.with_name(partition.name + "_compacting")
    temporary.mkdir()
    estimated_rows = max(1, int(table.num_rows * target_mb * 1024 * 1024 / max(table.nbytes, 1)))
    pq.write_table(table, temporary / "compact-0.parquet", row_group_size=min(estimated_rows, 500_000))
    shutil.rmtree(partition)
    temporary.rename(partition)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--threshold-mb", type=int, default=128)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    candidates = small_partitions(args.root, args.threshold_mb)
    for candidate in candidates:
        print(candidate)
        if args.apply and candidate["needs_compaction"]:
            compact(Path(candidate["partition"]), args.threshold_mb)

