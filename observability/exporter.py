import csv
from pathlib import Path

from prometheus_client import Gauge, start_http_server

RUNS = Gauge("pipeline_runs_total", "Pipeline runs in source snapshot", ["pipeline", "status"])
DURATION = Gauge("pipeline_duration_seconds", "Maximum pipeline duration in the snapshot", ["pipeline"])
STREAMING_LAG = Gauge("streaming_lag_seconds", "Maximum payment processing lag")
FILES = Gauge("lake_partition_file_count", "Parquet files in a lake partition", ["dataset", "partition"])
AVERAGE_SIZE = Gauge("lake_partition_average_file_bytes", "Average Parquet file size", ["dataset", "partition"])


def collect(path=Path("data/raw/pipeline_runs.csv"), lake=Path("data/lake"),
            payments=Path("data/raw/raw_payment_stream.csv")):
    totals = {}
    latest = {}
    with path.open() as handle:
        for row in csv.DictReader(handle):
            key = (row["pipeline_name"], row["status"].lower())
            totals[key] = totals.get(key, 0) + 1
            latest[row["pipeline_name"]] = max(latest.get(row["pipeline_name"], 0), float(row["duration_sec"]))
    for (pipeline, status), count in totals.items():
        RUNS.labels(pipeline, status).set(count)
    for pipeline, duration in latest.items():
        DURATION.labels(pipeline).set(duration)
    with payments.open() as handle:
        lags = [float(row["processing_lag_ms"]) / 1000 for row in csv.DictReader(handle) if row["processing_lag_ms"]]
    STREAMING_LAG.set(max(lags, default=0))
    for dataset in lake.iterdir() if lake.exists() else []:
        if not dataset.is_dir():
            continue
        for partition in (item for item in dataset.rglob("*") if item.is_dir() and "=" in item.name):
            files = list(partition.glob("*.parquet"))
            if files:
                total = sum(item.stat().st_size for item in files)
                FILES.labels(dataset.name, partition.name).set(len(files))
                AVERAGE_SIZE.labels(dataset.name, partition.name).set(total / len(files))


if __name__ == "__main__":
    collect()
    start_http_server(9101)
    import time
    while True:
        time.sleep(60)
        collect()
