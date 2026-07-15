"""Production compaction job. Submit with spark-submit on Glue/EMR."""
from __future__ import annotations

import argparse
import math

from pyspark.sql import SparkSession


def compact(source: str, destination: str, target_mb: int = 128) -> None:
    spark = (SparkSession.builder.appName("rentcars-small-file-compaction")
             .config("spark.sql.files.maxPartitionBytes", target_mb * 1024 * 1024)
             .config("spark.sql.files.maxRecordsPerFile", 500_000).getOrCreate())
    frame = spark.read.parquet(source)
    input_bytes = sum(f.getLen() for f in spark._jvm.org.apache.hadoop.fs.FileSystem
                      .get(spark._jsc.hadoopConfiguration()).listStatus(
                          spark._jvm.org.apache.hadoop.fs.Path(source)))
    partitions = max(1, math.ceil(input_bytes / (target_mb * 1024 * 1024)))
    (frame.repartition(partitions, "ingest_date").write.mode("overwrite")
     .partitionBy("ingest_date").option("maxRecordsPerFile", 500_000).parquet(destination))
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source")
    parser.add_argument("destination")
    parser.add_argument("--target-mb", type=int, default=128)
    args = parser.parse_args()
    compact(args.source, args.destination, args.target_mb)

