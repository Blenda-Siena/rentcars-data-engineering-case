from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from pipeline.checkpoint import CheckpointStore, pipeline_lock


def clean(value: str | None) -> str | None:
    value = (value or "").strip()
    return value or None


def lower(value: str | None) -> str | None:
    value = clean(value)
    return value.lower() if value else None


def boolean(value: str | None) -> bool | None:
    value = lower(value)
    if value is None:
        return None
    if value in {"true", "1", "yes"}:
        return True
    if value in {"false", "0", "no"}:
        return False
    raise ValueError(f"invalid boolean: {value}")


def number(value: str | None, cast: Callable = float) -> Any:
    value = clean(value)
    return cast(value) if value is not None else None


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def filter_since(rows: Iterable[dict], column: str, start: datetime | None) -> list[dict]:
    return [row for row in rows if start is None or datetime.fromisoformat(row[column]) >= start]


def filter_new_offsets(rows: Iterable[dict], offsets: dict[str, int]) -> list[dict]:
    return [row for row in rows if int(row["kafka_offset"]) > int(offsets.get(row["kafka_partition"], -1))]


def dedupe(rows: Iterable[dict], key: Callable[[dict], tuple], order: Callable[[dict], str]) -> list[dict]:
    winners: dict[tuple, dict] = {}
    for row in rows:
        identity = key(row)
        if identity not in winners or order(row) >= order(winners[identity]):
            winners[identity] = row
    return list(winners.values())


def write_partitioned(rows: list[dict], root: Path, partition: str) -> None:
    if not rows:
        return
    root.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    ds.write_dataset(
        table,
        root,
        format="parquet",
        partitioning=[partition],
        partitioning_flavor="hive",
        existing_data_behavior="delete_matching",
        max_rows_per_file=250_000,
        min_rows_per_group=10_000,
        max_rows_per_group=100_000,
    )


def transform_event_rows(source_rows: Iterable[dict]) -> tuple[list[dict], list[dict]]:
    rows = dedupe(source_rows, lambda r: (r["event_id"],), lambda r: r["ingest_date"])
    good, bots = [], []
    for r in rows:
        item = {
            "event_id": r["event_id"], "event_type": lower(r["event_type"]),
            "session_id": r["session_id"], "user_id": clean(r["user_id"]),
            "event_ts": r["event_ts"], "page": clean(r["page"]),
            "partner_id": clean(r["partner_id"]), "device": lower(r["device"]),
            "country": (clean(r["country"]) or "").upper(), "channel": clean(r["channel"]),
            "price_usd": number(r["price_usd"]), "metadata_json": clean(r["metadata_json"]),
            "is_bot_flag": boolean(r["is_bot_flag"]), "ingest_date": r["ingest_date"],
        }
        (bots if item["is_bot_flag"] else good).append(item)
    return good, bots


def transform_events(path: Path) -> tuple[list[dict], list[dict]]:
    return transform_event_rows(read_csv(path))


def transform_transaction_rows(source_rows: Iterable[dict], watermark_days: int) -> tuple[list[dict], list[dict]]:
    rows = dedupe(source_rows, lambda r: (r["transaction_id"],), lambda r: r["ingest_ts"])
    good, quarantine = [], []
    for r in rows:
        created = datetime.fromisoformat(r["created_at"])
        ingested = datetime.fromisoformat(r["ingest_ts"])
        item = {**r, "status": lower(r["status"]), "currency": r["currency"].upper(),
                "amount": number(r["amount"]), "retry_count": number(r["retry_count"], int),
                "processing_ms": number(r["processing_ms"], int), "ingest_date": r["ingest_ts"][:10],
                "is_late": ingested - created > timedelta(days=watermark_days)}
        if item["amount"] is not None and item["amount"] < 0:
            item["quarantine_reason"] = "negative_amount"
            quarantine.append(item)
        else:
            good.append(item)
    return good, quarantine


def transform_transactions(path: Path, watermark_days: int) -> tuple[list[dict], list[dict]]:
    return transform_transaction_rows(read_csv(path), watermark_days)


def transform_partners(path: Path) -> list[dict]:
    rows = read_csv(path)
    normalized = []
    for r in rows:
        normalized.append({
            "partner_id": r["partner_id"], "schema_version": lower(r["schema_version"]),
            "name": r["name"], "country_code": (clean(r["country_code"]) or "").upper(),
            "status": lower(r["status"]), "tier": lower(r["tier"]),
            "commission_rate": number(r["commission_rate"]), "created_at": r["created_at"],
            "updated_at": clean(r["updated_at"]), "sla_hours": number(r["sla_hours"], int),
            "avg_rating": number(r["avg_rating"]), "api_endpoint": clean(r["api_endpoint"]),
            "webhook_enabled": boolean(r["webhook_enabled"]),
        })
    return dedupe(normalized, lambda r: (r["partner_id"], r["schema_version"], r["updated_at"] or ""),
                  lambda r: r["updated_at"] or r["created_at"])


def transform_payment_rows(source_rows: Iterable[dict]) -> list[dict]:
    rows = dedupe(source_rows, lambda r: (r["event_id"],), lambda r: f'{int(r["kafka_partition"]):02}:{int(r["kafka_offset"]):012}')
    return [{**r, "status": lower(r["status"]), "amount": number(r["amount"]),
             "kafka_offset": number(r["kafka_offset"], int), "kafka_partition": number(r["kafka_partition"], int),
             "processing_lag_ms": number(r["processing_lag_ms"], int), "is_spike": boolean(r["is_spike"]),
             "event_date": r["event_ts"][:10]} for r in rows]


def transform_payments(path: Path) -> list[dict]:
    return transform_payment_rows(read_csv(path))


EVENT_COLUMNS = ("event_id", "event_type", "session_id", "user_id", "event_ts", "page", "partner_id",
                 "device", "country", "channel", "price_usd", "metadata_json", "is_bot_flag", "ingest_date")
TXN_COLUMNS = ("transaction_id", "booking_ref", "partner_id", "user_id", "created_at", "ingest_ts", "amount",
               "currency", "status", "payment_method", "gateway", "retry_count", "error_code", "notes",
               "processing_ms", "ingest_date", "is_late")
PARTNER_COLUMNS = ("partner_id", "schema_version", "name", "country_code", "status", "tier", "commission_rate",
                   "created_at", "updated_at", "sla_hours", "avg_rating", "api_endpoint", "webhook_enabled")


def _postgres_load(url: str, events: list[dict], txns: list[dict], partners: list[dict]) -> None:
    import psycopg

    with psycopg.connect(url) as con, con.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY,event_type TEXT NOT NULL,session_id TEXT NOT NULL,
          user_id TEXT,event_ts TIMESTAMP NOT NULL,page TEXT,partner_id TEXT,device TEXT,country TEXT,channel TEXT,
          price_usd DOUBLE PRECISION,metadata_json TEXT,is_bot_flag BOOLEAN NOT NULL,ingest_date DATE NOT NULL);
        CREATE TABLE IF NOT EXISTS transactions(transaction_id TEXT PRIMARY KEY,booking_ref TEXT,partner_id TEXT NOT NULL,
          user_id TEXT,created_at TIMESTAMP NOT NULL,ingest_ts TIMESTAMP NOT NULL,amount NUMERIC(18,2),currency CHAR(3),
          status TEXT,payment_method TEXT,gateway TEXT,retry_count INTEGER,error_code TEXT,notes TEXT,processing_ms INTEGER,
          ingest_date DATE,is_late BOOLEAN);
        CREATE TABLE IF NOT EXISTS partners(partner_id TEXT,schema_version TEXT,name TEXT,country_code TEXT,status TEXT,
          tier TEXT,commission_rate NUMERIC(8,6),created_at TIMESTAMP,updated_at TIMESTAMP,sla_hours INTEGER,
          avg_rating DOUBLE PRECISION,api_endpoint TEXT,webhook_enabled BOOLEAN,
          PRIMARY KEY(partner_id,schema_version,created_at));
        CREATE INDEX IF NOT EXISTS idx_events_ts ON events(event_ts);
        CREATE INDEX IF NOT EXISTS idx_tx_created ON transactions(created_at);
        CREATE INDEX IF NOT EXISTS idx_tx_partner ON transactions(partner_id);
        """)
        def upsert(table: str, columns: tuple[str, ...], rows: list[dict], conflict: str) -> None:
            if not rows:
                return
            names = ",".join(columns)
            values = ",".join(f"%({column})s" for column in columns)
            updates = ",".join(f"{column}=EXCLUDED.{column}" for column in columns if column not in conflict.split(","))
            cur.executemany(f"INSERT INTO {table} ({names}) VALUES ({values}) ON CONFLICT ({conflict}) DO UPDATE SET {updates}", rows)
        upsert("events", EVENT_COLUMNS, events, "event_id")
        upsert("transactions", TXN_COLUMNS, txns, "transaction_id")
        upsert("partners", PARTNER_COLUMNS, partners, "partner_id,schema_version,created_at")


def load_serving(db_path: Path | str, events: list[dict], txns: list[dict], partners: list[dict],
                 replace: bool = True) -> None:
    if str(db_path).startswith(("postgresql://", "postgres://")):
        _postgres_load(str(db_path), events, txns, partners)
        return
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as con:
        if replace:
            con.executescript("DROP TABLE IF EXISTS events; DROP TABLE IF EXISTS transactions; DROP TABLE IF EXISTS partners;")
        con.executescript("""
        CREATE TABLE IF NOT EXISTS events(event_id TEXT PRIMARY KEY,event_type TEXT,session_id TEXT,user_id TEXT,event_ts TEXT,page TEXT,partner_id TEXT,device TEXT,country TEXT,channel TEXT,price_usd REAL,metadata_json TEXT,is_bot_flag INTEGER,ingest_date TEXT);
        CREATE TABLE IF NOT EXISTS transactions(transaction_id TEXT PRIMARY KEY,booking_ref TEXT,partner_id TEXT,user_id TEXT,created_at TEXT,ingest_ts TEXT,amount REAL,currency TEXT,status TEXT,payment_method TEXT,gateway TEXT,retry_count INTEGER,error_code TEXT,notes TEXT,processing_ms INTEGER,ingest_date TEXT,is_late INTEGER);
        CREATE TABLE IF NOT EXISTS partners(partner_id TEXT,schema_version TEXT,name TEXT,country_code TEXT,status TEXT,tier TEXT,commission_rate REAL,created_at TEXT,updated_at TEXT,sla_hours INTEGER,avg_rating REAL,api_endpoint TEXT,webhook_enabled INTEGER,PRIMARY KEY(partner_id,schema_version,created_at));
        CREATE INDEX IF NOT EXISTS idx_events_ts ON events(event_ts); CREATE INDEX IF NOT EXISTS idx_tx_created ON transactions(created_at); CREATE INDEX IF NOT EXISTS idx_tx_partner ON transactions(partner_id);
        """)
        def upsert(table, columns, rows, conflict):
            if not rows:
                return
            names = ",".join(columns); values = ",".join(f":{c}" for c in columns)
            updates = ",".join(f"{c}=excluded.{c}" for c in columns if c not in conflict.split(","))
            con.executemany(f"INSERT INTO {table} ({names}) VALUES ({values}) ON CONFLICT ({conflict}) DO UPDATE SET {updates}", rows)
        upsert("events", EVENT_COLUMNS, events, "event_id")
        upsert("transactions", TXN_COLUMNS, txns, "transaction_id")
        upsert("partners", PARTNER_COLUMNS, partners, "partner_id,schema_version,created_at")


def run(input_dir: Path, output: Path, db: Path | str, watermark_days: int = 7,
        checkpoint_path: Path = Path("data/state/checkpoints.json"), full_refresh: bool = False) -> dict:
    checkpoints = CheckpointStore(checkpoint_path)
    event_raw = read_csv(input_dir / "raw_events.csv")
    txn_raw = read_csv(input_dir / "raw_transactions.csv")
    payment_raw = read_csv(input_dir / "raw_payment_stream.csv")
    event_start = None if full_refresh else checkpoints.window_start("events", watermark_days)
    txn_start = None if full_refresh else checkpoints.window_start("transactions", watermark_days)
    event_batch = filter_since(event_raw, "ingest_date", event_start)
    txn_batch = filter_since(txn_raw, "ingest_ts", txn_start)
    offsets = {} if full_refresh else checkpoints.values.get("payment_offsets", {})
    payment_batch = filter_new_offsets(payment_raw, offsets)
    events, bots = transform_event_rows(event_batch)
    txns, rejected = transform_transaction_rows(txn_batch, watermark_days)
    partners = transform_partners(input_dir / "raw_partner_catalog.csv")
    payments = transform_payment_rows(payment_batch)
    write_partitioned(events, output / "events", "ingest_date")
    write_partitioned(bots, output / "events_bots", "ingest_date")
    write_partitioned(txns, output / "transactions", "ingest_date")
    write_partitioned(payments, output / "payments", "event_date")
    pq.write_table(pa.Table.from_pylist(partners), output / "partners.parquet")
    if rejected:
        write_partitioned(rejected, Path("data/quarantine/transactions"), "ingest_date")
    load_serving(db, events, txns, partners, replace=full_refresh and not str(db).startswith("postgres"))
    checkpoints.set("events", max((r["ingest_date"] for r in event_raw), default=None))
    checkpoints.set("transactions", max((r["ingest_ts"] for r in txn_raw), default=None))
    new_offsets = dict(offsets)
    for row in payment_raw:
        partition = row["kafka_partition"]
        new_offsets[partition] = max(int(new_offsets.get(partition, -1)), int(row["kafka_offset"]))
    checkpoints.values["payment_offsets"] = new_offsets
    checkpoints.commit()
    metrics = {"events": len(events), "bots": len(bots), "transactions": len(txns),
               "rejected_transactions": len(rejected), "partners": len(partners), "payments": len(payments),
               "mode": "full" if full_refresh or event_start is None else "incremental",
               "event_window_start": event_start.isoformat() if event_start else None,
               "transaction_window_start": txn_start.isoformat() if txn_start else None}
    print(json.dumps(metrics, indent=2))
    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("data/raw"))
    parser.add_argument("--output", type=Path, default=Path("data/lake"))
    parser.add_argument("--db", default=os.getenv("DATABASE_URL", "data/serving.db"))
    parser.add_argument("--watermark-days", type=int, default=7)
    parser.add_argument("--checkpoint", type=Path, default=Path("data/state/checkpoints.json"))
    parser.add_argument("--full-refresh", action="store_true")
    args = parser.parse_args()
    with pipeline_lock(args.checkpoint.with_suffix(".lock")):
        run(args.input, args.output, args.db, args.watermark_days, args.checkpoint, args.full_refresh)
