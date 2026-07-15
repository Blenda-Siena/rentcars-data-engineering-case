from pathlib import Path

from datetime import datetime

from pipeline.checkpoint import CheckpointStore
from pipeline.run import filter_new_offsets, filter_since, transform_events, transform_partners, transform_transactions

RAW = Path("data/raw")


def test_event_ids_are_unique_and_bots_are_segregated():
    events, bots = transform_events(RAW / "raw_events.csv")
    ids = [r["event_id"] for r in events + bots]
    assert len(ids) == len(set(ids))
    assert events and bots
    assert not any(r["is_bot_flag"] for r in events)


def test_negative_transactions_are_quarantined():
    transactions, rejected = transform_transactions(RAW / "raw_transactions.csv", 7)
    assert all(r["amount"] >= 0 for r in transactions)
    assert rejected and all(r["quarantine_reason"] == "negative_amount" for r in rejected)


def test_partner_schema_evolution_has_unified_columns():
    partners = transform_partners(RAW / "raw_partner_catalog.csv")
    assert {r["schema_version"] for r in partners} == {"v1", "v2", "v3"}
    expected = {"sla_hours", "avg_rating", "api_endpoint", "webhook_enabled"}
    assert all(expected <= r.keys() for r in partners)


def test_incremental_window_reprocesses_watermark_and_offsets_only_once(tmp_path):
    store = CheckpointStore(tmp_path / "checkpoint.json")
    store.set("events", "2025-03-31")
    store.values["payment_offsets"] = {"0": 10, "1": 5}
    store.commit()
    restored = CheckpointStore(tmp_path / "checkpoint.json")
    assert restored.window_start("events", 7) == datetime(2025, 3, 24)
    events = [{"ingest_date": "2025-03-23"}, {"ingest_date": "2025-03-24"}]
    assert filter_since(events, "ingest_date", restored.window_start("events", 7)) == [events[1]]
    payments = [{"kafka_partition": "0", "kafka_offset": "10"},
                {"kafka_partition": "0", "kafka_offset": "11"},
                {"kafka_partition": "1", "kafka_offset": "4"}]
    assert filter_new_offsets(payments, restored.values["payment_offsets"]) == [payments[1]]
