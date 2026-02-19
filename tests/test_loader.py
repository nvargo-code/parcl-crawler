"""Tests for ETL loader."""

import pytest

from parcl.etl.loader import load_records


def test_load_permits(in_memory_db):
    # Register source first
    in_memory_db.execute(
        "INSERT INTO sources (id, name, source_type, target_table) "
        "VALUES ('test', 'Test', 'socrata', 'permits')"
    )

    records = [
        {
            "id": "uuid-1",
            "source_id": "test",
            "external_id": "P001",
            "permit_number": "P001",
            "permit_type": "Building",
            "permit_class": None,
            "work_class": None,
            "status": "Issued",
            "description": "New building",
            "address": "123 Main St",
            "address_norm": "123 MAIN ST",
            "applicant": None,
            "contractor": None,
            "valuation": 500000.0,
            "issued_date": "2024-01-15",
            "filed_date": None,
            "completed_date": None,
            "expired_date": None,
            "latitude": 30.267,
            "longitude": -97.743,
            "jurisdiction_id": "austin-tx",
            "raw_payload": "{}",
        }
    ]

    loaded = load_records(in_memory_db, "permits", records)
    assert loaded == 1

    row = in_memory_db.fetchone("SELECT permit_number, valuation FROM permits WHERE external_id = 'P001'")
    assert row[0] == "P001"
    assert row[1] == 500000.0


def test_upsert_dedup(in_memory_db):
    """Test that re-inserting same (source_id, external_id) updates instead of duplicating."""
    in_memory_db.execute(
        "INSERT INTO sources (id, name, source_type, target_table) "
        "VALUES ('test', 'Test', 'socrata', 'permits')"
    )

    record = {
        "id": "uuid-1",
        "source_id": "test",
        "external_id": "P001",
        "permit_number": "P001",
        "permit_type": "Building",
        "permit_class": None,
        "work_class": None,
        "status": "Issued",
        "description": "v1",
        "address": "123 Main St",
        "address_norm": "123 MAIN ST",
        "applicant": None,
        "contractor": None,
        "valuation": 100.0,
        "issued_date": None,
        "filed_date": None,
        "completed_date": None,
        "expired_date": None,
        "latitude": None,
        "longitude": None,
        "jurisdiction_id": "austin-tx",
        "raw_payload": "{}",
    }

    load_records(in_memory_db, "permits", [record])

    # Update the record
    record["id"] = "uuid-2"
    record["valuation"] = 999.0
    record["description"] = "v2"
    load_records(in_memory_db, "permits", [record])

    count = in_memory_db.fetchone("SELECT COUNT(*) FROM permits WHERE external_id = 'P001'")
    assert count[0] == 1

    row = in_memory_db.fetchone("SELECT valuation, description FROM permits WHERE external_id = 'P001'")
    assert row[0] == 999.0
    assert row[1] == "v2"


def test_load_empty_batch(in_memory_db):
    loaded = load_records(in_memory_db, "permits", [])
    assert loaded == 0
