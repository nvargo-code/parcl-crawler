"""Tests for ETL transformer."""

import pytest

from parcl.etl.transformer import coerce_value, transform_record, transform_batch


def test_coerce_text():
    assert coerce_value("  hello  ", "text") == "hello"
    assert coerce_value(123, "text") == "123"
    assert coerce_value(None, "text") is None


def test_coerce_float():
    assert coerce_value("3.14", "float") == 3.14
    assert coerce_value(42, "float") == 42.0
    assert coerce_value("not_a_number", "float") is None


def test_coerce_date():
    result = coerce_value("2024-01-15", "date")
    assert str(result) == "2024-01-15"

    result = coerce_value("2024-01-15T12:30:00.000", "date")
    assert str(result) == "2024-01-15"


def test_coerce_integer():
    assert coerce_value("42", "integer") == 42
    assert coerce_value("42.7", "integer") == 42


def test_transform_record_basic(sample_source_config):
    raw = {
        "permit_number": "BP-2024-001",
        "permit_type_desc": "Building",
        "status_current": "Issued",
        "original_address1": "123 Main St",
        "total_job_valuation": "500000",
        "issued_date": "2024-06-15",
        "latitude": "30.267",
        "longitude": "-97.743",
    }
    result = transform_record(raw, sample_source_config)
    assert result is not None
    assert result["permit_number"] == "BP-2024-001"
    assert result["permit_type"] == "Building"
    assert result["valuation"] == 500000.0
    assert result["address_norm"] == "123 MAIN ST"
    assert result["source_id"] == "test_permits"
    assert result["external_id"] == "BP-2024-001"
    assert result["raw_payload"] is not None


def test_transform_record_missing_required(sample_source_config):
    raw = {"status_current": "Issued"}  # Missing permit_number (required)
    result = transform_record(raw, sample_source_config)
    assert result is None


def test_transform_batch(sample_source_config):
    records = [
        {"permit_number": "P1", "status_current": "Issued"},
        {"status_current": "Issued"},  # Missing required field
        {"permit_number": "P3", "status_current": "Active"},
    ]
    results = transform_batch(records, sample_source_config)
    assert len(results) == 2
    assert results[0]["permit_number"] == "P1"
    assert results[1]["permit_number"] == "P3"
