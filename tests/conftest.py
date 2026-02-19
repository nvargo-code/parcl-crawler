"""Shared pytest fixtures for parcl-crawler tests."""

from __future__ import annotations

import pytest

import duckdb

from parcl.config import (
    CrawlerConfig,
    FieldMapping,
    SourceConfig,
)
from parcl.db import Database, init_schema


@pytest.fixture
def in_memory_db():
    """Create an in-memory DuckDB database with schema initialized."""
    conn = duckdb.connect(":memory:")
    db = Database(conn, "duckdb")

    # Manually run schema since init_schema reads from files
    schema_sql = """
    CREATE TABLE IF NOT EXISTS sources (
        id TEXT PRIMARY KEY, name TEXT NOT NULL, source_type TEXT NOT NULL,
        base_url TEXT, dataset_id TEXT, target_table TEXT NOT NULL,
        jurisdiction_id TEXT, license TEXT, refresh_cadence TEXT,
        last_run_at TIMESTAMP, last_row_count INTEGER DEFAULT 0,
        config JSON, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS jurisdictions (
        id TEXT PRIMARY KEY, name TEXT NOT NULL, level TEXT NOT NULL,
        parent_id TEXT, fips_code TEXT, state_code TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS parcels (
        id TEXT PRIMARY KEY, source_id TEXT NOT NULL, external_id TEXT NOT NULL,
        apn TEXT, address TEXT, address_norm TEXT, city TEXT, state TEXT,
        zip_code TEXT, county TEXT, latitude DOUBLE, longitude DOUBLE,
        base_zoning TEXT, zoning_desc TEXT, lot_size_sqft DOUBLE,
        jurisdiction_id TEXT, raw_payload JSON,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source_id, external_id)
    );
    CREATE TABLE IF NOT EXISTS permits (
        id TEXT PRIMARY KEY, source_id TEXT NOT NULL, external_id TEXT NOT NULL,
        permit_number TEXT, permit_type TEXT, permit_class TEXT, work_class TEXT,
        status TEXT, description TEXT, address TEXT, address_norm TEXT,
        applicant TEXT, contractor TEXT, valuation DOUBLE,
        issued_date DATE, filed_date DATE, completed_date DATE, expired_date DATE,
        latitude DOUBLE, longitude DOUBLE, jurisdiction_id TEXT, raw_payload JSON,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source_id, external_id)
    );
    CREATE TABLE IF NOT EXISTS zoning_cases (
        id TEXT PRIMARY KEY, source_id TEXT NOT NULL, external_id TEXT NOT NULL,
        case_number TEXT, case_name TEXT, address TEXT, address_norm TEXT,
        existing_zoning TEXT, proposed_zoning TEXT, status TEXT,
        filed_date DATE, decided_date DATE, council_district TEXT,
        description TEXT, jurisdiction_id TEXT, raw_payload JSON,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source_id, external_id)
    );
    CREATE TABLE IF NOT EXISTS boa_cases (
        id TEXT PRIMARY KEY, source_id TEXT NOT NULL, external_id TEXT NOT NULL,
        case_number TEXT, address TEXT, address_norm TEXT,
        variance_type TEXT, status TEXT, filed_date DATE, hearing_date DATE,
        decision TEXT, description TEXT, jurisdiction_id TEXT, raw_payload JSON,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source_id, external_id)
    );
    CREATE TABLE IF NOT EXISTS zoning_overlays (
        id TEXT PRIMARY KEY, source_id TEXT NOT NULL, external_id TEXT NOT NULL,
        overlay_name TEXT, overlay_type TEXT, layer_name TEXT, layer_id INTEGER,
        geometry_wkt TEXT, properties JSON, jurisdiction_id TEXT, raw_payload JSON,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source_id, external_id)
    );
    CREATE TABLE IF NOT EXISTS utility_capacity (
        id TEXT PRIMARY KEY, source_id TEXT NOT NULL, external_id TEXT NOT NULL,
        utility_type TEXT NOT NULL, facility_name TEXT, metric_name TEXT,
        metric_value DOUBLE, metric_unit TEXT, period_start DATE, period_end DATE,
        geometry_wkt TEXT, jurisdiction_id TEXT, raw_payload JSON,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source_id, external_id)
    );
    CREATE TABLE IF NOT EXISTS environmental_constraints (
        id TEXT PRIMARY KEY, source_id TEXT NOT NULL, external_id TEXT NOT NULL,
        constraint_type TEXT NOT NULL, name TEXT, severity TEXT, description TEXT,
        address TEXT, address_norm TEXT, latitude DOUBLE, longitude DOUBLE,
        geometry_wkt TEXT, properties JSON, jurisdiction_id TEXT, raw_payload JSON,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source_id, external_id)
    );
    CREATE TABLE IF NOT EXISTS rights_restrictions (
        id TEXT PRIMARY KEY, source_id TEXT NOT NULL, external_id TEXT NOT NULL,
        restriction_type TEXT NOT NULL, parcel_id TEXT, address TEXT,
        address_norm TEXT, grantor TEXT, grantee TEXT, recorded_date DATE,
        description TEXT, jurisdiction_id TEXT, raw_payload JSON,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source_id, external_id)
    );
    """
    for stmt in schema_sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            db.execute(stmt)

    # Seed jurisdictions
    db.execute("""
        INSERT INTO jurisdictions (id, name, level, parent_id, state_code)
        VALUES ('austin-tx', 'Austin', 'city', NULL, 'TX')
    """)

    yield db
    conn.close()


@pytest.fixture
def sample_source_config():
    """A sample Socrata source config for testing."""
    return SourceConfig(
        id="test_permits",
        source_type="socrata",
        target_table="permits",
        jurisdiction_id="austin-tx",
        base_url="https://data.austintexas.gov",
        dataset_id="3syk-w9eu",
        license="Public Domain",
        refresh_cadence="daily",
        field_map=[
            FieldMapping("permit_number", "permit_number", "text", True),
            FieldMapping("permit_type_desc", "permit_type", "text", False),
            FieldMapping("status_current", "status", "text", False),
            FieldMapping("original_address1", "address", "text", False),
            FieldMapping("total_job_valuation", "valuation", "float", False),
            FieldMapping("issued_date", "issued_date", "date", False),
            FieldMapping("latitude", "latitude", "float", False),
            FieldMapping("longitude", "longitude", "float", False),
        ],
    )


@pytest.fixture
def sample_crawler_config():
    """A sample crawler config for testing."""
    return CrawlerConfig(
        rate_limit_seconds=0,
        page_size=10,
        max_pages=2,
        timeout_seconds=10,
        max_retries=1,
        retry_backoff=0.1,
    )
