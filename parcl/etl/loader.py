"""Upsert records into the database by (source_id, external_id)."""

from __future__ import annotations

from typing import Any

from parcl.db import Database
from parcl.logger import get_logger

log = get_logger("loader")

# Column lists for each target table (order must match INSERT)
TABLE_COLUMNS: dict[str, list[str]] = {
    "parcels": [
        "id", "source_id", "external_id", "apn", "address", "address_norm",
        "city", "state", "zip_code", "county", "latitude", "longitude",
        "base_zoning", "zoning_desc", "lot_size_sqft", "jurisdiction_id", "raw_payload",
    ],
    "permits": [
        "id", "source_id", "external_id", "permit_number", "permit_type",
        "permit_class", "work_class", "status", "description", "address",
        "address_norm", "applicant", "contractor", "valuation", "issued_date",
        "filed_date", "completed_date", "expired_date", "latitude", "longitude",
        "jurisdiction_id", "raw_payload",
    ],
    "zoning_cases": [
        "id", "source_id", "external_id", "case_number", "case_name",
        "address", "address_norm", "existing_zoning", "proposed_zoning",
        "status", "filed_date", "decided_date", "council_district",
        "description", "jurisdiction_id", "raw_payload",
    ],
    "boa_cases": [
        "id", "source_id", "external_id", "case_number", "address",
        "address_norm", "variance_type", "status", "filed_date",
        "hearing_date", "decision", "description", "jurisdiction_id", "raw_payload",
    ],
    "zoning_overlays": [
        "id", "source_id", "external_id", "overlay_name", "overlay_type",
        "layer_name", "layer_id", "geometry_wkt", "properties",
        "jurisdiction_id", "raw_payload",
    ],
    "utility_capacity": [
        "id", "source_id", "external_id", "utility_type", "facility_name",
        "metric_name", "metric_value", "metric_unit", "period_start",
        "period_end", "geometry_wkt", "jurisdiction_id", "raw_payload",
    ],
    "environmental_constraints": [
        "id", "source_id", "external_id", "constraint_type", "name",
        "severity", "description", "address", "address_norm", "latitude",
        "longitude", "geometry_wkt", "properties", "jurisdiction_id", "raw_payload",
    ],
    "rights_restrictions": [
        "id", "source_id", "external_id", "restriction_type", "parcel_id",
        "address", "address_norm", "grantor", "grantee", "recorded_date",
        "description", "geometry_wkt", "jurisdiction_id", "raw_payload",
    ],
    "property_valuations": [
        "id", "source_id", "external_id", "prop_id", "geo_id",
        "address", "address_norm", "city", "zip_code", "subdivision",
        "entities", "acreage", "legal_description",
        "appraised_value", "land_value", "improvement_value",
        "tax_year", "geometry_wkt", "jurisdiction_id", "raw_payload",
    ],
    "transit_amenities": [
        "id", "source_id", "external_id", "amenity_type", "name",
        "description", "address", "address_norm",
        "stop_id", "route_id", "route_type", "park_type", "acreage",
        "latitude", "longitude", "geometry_wkt", "properties",
        "jurisdiction_id", "raw_payload",
    ],
}


def _build_upsert_sql(table: str, columns: list[str], db_type: str) -> str:
    """Build an INSERT ... ON CONFLICT upsert statement."""
    placeholders = ", ".join(["?"] * len(columns)) if db_type == "duckdb" else ", ".join(["%s"] * len(columns))
    cols = ", ".join(columns)

    # Columns to update on conflict (everything except id, source_id, external_id)
    update_cols = [c for c in columns if c not in ("id", "source_id", "external_id")]
    if db_type == "duckdb":
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        return (
            f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) "
            f"ON CONFLICT (source_id, external_id) DO UPDATE SET {set_clause}"
        )
    else:
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        return (
            f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) "
            f"ON CONFLICT (source_id, external_id) DO UPDATE SET {set_clause}"
        )


def load_records(
    db: Database,
    table: str,
    records: list[dict[str, Any]],
) -> int:
    """Load transformed records into the target table using upsert.

    Returns the number of records loaded.
    """
    if not records:
        return 0

    columns = TABLE_COLUMNS.get(table)
    if not columns:
        raise ValueError(f"Unknown table '{table}'. Available: {list(TABLE_COLUMNS.keys())}")

    sql = _build_upsert_sql(table, columns, db.db_type)
    loaded = 0

    for record in records:
        values = tuple(record.get(col) for col in columns)
        try:
            db.execute(sql, values)
            loaded += 1
        except Exception as e:
            log.warning(f"Failed to upsert record {record.get('external_id')}: {e}")

    db.commit()
    return loaded
