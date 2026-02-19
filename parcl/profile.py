"""Parcel risk profile API."""

from __future__ import annotations

from typing import Any

from parcl.address import normalize_address
from parcl.db import Database
from parcl.logger import get_logger

log = get_logger("profile")


def get_parcel_risk_profile(query: str, db: Database) -> dict[str, Any]:
    """Get a comprehensive risk profile for a parcel by address or ID.

    Resolution order:
    1. Try exact parcel UUID match
    2. Try normalized address LIKE match on parcels table
    3. Fallback to permits table address match
    """
    norm_query = normalize_address(query)
    result: dict[str, Any] = {
        "query": query,
        "matched_address": None,
        "zoning": {},
        "risks": [],
        "permits": [],
        "supporting_facts": {},
        "data_sources": [],
        "warnings": [],
    }

    # Try parcel match
    parcel = _find_parcel(db, query, norm_query)
    if parcel:
        result["matched_address"] = parcel.get("address_norm") or parcel.get("address")
        result["zoning"] = _get_zoning_info(db, parcel, norm_query)
        result["data_sources"].append("parcels")
    else:
        result["warnings"].append(f"No parcel record found for '{query}'. Using permit/case data only.")
        result["matched_address"] = norm_query

    # Get permits
    permits = _get_permits(db, norm_query)
    if permits:
        result["permits"] = permits
        result["data_sources"].append("permits")

    # Get risks
    result["risks"] = _get_risks(db, norm_query, parcel)

    # Get supporting facts
    result["supporting_facts"] = _get_facts(db, norm_query)

    # Add data sources for risks
    if any(r["type"] == "flood_zone" for r in result["risks"]):
        result["data_sources"].append("environmental_constraints")
    if result["zoning"].get("overlays"):
        result["data_sources"].append("zoning_overlays")

    return result


def _find_parcel(db: Database, raw_query: str, norm_query: str) -> dict[str, Any] | None:
    """Find a parcel by UUID or normalized address."""
    # Try UUID
    row = db.fetchone(
        "SELECT id, address, address_norm, base_zoning, zoning_desc, latitude, longitude, "
        "city, state, zip_code FROM parcels WHERE id = ?",
        (raw_query,),
    )
    if row:
        return dict(zip(
            ["id", "address", "address_norm", "base_zoning", "zoning_desc",
             "latitude", "longitude", "city", "state", "zip_code"],
            row,
        ))

    # Try normalized address LIKE match
    row = db.fetchone(
        "SELECT id, address, address_norm, base_zoning, zoning_desc, latitude, longitude, "
        "city, state, zip_code FROM parcels WHERE address_norm LIKE ? LIMIT 1",
        (f"%{norm_query}%",),
    )
    if row:
        return dict(zip(
            ["id", "address", "address_norm", "base_zoning", "zoning_desc",
             "latitude", "longitude", "city", "state", "zip_code"],
            row,
        ))

    return None


def _get_zoning_info(db: Database, parcel: dict, norm_query: str) -> dict[str, Any]:
    """Build zoning info from parcel and related cases."""
    zoning: dict[str, Any] = {
        "base_zone": parcel.get("base_zoning", ""),
        "description": parcel.get("zoning_desc", ""),
        "overlays": [],
        "pending_rezoning": False,
    }

    # Check for pending zoning cases
    row = db.fetchone(
        "SELECT COUNT(*) FROM zoning_cases WHERE address_norm LIKE ? "
        "AND status NOT IN ('Closed', 'Withdrawn', 'Denied')",
        (f"%{norm_query}%",),
    )
    if row and row[0] > 0:
        zoning["pending_rezoning"] = True

    return zoning


def _get_permits(db: Database, norm_query: str) -> list[dict[str, Any]]:
    """Get permits matching the address."""
    rows = db.fetchall(
        "SELECT permit_number, permit_type, status, valuation, issued_date, description "
        "FROM permits WHERE address_norm LIKE ? ORDER BY issued_date DESC LIMIT 20",
        (f"%{norm_query}%",),
    )
    return [
        {
            "permit_number": r[0],
            "type": r[1],
            "status": r[2],
            "valuation": r[3],
            "issued_date": str(r[4]) if r[4] else None,
            "description": r[5],
        }
        for r in rows
    ]


def _get_risks(db: Database, norm_query: str, parcel: dict | None) -> list[dict[str, Any]]:
    """Identify risks from environmental constraints and other data."""
    risks = []

    # Check environmental constraints by address
    rows = db.fetchall(
        "SELECT constraint_type, name, severity, description "
        "FROM environmental_constraints WHERE address_norm LIKE ? OR name LIKE ?",
        (f"%{norm_query}%", f"%{norm_query}%"),
    )
    for r in rows:
        risks.append({
            "type": r[0] or "environmental",
            "severity": r[2] or "medium",
            "label": r[1] or r[0] or "Environmental constraint",
            "detail": r[3] or "",
        })

    # Check for flood zones near parcel lat/lon
    if parcel and parcel.get("latitude"):
        lat, lon = parcel["latitude"], parcel["longitude"]
        rows = db.fetchall(
            "SELECT constraint_type, name, severity, description "
            "FROM environmental_constraints "
            "WHERE latitude IS NOT NULL "
            "AND ABS(latitude - ?) < 0.005 AND ABS(longitude - ?) < 0.005",
            (lat, lon),
        )
        seen = {r.get("label") for r in risks}
        for r in rows:
            label = r[1] or r[0] or "Nearby constraint"
            if label not in seen:
                risks.append({
                    "type": r[0] or "environmental",
                    "severity": r[2] or "medium",
                    "label": label,
                    "detail": r[3] or "",
                })

    return risks


def _get_facts(db: Database, norm_query: str) -> dict[str, Any]:
    """Get supporting aggregate facts."""
    facts: dict[str, Any] = {}

    row = db.fetchone(
        "SELECT COUNT(*) FROM permits WHERE address_norm LIKE ? "
        "AND issued_date >= CURRENT_DATE - INTERVAL '5 years'",
        (f"%{norm_query}%",),
    )
    facts["active_permits_5yr"] = row[0] if row else 0

    row = db.fetchone(
        "SELECT COUNT(*) FROM zoning_cases WHERE address_norm LIKE ? "
        "AND status NOT IN ('Closed', 'Withdrawn', 'Denied')",
        (f"%{norm_query}%",),
    )
    facts["open_zoning_cases"] = row[0] if row else 0

    row = db.fetchone(
        "SELECT COUNT(*) FROM boa_cases WHERE address_norm LIKE ?",
        (f"%{norm_query}%",),
    )
    facts["total_boa_cases"] = row[0] if row else 0

    row = db.fetchone(
        "SELECT COUNT(*) FROM environmental_constraints WHERE address_norm LIKE ?",
        (f"%{norm_query}%",),
    )
    facts["environmental_flags"] = row[0] if row else 0

    return facts
