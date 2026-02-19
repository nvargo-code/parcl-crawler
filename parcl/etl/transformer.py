"""Field mapping, type coercion, and normalization for ETL."""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from typing import Any

from parcl.address import normalize_address
from parcl.config import FieldMapping, SourceConfig
from parcl.logger import get_logger

log = get_logger("transformer")


def coerce_value(value: Any, target_type: str) -> Any:
    """Coerce a raw value to the target schema type."""
    if value is None or value == "":
        return None

    try:
        if target_type == "text":
            return str(value).strip()
        elif target_type == "float":
            return float(value)
        elif target_type == "integer":
            return int(float(value))
        elif target_type == "date":
            if isinstance(value, (date, datetime)):
                return value if isinstance(value, date) else value.date()
            s = str(value).strip()
            # Try ISO format first
            for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d",
                        "%m/%d/%Y", "%m-%d-%Y"):
                try:
                    return datetime.strptime(s[:26], fmt).date()
                except ValueError:
                    continue
            return s  # Return as-is if unparseable
        elif target_type == "boolean":
            return str(value).lower() in ("true", "1", "yes")
        else:
            return value
    except (ValueError, TypeError) as e:
        log.debug(f"Coercion failed for {value!r} -> {target_type}: {e}")
        return None


def transform_record(
    raw: dict[str, Any],
    source_config: SourceConfig,
) -> dict[str, Any] | None:
    """Transform a single raw record using the source's field_map.

    Returns a dict with schema field names, or None if required fields missing.
    """
    mapped: dict[str, Any] = {}

    for fm in source_config.field_map:
        raw_val = raw.get(fm.raw_field)
        if fm.required and (raw_val is None or raw_val == ""):
            return None
        mapped[fm.schema_field] = coerce_value(raw_val, fm.type)

    # Always include these standard fields
    mapped["id"] = str(uuid.uuid4())
    mapped["source_id"] = source_config.id
    mapped["jurisdiction_id"] = source_config.jurisdiction_id

    # Generate external_id from first required field or hash of record
    external_id = None
    for fm in source_config.field_map:
        if fm.required and mapped.get(fm.schema_field):
            external_id = str(mapped[fm.schema_field])
            break
    if not external_id:
        # Fallback: use a hash of the raw record
        external_id = str(uuid.uuid5(uuid.NAMESPACE_URL, json.dumps(raw, sort_keys=True, default=str)))
    mapped["external_id"] = external_id

    # Normalize address if present
    if "address" in mapped and mapped["address"]:
        mapped["address_norm"] = normalize_address(mapped["address"])

    # Store raw payload for audit
    mapped["raw_payload"] = json.dumps(raw, default=str)

    # Add constraint_type / utility_type defaults based on source
    table = source_config.target_table
    if table == "environmental_constraints" and "constraint_type" not in mapped:
        if "flood" in source_config.id.lower():
            mapped["constraint_type"] = "flood_zone"
            mapped["severity"] = "high"
        elif "brownfield" in source_config.id.lower():
            mapped["constraint_type"] = "brownfield"
            mapped["severity"] = "medium"
        else:
            mapped["constraint_type"] = "other"
            mapped["severity"] = "low"

    if table == "utility_capacity" and "utility_type" not in mapped:
        if "water" in source_config.id.lower() and "waste" not in source_config.id.lower():
            mapped["utility_type"] = "water"
            mapped["metric_unit"] = "million_gallons"
        elif "wastewater" in source_config.id.lower():
            mapped["utility_type"] = "wastewater"
            mapped["metric_unit"] = "million_gallons"
        else:
            mapped["utility_type"] = "other"

    return mapped


def transform_batch(
    records: list[dict[str, Any]],
    source_config: SourceConfig,
) -> list[dict[str, Any]]:
    """Transform a batch of raw records. Skips records with missing required fields."""
    results = []
    for raw in records:
        mapped = transform_record(raw, source_config)
        if mapped is not None:
            results.append(mapped)
    return results
