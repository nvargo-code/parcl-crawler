"""Dataclasses mirroring SQL tables for type safety."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any


@dataclass
class Source:
    id: str
    name: str
    source_type: str
    target_table: str
    base_url: str = ""
    dataset_id: str = ""
    jurisdiction_id: str = ""
    license: str = ""
    refresh_cadence: str = ""
    last_run_at: datetime | None = None
    last_row_count: int = 0
    config: dict[str, Any] | None = None


@dataclass
class Parcel:
    id: str
    source_id: str
    external_id: str
    address: str = ""
    address_norm: str = ""
    apn: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    county: str = ""
    latitude: float | None = None
    longitude: float | None = None
    base_zoning: str = ""
    zoning_desc: str = ""
    lot_size_sqft: float | None = None
    jurisdiction_id: str = ""
    raw_payload: dict[str, Any] | None = None


@dataclass
class Permit:
    id: str
    source_id: str
    external_id: str
    permit_number: str = ""
    permit_type: str = ""
    permit_class: str = ""
    work_class: str = ""
    status: str = ""
    description: str = ""
    address: str = ""
    address_norm: str = ""
    applicant: str = ""
    contractor: str = ""
    valuation: float | None = None
    issued_date: date | None = None
    filed_date: date | None = None
    completed_date: date | None = None
    expired_date: date | None = None
    latitude: float | None = None
    longitude: float | None = None
    jurisdiction_id: str = ""
    raw_payload: dict[str, Any] | None = None


@dataclass
class ZoningCase:
    id: str
    source_id: str
    external_id: str
    case_number: str = ""
    case_name: str = ""
    address: str = ""
    address_norm: str = ""
    existing_zoning: str = ""
    proposed_zoning: str = ""
    status: str = ""
    filed_date: date | None = None
    decided_date: date | None = None
    council_district: str = ""
    description: str = ""
    jurisdiction_id: str = ""
    raw_payload: dict[str, Any] | None = None


@dataclass
class BoaCase:
    id: str
    source_id: str
    external_id: str
    case_number: str = ""
    address: str = ""
    address_norm: str = ""
    variance_type: str = ""
    status: str = ""
    filed_date: date | None = None
    hearing_date: date | None = None
    decision: str = ""
    description: str = ""
    jurisdiction_id: str = ""
    raw_payload: dict[str, Any] | None = None


@dataclass
class ZoningOverlay:
    id: str
    source_id: str
    external_id: str
    overlay_name: str = ""
    overlay_type: str = ""
    layer_name: str = ""
    layer_id: int | None = None
    geometry_wkt: str = ""
    properties: dict[str, Any] | None = None
    jurisdiction_id: str = ""
    raw_payload: dict[str, Any] | None = None


@dataclass
class UtilityCapacity:
    id: str
    source_id: str
    external_id: str
    utility_type: str = ""
    facility_name: str = ""
    metric_name: str = ""
    metric_value: float | None = None
    metric_unit: str = ""
    period_start: date | None = None
    period_end: date | None = None
    geometry_wkt: str = ""
    jurisdiction_id: str = ""
    raw_payload: dict[str, Any] | None = None


@dataclass
class EnvironmentalConstraint:
    id: str
    source_id: str
    external_id: str
    constraint_type: str = ""
    name: str = ""
    severity: str = ""
    description: str = ""
    address: str = ""
    address_norm: str = ""
    latitude: float | None = None
    longitude: float | None = None
    geometry_wkt: str = ""
    properties: dict[str, Any] | None = None
    jurisdiction_id: str = ""
    raw_payload: dict[str, Any] | None = None


@dataclass
class RightsRestriction:
    id: str
    source_id: str
    external_id: str
    restriction_type: str = ""
    parcel_id: str = ""
    address: str = ""
    address_norm: str = ""
    grantor: str = ""
    grantee: str = ""
    recorded_date: date | None = None
    description: str = ""
    jurisdiction_id: str = ""
    raw_payload: dict[str, Any] | None = None
