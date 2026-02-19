"""Tests for risk profile generation."""

import pytest

from parcl.profile import get_parcel_risk_profile


def test_profile_no_data(in_memory_db):
    """Profile should return a valid structure even with no data."""
    result = get_parcel_risk_profile("123 Fake St", in_memory_db)
    assert result["query"] == "123 Fake St"
    assert "warnings" in result
    assert len(result["warnings"]) > 0  # Should warn about no parcel found
    assert result["permits"] == []
    assert result["risks"] == []


def test_profile_with_parcel(in_memory_db):
    """Profile should find a parcel and return zoning info."""
    in_memory_db.execute(
        "INSERT INTO sources (id, name, source_type, target_table) "
        "VALUES ('test_zoning', 'Test Zoning', 'socrata', 'parcels')"
    )
    in_memory_db.execute(
        "INSERT INTO parcels (id, source_id, external_id, address, address_norm, "
        "base_zoning, zoning_desc, city, state, zip_code, jurisdiction_id) "
        "VALUES ('p1', 'test_zoning', 'addr1', '600 Congress Ave', '600 CONGRESS AVE', "
        "'CS-MU', 'Commercial Services Mixed Use', 'Austin', 'TX', '78701', 'austin-tx')"
    )

    result = get_parcel_risk_profile("600 Congress Ave", in_memory_db)
    assert result["matched_address"] == "600 CONGRESS AVE"
    assert result["zoning"]["base_zone"] == "CS-MU"
    assert "parcels" in result["data_sources"]


def test_profile_with_permits(in_memory_db):
    """Profile should include permits for matching address."""
    in_memory_db.execute(
        "INSERT INTO sources (id, name, source_type, target_table) "
        "VALUES ('test_permits', 'Test Permits', 'socrata', 'permits')"
    )
    in_memory_db.execute(
        "INSERT INTO permits (id, source_id, external_id, permit_number, permit_type, "
        "status, address, address_norm, valuation, jurisdiction_id, raw_payload) "
        "VALUES ('pm1', 'test_permits', 'BP001', 'BP-2024-001', 'Building', "
        "'Issued', '600 Congress Ave', '600 CONGRESS AVE', 2500000, 'austin-tx', '{}')"
    )

    result = get_parcel_risk_profile("600 Congress Ave", in_memory_db)
    assert len(result["permits"]) == 1
    assert result["permits"][0]["permit_number"] == "BP-2024-001"
    assert result["permits"][0]["valuation"] == 2500000
