-- parcl-crawler schema (DuckDB + PostgreSQL compatible)
-- Use {JSON_TYPE} placeholder: JSON for DuckDB, JSONB for PG

CREATE TABLE IF NOT EXISTS sources (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    source_type     TEXT NOT NULL,
    base_url        TEXT,
    dataset_id      TEXT,
    target_table    TEXT NOT NULL,
    jurisdiction_id TEXT,
    license         TEXT,
    refresh_cadence TEXT,
    last_run_at     TIMESTAMP,
    last_row_count  INTEGER DEFAULT 0,
    config          {JSON_TYPE},
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS jurisdictions (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    level       TEXT NOT NULL,  -- country, state, county, city
    parent_id   TEXT REFERENCES jurisdictions(id),
    fips_code   TEXT,
    state_code  TEXT,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS parcels (
    id              TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL REFERENCES sources(id),
    external_id     TEXT NOT NULL,
    apn             TEXT,
    address         TEXT,
    address_norm    TEXT,
    city            TEXT,
    state           TEXT,
    zip_code        TEXT,
    county          TEXT,
    latitude        DOUBLE,
    longitude       DOUBLE,
    base_zoning     TEXT,
    zoning_desc     TEXT,
    lot_size_sqft   DOUBLE,
    jurisdiction_id TEXT REFERENCES jurisdictions(id),
    raw_payload     {JSON_TYPE},
    fetched_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, external_id)
);

CREATE TABLE IF NOT EXISTS permits (
    id              TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL REFERENCES sources(id),
    external_id     TEXT NOT NULL,
    permit_number   TEXT,
    permit_type     TEXT,
    permit_class    TEXT,
    work_class      TEXT,
    status          TEXT,
    description     TEXT,
    address         TEXT,
    address_norm    TEXT,
    applicant       TEXT,
    contractor      TEXT,
    valuation       DOUBLE,
    issued_date     DATE,
    filed_date      DATE,
    completed_date  DATE,
    expired_date    DATE,
    latitude        DOUBLE,
    longitude       DOUBLE,
    jurisdiction_id TEXT REFERENCES jurisdictions(id),
    raw_payload     {JSON_TYPE},
    fetched_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, external_id)
);

CREATE TABLE IF NOT EXISTS zoning_cases (
    id              TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL REFERENCES sources(id),
    external_id     TEXT NOT NULL,
    case_number     TEXT,
    case_name       TEXT,
    address         TEXT,
    address_norm    TEXT,
    existing_zoning TEXT,
    proposed_zoning TEXT,
    status          TEXT,
    filed_date      DATE,
    decided_date    DATE,
    council_district TEXT,
    description     TEXT,
    jurisdiction_id TEXT REFERENCES jurisdictions(id),
    raw_payload     {JSON_TYPE},
    fetched_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, external_id)
);

CREATE TABLE IF NOT EXISTS boa_cases (
    id              TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL REFERENCES sources(id),
    external_id     TEXT NOT NULL,
    case_number     TEXT,
    address         TEXT,
    address_norm    TEXT,
    variance_type   TEXT,
    status          TEXT,
    filed_date      DATE,
    hearing_date    DATE,
    decision        TEXT,
    description     TEXT,
    jurisdiction_id TEXT REFERENCES jurisdictions(id),
    raw_payload     {JSON_TYPE},
    fetched_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, external_id)
);

CREATE TABLE IF NOT EXISTS zoning_overlays (
    id              TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL REFERENCES sources(id),
    external_id     TEXT NOT NULL,
    overlay_name    TEXT,
    overlay_type    TEXT,
    layer_name      TEXT,
    layer_id        INTEGER,
    geometry_wkt    TEXT,
    properties      {JSON_TYPE},
    jurisdiction_id TEXT REFERENCES jurisdictions(id),
    raw_payload     {JSON_TYPE},
    fetched_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, external_id)
);

CREATE TABLE IF NOT EXISTS utility_capacity (
    id              TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL REFERENCES sources(id),
    external_id     TEXT NOT NULL,
    utility_type    TEXT NOT NULL,  -- water, wastewater, electric, gas
    facility_name   TEXT,
    metric_name     TEXT,
    metric_value    DOUBLE,
    metric_unit     TEXT,
    period_start    DATE,
    period_end      DATE,
    geometry_wkt    TEXT,
    jurisdiction_id TEXT REFERENCES jurisdictions(id),
    raw_payload     {JSON_TYPE},
    fetched_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, external_id)
);

CREATE TABLE IF NOT EXISTS environmental_constraints (
    id              TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL REFERENCES sources(id),
    external_id     TEXT NOT NULL,
    constraint_type TEXT NOT NULL,  -- flood_zone, brownfield, hazmat, wetland
    name            TEXT,
    severity        TEXT,           -- high, medium, low
    description     TEXT,
    address         TEXT,
    address_norm    TEXT,
    latitude        DOUBLE,
    longitude       DOUBLE,
    geometry_wkt    TEXT,
    properties      {JSON_TYPE},
    jurisdiction_id TEXT REFERENCES jurisdictions(id),
    raw_payload     {JSON_TYPE},
    fetched_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, external_id)
);

CREATE TABLE IF NOT EXISTS rights_restrictions (
    id              TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL REFERENCES sources(id),
    external_id     TEXT NOT NULL,
    restriction_type TEXT NOT NULL, -- deed_covenant, easement, hoa, lien
    parcel_id       TEXT,
    address         TEXT,
    address_norm    TEXT,
    grantor         TEXT,
    grantee         TEXT,
    recorded_date   DATE,
    description     TEXT,
    geometry_wkt    TEXT,
    jurisdiction_id TEXT REFERENCES jurisdictions(id),
    raw_payload     {JSON_TYPE},
    fetched_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, external_id)
);

ALTER TABLE rights_restrictions ADD COLUMN IF NOT EXISTS geometry_wkt TEXT;

CREATE TABLE IF NOT EXISTS property_valuations (
    id                TEXT PRIMARY KEY,
    source_id         TEXT NOT NULL REFERENCES sources(id),
    external_id       TEXT NOT NULL,
    prop_id           TEXT,
    geo_id            TEXT,
    address           TEXT,
    address_norm      TEXT,
    city              TEXT,
    zip_code          TEXT,
    subdivision       TEXT,
    entities          TEXT,
    acreage           DOUBLE,
    legal_description TEXT,
    appraised_value   DOUBLE,
    land_value        DOUBLE,
    improvement_value DOUBLE,
    tax_year          INTEGER,
    geometry_wkt      TEXT,
    jurisdiction_id   TEXT REFERENCES jurisdictions(id),
    raw_payload       {JSON_TYPE},
    fetched_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, external_id)
);

CREATE TABLE IF NOT EXISTS transit_amenities (
    id              TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL REFERENCES sources(id),
    external_id     TEXT NOT NULL,
    amenity_type    TEXT NOT NULL,  -- park, bus_stop, bus_route, rail_route, city_land, other
    name            TEXT,
    description     TEXT,
    address         TEXT,
    address_norm    TEXT,
    stop_id         TEXT,
    route_id        TEXT,
    route_type      TEXT,
    park_type       TEXT,
    acreage         DOUBLE,
    latitude        DOUBLE,
    longitude       DOUBLE,
    geometry_wkt    TEXT,
    properties      {JSON_TYPE},
    jurisdiction_id TEXT REFERENCES jurisdictions(id),
    raw_payload     {JSON_TYPE},
    fetched_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id, external_id)
);
